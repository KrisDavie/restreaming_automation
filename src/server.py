"""FastAPI backend – REST + WebSocket API for the restreaming automation system.

Endpoints
---------
Ingest:
    POST   /api/ingest/start       – start a feed for a slot
    POST   /api/ingest/stop        – stop a feed
    GET    /api/ingest/status      – list active feeds

Crop:
    POST   /api/detect/manual      – submit manual crop coordinates

OBS:
    POST   /api/obs/connect        – connect to OBS WebSocket
    POST   /api/obs/disconnect     – disconnect
    GET    /api/obs/status         – connection status
    POST   /api/obs/crop           – apply crop to a source
    POST   /api/obs/sync           – nudge sync offset
    POST   /api/obs/scene          – switch scene
    POST   /api/obs/stream/start   – start streaming
    POST   /api/obs/stream/stop    – stop streaming
    GET    /api/obs/scenes         – list scenes
    GET    /api/obs/sources        – list sources

WebSocket:
    WS     /ws                     – real-time event bus for the dashboard
"""

from __future__ import annotations

import asyncio
import base64
import logging
import shutil
import subprocess
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Ensure application-level loggers have handlers (uvicorn only sets up its own)
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:  %(name)s  %(message)s",
)

from io import BytesIO

from PIL import Image
from fastapi import FastAPI, File, HTTPException, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .config import Config, load_config
from .ingest import IngestManager
from .obs_control import OBSController, SourceCrop
from .presets import PresetStore

_STATIC_DIR = Path(__file__).parent / "static"
_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_TEMPLATES_UPLOAD_DIR = _DATA_DIR / "template_images"

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CropRect:
    """Pixel-level crop rectangle (top-left origin)."""

    x: int
    y: int
    width: int
    height: int

    def to_dict(self) -> dict[str, int]:
        return {"x": self.x, "y": self.y, "width": self.width, "height": self.height}

    def to_obs_crop(self, source_w: int, source_h: int) -> dict[str, int]:
        """Convert to OBS Crop/Pad filter values (top/bottom/left/right)."""
        return {
            "left": self.x,
            "top": self.y,
            "right": max(0, source_w - (self.x + self.width)),
            "bottom": max(0, source_h - (self.y + self.height)),
        }

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class IngestStartRequest(BaseModel):
    slot: int
    url: str
    quality: str = "best"
    start_offset: str = ""  # VOD start time: seconds, HH:MM:SS, or 1h2m3s

class IngestStopRequest(BaseModel):
    slot: int

class ManualCropRequest(BaseModel):
    source_name: str
    x: int
    y: int
    width: int
    height: int
    source_width: int = 1920
    source_height: int = 1080

class CropApplyRequest(BaseModel):
    source_name: str
    top: int = 0
    bottom: int = 0
    left: int = 0
    right: int = 0

class SyncNudgeRequest(BaseModel):
    source_name: str
    delta_ms: int

class SceneRequest(BaseModel):
    scene_name: str

class AudioSwitchRequest(BaseModel):
    active_slot: int  # 0-based slot to unmute; -1 = mute all
    num_slots: int = 2  # total number of player slots

class AudioVolumeRequest(BaseModel):
    input_name: str
    volume_db: float  # dB: 0 = unity, negative = quieter, max ~26

class AudioMuteRequest(BaseModel):
    input_name: str
    muted: bool

class DiscordSourceRequest(BaseModel):
    device_id: str = "default"  # audio device ID
    window: str = ""             # Windows-only: app window match string for Application Audio Capture

class ProjectorRequest(BaseModel):
    scene_name: str = "Race Scene"
    monitor: int = -1  # -1 = windowed, 0+ = fullscreen on that monitor
    width: int = 0     # projector window width  (0 = OBS default)
    height: int = 0    # projector window height (0 = OBS default)

class AudioMonitorRequest(BaseModel):
    input_name: str
    monitor_type: str = "OBS_MONITORING_TYPE_MONITOR_ONLY"

class PresetSaveRequest(BaseModel):
    channel: str
    name: str
    game_crop: dict[str, int] | None = None
    tracker_crop: dict[str, int] | None = None
    timer_crop: dict[str, int] | None = None


class TemplateRegionsRequest(BaseModel):
    regions: dict[str, Any]  # e.g. {"slots": {"0": {"game": {x,y,w,h}, ...}}}


class TemplateApplyRequest(BaseModel):
    scene_name: str = "Race Scene"


class TextSourceRequest(BaseModel):
    source_name: str
    text: str
    font_size: int = 36
    color: str = "#ffffff"
    x: float = 0
    y: float = 0
    width: float = 0
    height: float = 0

# ---------------------------------------------------------------------------
# App globals (set during lifespan)
# ---------------------------------------------------------------------------

config: Config
ingest: IngestManager
obs: OBSController
presets: PresetStore

RACE_SCENE = "Race Scene"  # Default OBS scene used for auto-setup
_active_template_id: int | None = None  # Currently applied template


def _obs_image_path(path: str) -> str:
    """Translate a container file path to one accessible by OBS on the host.

    When the server runs in Docker but OBS runs on the host, the stored
    image paths (``/app/data/...``) are not reachable by OBS.  If
    ``OBS_DATA_DIR`` is set, replace the container ``data/`` prefix with
    the host-side equivalent.

    Normalises path separators to match the OBS host: if OBS_DATA_DIR
    contains a backslash (Windows), the final path uses backslashes;
    otherwise forward slashes.
    """
    host_dir = config.obs_data_dir
    if not host_dir:
        return path
    container_data = str(_DATA_DIR)
    if path.startswith(container_data):
        relative = path[len(container_data):]
        # Detect Windows host paths (contain backslash or drive letter)
        if "\\" in host_dir or (len(host_dir) >= 2 and host_dir[1] == ":"):
            relative = relative.replace("/", "\\")
        result = host_dir + relative
        return result
    return path


async def _provision_running_feeds() -> None:
    """Create OBS scene + sources for every feed that is already running."""
    if not obs.connected:
        return
    for slot, feed in ingest.feeds.items():
        if feed.process is not None and feed.process.returncode is None:
            try:
                sources = await obs.setup_full_scene(
                    RACE_SCENE, slot, feed.obs_input_url,
                )
                log.info("Retroactively provisioned slot %d → %s", slot, sources)
            except Exception as exc:
                log.warning("Retroactive provision failed for slot %d: %s", slot, exc)


async def _ingest_event_handler(event: str, data: Any) -> None:
    """Forward ingest-layer events to all dashboard WebSocket clients."""
    await broadcast(event, data)


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
    global config, ingest, obs, presets, _active_template_id
    config = load_config()
    ingest = IngestManager(config, on_event=_ingest_event_handler)
    obs = OBSController(config)
    presets = PresetStore()
    _TEMPLATES_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # Restore persisted active template
    saved_tpl = presets.get_setting("active_template_id")
    if saved_tpl is not None:
        try:
            _active_template_id = int(saved_tpl)
            presets.get_template(_active_template_id)  # verify it still exists
            log.info("Restored active template: %d", _active_template_id)
        except (ValueError, KeyError):
            _active_template_id = None

    log.info("Restreaming Automation API ready  (OBS target: %s)", config.obs_ws_url)

    # Try auto-connecting to OBS at startup
    try:
        await obs.connect()
        log.info("Auto-connected to OBS at startup")
        # Rebuild scene-item cache so existing Feed items map to logical names
        await obs._rebuild_scene_cache(RACE_SCENE)
    except Exception as exc:
        log.info("OBS not available at startup (will connect later): %s", exc)

    yield
    await ingest.stop_all()
    if obs.connected:
        await obs.disconnect()
    presets.close()


app = FastAPI(title="Restreaming Automation API", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve the standalone dashboard at /dashboard
if _STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


@app.get("/api/health")
async def health_check() -> dict[str, Any]:
    """Lightweight health endpoint for Docker HEALTHCHECK and monitoring."""
    return {
        "status": "ok",
        "obs_connected": obs.connected,
        "active_feeds": len(ingest._feeds),
    }


@app.get("/dashboard")
async def dashboard_redirect():
    from fastapi.responses import FileResponse
    return FileResponse(str(_STATIC_DIR / "dashboard.html"))

# ---------------------------------------------------------------------------
# WebSocket broadcast hub
# ---------------------------------------------------------------------------

_ws_clients: set[WebSocket] = set()


async def broadcast(event: str, data: Any = None) -> None:
    """Send a JSON event to all connected dashboard WebSocket clients."""
    import json
    msg = json.dumps({"event": event, "data": data})
    disconnected: set[WebSocket] = set()
    for ws in _ws_clients:
        try:
            await ws.send_text(msg)
        except Exception:
            disconnected.add(ws)
    _ws_clients.difference_update(disconnected)


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    _ws_clients.add(ws)
    try:
        while True:
            data = await ws.receive_text()
            # Dashboard can send commands via WS too – future extension
            log.debug("WS received: %s", data[:200])
    except WebSocketDisconnect:
        _ws_clients.discard(ws)


# ---------------------------------------------------------------------------
# Ingest endpoints
# ---------------------------------------------------------------------------

@app.post("/api/ingest/start")
async def ingest_start(req: IngestStartRequest) -> dict[str, Any]:
    feed = await ingest.start_feed(req.slot, req.url, req.quality, req.start_offset)
    await broadcast("ingest:started", {
        "slot": feed.slot, "url": feed.url, "local_url": feed.obs_input_url,
    })

    # Auto-provision the media source in OBS if connected
    if obs.connected:
        try:
            sources = await obs.setup_full_scene(
                RACE_SCENE, req.slot, feed.obs_input_url,
            )
            log.info("Auto-created OBS sources for slot %d: %s", req.slot, sources)
            await broadcast("obs:source_created", {
                "slot": req.slot, "source": sources.get("game", ""),
            })
        except Exception as exc:
            log.warning("Failed to auto-create OBS source for slot %d: %s", req.slot, exc)

        # Auto-apply active template positioning if one is set
        if _active_template_id is not None:
            try:
                tpl = presets.get_template(_active_template_id)
                regions = tpl.get("regions", {})
                slot_regions = regions.get("slots", {})
                if slot_regions:
                    img_path = tpl.get("image_path", "")
                    if img_path and Path(img_path).exists():
                        img_path = _obs_image_path(img_path)
                    else:
                        img_path = None
                    text_entries = regions.get("texts", [])
                    await obs.apply_template_layout(
                        RACE_SCENE, img_path, slot_regions, text_entries,
                    )
                    log.info("Auto-applied template %d after feed start", _active_template_id)
            except Exception as exc:
                log.warning("Failed to auto-apply template: %s", exc)

    return {"status": "ok", "local_url": feed.obs_input_url}


@app.get("/api/active-template")
async def get_active_template() -> dict[str, Any]:
    """Return the currently active template ID and slot count."""
    if _active_template_id is None:
        return {"template_id": None, "num_slots": 2}
    try:
        tpl = presets.get_template(_active_template_id)
        regions = tpl.get("regions", {})
        return {
            "template_id": _active_template_id,
            "num_slots": regions.get("num_slots", 2),
            "template_name": tpl.get("name", ""),
        }
    except KeyError:
        return {"template_id": None, "num_slots": 2}


@app.get("/api/ingest/qualities")
async def ingest_qualities(url: str) -> dict[str, Any]:
    """Query available stream qualities for a URL via streamlink."""
    if not url.strip():
        return {"status": "error", "error": "No URL provided"}
    try:
        qualities = await ingest.query_qualities(url.strip())
        return {"status": "ok", "qualities": qualities}
    except Exception as exc:
        log.warning("Quality query failed for %s: %s", url, exc)
        return {"status": "error", "error": str(exc), "qualities": ["best", "worst"]}


@app.post("/api/ingest/stop")
async def ingest_stop(req: IngestStopRequest) -> dict[str, str]:
    await ingest.stop_feed(req.slot)
    await broadcast("ingest:stopped", {"slot": req.slot})
    return {"status": "ok"}


@app.post("/api/ingest/reconnect")
async def ingest_reconnect(req: IngestStopRequest) -> dict[str, Any]:
    """Reconnect (restart) the feed on the given slot using its existing settings."""
    feed = ingest.get_feed(req.slot)
    if feed is None:
        raise HTTPException(status_code=404, detail=f"No feed running on slot {req.slot}")
    url, quality, offset = feed.url, feed.quality, str(feed.start_offset)
    await ingest.stop_feed(req.slot)
    await broadcast("ingest:stopped", {"slot": req.slot})
    new_feed = await ingest.start_feed(req.slot, url, quality, offset)
    await broadcast("ingest:started", {
        "slot": new_feed.slot, "url": new_feed.url,
        "local_url": new_feed.obs_input_url,
    })
    return {"status": "ok", "local_url": new_feed.obs_input_url}


@app.get("/api/ingest/status")
async def ingest_status() -> dict[str, Any]:
    feeds_info = {}
    for slot, feed in ingest.feeds.items():
        feeds_info[str(slot)] = {
            "slot": slot,
            "url": feed.url,
            "quality": feed.quality,
            "local_url": feed.obs_input_url,
            "running": feed.process is not None and feed.process.returncode is None,
        }
    return {"feeds": feeds_info}


@app.get("/api/ingest/token")
async def get_twitch_token() -> dict[str, Any]:
    """Check whether a Twitch OAuth token is configured (does not reveal the token)."""
    return {"has_token": bool(ingest.twitch_token)}


@app.post("/api/ingest/token")
async def set_twitch_token(body: dict[str, Any]) -> dict[str, str]:
    """Set or clear the Twitch OAuth token used by streamlink.

    Body: ``{ "token": "<oauth_token>" }``  (empty string to clear).
    Takes effect on the next feed start or reconnect.
    """
    token = str(body.get("token", "")).strip()
    ingest.twitch_token = token
    state = "set" if token else "cleared"
    log.info("Twitch OAuth token %s", state)
    return {"status": "ok", "token_state": state}


@app.get("/api/ingest/preview/{slot}")
async def ingest_preview(slot: int) -> dict[str, Any]:
    """Return the latest snapshot frame from a running feed as base64 JPEG.

    The ingest FFmpeg pipeline writes a periodic snapshot JPEG to disk
    (every ~2 s).  We read that file instead of opening the UDP stream,
    which would conflict with OBS already bound to the same port.
    """
    feed = ingest.get_feed(slot)
    if feed is None:
        return {"success": False, "error": "No feed running on this slot"}

    snapshot = feed.snapshot_path
    if not snapshot.exists() or snapshot.stat().st_size == 0:
        return {"success": False, "error": "Snapshot not available yet"}

    try:
        raw = await asyncio.to_thread(snapshot.read_bytes)
        img = Image.open(BytesIO(raw))
        w, h = img.size
        buf = BytesIO()
        img.save(buf, format="JPEG", quality=80)
        b64 = base64.b64encode(buf.getvalue()).decode()
        return {"success": True, "image_b64": b64, "width": w, "height": h}
    except Exception as exc:
        log.warning("Preview read failed for slot %d: %s", slot, exc)
        return {"success": False, "error": str(exc)}


# ---------------------------------------------------------------------------
# Crop endpoints
# ---------------------------------------------------------------------------

@app.post("/api/detect/manual")
async def detect_manual(req: ManualCropRequest) -> dict[str, str]:
    crop_rect = CropRect(x=req.x, y=req.y, width=req.width, height=req.height)
    obs_crop = crop_rect.to_obs_crop(req.source_width, req.source_height)
    if obs.connected:
        # Check that the source actually exists in OBS before applying crop
        exists = await obs.ensure_source_in_scene(RACE_SCENE, req.source_name)
        if not exists:
            return {"status": "error", "error": f"Source '{req.source_name}' not found in OBS. Start the feed first."}
        await obs.set_source_crop(req.source_name, SourceCrop(**obs_crop))
        await broadcast("crop:manual", {"source": req.source_name, "crop": crop_rect.to_dict()})
    else:
        return {"status": "error", "error": "OBS not connected"}
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# OBS endpoints
# ---------------------------------------------------------------------------

@app.post("/api/obs/connect")
async def obs_connect() -> dict[str, Any]:
    if obs.connected:
        # Rebuild the scene-item cache so logical names resolve correctly
        await obs._rebuild_scene_cache(RACE_SCENE)
        # Still provision any feeds that haven't been set up yet
        await _provision_running_feeds()
        return {"status": "already_connected"}
    await obs.connect()
    await broadcast("obs:connected", {})
    # Rebuild cache for existing scene items
    await obs._rebuild_scene_cache(RACE_SCENE)
    # Retroactively provision all running feeds
    await _provision_running_feeds()
    return {"status": "ok"}


@app.post("/api/obs/disconnect")
async def obs_disconnect() -> dict[str, str]:
    await obs.disconnect()
    await broadcast("obs:disconnected", {})
    return {"status": "ok"}


@app.get("/api/obs/status")
async def obs_status() -> dict[str, Any]:
    resp: dict[str, Any] = {"connected": obs.connected}
    if obs.connected:
        resp["platform"] = obs.platform
    return resp


@app.post("/api/obs/init")
async def obs_init() -> dict[str, Any]:
    """(Re-)provision OBS fully: scene, sources for all running feeds, mute all.

    Call this when you want to rebuild the OBS scene from scratch.
    """
    if not obs.connected:
        return {"status": "error", "error": "OBS not connected"}
    await _provision_running_feeds()
    return {"status": "ok"}


@app.post("/api/obs/launch")
async def obs_launch() -> dict[str, str]:
    """Attempt to launch OBS Studio as a detached process."""
    try:
        if sys.platform == "win32":
            # Common install locations on Windows
            candidates = [
                Path(r"C:\Program Files\obs-studio\bin\64bit\obs64.exe"),
                Path(r"C:\Program Files (x86)\obs-studio\bin\64bit\obs64.exe"),
            ]
            obs_path = shutil.which("obs64") or shutil.which("obs")
            if obs_path:
                candidates.insert(0, Path(obs_path))
            for p in candidates:
                if p.exists():
                    subprocess.Popen(
                        [str(p)],
                        creationflags=subprocess.DETACHED_PROCESS
                        | subprocess.CREATE_NEW_PROCESS_GROUP,
                    )
                    return {"status": "ok"}
            return {"status": "error", "error": "OBS not found"}
        elif sys.platform == "darwin":
            subprocess.Popen(["open", "-a", "OBS"])
            return {"status": "ok"}
        else:
            obs_bin = shutil.which("obs")
            if not obs_bin:
                return {"status": "error", "error": "OBS not found on PATH"}
            subprocess.Popen(
                [obs_bin],
                start_new_session=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return {"status": "ok"}
    except Exception as exc:
        log.warning("Failed to launch OBS: %s", exc)
        return {"status": "error", "error": str(exc)}


@app.post("/api/obs/crop")
async def obs_crop(req: CropApplyRequest) -> dict[str, str]:
    await obs.set_source_crop(
        req.source_name,
        SourceCrop(top=req.top, bottom=req.bottom, left=req.left, right=req.right),
    )
    await broadcast("crop:applied", {"source": req.source_name})
    return {"status": "ok"}


@app.post("/api/obs/sync")
async def obs_sync(req: SyncNudgeRequest) -> dict[str, Any]:
    new_ms = await obs.nudge_sync_offset(req.source_name, req.delta_ms)
    await broadcast("sync:nudged", {
        "source": req.source_name, "delta_ms": req.delta_ms, "total_ms": new_ms,
    })
    return {"status": "ok", "total_ms": new_ms}


@app.get("/api/obs/sync")
async def obs_sync_status(num_slots: int = 2) -> dict[str, Any]:
    """Return current sync delay for each racer slot."""
    result = {}
    for slot in range(num_slots):
        src = f"Racer{slot + 1}_Feed"
        try:
            ms = await obs.get_sync_offset(src)
            result[str(slot)] = {"source": src, "delay_ms": ms}
        except Exception:
            result[str(slot)] = {"source": src, "delay_ms": 0}
    return result


@app.post("/api/obs/scene")
async def obs_scene(req: SceneRequest) -> dict[str, str]:
    await obs.set_scene(req.scene_name)
    await broadcast("scene:changed", {"scene": req.scene_name})
    return {"status": "ok"}


@app.post("/api/obs/stream/start")
async def obs_stream_start() -> dict[str, str]:
    await obs.start_streaming()
    await broadcast("stream:started", {})
    return {"status": "ok"}


@app.post("/api/obs/stream/stop")
async def obs_stream_stop() -> dict[str, str]:
    await obs.stop_streaming()
    await broadcast("stream:stopped", {})
    return {"status": "ok"}


@app.get("/api/obs/scenes")
async def obs_scenes() -> dict[str, Any]:
    scenes = await obs.get_scene_list()
    return {"scenes": scenes}


@app.get("/api/obs/sources")
async def obs_sources() -> dict[str, Any]:
    sources = await obs.get_source_list()
    return {"sources": sources}


@app.post("/api/obs/audio")
async def obs_audio_switch(req: AudioSwitchRequest) -> dict[str, Any]:
    """Switch active audio to a specific racer slot (mutes all others).
    Use active_slot=-1 to mute all.
    """
    for slot in range(req.num_slots):
        source_name = f"Racer{slot + 1}_Feed"
        muted = (req.active_slot == -1) or (slot != req.active_slot)
        try:
            await obs.mute_input(source_name, muted)
        except Exception as exc:
            log.warning("Failed to set mute for '%s': %s", source_name, exc)
    await broadcast("audio:switched", {"active_slot": req.active_slot})
    return {"status": "ok", "active_slot": req.active_slot}


@app.get("/api/obs/audio")
async def obs_audio_status(num_slots: int = 2) -> dict[str, Any]:
    """Return mute status for racer sources."""
    result = {}
    for slot in range(num_slots):
        source_name = f"Racer{slot + 1}_Feed"
        try:
            muted = await obs.get_input_mute(source_name)
            result[str(slot)] = {"source": source_name, "muted": muted}
        except Exception:
            result[str(slot)] = {"source": source_name, "muted": None}
    return result


@app.get("/api/obs/audio/mixer")
async def obs_audio_mixer() -> dict[str, Any]:
    """Return volume & mute status for all audio-capable inputs."""
    if not obs.connected:
        return {"status": "error", "error": "OBS not connected", "inputs": []}
    inputs = await obs.list_audio_inputs()
    return {"status": "ok", "inputs": inputs}


@app.post("/api/obs/audio/volume")
async def obs_audio_volume(req: AudioVolumeRequest) -> dict[str, Any]:
    """Set volume (dB) for any OBS input."""
    await obs.set_input_volume(req.input_name, req.volume_db)
    await broadcast("audio:volume", {"input": req.input_name, "db": req.volume_db})
    return {"status": "ok"}


@app.post("/api/obs/audio/mute")
async def obs_audio_mute(req: AudioMuteRequest) -> dict[str, Any]:
    """Mute or unmute any OBS input."""
    await obs.mute_input(req.input_name, req.muted)
    await broadcast("audio:mute", {"input": req.input_name, "muted": req.muted})
    return {"status": "ok"}


@app.post("/api/obs/audio/discord")
async def obs_audio_discord(req: DiscordSourceRequest) -> dict[str, Any]:
    """Create (or update) a Discord / commentary audio capture source."""
    if not obs.connected:
        return {"status": "error", "error": "OBS not connected"}
    try:
        await obs.create_audio_capture(RACE_SCENE, "Commentary", req.device_id, window=req.window)
        return {"status": "ok", "source": "Commentary"}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@app.get("/api/obs/video-settings")
async def obs_video_settings() -> dict[str, Any]:
    """Return the OBS canvas (base) resolution so the UI can offer scaled options."""
    if not obs.connected:
        return {"status": "error", "error": "OBS not connected"}
    try:
        vs = await obs.get_video_settings()
        return {
            "status": "ok",
            "baseWidth": vs.get("baseWidth", 1920),
            "baseHeight": vs.get("baseHeight", 1080),
            "outputWidth": vs.get("outputWidth", 1920),
            "outputHeight": vs.get("outputHeight", 1080),
        }
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@app.post("/api/obs/projector")
async def obs_projector(req: ProjectorRequest) -> dict[str, Any]:
    """Open a windowed or fullscreen projector for a scene."""
    if not obs.connected:
        return {"status": "error", "error": "OBS not connected"}
    try:
        await obs.open_projector(req.scene_name, req.monitor, req.width, req.height)
        return {"status": "ok"}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@app.post("/api/obs/audio/monitor")
async def obs_audio_monitor(req: AudioMonitorRequest) -> dict[str, Any]:
    """Set monitoring type for an audio source (e.g. Monitor Only)."""
    if not obs.connected:
        return {"status": "error", "error": "OBS not connected"}
    try:
        await obs.set_audio_monitor_type(req.input_name, req.monitor_type)
        return {"status": "ok"}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@app.get("/api/obs/audio/devices")
async def obs_audio_devices() -> dict[str, Any]:
    """List audio output capture devices known to OBS.

    Creates a temporary probe source of the platform-appropriate kind,
    queries its device_id property items, then removes it.
    Falls back to a single "Default" entry on failure.
    """
    if not obs.connected:
        return {"status": "error", "error": "OBS not connected", "devices": []}
    probe_name = "_device_probe_tmp"
    kind = obs.audio_capture_kind()
    try:
        # Create a hidden temporary source so we can list its device options
        try:
            await obs.request("CreateInput", {
                "sceneName": RACE_SCENE,
                "inputName": probe_name,
                "inputKind": kind,
                "inputSettings": {},
                "sceneItemEnabled": False,
            })
        except Exception:
            pass  # may already exist from a previous failed cleanup
        resp = await obs.request("GetInputPropertiesListPropertyItems", {
            "inputName": probe_name,
            "propertyName": "device_id",
        })
        items = resp.get("propertyItems", [])
        return {"status": "ok", "devices": items}
    except Exception:
        return {"status": "ok", "devices": [{"itemName": "Default", "itemValue": "default"}]}
    finally:
        try:
            await obs.request("RemoveInput", {"inputName": probe_name})
        except Exception:
            pass


@app.post("/api/obs/text")
async def obs_text_source(req: TextSourceRequest) -> dict[str, Any]:
    """Create or update a text source in the OBS scene."""
    if not obs.connected:
        return {"status": "error", "error": "OBS not connected"}
    try:
        await obs.create_text_source(
            RACE_SCENE, req.source_name, req.text,
            font_size=req.font_size, color_hex=req.color,
        )
        resp = await obs.request("GetSceneItemId", {
            "sceneName": RACE_SCENE,
            "sourceName": req.source_name,
        })
        transform: dict[str, Any] = {
            "positionX": req.x,
            "positionY": req.y,
        }
        if req.width and req.height:
            transform["boundsType"] = "OBS_BOUNDS_SCALE_INNER"
            transform["boundsWidth"] = req.width
            transform["boundsHeight"] = req.height
        await obs.request("SetSceneItemTransform", {
            "sceneName": RACE_SCENE,
            "sceneItemId": resp["sceneItemId"],
            "sceneItemTransform": transform,
        })
    except Exception as exc:
        log.warning("Text source error: %s", exc)
        return {"status": "error", "error": str(exc)}
    return {"status": "ok"}


@app.get("/api/obs/screenshot")
async def obs_screenshot(scene: str = RACE_SCENE) -> dict[str, Any]:
    """Return a base64 JPEG screenshot of the current OBS scene."""
    if not obs.connected:
        return {"success": False, "error": "OBS not connected"}
    try:
        b64 = await obs.get_scene_screenshot(scene)
        return {"success": True, "image_b64": b64}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


# ---------------------------------------------------------------------------
# Preset endpoints
# ---------------------------------------------------------------------------

@app.get("/api/presets")
async def list_presets(channel: str = "") -> dict[str, Any]:
    items = presets.list_presets(channel or None)
    return {"presets": [p.to_dict() for p in items]}


@app.post("/api/presets")
async def save_preset(req: PresetSaveRequest) -> dict[str, Any]:
    p = presets.save_preset(
        channel=req.channel,
        name=req.name,
        game_crop=req.game_crop,
        tracker_crop=req.tracker_crop,
        timer_crop=req.timer_crop,
    )
    return {"status": "ok", "preset": p.to_dict()}


@app.delete("/api/presets/{preset_id}")
async def delete_preset(preset_id: int) -> dict[str, Any]:
    ok = presets.delete_preset(preset_id)
    return {"status": "ok" if ok else "not_found"}


@app.post("/api/presets/{preset_id}/apply")
async def apply_preset(preset_id: int, slot: int = 0) -> dict[str, Any]:
    """Apply a saved preset's crop regions to OBS sources for the given slot.

    Crop coordinates are rescaled from the resolution they were saved at
    to the resolution of the currently-running feed so that presets remain
    correct after a quality change.
    """
    try:
        p = presets.get_preset(preset_id)
    except KeyError:
        return {"status": "error", "error": "Preset not found"}

    if not obs.connected:
        return {"status": "error", "error": "OBS not connected"}

    # Determine the current feed resolution from the snapshot
    cur_w, cur_h = 0, 0
    feed = ingest.get_feed(slot)
    if feed and feed.snapshot_path.exists():
        try:
            raw = await asyncio.to_thread(feed.snapshot_path.read_bytes)
            img = Image.open(BytesIO(raw))
            cur_w, cur_h = img.size
        except Exception:
            pass

    applied = []
    for region_name, crop_data in [
        ("game", p.game_crop), ("tracker", p.tracker_crop), ("timer", p.timer_crop),
    ]:
        if not crop_data:
            continue
        source_name = f"Racer{slot + 1}_{region_name.capitalize()}"

        saved_w = crop_data.get("source_width", 1920)
        saved_h = crop_data.get("source_height", 1080)
        # Use current feed resolution if available, otherwise fall back to saved
        target_w = cur_w or saved_w
        target_h = cur_h or saved_h
        # Rescale crop from saved resolution to current resolution
        scale_x = target_w / saved_w if saved_w else 1
        scale_y = target_h / saved_h if saved_h else 1
        rect = CropRect(
            x=round(crop_data["x"] * scale_x),
            y=round(crop_data["y"] * scale_y),
            width=round(crop_data["width"] * scale_x),
            height=round(crop_data["height"] * scale_y),
        )
        obs_crop = rect.to_obs_crop(target_w, target_h)
        try:
            await obs.set_source_crop(source_name, SourceCrop(**obs_crop))
            applied.append(region_name)
        except Exception as exc:
            log.warning("Preset apply failed for %s: %s", source_name, exc)

    return {"status": "ok", "applied": applied}


# ---------------------------------------------------------------------------
# Template endpoints
# ---------------------------------------------------------------------------

@app.get("/api/templates")
async def list_templates() -> dict[str, Any]:
    return {"templates": presets.list_templates()}


@app.post("/api/templates/upload")
async def upload_template(name: str = "Untitled", file: UploadFile = File(...)) -> dict[str, Any]:
    """Upload a template image and create a template record."""
    if not file.filename:
        return {"status": "error", "error": "No file uploaded"}

    ext = Path(file.filename).suffix.lower()
    if ext not in (".jpg", ".jpeg", ".png", ".webp"):
        return {"status": "error", "error": "Only JPG/PNG/WebP images allowed"}

    import uuid
    fname = f"{uuid.uuid4().hex}{ext}"
    dest = _TEMPLATES_UPLOAD_DIR / fname
    _TEMPLATES_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    with open(dest, "wb") as f:
        content = await file.read()
        f.write(content)

    tpl = presets.save_template(name=name, image_path=str(dest), regions={})
    return {"status": "ok", "template": tpl}


@app.get("/api/templates/{template_id}")
async def get_template(template_id: int) -> dict[str, Any]:
    try:
        tpl = presets.get_template(template_id)
        # Include base64 image data
        img_path = Path(tpl["image_path"])
        if img_path.exists():
            raw = img_path.read_bytes()
            tpl["image_b64"] = base64.b64encode(raw).decode()
        return {"status": "ok", "template": tpl}
    except KeyError:
        return {"status": "error", "error": "Template not found"}


@app.put("/api/templates/{template_id}/regions")
async def update_template_regions(template_id: int, req: TemplateRegionsRequest) -> dict[str, Any]:
    try:
        tpl = presets.update_template_regions(template_id, req.regions)
        return {"status": "ok", "template": tpl}
    except KeyError:
        return {"status": "error", "error": "Template not found"}


@app.delete("/api/templates/{template_id}")
async def delete_template(template_id: int) -> dict[str, Any]:
    try:
        tpl = presets.get_template(template_id)
        # Clean up image file
        img_path = Path(tpl["image_path"])
        if img_path.exists():
            img_path.unlink()
    except KeyError:
        pass
    ok = presets.delete_template(template_id)
    return {"status": "ok" if ok else "not_found"}


@app.post("/api/templates/{template_id}/apply")
async def apply_template_to_obs(
    template_id: int, req: TemplateApplyRequest | None = None,
) -> dict[str, Any]:
    """Apply a template layout to the OBS scene.

    Sets the template image as the scene background and positions every
    defined source (Racer{N}_Game / Tracker / Timer) according to the
    template's per-slot regions.
    """
    global _active_template_id
    scene = req.scene_name if req else RACE_SCENE
    if not obs.connected:
        return {"status": "error", "error": "OBS not connected"}
    try:
        tpl = presets.get_template(template_id)
    except KeyError:
        return {"status": "error", "error": "Template not found"}

    regions = tpl.get("regions", {})
    slot_regions = regions.get("slots", {})
    if not slot_regions:
        return {"status": "error", "error": "Template has no slot regions defined"}

    # Use the stored image path for OBS background — translate for host-side OBS
    img_path = tpl.get("image_path", "")
    if img_path and Path(img_path).exists():
        img_path = _obs_image_path(img_path)
    else:
        img_path = None

    text_entries = regions.get("texts", [])
    applied = await obs.apply_template_layout(
        scene, img_path, slot_regions, text_entries,
    )
    _active_template_id = template_id
    presets.set_setting("active_template_id", str(template_id))
    await broadcast("template:applied", {
        "template_id": template_id,
        "template_name": tpl.get("name", ""),
        "num_slots": regions.get("num_slots", 2),
        "applied": applied,
    })
    return {"status": "ok", "applied": applied}


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
import json
import logging
import re
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
from .obs_control import OBSController, OBSRequestError, SourceCrop
from .presets import PresetStore
from .slobs_control import SlobsController

_STATIC_DIR = Path(__file__).parent / "static"
_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_TEMPLATES_UPLOAD_DIR = _DATA_DIR / "template_images"
_PRESET_IMAGES_DIR = _DATA_DIR / "preset_images"
_IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".webp")

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


class AppSelectRequest(BaseModel):
    app: str  # "obs" | "streamlabs"
    host: str | None = None    # Streamlabs only
    port: int | None = None    # Streamlabs only
    token: str | None = None   # Streamlabs only (empty string clears)

class PresetSaveRequest(BaseModel):
    channel: str
    name: str
    game_crop: dict[str, int] | None = None
    tracker_crop: dict[str, int] | None = None
    timer_crop: dict[str, int] | None = None
    # Custom named regions: {region_key: {x, y, width, height, source_width, source_height}}
    extra_crops: dict[str, dict[str, int]] | None = None


class CustomRegionRequest(BaseModel):
    name: str


class TemplateRegionsRequest(BaseModel):
    regions: dict[str, Any]  # e.g. {"slots": {"0": {"game": {x,y,w,h}, ...}}}


class TemplateApplyRequest(BaseModel):
    scene_name: str = "Race Scene"


class BlankTemplateRequest(BaseModel):
    name: str = "Untitled"
    width: int = 1920
    height: int = 1080


class TextSourceRequest(BaseModel):
    source_name: str
    text: str
    font_size: int = 36
    color: str = "#ffffff"
    x: float = 0
    y: float = 0

# ---------------------------------------------------------------------------
# App globals (set during lifespan)
# ---------------------------------------------------------------------------

config: Config
ingest: IngestManager
obs: OBSController | SlobsController  # the ACTIVE streaming-app controller
presets: PresetStore

RACE_SCENE = "Race Scene"  # Default scene used for auto-setup
_active_template_id: int | None = None  # Currently applied template


def _slobs_conn_settings() -> dict[str, Any]:
    """Streamlabs connection settings: dashboard-saved values override .env."""
    port_raw = presets.get_setting("slobs_port") or ""
    try:
        port = int(port_raw) if port_raw else None
    except ValueError:
        port = None
    return {
        "host": presets.get_setting("slobs_host") or None,
        "port": port,
        "token": presets.get_setting("slobs_token"),  # None if never set
    }


def _make_controller(app: str) -> OBSController | SlobsController:
    if app == "streamlabs":
        return SlobsController(config, **_slobs_conn_settings())
    return OBSController(config)


def _active_app() -> str:
    app = presets.get_setting("streaming_app") or config.streaming_app
    return app if app in ("obs", "streamlabs") else "obs"


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


_RESERVED_REGIONS = {"game", "tracker", "timer", "feed"}


def _norm_region_key(name: str) -> str:
    """Normalize a user-supplied region name to a lowercase key."""
    return re.sub(r"[^a-z0-9_]", "", name.strip().lower().replace(" ", "_"))[:20]


def _get_custom_regions() -> list[str]:
    """User-defined region keys (lowercase), shared across all racer slots."""
    raw = presets.get_setting("custom_regions")
    if not raw:
        return []
    try:
        return [k for k in json.loads(raw) if isinstance(k, str)]
    except (ValueError, TypeError):
        return []


def _save_custom_regions(keys: list[str]) -> None:
    presets.set_setting("custom_regions", json.dumps(keys))
    obs.set_extra_regions([k.capitalize() for k in keys])


async def _current_scene(default: str = RACE_SCENE) -> str:
    """Current OBS program scene, or *default* if it can't be determined."""
    try:
        return await obs.get_current_scene() or default
    except Exception:
        return default


async def _template_layout_args(tpl: dict[str, Any]) -> dict[str, Any]:
    """Resolve a template record into kwargs for ``obs.apply_template_layout``.

    Determines the template's coordinate space (drawn-canvas size stored in
    the regions, else the image's pixel size) so the layout can be rescaled
    to the OBS canvas resolution.
    """
    regions = tpl.get("regions", {})
    img_path = tpl.get("image_path", "")
    obs_path: str | None = None
    template_size: tuple[int, int] | None = None

    if img_path and Path(img_path).exists():
        try:
            def _size(p: str = img_path) -> tuple[int, int]:
                with Image.open(p) as im:
                    return im.size
            template_size = await asyncio.to_thread(_size)
        except Exception:
            template_size = None
        obs_path = _obs_image_path(img_path)

    canvas = regions.get("canvas") or {}
    if canvas.get("width") and canvas.get("height"):
        template_size = (int(canvas["width"]), int(canvas["height"]))

    # Per-slot/region images, translated to host paths (existing files only)
    region_images: dict[str, dict[str, str]] = {}
    for slot_str, imgs in (regions.get("images") or {}).items():
        for region, info in (imgs or {}).items():
            path = (info or {}).get("path", "")
            if path and Path(path).is_file():
                region_images.setdefault(slot_str, {})[region] = _obs_image_path(path)

    return {
        "image_path": obs_path,
        "slot_regions": regions.get("slots", {}),
        "text_entries": regions.get("texts", []),
        "template_size": template_size,
        "region_images": region_images,
    }


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
    presets = PresetStore()
    obs = _make_controller(_active_app())
    _TEMPLATES_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    _PRESET_IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    # Load user-defined custom regions so scene provisioning knows about them
    obs.set_extra_regions([k.capitalize() for k in _get_custom_regions()])

    # Restore persisted active template
    saved_tpl = presets.get_setting("active_template_id")
    if saved_tpl is not None:
        try:
            _active_template_id = int(saved_tpl)
            presets.get_template(_active_template_id)  # verify it still exists
            log.info("Restored active template: %d", _active_template_id)
        except (ValueError, KeyError):
            _active_template_id = None

    log.info("Restreaming Automation API ready  (app: %s)", obs.app)

    # Try auto-connecting to OBS at startup
    try:
        await obs.connect()
        log.info("Auto-connected to %s at startup", obs.app)
        # Rebuild scene-item cache so existing Feed items map to logical names
        await obs.rebuild_scene_cache(RACE_SCENE)
    except Exception as exc:
        log.info("Streaming app not available at startup (will connect later): %s", exc)

    yield
    await ingest.stop_all()
    if obs.connected:
        await obs.disconnect()
    presets.close()


app = FastAPI(title="Restreaming Automation API", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    # Wildcard origins must not be combined with credentials (browsers
    # reject the combination, and this API has no cookie auth anyway)
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(OBSRequestError)
async def obs_error_handler(request, exc: OBSRequestError):  # type: ignore[no-untyped-def]
    """Turn OBS failures into structured 502s instead of bare 500s so the
    dashboard can show the actual reason."""
    from fastapi.responses import JSONResponse
    return JSONResponse(status_code=502, content={"detail": str(exc)})

# Serve the standalone dashboard at /dashboard
if _STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


@app.get("/api/health")
async def health_check() -> dict[str, Any]:
    """Lightweight health endpoint for Docker HEALTHCHECK and monitoring."""
    return {
        "status": "ok",
        "obs_connected": obs.connected,
        "active_feeds": len(ingest.feeds),
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
    for ws in list(_ws_clients):
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
                layout = await _template_layout_args(tpl)
                if layout["slot_regions"]:
                    await obs.apply_template_layout(RACE_SCENE, **layout)
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
    try:
        st = snapshot.stat()
    except OSError:
        return {"success": False, "error": "Snapshot not available yet"}
    if st.st_size == 0:
        return {"success": False, "error": "Snapshot not available yet"}
    # A file older than this feed is a leftover from a previous stream
    # (e.g. after an unclean shutdown) — never show a stale frame.
    if feed.started_at and st.st_mtime < feed.started_at:
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
        await obs.rebuild_scene_cache(RACE_SCENE)
        # Still provision any feeds that haven't been set up yet
        await _provision_running_feeds()
        return {"status": "already_connected"}
    try:
        await obs.connect()
    except Exception as exc:
        log.warning("OBS connect failed: %s", exc)
        return {"status": "error", "error": str(exc)}
    await broadcast("obs:connected", {})
    # Rebuild cache for existing scene items
    await obs.rebuild_scene_cache(RACE_SCENE)
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
    resp: dict[str, Any] = {
        "connected": obs.connected,
        "app": obs.app,
        "capabilities": obs.capabilities,
    }
    if obs.connected:
        resp["platform"] = obs.platform
        try:
            # Lets the dashboard mirror the app's font-size semantics exactly
            resp["text_kind"] = await obs.text_source_kind()
        except Exception:
            pass
    return resp


# ---------------------------------------------------------------------------
# Streaming-app selection (OBS Studio ↔ Streamlabs Desktop)
# ---------------------------------------------------------------------------

@app.get("/api/app")
async def get_streaming_app() -> dict[str, Any]:
    conn = _slobs_conn_settings()
    return {
        "app": obs.app,
        "connected": obs.connected,
        "capabilities": obs.capabilities,
        "slobs_host": conn["host"] or config.slobs_host,
        "slobs_port": conn["port"] or config.slobs_port,
        "slobs_has_token": bool(conn["token"] or config.slobs_token),
    }


@app.post("/api/app")
async def select_streaming_app(req: AppSelectRequest) -> dict[str, Any]:
    """Switch between OBS Studio and Streamlabs Desktop (and save connection
    settings for the latter). Reconnects with the new settings."""
    global obs
    if req.app not in ("obs", "streamlabs"):
        return {"status": "error", "error": f"Unknown app '{req.app}'"}

    if req.host is not None:
        presets.set_setting("slobs_host", req.host.strip())
    if req.port is not None:
        presets.set_setting("slobs_port", str(req.port))
    if req.token is not None:
        presets.set_setting("slobs_token", req.token.strip())
    presets.set_setting("streaming_app", req.app)

    # Swap the active controller
    if obs.connected:
        try:
            await obs.disconnect()
        except Exception:
            pass
        await broadcast("obs:disconnected", {})
    obs = _make_controller(req.app)
    obs.set_extra_regions([k.capitalize() for k in _get_custom_regions()])

    result: dict[str, Any] = {"status": "ok", "app": req.app, "connected": False}
    try:
        await obs.connect()
        await obs.rebuild_scene_cache(RACE_SCENE)
        await _provision_running_feeds()
        await broadcast("obs:connected", {})
        result["connected"] = True
    except Exception as exc:
        log.info("Switched to %s but connect failed: %s", req.app, exc)
        result["error"] = str(exc)
    result["capabilities"] = obs.capabilities
    return result


# ---------------------------------------------------------------------------
# Custom crop regions (shared across all racer slots)
# ---------------------------------------------------------------------------

@app.get("/api/custom-regions")
async def list_custom_regions() -> dict[str, Any]:
    return {"regions": _get_custom_regions()}


@app.post("/api/custom-regions")
async def add_custom_region(req: CustomRegionRequest) -> dict[str, Any]:
    key = _norm_region_key(req.name)
    if not key:
        return {"status": "error", "error": "Name must contain letters/numbers"}
    if key in _RESERVED_REGIONS:
        return {"status": "error", "error": f"'{key}' is a reserved name"}
    regions = _get_custom_regions()
    if key in regions:
        return {"status": "error", "error": f"Region '{key}' already exists"}
    if len(regions) >= 5:
        return {"status": "error", "error": "Maximum of 5 custom regions"}
    regions.append(key)
    _save_custom_regions(regions)
    # Existing feeds need an extra scene item per racer for the new region
    if obs.connected:
        await _provision_running_feeds()
    await broadcast("regions:changed", {"regions": regions})
    return {"status": "ok", "regions": regions}


@app.delete("/api/custom-regions/{name}")
async def delete_custom_region(name: str) -> dict[str, Any]:
    key = _norm_region_key(name)
    regions = [r for r in _get_custom_regions() if r != key]
    _save_custom_regions(regions)
    if obs.connected:
        await _provision_running_feeds()
    await broadcast("regions:changed", {"regions": regions})
    return {"status": "ok", "regions": regions}


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
    """Attempt to launch the active streaming app as a detached process."""
    import os
    slobs = obs.app == "streamlabs"
    app_label = "Streamlabs Desktop" if slobs else "OBS"
    try:
        if sys.platform == "win32":
            if slobs:
                pf = os.environ.get("ProgramFiles", r"C:\Program Files")
                candidates = [
                    Path(pf) / "Streamlabs Desktop" / "Streamlabs Desktop.exe",
                    Path(pf) / "Streamlabs OBS" / "Streamlabs OBS.exe",
                ]
            else:
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
                        cwd=str(p.parent),
                        creationflags=subprocess.DETACHED_PROCESS
                        | subprocess.CREATE_NEW_PROCESS_GROUP,
                    )
                    return {"status": "ok"}
            return {"status": "error", "error": f"{app_label} not found"}
        elif sys.platform == "darwin":
            subprocess.Popen(["open", "-a", "Streamlabs Desktop" if slobs else "OBS"])
            return {"status": "ok"}
        else:
            if slobs:
                return {"status": "error",
                        "error": "Streamlabs Desktop has no Linux build — run it on a "
                                 "Windows/macOS machine and connect over the network"}
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
        log.warning("Failed to launch %s: %s", app_label, exc)
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


@app.get("/api/obs/stream/status")
async def obs_stream_status() -> dict[str, Any]:
    """Return live-stream state (active, timecode, dropped frames)."""
    if not obs.connected:
        return {"status": "error", "error": "OBS not connected", "active": False}
    try:
        st = await obs.get_stream_status()
        return {"status": "ok", **st}
    except Exception as exc:
        return {"status": "error", "error": str(exc), "active": False}


@app.get("/api/obs/scenes")
async def obs_scenes() -> dict[str, Any]:
    scenes = await obs.get_scene_list()
    return {"scenes": scenes, "current": await _current_scene(default="")}


@app.get("/api/obs/sources")
async def obs_sources() -> dict[str, Any]:
    sources = await obs.get_source_list()
    return {"sources": sources}


@app.post("/api/obs/audio")
async def obs_audio_switch(req: AudioSwitchRequest) -> dict[str, Any]:
    """Switch active audio to a specific racer slot (mutes all others).
    Use active_slot=-1 to mute all.
    """
    failed: list[str] = []
    for slot in range(req.num_slots):
        source_name = f"Racer{slot + 1}_Feed"
        muted = (req.active_slot == -1) or (slot != req.active_slot)
        try:
            await obs.mute_input(source_name, muted)
        except Exception as exc:
            log.warning("Failed to set mute for '%s': %s", source_name, exc)
            failed.append(source_name)
    if len(failed) == req.num_slots:
        return {"status": "error",
                "error": f"No racer feeds found in OBS ({', '.join(failed)}) — start feeds first"}
    await broadcast("audio:switched", {"active_slot": req.active_slot})
    return {"status": "ok", "active_slot": req.active_slot, "failed": failed}


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
async def obs_audio_mixer(scope: str = "scene") -> dict[str, Any]:
    """Return volume & mute status for audio-capable inputs.

    ``scope=scene`` (default) limits the list to inputs that are part of the
    current program scene plus OBS global audio inputs; ``scope=all`` returns
    every audio input OBS knows about.
    """
    if not obs.connected:
        return {"status": "error", "error": "OBS not connected", "inputs": []}
    scene = await _current_scene() if scope != "all" else None
    inputs = await obs.list_audio_inputs(scene)
    return {"status": "ok", "inputs": inputs, "scene": scene}


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
        return {"status": "error", "error": "Streaming app not connected", "devices": []}
    try:
        devices = await obs.list_audio_devices(await _current_scene())
        return {"status": "ok", "devices": devices}
    except Exception:
        return {"status": "ok", "devices": [{"itemName": "Default", "itemValue": "default"}]}


@app.get("/api/obs/audio/apps")
async def obs_audio_apps() -> dict[str, Any]:
    """List application windows available for Application Audio Capture
    (Windows only).  OBS expects a full ``title:class:executable`` window
    spec — a bare exe name silently matches nothing — so the dashboard
    offers these as a dropdown.
    """
    if not obs.connected:
        return {"status": "error", "error": "Streaming app not connected", "apps": []}
    try:
        apps = await obs.list_audio_apps(await _current_scene())
        return {"status": "ok", "apps": apps}
    except Exception as exc:
        return {"status": "error", "error": str(exc), "apps": []}


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
        await obs.set_scene_item_transform(RACE_SCENE, req.source_name, {
            "positionX": req.x,
            "positionY": req.y,
            "boundsType": "OBS_BOUNDS_NONE",
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
    extra = {k: v for k, v in (req.extra_crops or {}).items() if v} or None
    p = presets.save_preset(
        channel=req.channel,
        name=req.name,
        game_crop=req.game_crop,
        tracker_crop=req.tracker_crop,
        timer_crop=req.timer_crop,
        extra_crops=extra,
    )
    return {"status": "ok", "preset": p.to_dict()}


@app.delete("/api/presets/{preset_id}")
async def delete_preset(preset_id: int) -> dict[str, Any]:
    # Clean up any attached image files
    try:
        p = presets.get_preset(preset_id)
        for info in (p.images or {}).values():
            path = (info or {}).get("path", "")
            if path:
                Path(path).unlink(missing_ok=True)
    except (KeyError, OSError):
        pass
    ok = presets.delete_preset(preset_id)
    return {"status": "ok" if ok else "not_found"}


@app.post("/api/presets/{preset_id}/image")
async def upload_preset_image(
    preset_id: int, region: str = "tracker", file: UploadFile = File(...),
) -> dict[str, Any]:
    """Attach an image to a preset for a region (e.g. a tracker placeholder,
    or something personal to the racer).  Replaces any existing image for
    that region."""
    try:
        p = presets.get_preset(preset_id)
    except KeyError:
        return {"status": "error", "error": "Preset not found"}
    key = _norm_region_key(region)
    if not key:
        return {"status": "error", "error": "Invalid region"}
    if not file.filename:
        return {"status": "error", "error": "No file uploaded"}
    ext = Path(file.filename).suffix.lower()
    if ext not in _IMAGE_EXTS:
        return {"status": "error", "error": "Only JPG/PNG/WebP images allowed"}

    import uuid
    dest = _PRESET_IMAGES_DIR / f"{uuid.uuid4().hex}{ext}"
    _PRESET_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    content = await file.read()
    await asyncio.to_thread(dest.write_bytes, content)

    images = dict(p.images or {})
    old = (images.get(key) or {}).get("path", "")
    if old:
        try:
            Path(old).unlink(missing_ok=True)
        except OSError:
            pass
    images[key] = {"path": str(dest), "original_name": file.filename}
    p = presets.update_preset_images(preset_id, images)
    return {"status": "ok", "preset": p.to_dict()}


@app.delete("/api/presets/{preset_id}/image")
async def delete_preset_image(preset_id: int, region: str) -> dict[str, Any]:
    try:
        p = presets.get_preset(preset_id)
    except KeyError:
        return {"status": "error", "error": "Preset not found"}
    key = _norm_region_key(region)
    images = dict(p.images or {})
    info = images.pop(key, None)
    if info and info.get("path"):
        try:
            Path(info["path"]).unlink(missing_ok=True)
        except OSError:
            pass
    p = presets.update_preset_images(preset_id, images)
    return {"status": "ok", "preset": p.to_dict()}


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

    regions_map: dict[str, dict[str, int] | None] = {
        "game": p.game_crop, "tracker": p.tracker_crop, "timer": p.timer_crop,
        **(p.extra_crops or {}),
    }
    applied = []
    for region_name, crop_data in regions_map.items():
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

    # Place any images attached to the preset (e.g. a tracker placeholder)
    placed = await _place_preset_images(slot, p.images or {})
    applied.extend(placed)

    return {"status": "ok", "applied": applied}


async def _place_preset_images(slot: int, images: dict[str, Any],
                               scene: str = RACE_SCENE) -> list[str]:
    """Create image sources for a preset's attached images.

    Each image is keyed by a region ("tracker", custom key, …).  When the
    active template defines a rect for that region+slot, the image is
    stretched into it and the corresponding live feed item is hidden —
    e.g. a placeholder shown instead of a tracker the racer doesn't run.
    Stale ``PresetImg_R{N}_*`` sources from a previously applied preset
    are removed.
    """
    # Resolve template rects for this slot and the template→canvas scale
    tpl_rects: dict[str, Any] = {}
    tpl_size = None
    if _active_template_id is not None:
        try:
            tpl = presets.get_template(_active_template_id)
            layout = await _template_layout_args(tpl)
            tpl_rects = layout["slot_regions"].get(str(slot), {}) or {}
            tpl_size = layout["template_size"]
        except Exception:
            pass
    try:
        vs = await obs.get_video_settings()
        canvas_w = float(vs.get("baseWidth", 1920))
        canvas_h = float(vs.get("baseHeight", 1080))
    except Exception:
        canvas_w, canvas_h = 1920.0, 1080.0
    tpl_w, tpl_h = tpl_size if tpl_size else (canvas_w, canvas_h)
    sx = canvas_w / tpl_w if tpl_w else 1.0
    sy = canvas_h / tpl_h if tpl_h else 1.0

    placed: list[str] = []
    wanted: set[str] = set()
    for region, info in images.items():
        path = (info or {}).get("path", "")
        if not path or not Path(path).exists():
            continue
        src_name = f"PresetImg_R{slot + 1}_{region}"
        wanted.add(src_name)
        try:
            await obs.create_image_source(scene, src_name, _obs_image_path(path))
            rect = tpl_rects.get(region)
            if rect:
                await obs.set_scene_item_transform(scene, src_name, {
                    "positionX": float(rect["x"]) * sx,
                    "positionY": float(rect["y"]) * sy,
                    "boundsType": "OBS_BOUNDS_STRETCH",
                    "boundsWidth": float(rect["width"]) * sx,
                    "boundsHeight": float(rect["height"]) * sy,
                })
                # The image stands in for the live region → hide the feed item
                try:
                    await obs.set_scene_item_enabled(
                        scene, f"Racer{slot + 1}_{region.capitalize()}", False)
                except Exception:
                    pass
            else:
                log.info("Preset image '%s': no template rect for region '%s' — "
                         "placed unpositioned", src_name, region)
            placed.append(src_name)
        except Exception as exc:
            log.warning("Preset image '%s' failed: %s", src_name, exc)

    # Remove leftovers from a previously applied preset for this slot
    try:
        for name in await obs.list_input_names():
            if name.startswith(f"PresetImg_R{slot + 1}_") and name not in wanted:
                await obs.remove_input_if_exists(name)
    except Exception:
        pass
    return placed


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

    content = await file.read()
    await asyncio.to_thread(dest.write_bytes, content)

    tpl = presets.save_template(name=name, image_path=str(dest), regions={})
    return {"status": "ok", "template": tpl}


@app.post("/api/templates/blank")
async def create_blank_template(req: BlankTemplateRequest) -> dict[str, Any]:
    """Create a template with no background image — just a canvas size.

    For restreamers who want a simple layout without designing artwork:
    regions and text are placed on the bare OBS canvas.
    """
    w = max(16, min(req.width, 16384))
    h = max(16, min(req.height, 16384))
    regions: dict[str, Any] = {
        "slots": {"0": {}, "1": {}},
        "num_slots": 2,
        "texts": [],
        "canvas": {"width": w, "height": h},
    }
    tpl = presets.save_template(name=req.name.strip() or "Untitled", image_path="", regions=regions)
    return {"status": "ok", "template": tpl}


@app.post("/api/templates/{template_id}/region-image")
async def upload_template_region_image(
    template_id: int, slot: int = 0, region: str = "tracker",
    file: UploadFile = File(...),
) -> dict[str, Any]:
    """Attach an image to a template region (per racer slot).

    The image is shown in the layout editor and, on apply, placed in OBS
    instead of that region's live feed — e.g. a placeholder for a racer
    who doesn't run a tracker.
    """
    try:
        tpl = presets.get_template(template_id)
    except KeyError:
        return {"status": "error", "error": "Template not found"}
    key = _norm_region_key(region)
    if not key:
        return {"status": "error", "error": "Invalid region"}
    if not file.filename:
        return {"status": "error", "error": "No file uploaded"}
    ext = Path(file.filename).suffix.lower()
    if ext not in _IMAGE_EXTS:
        return {"status": "error", "error": "Only JPG/PNG/WebP images allowed"}

    import uuid
    dest = _TEMPLATES_UPLOAD_DIR / f"region_{uuid.uuid4().hex}{ext}"
    _TEMPLATES_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    content = await file.read()
    await asyncio.to_thread(dest.write_bytes, content)

    regions = tpl.get("regions") or {}
    images = regions.setdefault("images", {})
    slot_imgs = images.setdefault(str(slot), {})
    old = (slot_imgs.get(key) or {}).get("path", "")
    if old:
        try:
            Path(old).unlink(missing_ok=True)
        except OSError:
            pass
    slot_imgs[key] = {"path": str(dest), "original_name": file.filename}
    tpl = presets.update_template_regions(template_id, regions)
    return {"status": "ok", "template": tpl}


@app.delete("/api/templates/{template_id}/region-image")
async def delete_template_region_image(
    template_id: int, slot: int = 0, region: str = "tracker",
) -> dict[str, Any]:
    try:
        tpl = presets.get_template(template_id)
    except KeyError:
        return {"status": "error", "error": "Template not found"}
    key = _norm_region_key(region)
    regions = tpl.get("regions") or {}
    slot_imgs = (regions.get("images") or {}).get(str(slot), {})
    info = slot_imgs.pop(key, None)
    if info and info.get("path"):
        try:
            Path(info["path"]).unlink(missing_ok=True)
        except OSError:
            pass
    tpl = presets.update_template_regions(template_id, regions)
    return {"status": "ok", "template": tpl}


@app.get("/api/templates/{template_id}/region-image")
async def get_template_region_image(
    template_id: int, slot: int = 0, region: str = "tracker",
):
    """Serve a template's region image file (for the layout editor preview)."""
    from fastapi.responses import FileResponse, JSONResponse
    try:
        tpl = presets.get_template(template_id)
    except KeyError:
        return JSONResponse(status_code=404, content={"detail": "Template not found"})
    key = _norm_region_key(region)
    info = ((tpl.get("regions") or {}).get("images") or {}).get(str(slot), {}).get(key)
    path = (info or {}).get("path", "")
    if not path or not Path(path).is_file():
        return JSONResponse(status_code=404, content={"detail": "No image for this region"})
    return FileResponse(path)


@app.get("/api/templates/{template_id}")
async def get_template(template_id: int) -> dict[str, Any]:
    try:
        tpl = presets.get_template(template_id)
        # Include base64 image data (blank templates have no image)
        if tpl["image_path"]:
            img_path = Path(tpl["image_path"])
            if img_path.is_file():
                raw = await asyncio.to_thread(img_path.read_bytes)
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
    global _active_template_id
    try:
        tpl = presets.get_template(template_id)
        # Clean up image files (background + any region images)
        if tpl["image_path"]:
            img_path = Path(tpl["image_path"])
            if img_path.is_file():
                img_path.unlink()
        for slot_imgs in ((tpl.get("regions") or {}).get("images") or {}).values():
            for info in (slot_imgs or {}).values():
                path = (info or {}).get("path", "")
                if path:
                    Path(path).unlink(missing_ok=True)
    except (KeyError, OSError):
        pass
    ok = presets.delete_template(template_id)
    if _active_template_id == template_id:
        _active_template_id = None
        presets.set_setting("active_template_id", "")
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
    layout = await _template_layout_args(tpl)
    if not layout["slot_regions"] and not layout["text_entries"]:
        return {"status": "error", "error": "Template has no regions defined yet — draw some first"}

    applied = await obs.apply_template_layout(scene, **layout)
    _active_template_id = template_id
    presets.set_setting("active_template_id", str(template_id))
    await broadcast("template:applied", {
        "template_id": template_id,
        "template_name": tpl.get("name", ""),
        "num_slots": regions.get("num_slots", 2),
        "applied": applied,
    })
    return {"status": "ok", "applied": applied}


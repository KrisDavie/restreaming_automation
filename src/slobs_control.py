"""Control Layer – Streamlabs Desktop client (JSON-RPC 2.0 over WebSocket).

Implements the same public surface as :class:`~.obs_control.OBSController`
so the server can drive either application interchangeably.

Streamlabs Desktop (formerly Streamlabs OBS) exposes a JSON-RPC API on
``ws://<host>:59650/api/websocket`` (SockJS raw-websocket endpoint), enabled
in *Settings → Remote Control*; websocket clients must authenticate with the
token shown there.

Two kinds of API surface are used:

* The **documented external API** (ScenesService, SourcesService,
  AudioService, StreamingService, …) for scenes, sources, items, transforms.
* The **internal-service fallback**: the remote API forwards any resource
  not present in the external API to Streamlabs' internal services (only
  file access is blacklisted).  We use this for SourceFiltersService (video
  sync delay), AudioSource.setSettings (audio syncOffset / monitoring) and
  ProjectorService.  These are undocumented — every call is wrapped so a
  future Streamlabs release that removes them degrades into a clear error
  instead of breaking the app.

Differences from OBS handled here:

* Scenes/sources are addressed by **id**, our code uses **names** → cached
  name→id maps, rebuilt on demand.
* Scene items have **no bounds**: stretch-to-rectangle is emulated with
  ``scale = target / (source_size - crop)``.  Target rects are remembered
  per item so later crop changes keep the on-screen size.
* No screenshot API (capabilities flag hides the Scene Preview panel).
* "Expensive" calls (getPropertiesFormData, …) are rate-limited by
  Streamlabs (~2/s) — such calls are serialized with a minimum interval.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import Any

import websockets

from .config import Config
from .obs_control import OBSController, OBSRequestError, SourceCrop

log = logging.getLogger(__name__)

# Streamlabs rate-limits "expensive" API calls; keep a safety margin.
_EXPENSIVE_METHODS = {"getPropertiesFormData", "setPropertiesFormData"}
_EXPENSIVE_MIN_INTERVAL = 0.6  # seconds

_MONITORING_MAP = {
    "OBS_MONITORING_TYPE_NONE": 0,
    "OBS_MONITORING_TYPE_MONITOR_ONLY": 1,
    "OBS_MONITORING_TYPE_MONITOR_AND_OUTPUT": 2,
}


def _db_to_mul(db: float) -> float:
    return 10 ** (max(-100.0, min(26.0, db)) / 20.0) if db > -100.0 else 0.0


def _local_lan_ip() -> str | None:
    """Best-effort primary LAN IP of this machine (no traffic is sent)."""
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        finally:
            s.close()
    except OSError:
        return None


class SlobsController:
    """Async Streamlabs Desktop JSON-RPC client (OBSController-compatible)."""

    def __init__(
        self, config: Config,
        host: str | None = None, port: int | None = None, token: str | None = None,
    ) -> None:
        self._config = config
        # Dashboard-supplied connection settings override the environment
        self._host = host or config.slobs_host
        self._port = port or config.slobs_port
        self._token = token if token is not None else config.slobs_token
        self._ws: Any = None
        self._msg_id = 0
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._recv_task: asyncio.Task[None] | None = None
        self._connected = False
        self._platform = ""
        self._extra_regions: list[str] = []
        self._kinds: set[str] = set()

        # name → id caches
        self._scene_ids: dict[str, str] = {}
        self._source_ids: dict[str, str] = {}
        # (scene_name, logical_name) → sceneItemId
        self._scene_items: dict[tuple[str, str], str] = {}
        # (scene_name, sceneItemId) → (target_w, target_h) for bounds emulation
        self._item_bounds: dict[tuple[str, str], tuple[float, float]] = {}

        self._expensive_lock = asyncio.Lock()
        self._last_expensive = 0.0

    # ------------------------------------------------------------------
    # Identity / capabilities
    # ------------------------------------------------------------------

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def platform(self) -> str:
        return self._platform

    @property
    def app(self) -> str:
        return "streamlabs"

    @property
    def capabilities(self) -> dict[str, bool]:
        return {
            "screenshot": False,           # no screenshot API exists
            "projector_geometry": False,   # projector opens, size not settable
            "app_audio_capture": self._platform == "windows",
        }

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        token = self._token
        if not token:
            raise RuntimeError(
                "No Streamlabs API token configured. In Streamlabs Desktop open "
                "Settings → Mobile → 'Third Party Connections', enable 'Allow "
                "third party connections', copy the API Token shown there and "
                "set it in this dashboard (or SLOBS_TOKEN in .env)."
            )

        # Streamlabs' websocket server may not listen on 127.0.0.1 — its
        # settings page lists the addresses it does listen on.  When the
        # configured host is loopback, also try this machine's LAN IP.
        candidates = [self._host]
        if self._host in ("127.0.0.1", "localhost"):
            lan = _local_lan_ip()
            if lan and lan not in candidates:
                candidates.append(lan)

        last_err: Exception | None = None
        for host in candidates:
            url = f"ws://{host}:{self._port}/api/websocket"
            log.info("Connecting to Streamlabs Desktop at %s …", url)
            try:
                self._ws = await websockets.connect(
                    url, max_size=2**23,
                    ping_interval=20, ping_timeout=15,
                    open_timeout=6,
                )
                self._host = host
                break
            except (OSError, asyncio.TimeoutError, websockets.WebSocketException) as exc:
                last_err = exc
                log.info("Streamlabs connect to %s failed: %s", host, exc)
        else:
            tried = ", ".join(f"{h}:{self._port}" for h in candidates)
            raise RuntimeError(
                f"Could not reach Streamlabs Desktop (tried {tried}): {last_err}. "
                "In Streamlabs open Settings → Mobile → 'Third Party Connections' "
                "and make sure 'Allow third party connections' is enabled, then "
                "restart Streamlabs. If it still fails, set Host to one of the "
                "addresses shown in its 'IP Addresses' box (Streamlabs may not "
                "listen on 127.0.0.1)."
            )
        try:
            self._recv_task = asyncio.create_task(self._recv_loop())
            self._connected = True
            ok = await self.request("TcpServerService", "auth", token)
            if ok is not True:
                raise RuntimeError(
                    "Streamlabs Desktop rejected the API token. Re-copy it from "
                    "Settings → Mobile → 'Third Party Connections' (the token "
                    "changes if you click 'Generate new')."
                )
            await self._detect_platform()
        except BaseException:
            await self.disconnect()
            raise
        log.info("Connected to Streamlabs Desktop (platform: %s)", self._platform)

    async def disconnect(self) -> None:
        self._connected = False
        self._scene_ids.clear()
        self._source_ids.clear()
        self._scene_items.clear()
        self._item_bounds.clear()
        if self._recv_task:
            self._recv_task.cancel()
            self._recv_task = None
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        for rid, fut in list(self._pending.items()):
            if not fut.done():
                fut.set_exception(OBSRequestError("(pending)", -1, "Streamlabs connection closed"))
            self._pending.pop(rid, None)
        log.info("Disconnected from Streamlabs Desktop")

    async def _detect_platform(self) -> None:
        try:
            kinds = await self.request("SourcesService", "getAvailableSourcesTypesList")
            self._kinds = {k.get("value", "") for k in (kinds or []) if isinstance(k, dict)}
        except OBSRequestError:
            self._kinds = set()
        if "wasapi_output_capture" in self._kinds:
            self._platform = "windows"
        elif "coreaudio_output_capture" in self._kinds:
            self._platform = "macos"
        else:
            # Streamlabs Desktop only ships on Windows/macOS; default windows
            self._platform = "windows"

    # ------------------------------------------------------------------
    # Low-level JSON-RPC
    # ------------------------------------------------------------------

    async def request(self, resource: str, method: str, *args: Any) -> Any:
        """Send a JSON-RPC request to a Streamlabs service/helper resource."""
        if not self._connected or self._ws is None:
            raise OBSRequestError(f"{resource}.{method}", -1, "Streamlabs Desktop not connected")

        if method in _EXPENSIVE_METHODS:
            # Streamlabs meters these (~2/s) — serialize with a gap
            async with self._expensive_lock:
                wait = _EXPENSIVE_MIN_INTERVAL - (time.monotonic() - self._last_expensive)
                if wait > 0:
                    await asyncio.sleep(wait)
                self._last_expensive = time.monotonic()
                return await self._request_raw(resource, method, args)
        return await self._request_raw(resource, method, args)

    async def _request_raw(self, resource: str, method: str, args: tuple[Any, ...]) -> Any:
        self._msg_id += 1
        rid = str(self._msg_id)
        payload = {
            "jsonrpc": "2.0",
            "id": rid,
            "method": method,
            "params": {"resource": resource, "args": list(args)},
        }
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[rid] = future
        try:
            await self._ws.send(json.dumps(payload))
            msg = await asyncio.wait_for(future, timeout=15)
        except asyncio.TimeoutError:
            raise OBSRequestError(f"{resource}.{method}", -1, "request timed out after 15s")
        except websockets.WebSocketException as exc:
            self._connected = False
            raise OBSRequestError(f"{resource}.{method}", -1, f"connection error: {exc}")
        finally:
            self._pending.pop(rid, None)

        if "error" in msg and msg["error"]:
            err = msg["error"]
            code = err.get("code", -1)
            message = err.get("message", "unknown error")
            raise OBSRequestError(f"{resource}.{method}", code, str(message))
        return msg.get("result")

    async def _recv_loop(self) -> None:
        assert self._ws is not None
        try:
            async for raw in self._ws:
                try:
                    msg = json.loads(raw)
                except ValueError:
                    continue
                rid = str(msg.get("id")) if msg.get("id") is not None else None
                if rid is None:
                    continue  # event/subscription push — we poll instead
                future = self._pending.pop(rid, None)
                if future and not future.done():
                    future.set_result(msg)
        except websockets.ConnectionClosed:
            log.warning("Streamlabs WebSocket connection closed")
            self._connected = False
            for rid, fut in list(self._pending.items()):
                if not fut.done():
                    fut.set_exception(
                        OBSRequestError("(pending)", -1, "Streamlabs connection closed"))
                self._pending.pop(rid, None)

    # ------------------------------------------------------------------
    # Model helpers (name → id resolution)
    # ------------------------------------------------------------------

    @staticmethod
    def _model(obj: Any) -> dict[str, Any]:
        """Normalize a serialized helper/model into a plain dict."""
        return obj if isinstance(obj, dict) else {}

    @staticmethod
    def _id_of(obj: dict[str, Any]) -> str:
        """Extract a source/scene id from a serialized helper or model."""
        for key in ("sourceId", "id"):
            if obj.get(key):
                return str(obj[key])
        rid = obj.get("resourceId", "")
        m = re.search(r'\["([^"]+)"', rid)
        return m.group(1) if m else ""

    async def _refresh_scenes(self) -> None:
        scenes = await self.request("ScenesService", "getScenes")
        self._scene_ids = {
            s.get("name", ""): self._id_of(s) for s in (scenes or []) if isinstance(s, dict)
        }

    async def _scene_id(self, scene_name: str, create: bool = False) -> str:
        if scene_name not in self._scene_ids:
            await self._refresh_scenes()
        if scene_name not in self._scene_ids and create:
            created = await self.request("ScenesService", "createScene", scene_name)
            self._scene_ids[scene_name] = self._id_of(self._model(created))
        if scene_name not in self._scene_ids:
            raise OBSRequestError("ScenesService.getScene", 600,
                                  f"Scene '{scene_name}' not found")
        return self._scene_ids[scene_name]

    def _scene_res(self, scene_id: str) -> str:
        return f'Scene["{scene_id}"]'

    async def _find_source(self, name: str) -> dict[str, Any] | None:
        """Find a source model by name (refreshes the cache)."""
        results = await self.request("SourcesService", "getSourcesByName", name)
        for s in results or []:
            model = self._model(s)
            if model.get("name") == name:
                self._source_ids[name] = self._id_of(model)
                return model
        self._source_ids.pop(name, None)
        return None

    async def _source_id(self, name: str) -> str:
        if name in self._source_ids:
            return self._source_ids[name]
        model = await self._find_source(name)
        if not model:
            raise OBSRequestError("SourcesService.getSourcesByName", 600,
                                  f"Source '{name}' not found")
        return self._source_ids[name]

    async def _scene_items_of(self, scene_name: str) -> list[dict[str, Any]]:
        scene_id = await self._scene_id(scene_name)
        items = await self.request(self._scene_res(scene_id), "getItems")
        return [self._model(i) for i in (items or [])]

    @staticmethod
    def _item_id_of(item: dict[str, Any]) -> str:
        for key in ("sceneItemId", "nodeId", "id"):
            if item.get(key):
                return str(item[key])
        return ""

    def _item_res(self, scene_id: str, item_id: str) -> str:
        return f'SceneItem["{scene_id}","{item_id}",""]'

    async def _resolve_item(self, scene_name: str, logical_name: str) -> str:
        """Resolve a logical name (Racer1_Game, Text_txt0, …) to a sceneItemId."""
        key = (scene_name, logical_name)
        if key in self._scene_items:
            return self._scene_items[key]
        # Direct: an item whose source has this exact name (text/image sources)
        try:
            src_id = await self._source_id(logical_name)
            for item in await self._scene_items_of(scene_name):
                if item.get("sourceId") == src_id:
                    item_id = self._item_id_of(item)
                    self._scene_items[key] = item_id
                    return item_id
        except OBSRequestError:
            pass
        # Feed-derived logical names → rebuild the per-feed mapping
        if self._input_name_for(logical_name) != logical_name:
            await self.rebuild_scene_cache(scene_name)
            if key in self._scene_items:
                return self._scene_items[key]
        raise OBSRequestError("SceneItem.resolve", 600,
                              f"No scene item found for '{logical_name}'")

    # ------------------------------------------------------------------
    # Region suffixes (same model as OBSController)
    # ------------------------------------------------------------------

    def set_extra_regions(self, suffixes: list[str]) -> None:
        self._extra_regions = list(suffixes)

    def _region_suffixes(self) -> list[str]:
        return ["Game", "Tracker", "Timer"] + self._extra_regions

    def _input_name_for(self, logical_name: str) -> str:
        m = re.match(r'^(Racer\d+)_(\w+)$', logical_name)
        if m and m.group(2) in self._region_suffixes():
            return f"{m.group(1)}_Feed"
        return logical_name

    async def rebuild_scene_cache(self, scene_name: str) -> None:
        """Map each Racer{N}_Feed's scene items to logical names in node order."""
        try:
            items = await self._scene_items_of(scene_name)
        except OBSRequestError:
            return
        # sourceId → feed name lookup
        feed_ids: dict[str, str] = {}
        sources = await self.request("SourcesService", "getSources")
        for s in sources or []:
            model = self._model(s)
            name = model.get("name", "")
            if re.match(r'^Racer\d+_Feed$', name):
                feed_ids[self._id_of(model)] = name
                self._source_ids[name] = self._id_of(model)
        suffixes = self._region_suffixes()
        grouped: dict[str, list[str]] = {}
        for item in items:
            feed = feed_ids.get(str(item.get("sourceId", "")))
            if feed:
                grouped.setdefault(feed, []).append(self._item_id_of(item))
        for feed_name, ids in grouped.items():
            prefix = feed_name.replace("_Feed", "")
            for i, item_id in enumerate(ids[:len(suffixes)]):
                self._scene_items[(scene_name, f"{prefix}_{suffixes[i]}")] = item_id
        log.info("Rebuilt Streamlabs scene cache: %s", self._scene_items)

    # Compatibility alias (server historically called the underscore name)
    async def _rebuild_scene_cache(self, scene_name: str) -> None:
        await self.rebuild_scene_cache(scene_name)

    # ------------------------------------------------------------------
    # Sources
    # ------------------------------------------------------------------

    async def _create_or_update_source(
        self, name: str, kind: str, settings: dict[str, Any],
    ) -> str:
        """Create a source (unattached) or update settings; returns sourceId.

        If a source with this name exists under a different kind it is
        recreated (mirrors the OBS controller's text-kind recovery).
        """
        model = await self._find_source(name)
        if model and model.get("type") == kind:
            src_id = self._id_of(model)
            await self.request(f'Source["{src_id}"]', "updateSettings", settings)
            return src_id
        if model:
            await self.request("SourcesService", "removeSource", self._id_of(model))
            self._source_ids.pop(name, None)
        created = await self.request("SourcesService", "createSource", name, kind, settings)
        src_id = self._id_of(self._model(created))
        self._source_ids[name] = src_id
        return src_id

    async def _ensure_item(self, scene_name: str, source_name: str) -> str:
        """Ensure the named source has at least one item in the scene."""
        src_id = await self._source_id(source_name)
        for item in await self._scene_items_of(scene_name):
            if str(item.get("sourceId")) == src_id:
                return self._item_id_of(item)
        scene_id = await self._scene_id(scene_name, create=True)
        added = await self.request(self._scene_res(scene_id), "addSource", src_id)
        return self._item_id_of(self._model(added))

    async def remove_input_if_exists(self, input_name: str) -> bool:
        model = await self._find_source(input_name)
        if not model:
            return False
        await self.request("SourcesService", "removeSource", self._id_of(model))
        self._source_ids.pop(input_name, None)
        return True

    async def list_input_names(self) -> list[str]:
        sources = await self.request("SourcesService", "getSources")
        return [self._model(s).get("name", "") for s in (sources or [])]

    async def get_source_list(self) -> list[dict[str, Any]]:
        sources = await self.request("SourcesService", "getSources")
        return [
            {"inputName": self._model(s).get("name", ""),
             "inputKind": self._model(s).get("type", "")}
            for s in (sources or [])
        ]

    async def ensure_scene(self, scene_name: str) -> None:
        await self._scene_id(scene_name, create=True)

    async def get_scene_list(self) -> list[str]:
        await self._refresh_scenes()
        return list(self._scene_ids.keys())

    async def get_current_scene(self) -> str:
        active = await self.request("ScenesService", "activeScene")
        return self._model(active).get("name", "")

    async def set_scene(self, scene_name: str) -> None:
        scene_id = await self._scene_id(scene_name)
        await self.request("ScenesService", "makeSceneActive", scene_id)

    # ------------------------------------------------------------------
    # Transforms (OBS-style dicts → Streamlabs setTransform)
    # ------------------------------------------------------------------

    async def _source_dims(self, source_name: str) -> tuple[float, float]:
        try:
            model = await self._find_source(source_name)
            if model:
                return float(model.get("width") or 0), float(model.get("height") or 0)
        except OBSRequestError:
            pass
        return 0.0, 0.0

    async def _apply_transform(
        self, scene_name: str, logical_name: str, transform: dict[str, Any],
    ) -> None:
        """Translate an OBS-style transform dict and apply it.

        Handled keys: positionX/Y, cropTop/Bottom/Left/Right,
        boundsType + boundsWidth/Height (emulated via scale), scaleX/Y.
        """
        item_id = await self._resolve_item(scene_name, logical_name)
        scene_id = await self._scene_id(scene_name)
        patch: dict[str, Any] = {}

        if "positionX" in transform or "positionY" in transform:
            patch["position"] = {
                "x": float(transform.get("positionX", 0)),
                "y": float(transform.get("positionY", 0)),
            }
        crop_keys = ("cropTop", "cropBottom", "cropLeft", "cropRight")
        crop = None
        if any(k in transform for k in crop_keys):
            crop = {
                "top": int(transform.get("cropTop", 0)),
                "bottom": int(transform.get("cropBottom", 0)),
                "left": int(transform.get("cropLeft", 0)),
                "right": int(transform.get("cropRight", 0)),
            }
            patch["crop"] = crop

        bounds_type = transform.get("boundsType")
        if bounds_type == "OBS_BOUNDS_STRETCH" and transform.get("boundsWidth"):
            target_w = float(transform["boundsWidth"])
            target_h = float(transform["boundsHeight"])
            self._item_bounds[(scene_name, item_id)] = (target_w, target_h)
            scale = await self._bounds_scale(
                scene_name, item_id, logical_name, target_w, target_h, crop)
            if scale:
                patch["scale"] = scale
        elif bounds_type == "OBS_BOUNDS_NONE":
            self._item_bounds.pop((scene_name, item_id), None)
            if "scaleX" in transform:
                patch["scale"] = {
                    "x": float(transform.get("scaleX", 1.0)),
                    "y": float(transform.get("scaleY", 1.0)),
                }
        elif "scaleX" in transform:
            patch["scale"] = {
                "x": float(transform.get("scaleX", 1.0)),
                "y": float(transform.get("scaleY", 1.0)),
            }

        # Crop changed on an item with remembered bounds → keep on-screen size
        if crop is not None and "scale" not in patch:
            remembered = self._item_bounds.get((scene_name, item_id))
            if remembered:
                scale = await self._bounds_scale(
                    scene_name, item_id, logical_name, remembered[0], remembered[1], crop)
                if scale:
                    patch["scale"] = scale

        if patch:
            await self.request(self._item_res(scene_id, item_id), "setTransform", patch)

    async def _bounds_scale(
        self, scene_name: str, item_id: str, logical_name: str,
        target_w: float, target_h: float, crop: dict[str, int] | None,
    ) -> dict[str, float] | None:
        """Compute the scale that makes the (cropped) source fill the target rect.

        Streamlabs has no bounds — the source's decoded dimensions are needed.
        Returns None when they aren't known yet (media not decoding); the
        template re-apply on feed start fixes placement then.
        """
        source_name = self._input_name_for(logical_name)
        src_w, src_h = await self._source_dims(source_name)
        if not src_w or not src_h:
            log.info("No dimensions for '%s' yet — position set, scale deferred", source_name)
            return None
        if crop is None:
            crop = await self._current_crop(scene_name, item_id)
        eff_w = max(1.0, src_w - crop.get("left", 0) - crop.get("right", 0))
        eff_h = max(1.0, src_h - crop.get("top", 0) - crop.get("bottom", 0))
        return {"x": target_w / eff_w, "y": target_h / eff_h}

    async def _current_crop(self, scene_name: str, item_id: str) -> dict[str, int]:
        try:
            for item in await self._scene_items_of(scene_name):
                if self._item_id_of(item) == item_id:
                    return dict((item.get("transform") or {}).get("crop") or {})
        except OBSRequestError:
            pass
        return {}

    async def set_scene_item_transform(
        self, scene_name: str, source_name: str, transform: dict[str, Any],
    ) -> None:
        await self._apply_transform(scene_name, source_name, transform)
        log.info("Transform set for '%s' in '%s' (slobs)", source_name, scene_name)

    async def set_scene_item_enabled(
        self, scene_name: str, source_name: str, enabled: bool,
    ) -> None:
        item_id = await self._resolve_item(scene_name, source_name)
        scene_id = await self._scene_id(scene_name)
        await self.request(self._item_res(scene_id, item_id), "setVisibility", enabled)

    async def set_source_crop(
        self, source_name: str, crop: SourceCrop, scene_name: str = "Race Scene",
    ) -> None:
        await self._apply_transform(scene_name, source_name, {
            "cropTop": crop.top, "cropBottom": crop.bottom,
            "cropLeft": crop.left, "cropRight": crop.right,
        })
        await self.set_scene_item_enabled(scene_name, source_name, True)
        log.info("Applied crop to '%s' (slobs): %s", source_name, crop)

    async def ensure_source_in_scene(self, scene_name: str, source_name: str) -> bool:
        try:
            await self._resolve_item(scene_name, source_name)
            return True
        except OBSRequestError:
            return False

    # ------------------------------------------------------------------
    # Feed provisioning (mirror of OBSController.setup_full_scene)
    # ------------------------------------------------------------------

    async def setup_full_scene(
        self, scene_name: str, slot: int, input_url: str,
        canvas_width: int = 1920, canvas_height: int = 1080,
    ) -> dict[str, str]:
        await self.ensure_scene(scene_name)
        suffixes = self._region_suffixes()
        n_items = len(suffixes)
        feed_name = f"Racer{slot + 1}_Feed"
        logical_names = [f"Racer{slot + 1}_{s}" for s in suffixes]

        settings = {
            "input": input_url,
            "is_local_file": False,
            "restart_on_activate": False,
            "buffering_mb": 2,
            "reconnect_delay_sec": 2,
            "clear_on_media_end": False,
            "close_when_inactive": False,
        }
        feed_id = await self._create_or_update_source(feed_name, "ffmpeg_source", settings)

        scene_id = await self._scene_id(scene_name)
        existing = [
            self._item_id_of(i) for i in await self._scene_items_of(scene_name)
            if str(i.get("sourceId")) == feed_id
        ]
        while len(existing) > n_items:
            item_id = existing.pop()
            try:
                await self.request(self._scene_res(scene_id), "removeItem", item_id)
            except OBSRequestError:
                pass
        while len(existing) < n_items:
            added = await self.request(self._scene_res(scene_id), "addSource", feed_id)
            existing.append(self._item_id_of(self._model(added)))

        for name in logical_names:
            self._scene_items.pop((scene_name, name), None)
        for i, item_id in enumerate(existing[:n_items]):
            self._scene_items[(scene_name, logical_names[i])] = item_id

        # Default side-by-side placement; only the Game item visible
        half_w = canvas_width / 2
        for i, item_id in enumerate(existing[:n_items]):
            try:
                await self.request(self._item_res(scene_id, item_id), "setTransform", {
                    "position": {"x": float(slot * half_w), "y": 0.0},
                })
                await self.request(self._item_res(scene_id, item_id),
                                   "setVisibility", i == 0)
            except OBSRequestError as exc:
                log.warning("Default transform failed for item %s: %s", item_id, exc)

        try:
            await self.mute_input(feed_name, True)
        except OBSRequestError:
            pass

        # Streamlabs adds Desktop Audio + Mic/Aux globals to every scene
        # collection — mute them so the restream carries only racer /
        # commentary audio (unmute in the mixer if actually wanted).
        await self.mute_global_audio()

        try:
            await self.set_scene(scene_name)
        except OBSRequestError:
            pass

        log.info("Full scene setup (slobs): scene='%s' slot=%d feed='%s'",
                 scene_name, slot, feed_name)
        return {s.lower(): n for s, n in zip(suffixes, logical_names)}

    async def mute_global_audio(self) -> None:
        """Mute channel-bound global audio sources (Desktop Audio, Mic/Aux)."""
        try:
            for src in await self.request("SourcesService", "getSources") or []:
                model = self._model(src)
                if model.get("channel") is None or not model.get("audio"):
                    continue
                sid = self._id_of(model)
                try:
                    await self.request(f'AudioSource["{sid}"]', "setMuted", True)
                    log.info("Muted global audio '%s' (unmute in the mixer if needed)",
                             model.get("name"))
                except OBSRequestError:
                    pass
        except OBSRequestError:
            pass

    # ------------------------------------------------------------------
    # Text / image sources
    # ------------------------------------------------------------------

    async def text_source_kind(self) -> str:
        if "text_gdiplus" in self._kinds:
            return "text_gdiplus"
        if "text_ft2_source" in self._kinds:
            return "text_ft2_source"
        return "text_gdiplus" if self._platform == "windows" else "text_ft2_source"

    async def create_text_source(
        self, scene_name: str, source_name: str, text: str,
        *, font_size: int = 36, color_hex: str = "#ffffff",
        font_face: str = "Arial", align: str = "left",
    ) -> None:
        kind = await self.text_source_kind()
        settings = OBSController._text_settings(
            kind, text, font_size, color_hex, font_face, align)
        await self._create_or_update_source(source_name, kind, settings)
        await self._ensure_item(scene_name, source_name)
        log.info("Text source '%s' (%s, slobs) → '%s'", source_name, kind, text[:50])

    async def create_image_source(
        self, scene_name: str, source_name: str, file_path: str,
    ) -> None:
        await self._create_or_update_source(
            source_name, "image_source", {"file": file_path, "unload": False})
        await self._ensure_item(scene_name, source_name)
        log.info("Image source '%s' (slobs) → %s", source_name, file_path)

    async def move_source_to_bottom(self, scene_name: str, source_name: str) -> None:
        try:
            item_id = await self._resolve_item(scene_name, source_name)
            scene_id = await self._scene_id(scene_name)
            items = await self._scene_items_of(scene_name)
            if not items:
                return
            last = items[-1]
            last_node = str(last.get("nodeId") or self._item_id_of(last))
            if self._item_id_of(last) == item_id:
                return
            await self.request(self._item_res(scene_id, item_id), "placeAfter", last_node)
        except OBSRequestError as exc:
            log.warning("Failed to move '%s' to bottom (slobs): %s", source_name, exc)

    # ------------------------------------------------------------------
    # Template layout (mirror of OBSController.apply_template_layout)
    # ------------------------------------------------------------------

    async def apply_template_layout(
        self,
        scene_name: str,
        image_path: str | None,
        slot_regions: dict[str, dict[str, dict[str, int]]],
        text_entries: list[dict[str, Any]] | None = None,
        template_size: tuple[int, int] | None = None,
        region_images: dict[str, dict[str, str]] | None = None,
    ) -> list[str]:
        await self.ensure_scene(scene_name)
        applied: list[str] = []

        try:
            vs = await self.get_video_settings()
            canvas_w = float(vs.get("baseWidth", 1920))
            canvas_h = float(vs.get("baseHeight", 1080))
        except OBSRequestError:
            canvas_w, canvas_h = 1920.0, 1080.0
        tpl_w, tpl_h = template_size if template_size else (canvas_w, canvas_h)
        sx = canvas_w / tpl_w if tpl_w else 1.0
        sy = canvas_h / tpl_h if tpl_h else 1.0

        bg_name = "Template_Background"
        if image_path:
            await self.create_image_source(scene_name, bg_name, image_path)
            await self.move_source_to_bottom(scene_name, bg_name)
            try:
                await self.set_scene_item_transform(scene_name, bg_name, {
                    "positionX": 0.0, "positionY": 0.0,
                    "boundsType": "OBS_BOUNDS_STRETCH",
                    "boundsWidth": canvas_w, "boundsHeight": canvas_h,
                })
            except OBSRequestError as exc:
                log.warning("Failed to size background (slobs): %s", exc)
            applied.append(bg_name)
        else:
            await self.remove_input_if_exists(bg_name)

        for slot_str, regions in slot_regions.items():
            slot = int(slot_str)
            for source_type, rect in regions.items():
                if not rect:
                    continue
                src_name = f"Racer{slot + 1}_{source_type.capitalize()}"
                try:
                    if not await self.ensure_source_in_scene(scene_name, src_name):
                        log.info("Source '%s' not in scene yet — skipping placement", src_name)
                        continue
                    await self.set_scene_item_transform(scene_name, src_name, {
                        "positionX": float(rect["x"]) * sx,
                        "positionY": float(rect["y"]) * sy,
                        "boundsType": "OBS_BOUNDS_STRETCH",
                        "boundsWidth": float(rect["width"]) * sx,
                        "boundsHeight": float(rect["height"]) * sy,
                    })
                    await self.set_scene_item_enabled(scene_name, src_name, True)
                    applied.append(src_name)
                except OBSRequestError as exc:
                    log.warning("Failed to place '%s' (slobs): %s", src_name, exc)

        wanted_texts: set[str] = set()
        for entry in (text_entries or []):
            txt_name = f"Text_{entry.get('id', 'unknown')}"
            wanted_texts.add(txt_name)
            try:
                await self.create_text_source(
                    scene_name, txt_name,
                    entry.get("text", ""),
                    font_size=max(8, round(float(entry.get("font_size") or 36) * sy)),
                    color_hex=entry.get("color") or "#ffffff",
                    font_face=entry.get("font") or "Arial",
                    align=entry.get("align") or "left",
                )
                await self.set_scene_item_transform(scene_name, txt_name, {
                    "positionX": float(entry.get("x") or 0) * sx,
                    "positionY": float(entry.get("y") or 0) * sy,
                    "boundsType": "OBS_BOUNDS_NONE",
                    "scaleX": 1.0, "scaleY": 1.0,
                })
                applied.append(txt_name)
            except OBSRequestError as exc:
                log.warning("Failed to create/place text '%s' (slobs): %s", txt_name, exc)

        wanted_imgs: set[str] = set()
        for slot_str, imgs in (region_images or {}).items():
            slot = int(slot_str)
            for region, path in (imgs or {}).items():
                if not path:
                    continue
                img_name = f"TplImg_R{slot + 1}_{region}"
                wanted_imgs.add(img_name)
                rect = (slot_regions.get(slot_str) or {}).get(region)
                try:
                    await self.create_image_source(scene_name, img_name, path)
                    if rect:
                        await self.set_scene_item_transform(scene_name, img_name, {
                            "positionX": float(rect["x"]) * sx,
                            "positionY": float(rect["y"]) * sy,
                            "boundsType": "OBS_BOUNDS_STRETCH",
                            "boundsWidth": float(rect["width"]) * sx,
                            "boundsHeight": float(rect["height"]) * sy,
                        })
                        try:
                            await self.set_scene_item_enabled(
                                scene_name, f"Racer{slot + 1}_{region.capitalize()}", False)
                        except OBSRequestError:
                            pass
                    applied.append(img_name)
                except OBSRequestError as exc:
                    log.warning("Failed to place region image '%s' (slobs): %s", img_name, exc)

        # Stale text/region-image cleanup
        try:
            for name in await self.list_input_names():
                if re.match(r'^Text_txt\d+$', name) and name not in wanted_texts:
                    await self.remove_input_if_exists(name)
                if re.match(r'^TplImg_R\d+_', name) and name not in wanted_imgs:
                    await self.remove_input_if_exists(name)
        except OBSRequestError as exc:
            log.warning("Stale source cleanup failed (slobs): %s", exc)

        return applied

    # ------------------------------------------------------------------
    # Audio
    # ------------------------------------------------------------------

    def audio_capture_kind(self) -> str:
        if self._platform == "macos":
            return "coreaudio_output_capture"
        return "wasapi_output_capture"

    async def _audio_res(self, input_name: str) -> str:
        src_id = await self._source_id(input_name)
        return f'AudioSource["{src_id}"]'

    async def mute_input(self, input_name: str, muted: bool) -> None:
        await self.request(await self._audio_res(input_name), "setMuted", muted)

    async def get_input_mute(self, input_name: str) -> bool:
        model = self._model(await self.request(await self._audio_res(input_name), "getModel"))
        return bool(model.get("muted", True))

    async def set_input_volume(self, input_name: str, db: float) -> None:
        res = await self._audio_res(input_name)
        mul = _db_to_mul(db)
        try:
            # Internal-API fallback; exact dB independent of fader curve
            await self.request(res, "setMul", mul)
        except OBSRequestError as exc:
            raise OBSRequestError(
                "AudioSource.setMul", exc.code,
                f"Volume control failed — your Streamlabs version may block it ({exc.comment})")

    async def list_audio_inputs(self, scene_name: str | None = None) -> list[dict[str, Any]]:
        sources = [self._model(s) for s in await self.request("SourcesService", "getSources") or []]
        allowed_ids: set[str] | None = None
        if scene_name:
            try:
                items = await self._scene_items_of(scene_name)
                allowed_ids = {str(i.get("sourceId")) for i in items}
            except OBSRequestError:
                allowed_ids = None
        result = []
        for src in sources:
            if not src.get("audio"):
                continue
            name = src.get("name", "")
            if name.startswith(("_probe_", "_device_probe")):
                continue
            sid = self._id_of(src)
            is_global = src.get("channel") is not None
            if allowed_ids is not None and not is_global and sid not in allowed_ids:
                continue
            try:
                model = self._model(await self.request(f'AudioSource["{sid}"]', "getModel"))
                fader = model.get("fader") or {}
                result.append({
                    "name": name,
                    "kind": src.get("type", ""),
                    "volume_db": float(fader.get("db", 0.0)),
                    "volume_mul": float(fader.get("mul", 1.0)),
                    "muted": bool(model.get("muted", False)),
                })
            except OBSRequestError:
                pass
        return result

    async def set_audio_monitor_type(
        self, input_name: str, monitor_type: str = "OBS_MONITORING_TYPE_MONITOR_ONLY",
    ) -> None:
        value = _MONITORING_MAP.get(monitor_type, 1)
        res = await self._audio_res(input_name)
        try:
            # Undocumented internal setter (external API has no monitoring call)
            await self.request(res, "setSettings", {"monitoringType": value})
        except OBSRequestError as exc:
            raise OBSRequestError(
                "AudioSource.setSettings", exc.code,
                f"Monitoring not settable — your Streamlabs version may block it ({exc.comment})")

    async def create_audio_capture(
        self, scene_name: str, source_name: str,
        device_id: str = "default", window: str = "",
    ) -> None:
        if window and self._platform == "windows":
            kind, settings = "wasapi_process_output_capture", {"window": window}
        else:
            kind, settings = self.audio_capture_kind(), {"device_id": device_id}
        await self._create_or_update_source(source_name, kind, settings)
        await self._ensure_item(scene_name, source_name)
        log.info("Audio capture '%s' (%s, slobs)", source_name, kind)

    async def _probe_options(self, kind: str, prop: str) -> list[dict[str, Any]]:
        """Enumerate a property's options via an unattached probe source."""
        probe_name = f"_probe_{prop}_tmp"
        src_id = None
        try:
            created = await self.request(
                "SourcesService", "createSource", probe_name, kind, {})
            src_id = self._id_of(self._model(created))
            form = await self.request(f'Source["{src_id}"]', "getPropertiesFormData")
            for field in form or []:
                f = self._model(field)
                if f.get("name") == prop:
                    return [
                        {"itemName": self._model(o).get("description", ""),
                         "itemValue": self._model(o).get("value", "")}
                        for o in (f.get("options") or [])
                    ]
            return []
        finally:
            if src_id:
                try:
                    await self.request("SourcesService", "removeSource", src_id)
                except OBSRequestError:
                    pass
            self._source_ids.pop(probe_name, None)

    async def list_audio_devices(self, scene_name: str) -> list[dict[str, Any]]:
        return await self._probe_options(self.audio_capture_kind(), "device_id")

    async def list_audio_apps(self, scene_name: str) -> list[dict[str, Any]]:
        if self._platform != "windows":
            return []
        return await self._probe_options("wasapi_process_output_capture", "window")

    # ------------------------------------------------------------------
    # Sync (video delay via internal SourceFiltersService + audio syncOffset)
    # ------------------------------------------------------------------

    _SYNC_HINT = ("Sync uses Streamlabs' internal filter API; your Streamlabs "
                  "version may block it")

    def _delay_filter_name(self, input_name: str) -> str:
        return f"{input_name}_Delay"

    async def get_sync_offset(self, source_name: str) -> int:
        input_name = self._input_name_for(source_name)
        try:
            src_id = await self._source_id(input_name)
            filters = await self.request("SourceFiltersService", "getFilters", src_id)
            for f in filters or []:
                model = self._model(f)
                if model.get("name") == self._delay_filter_name(input_name):
                    return int((model.get("settings") or {}).get("delay_ms", 0))
        except OBSRequestError:
            pass
        return 0

    async def set_sync_offset(self, source_name: str, offset_ms: int) -> int:
        input_name = self._input_name_for(source_name)
        delay = max(0, offset_ms)
        src_id = await self._source_id(input_name)
        fname = self._delay_filter_name(input_name)
        try:
            filters = await self.request("SourceFiltersService", "getFilters", src_id)
            existing = any(self._model(f).get("name") == fname for f in filters or [])
            if existing:
                await self.request("SourceFiltersService", "setSettings",
                                   src_id, fname, {"delay_ms": delay})
            else:
                await self.request("SourceFiltersService", "add",
                                   src_id, "async_delay_filter", fname, {"delay_ms": delay})
        except OBSRequestError as exc:
            raise OBSRequestError("SourceFiltersService", exc.code,
                                  f"{self._SYNC_HINT} ({exc.comment})")
        # Audio offset in ms via internal AudioSource.setSettings
        try:
            await self.request(f'AudioSource["{src_id}"]', "setSettings",
                               {"syncOffset": delay})
        except OBSRequestError as exc:
            log.warning("Audio syncOffset failed (slobs): %s", exc)
        log.info("Sync delay for '%s' set to %d ms (slobs)", source_name, delay)
        return delay

    async def nudge_sync_offset(self, source_name: str, delta_ms: int) -> int:
        current = await self.get_sync_offset(source_name)
        new_ms = max(0, current + delta_ms)
        await self.set_sync_offset(source_name, new_ms)
        return new_ms

    # ------------------------------------------------------------------
    # Streaming / status / video / projector
    # ------------------------------------------------------------------

    async def _streaming_model(self) -> dict[str, Any]:
        return self._model(await self.request("StreamingService", "getModel"))

    async def start_streaming(self) -> None:
        model = await self._streaming_model()
        if model.get("streamingStatus") in ("live", "starting", "reconnecting"):
            return
        await self.request("StreamingService", "toggleStreaming")

    async def stop_streaming(self) -> None:
        model = await self._streaming_model()
        if model.get("streamingStatus") in ("offline", "ending"):
            return
        await self.request("StreamingService", "toggleStreaming")

    @staticmethod
    def _parse_status_time(value: Any) -> float | None:
        if not value:
            return None
        try:
            from datetime import datetime
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None

    async def get_stream_status(self) -> dict[str, Any]:
        model = await self._streaming_model()
        status = model.get("streamingStatus", "offline")
        active = status in ("live", "ending")
        duration_ms = 0
        started = self._parse_status_time(model.get("streamingStatusTime"))
        if active and started:
            duration_ms = max(0, int((time.time() - started) * 1000))
        secs = duration_ms // 1000
        timecode = f"{secs // 3600:02d}:{(secs % 3600) // 60:02d}:{secs % 60:02d}"
        dropped = total = 0
        try:
            perf = self._model(await self.request("PerformanceService", "getModel"))
            dropped = int(perf.get("numberDroppedFrames", 0))
        except OBSRequestError:
            pass
        return {
            "active": active,
            "reconnecting": status == "reconnecting",
            "timecode": timecode,
            "duration_ms": duration_ms,
            "congestion": 0.0,
            "dropped_frames": dropped,
            "total_frames": total,
        }

    async def get_video_settings(self) -> dict[str, Any]:
        # NOTE: VideoSettingsService.contexts serializes to empty dicts over
        # JSON-RPC (native objects) — verified live. 'state' carries plain
        # dicts with the real values; 'baseResolutions' is the fallback.
        for method in ("state", "baseResolutions"):
            try:
                data = self._model(await self.request("VideoSettingsService", method))
                ctx = self._model(data.get("horizontal")
                                  or next(iter(data.values()), {}))
                if ctx.get("baseWidth"):
                    return {
                        "baseWidth": ctx.get("baseWidth", 1920),
                        "baseHeight": ctx.get("baseHeight", 1080),
                        "outputWidth": ctx.get("outputWidth", ctx.get("baseWidth", 1920)),
                        "outputHeight": ctx.get("outputHeight", ctx.get("baseHeight", 1080)),
                    }
            except OBSRequestError as exc:
                log.warning("VideoSettingsService.%s unavailable (slobs): %s", method, exc)
        log.warning("Could not read Streamlabs canvas resolution — assuming 1920x1080")
        return {"baseWidth": 1920, "baseHeight": 1080,
                "outputWidth": 1920, "outputHeight": 1080}

    async def open_projector(
        self, scene_name: str, monitor: int = -1, width: int = 0, height: int = 0,
    ) -> None:
        """Open a projector of the main output (undocumented internal API).

        Streamlabs' projector has no size/monitor parameters — the user can
        resize the window it opens.
        """
        try:
            await self.request("ProjectorService", "createProjector", 0)
        except OBSRequestError as exc:
            raise OBSRequestError(
                "ProjectorService.createProjector", exc.code,
                f"Projector not available — your Streamlabs version may block it ({exc.comment})")

    async def get_scene_screenshot(
        self, scene_name: str, width: int = 1280, height: int = 720,
        fmt: str = "jpg", quality: int = 75,
    ) -> str:
        raise OBSRequestError(
            "GetSourceScreenshot", -1,
            "Streamlabs Desktop has no screenshot API — Scene Preview is OBS-only")

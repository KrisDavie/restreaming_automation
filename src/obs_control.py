"""Control Layer – OBS WebSocket client for remote scene/source manipulation.

Async client for the OBS WebSocket v5 protocol (using the ``websockets``
library) behind a clean async API used by the rest of the system to:
  • Set crop filters on sources
  • Adjust sync offsets (network buffer)
  • Switch scenes
  • Start / stop streaming
  • Create and configure media sources for ingest feeds
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import re
import struct
from dataclasses import dataclass
from typing import Any

import websockets
from websockets.client import WebSocketClientProtocol

from .config import Config

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SourceCrop:
    top: int = 0
    bottom: int = 0
    left: int = 0
    right: int = 0

    def to_dict(self) -> dict[str, int]:
        return {"cropTop": self.top, "cropBottom": self.bottom,
                "cropLeft": self.left, "cropRight": self.right}


# ---------------------------------------------------------------------------
# OBS Controller
# ---------------------------------------------------------------------------

class OBSController:
    """Async OBS WebSocket v5 client."""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._ws: WebSocketClientProtocol | None = None
        self._msg_id = 0
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._recv_task: asyncio.Task[None] | None = None
        self._connected = False
        # (scene_name, logical_name) → sceneItemId
        self._scene_items: dict[tuple[str, str], int] = {}
        self._obs_platform: str = ""  # "windows", "macos", or "linux"
        self._text_kind: str | None = None  # resolved text-source input kind
        # Extra per-racer region suffixes beyond Game/Tracker/Timer
        # (user-defined, e.g. "Deaths"); set by the server layer.
        self._extra_regions: list[str] = []

    def set_extra_regions(self, suffixes: list[str]) -> None:
        """Set user-defined region suffixes (capitalized, e.g. ['Deaths'])."""
        self._extra_regions = list(suffixes)

    def _region_suffixes(self) -> list[str]:
        return ["Game", "Tracker", "Timer"] + self._extra_regions

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        url = self._config.obs_ws_url
        log.info("Connecting to OBS WebSocket at %s …", url)
        self._ws = await websockets.connect(
            url, max_size=2**22,
            # Protocol-level pings detect half-open connections (OBS killed,
            # network drop) so _connected flips to False instead of hanging.
            ping_interval=20,
            ping_timeout=15,
        )

        # The OBS-ws v5 handshake: wait for Hello, send Identify
        # NOTE: _recv_loop must NOT run yet — it would steal these messages.
        try:
            hello = await self._read_message()
            if hello.get("op") != 0:
                raise RuntimeError(f"Unexpected OBS hello: {hello}")

            identify_payload: dict[str, Any] = {"rpcVersion": 1}
            # v5 uses authentication challenge – check if OBS requires it
            auth = hello.get("d", {}).get("authentication")
            if auth:
                password = self._config.obs_ws_password
                if not password:
                    raise RuntimeError(
                        "OBS requires authentication but OBS_WS_PASSWORD is not set. "
                        "Set it in your .env file or environment."
                    )
                secret = base64.b64encode(
                    hashlib.sha256(
                        (password + auth["salt"]).encode()
                    ).digest()
                ).decode()
                auth_response = base64.b64encode(
                    hashlib.sha256(
                        (secret + auth["challenge"]).encode()
                    ).digest()
                ).decode()
                identify_payload["authentication"] = auth_response

            await self._send({"op": 1, "d": identify_payload})
            identified = await self._read_message()
            if identified.get("op") != 2:
                raise RuntimeError(f"OBS Identify failed: {identified}")
        except BaseException:
            # Don't leak a half-open socket on handshake/auth failure
            await self._ws.close()
            self._ws = None
            raise

        # Handshake done — now start the background recv loop for requests
        self._recv_task = asyncio.create_task(self._recv_loop())
        self._connected = True
        self._text_kind = None
        log.info("Connected to OBS WebSocket (negotiated rpcVersion 1)")

        # Detect OBS host platform for source-kind selection
        try:
            ver = await self.request("GetVersion", {})
            plat = ver.get("platform", "").lower()
            if "win" in plat:
                self._obs_platform = "windows"
            elif "mac" in plat or "darwin" in plat:
                self._obs_platform = "macos"
            else:
                self._obs_platform = "linux"
            log.info("OBS platform detected: %s (raw: %s)",
                     self._obs_platform, ver.get("platform", "?"))
        except Exception:
            self._obs_platform = "linux"
            log.warning("Could not detect OBS platform, defaulting to linux")

    async def disconnect(self) -> None:
        self._connected = False
        self._scene_items.clear()
        self._text_kind = None
        if self._recv_task:
            self._recv_task.cancel()
        if self._ws:
            await self._ws.close()
        self._fail_pending("OBS connection closed")
        log.info("Disconnected from OBS WebSocket")

    def _fail_pending(self, reason: str) -> None:
        """Fail all in-flight requests so callers error fast instead of timing out."""
        for rid, future in list(self._pending.items()):
            if not future.done():
                future.set_exception(OBSRequestError("(pending)", -1, reason))
            self._pending.pop(rid, None)

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def platform(self) -> str:
        """Return the detected OBS host platform ('windows', 'macos', 'linux')."""
        return self._obs_platform

    @property
    def app(self) -> str:
        """Which streaming application this controller drives."""
        return "obs"

    @property
    def capabilities(self) -> dict[str, bool]:
        """Feature flags the dashboard uses to show/hide panels."""
        return {
            "screenshot": True,
            "projector_geometry": True,
            "app_audio_capture": self._obs_platform == "windows",
        }

    async def rebuild_scene_cache(self, scene_name: str) -> None:
        """Public alias for scene-item cache rebuilds (see _rebuild_scene_cache)."""
        await self._rebuild_scene_cache(scene_name)

    async def list_input_names(self) -> list[str]:
        """Names of every input the app knows about."""
        resp = await self.request("GetInputList", {})
        return [i.get("inputName", "") for i in resp.get("inputs", [])]

    async def _probe_property_items(
        self, scene_name: str, kind: str, prop: str,
    ) -> list[dict[str, Any]]:
        """List a source property's options via a hidden temporary input."""
        probe_name = f"_probe_{prop}_tmp"
        try:
            try:
                await self.request("CreateInput", {
                    "sceneName": scene_name,
                    "inputName": probe_name,
                    "inputKind": kind,
                    "inputSettings": {},
                    "sceneItemEnabled": False,
                })
            except OBSRequestError:
                pass  # may already exist from a previous failed cleanup
            resp = await self.request("GetInputPropertiesListPropertyItems", {
                "inputName": probe_name,
                "propertyName": prop,
            })
            return resp.get("propertyItems", [])
        finally:
            try:
                await self.request("RemoveInput", {"inputName": probe_name})
            except OBSRequestError:
                pass

    async def list_audio_devices(self, scene_name: str) -> list[dict[str, Any]]:
        """Audio output-capture devices as [{itemName, itemValue}]."""
        return await self._probe_property_items(
            scene_name, self.audio_capture_kind(), "device_id")

    async def list_audio_apps(self, scene_name: str) -> list[dict[str, Any]]:
        """Capturable application windows (Windows only) as [{itemName, itemValue}]."""
        if self._obs_platform != "windows":
            return []
        return await self._probe_property_items(
            scene_name, "wasapi_process_output_capture", "window")

    # ------------------------------------------------------------------
    # Scene-item helpers (single-input, multi-item architecture)
    # ------------------------------------------------------------------

    async def _resolve_item_id(
        self, scene_name: str, logical_name: str,
    ) -> int:
        """Resolve a logical source name (e.g. Racer1_Game) to a sceneItemId.

        Checks the in-memory cache first, falls back to OBS GetSceneItemId
        (which works for legacy single-input sources), and finally scans the
        scene-item list for matching Feed-based items.
        """
        key = (scene_name, logical_name)
        if key in self._scene_items:
            return self._scene_items[key]
        # Direct lookup (works for legacy single-input sources)
        try:
            resp = await self.request("GetSceneItemId", {
                "sceneName": scene_name,
                "sourceName": logical_name,
            })
            item_id = resp["sceneItemId"]
            self._scene_items[key] = item_id
            return item_id
        except OBSRequestError:
            pass
        # Not found – try to rebuild the cache from the scene
        feed_input = self._input_name_for(logical_name)
        if feed_input != logical_name:
            await self._rebuild_scene_cache(scene_name)
            if key in self._scene_items:
                return self._scene_items[key]
        raise OBSRequestError("GetSceneItemId", 600,
                              f"No cached or discoverable item for '{logical_name}'")

    async def _item_request(
        self, request_type: str, scene_name: str, source_name: str,
        extra: dict[str, Any],
    ) -> None:
        """Send a scene-item request, retrying once if the cached item id is
        stale (e.g. the user deleted/re-added the item in OBS directly)."""
        try:
            item_id = await self._resolve_item_id(scene_name, source_name)
            await self.request(request_type, {
                "sceneName": scene_name, "sceneItemId": item_id, **extra,
            })
            return
        except OBSRequestError:
            self._scene_items.pop((scene_name, source_name), None)
        await self._rebuild_scene_cache(scene_name)
        item_id = await self._resolve_item_id(scene_name, source_name)
        await self.request(request_type, {
            "sceneName": scene_name, "sceneItemId": item_id, **extra,
        })

    def _input_name_for(self, logical_name: str) -> str:
        """Translate a logical name (Racer1_Game, Racer1_<Custom>) to the real
        OBS input (Racer1_Feed)."""
        m = re.match(r'^(Racer\d+)_(\w+)$', logical_name)
        if m and m.group(2) in self._region_suffixes():
            return f"{m.group(1)}_Feed"
        return logical_name

    async def _get_feed_scene_items(
        self, scene_name: str, feed_name: str,
    ) -> list[int]:
        """Return sceneItemIds for all scene items referencing *feed_name*."""
        try:
            resp = await self.request("GetSceneItemList", {"sceneName": scene_name})
            return [
                item["sceneItemId"]
                for item in resp.get("sceneItems", [])
                if item.get("sourceName") == feed_name
            ]
        except OBSRequestError:
            return []

    async def _remove_feed_items(
        self, scene_name: str, source_name: str,
    ) -> None:
        """Remove all scene items referencing *source_name* (for idempotent setup)."""
        for item_id in await self._get_feed_scene_items(scene_name, source_name):
            try:
                await self.request("RemoveSceneItem", {
                    "sceneName": scene_name,
                    "sceneItemId": item_id,
                })
            except OBSRequestError:
                pass
        # Clear cached IDs for this feed
        prefix = source_name.replace("_Feed", "")
        stale = [k for k in self._scene_items
                 if k[0] == scene_name and k[1].startswith(prefix)]
        for k in stale:
            del self._scene_items[k]

    async def _rebuild_scene_cache(self, scene_name: str) -> None:
        """Scan the scene and rebuild _scene_items for all Feed-based items.

        Each Racer{N}_Feed input has one scene item per region suffix
        (Game / Tracker / Timer / user-defined extras), assigned in the
        order they appear (lowest index first).

        Also removes legacy separate inputs (Racer{N}_Game/Tracker/Timer as
        standalone ``ffmpeg_source`` inputs) that conflict with the new
        single-Feed architecture.
        """
        # --- Remove legacy separate-input sources that are actual inputs ---
        try:
            input_resp = await self.request("GetInputList", {})
            for inp in input_resp.get("inputs", []):
                name = inp.get("inputName", "")
                if re.match(r'^Racer\d+_(Game|Tracker|Timer)$', name):
                    try:
                        resp = await self.request("RemoveInput", {"inputName": name})
                        log.info("Removed legacy input '%s' → resp=%s", name, resp)
                    except OBSRequestError as exc:
                        log.warning("Failed to remove legacy input '%s': %s", name, exc)
        except OBSRequestError as exc:
            log.warning("Failed to get input list for legacy cleanup: %s", exc)

        # --- Scan scene items and map Feed items to logical names ---
        try:
            resp = await self.request("GetSceneItemList", {"sceneName": scene_name})
        except OBSRequestError:
            return
        # Group items by feed name
        feeds: dict[str, list[int]] = {}
        for item in resp.get("sceneItems", []):
            name = item.get("sourceName", "")
            if re.match(r'^Racer\d+_Feed$', name):
                feeds.setdefault(name, []).append(item["sceneItemId"])
        # Assign logical names
        suffixes = self._region_suffixes()
        for feed_name, ids in feeds.items():
            prefix = feed_name.replace("_Feed", "")
            ids.sort()  # deterministic order
            for i, sid in enumerate(ids[:len(suffixes)]):
                logical = f"{prefix}_{suffixes[i]}"
                self._scene_items[(scene_name, logical)] = sid
        log.info("Rebuilt scene cache: %s", self._scene_items)

    # ------------------------------------------------------------------
    # High-level commands
    # ------------------------------------------------------------------

    async def set_source_crop(
        self, source_name: str, crop: SourceCrop, scene_name: str = "Race Scene",
    ) -> None:
        """Apply crop via the scene-item transform (more reliable than a filter).

        Also un-hides the source if it was previously hidden (tracker/timer).
        """
        try:
            await self._item_request("SetSceneItemTransform", scene_name, source_name, {
                "sceneItemTransform": {
                    "cropTop": crop.top,
                    "cropBottom": crop.bottom,
                    "cropLeft": crop.left,
                    "cropRight": crop.right,
                },
            })
            # Make sure the source is visible after cropping
            await self._item_request("SetSceneItemEnabled", scene_name, source_name, {
                "sceneItemEnabled": True,
            })
            log.info("Applied scene-item crop to '%s': %s", source_name, crop)
        except OBSRequestError as exc:
            log.warning("Failed to apply crop to '%s': %s", source_name, exc)
            raise

    async def get_sync_offset(self, source_name: str) -> int:
        """Return current sync delay in ms for a source (from video delay filter)."""
        input_name = self._input_name_for(source_name)
        filter_name = f"{input_name}_Delay"
        try:
            resp = await self.request("GetSourceFilter", {
                "sourceName": input_name,
                "filterName": filter_name,
            })
            return resp.get("filterSettings", {}).get("delay_ms", 0)
        except OBSRequestError:
            return 0

    # obs-websocket caps the audio sync offset at 20 s; keep video and audio
    # delay matched by clamping both to it (OBS does NOT auto-delay audio to
    # a video-delay filter, so an unmatched pair desyncs).
    MAX_SYNC_MS = 20_000

    async def set_sync_offset(self, source_name: str, offset_ms: int) -> int:
        """Delay a racer feed's video AND audio by the same amount, live.

        Video is delayed with an OBS *Video Delay (Async)* filter; audio with
        ``SetInputAudioSyncOffset`` (both in ms).  A delay of 0 removes the
        filter and zeroes the offset.  Returns the applied offset in ms.
        """
        input_name = self._input_name_for(source_name)
        delay = max(0, min(int(offset_ms), self.MAX_SYNC_MS))
        filter_name = f"{input_name}_Delay"

        if delay <= 0:
            # Remove the video delay filter entirely (a lingering 0-delay
            # filter is harmless but we keep the scene clean)
            try:
                await self.request("RemoveSourceFilter", {
                    "sourceName": input_name, "filterName": filter_name,
                })
            except OBSRequestError:
                pass
        else:
            try:
                await self.request("GetSourceFilter", {
                    "sourceName": input_name, "filterName": filter_name,
                })
                await self.request("SetSourceFilterSettings", {
                    "sourceName": input_name,
                    "filterName": filter_name,
                    "filterSettings": {"delay_ms": delay},
                })
            except OBSRequestError:
                await self.request("CreateSourceFilter", {
                    "sourceName": input_name,
                    "filterName": filter_name,
                    "filterKind": "async_delay_filter",
                    "filterSettings": {"delay_ms": delay},
                })

        # Matching audio delay (ms). OBS won't auto-delay audio to the filter.
        await self.request("SetInputAudioSyncOffset", {
            "inputName": input_name,
            "inputAudioSyncOffset": delay,
        })
        log.info("Sync delay for '%s' set to %d ms", source_name, delay)
        return delay

    async def clear_app_delay(self, source_name: str) -> None:
        """Remove any OBS-side delay (video filter + audio offset).

        Race sync is done in the ingest relay now; this clears residue from
        earlier filter/offset-based versions so it can't double-delay.
        """
        input_name = self._input_name_for(source_name)
        try:
            await self.request("RemoveSourceFilter", {
                "sourceName": input_name, "filterName": f"{input_name}_Delay",
            })
        except OBSRequestError:
            pass
        try:
            await self.request("SetInputAudioSyncOffset", {
                "inputName": input_name, "inputAudioSyncOffset": 0,
            })
        except OBSRequestError:
            pass

    async def restart_media_source(self, source_name: str) -> None:
        """Reopen a media input so it re-reads its (relay-delayed) stream and
        adopts the current delay.  Re-applying the input settings makes the
        ffmpeg source reconnect."""
        input_name = self._input_name_for(source_name)
        try:
            cur = await self.request("GetInputSettings", {"inputName": input_name})
            await self.request("SetInputSettings", {
                "inputName": input_name,
                "inputSettings": cur.get("inputSettings", {}),
                "overlay": True,
            })
        except OBSRequestError as exc:
            log.warning("restart_media_source('%s') failed: %s", input_name, exc)

    async def nudge_sync_offset(self, source_name: str, delta_ms: int) -> int:
        """Increment/decrement the sync delay by *delta_ms* ms.

        Returns the new offset in ms.
        """
        current = await self.get_sync_offset(source_name)
        new_ms = max(0, current + delta_ms)
        await self.set_sync_offset(source_name, new_ms)
        log.info("Nudged sync for '%s': %+d ms → %d ms total",
                 source_name, delta_ms, new_ms)
        return new_ms

    async def create_media_source(
        self, scene_name: str, source_name: str, input_url: str,
    ) -> dict[str, Any]:
        """Create (or update) an ffmpeg/Media Source pointing at *input_url*.

        Returns ``{"created": True/False, "sceneItemId": int|None}``.
        ``sceneItemId`` is set only when the input was newly created (OBS
        adds an initial scene item automatically via CreateInput).
        """
        settings = {
            "input": input_url,
            "is_local_file": False,
            "restart_on_activate": False,
            "buffering_mb": 2,
            "reconnect_delay_sec": 2,
            "clear_on_media_end": False,
            "close_when_inactive": False,
        }
        try:
            await self.request("GetInputSettings", {"inputName": source_name})
            # Already exists → update settings (does NOT disrupt the stream)
            await self.request("SetInputSettings", {
                "inputName": source_name,
                "inputSettings": settings,
            })
            log.info("Media source '%s' updated → %s", source_name, input_url)
            return {"created": False, "sceneItemId": None}
        except OBSRequestError:
            resp = await self.request("CreateInput", {
                "sceneName": scene_name,
                "inputName": source_name,
                "inputKind": "ffmpeg_source",
                "inputSettings": settings,
                "sceneItemEnabled": True,
            })
            log.info("Media source '%s' created → %s", source_name, input_url)
            return {"created": True, "sceneItemId": resp.get("sceneItemId")}

    async def set_scene(self, scene_name: str) -> None:
        await self.request("SetCurrentProgramScene", {"sceneName": scene_name})

    async def start_streaming(self) -> None:
        await self.request("StartStream", {})

    async def stop_streaming(self) -> None:
        await self.request("StopStream", {})

    async def get_scene_list(self) -> list[str]:
        resp = await self.request("GetSceneList", {})
        return [s["sceneName"] for s in resp.get("scenes", [])]

    async def get_source_list(self) -> list[dict[str, Any]]:
        resp = await self.request("GetInputList", {})
        return resp.get("inputs", [])

    async def mute_input(self, input_name: str, muted: bool) -> None:
        """Mute or unmute an input source."""
        await self.request("SetInputMute", {
            "inputName": input_name,
            "inputMuted": muted,
        })
        log.info("Set mute for '%s' → %s", input_name, muted)

    async def get_input_mute(self, input_name: str) -> bool:
        resp = await self.request("GetInputMute", {"inputName": input_name})
        return resp.get("inputMuted", True)

    async def set_input_volume(self, input_name: str, db: float) -> None:
        """Set the volume of an input in decibels (0 = unity, -100 = silence)."""
        await self.request("SetInputVolume", {
            "inputName": input_name,
            "inputVolumeDb": max(-100.0, min(26.0, db)),
        })
        log.info("Volume for '%s' set to %.1f dB", input_name, db)

    async def get_input_volume(self, input_name: str) -> dict[str, float]:
        """Return ``{"db": float, "mul": float}`` for an input."""
        resp = await self.request("GetInputVolume", {"inputName": input_name})
        return {
            "db": resp.get("inputVolumeDb", 0.0),
            "mul": resp.get("inputVolumeMul", 1.0),
        }

    def audio_capture_kind(self) -> str:
        """Return the OBS input kind for audio output capture on the current platform."""
        if self._obs_platform == "windows":
            return "wasapi_output_capture"
        if self._obs_platform == "macos":
            return "coreaudio_output_capture"
        return "pulse_output_capture"

    async def create_audio_capture(self, scene_name: str, source_name: str,
                                   device_id: str = "default",
                                   window: str = "") -> None:
        """Create or update an audio capture source.

        If *window* is provided **and** the OBS host is Windows, an
        Application Audio Capture source (``wasapi_process_output_capture``)
        is created that captures only the named application's audio.
        Otherwise falls back to the platform-appropriate device capture.
        """
        if window and self._obs_platform == "windows":
            kind = "wasapi_process_output_capture"
            settings: dict[str, Any] = {"window": window}
        else:
            kind = self.audio_capture_kind()
            settings = {"device_id": device_id}
        try:
            await self.request("GetInputSettings", {"inputName": source_name})
            await self.request("SetInputSettings", {
                "inputName": source_name,
                "inputSettings": settings,
            })
        except OBSRequestError:
            await self.request("CreateInput", {
                "sceneName": scene_name,
                "inputName": source_name,
                "inputKind": kind,
                "inputSettings": settings,
                "sceneItemEnabled": True,
            })
        log.info("Audio capture '%s' (%s) → %s", source_name, kind,
                 window if window else f"device '{device_id}'")

    _SILENT_KIND_PREFIXES = (
        "image_source", "text_ft2_source", "text_gdiplus", "browser_source",
        "color_source", "slideshow", "scene",
    )

    async def get_current_scene(self) -> str:
        """Return the name of the current program scene."""
        resp = await self.request("GetCurrentProgramScene", {})
        return resp.get("currentProgramSceneName") or resp.get("sceneName", "")

    async def _scene_source_names(self, scene_name: str) -> set[str]:
        """Return the source names present in a scene, recursing into groups
        and nested scenes one level deep."""
        names: set[str] = set()
        try:
            resp = await self.request("GetSceneItemList", {"sceneName": scene_name})
        except OBSRequestError:
            return names
        for item in resp.get("sceneItems", []):
            name = item.get("sourceName", "")
            names.add(name)
            if item.get("isGroup") or item.get("sourceType") == "OBS_SOURCE_TYPE_SCENE":
                try:
                    req = ("GetGroupSceneItemList" if item.get("isGroup")
                           else "GetSceneItemList")
                    sub = await self.request(req, {"sceneName": name})
                    for s in sub.get("sceneItems", []):
                        names.add(s.get("sourceName", ""))
                except OBSRequestError:
                    pass
        return names

    async def list_audio_inputs(self, scene_name: str | None = None) -> list[dict[str, Any]]:
        """Return info for OBS inputs that produce audio.

        When *scene_name* is given, only inputs that are part of that scene
        (plus OBS global audio inputs like Desktop Audio / Mic) are returned —
        this keeps the mixer manageable for users with large OBS setups.
        """
        resp = await self.request("GetInputList", {})
        inputs = resp.get("inputs", [])

        allowed: set[str] | None = None
        if scene_name:
            allowed = await self._scene_source_names(scene_name)
            # Global audio inputs (Desktop Audio, Mic/Aux, …) are always live
            try:
                specials = await self.request("GetSpecialInputs", {})
                allowed |= {v for v in specials.values() if v}
            except OBSRequestError:
                pass

        candidates = []
        for inp in inputs:
            kind = inp.get("inputKind") or ""
            name = inp.get("inputName", "")
            if kind.startswith(self._SILENT_KIND_PREFIXES):
                continue
            if name.startswith("_device_probe"):
                continue  # our own temporary probe source
            if allowed is not None and name not in allowed:
                continue
            candidates.append((name, kind))

        async def _query(name: str, kind: str) -> dict[str, Any] | None:
            try:
                vol = await self.get_input_volume(name)
                muted = await self.get_input_mute(name)
                return {
                    "name": name,
                    "kind": kind,
                    "volume_db": vol["db"],
                    "volume_mul": vol["mul"],
                    "muted": muted,
                }
            except OBSRequestError:
                return None

        results = await asyncio.gather(*(_query(n, k) for n, k in candidates))
        return [r for r in results if r is not None]

    async def get_stream_status(self) -> dict[str, Any]:
        """Return streaming state: active flag, timecode, and stats."""
        resp = await self.request("GetStreamStatus", {})
        return {
            "active": resp.get("outputActive", False),
            "reconnecting": resp.get("outputReconnecting", False),
            "timecode": resp.get("outputTimecode", "00:00:00"),
            "duration_ms": resp.get("outputDuration", 0),
            "congestion": resp.get("outputCongestion", 0.0),
            "dropped_frames": resp.get("outputSkippedFrames", 0),
            "total_frames": resp.get("outputTotalFrames", 0),
        }

    async def ensure_scene(self, scene_name: str) -> None:
        """Create a scene if it doesn't already exist."""
        scenes = await self.get_scene_list()
        if scene_name not in scenes:
            await self.request("CreateScene", {"sceneName": scene_name})
            log.info("Created scene '%s'", scene_name)

    async def set_scene_item_transform(
        self, scene_name: str, source_name: str, transform: dict[str, Any]
    ) -> None:
        """Set transform (position, size, crop) for a scene item."""
        await self._item_request("SetSceneItemTransform", scene_name, source_name, {
            "sceneItemTransform": transform,
        })
        log.info("Transform set for '%s' in '%s'", source_name, scene_name)

    async def set_scene_item_enabled(
        self, scene_name: str, source_name: str, enabled: bool
    ) -> None:
        """Show or hide a scene item."""
        await self._item_request("SetSceneItemEnabled", scene_name, source_name, {
            "sceneItemEnabled": enabled,
        })

    async def setup_racer_source(
        self,
        scene_name: str,
        slot: int,
        input_url: str,
        *,
        position_x: float = 0,
        position_y: float = 0,
        width: float = 960,
        height: float = 540,
    ) -> str:
        """Create (or update) a media source for a racer and position it.

        Returns the source name.
        """
        source_name = f"Racer{slot + 1}_Game"
        await self.ensure_scene(scene_name)
        await self.create_media_source(scene_name, source_name, input_url)

        # Set position and bounds
        try:
            await self.set_scene_item_transform(scene_name, source_name, {
                "positionX": position_x,
                "positionY": position_y,
                "boundsType": "OBS_BOUNDS_STRETCH",
                "boundsWidth": width,
                "boundsHeight": height,
            })
        except OBSRequestError as exc:
            log.warning("Failed to set transform for '%s': %s", source_name, exc)

        # Mute by default (user switches audio explicitly)
        try:
            await self.mute_input(source_name, True)
        except OBSRequestError:
            pass

        # Switch to the scene so the user can see it
        try:
            await self.set_scene(scene_name)
        except OBSRequestError:
            pass

        return source_name

    async def setup_full_scene(
        self,
        scene_name: str,
        slot: int,
        input_url: str,
        canvas_width: int = 1920,
        canvas_height: int = 1080,
    ) -> dict[str, str]:
        """Fully provision OBS for a racer slot.

        Creates a *single* media input (``Racer{N}_Feed``) and adds one
        scene-item reference to it per region (Game / Tracker / Timer plus
        any user-defined custom regions).  Each scene item can have its own
        independent transform & crop, but all display the same decoded video
        feed — no UDP port conflicts.

        The method is idempotent: calling it again for the same slot updates
        the feed URL and ensures the right number of scene items exist
        without tearing down the connection (preserves the UDP stream).
        """
        await self.ensure_scene(scene_name)

        suffixes = self._region_suffixes()
        n_items = len(suffixes)
        feed_name = f"Racer{slot + 1}_Feed"
        logical_names = [f"Racer{slot + 1}_{s}" for s in suffixes]

        # 1. Clean up legacy separate-input sources from old architecture
        for legacy in logical_names[:3]:
            try:
                await self.request("RemoveInput", {"inputName": legacy})
                log.info("Removed legacy input '%s'", legacy)
            except OBSRequestError:
                pass

        # 2. Create or update the single media input.
        #    (The server re-applies the slot's race-sync delay after
        #    provisioning via set_sync_offset — 0 for a fresh stream, which
        #    clears any leftover filter/offset.)
        await self.create_media_source(scene_name, feed_name, input_url)

        # 3. Count existing scene items for this feed
        existing = await self._get_feed_scene_items(scene_name, feed_name)
        existing.sort()

        # 4. Trim excess items (newest first, so base regions keep their ids)
        while len(existing) > n_items:
            try:
                await self.request("RemoveSceneItem", {
                    "sceneName": scene_name,
                    "sceneItemId": existing.pop(),
                })
            except OBSRequestError:
                existing.pop()  # drop from list anyway

        # 5. Add items until we have exactly one per region
        while len(existing) < n_items:
            try:
                resp = await self.request("CreateSceneItem", {
                    "sceneName": scene_name,
                    "sourceName": feed_name,
                    "sceneItemEnabled": True,
                })
                existing.append(resp["sceneItemId"])
            except OBSRequestError as exc:
                log.warning("CreateSceneItem failed for '%s': %s", feed_name, exc)
                break

        # 6. Map logical names → scene-item IDs (stable sort for determinism)
        existing.sort()
        for name in logical_names:
            self._scene_items.pop((scene_name, name), None)
        for i, sid in enumerate(existing[:n_items]):
            self._scene_items[(scene_name, logical_names[i])] = sid

        # 7. Set default transforms (side-by-side halves)
        half_w = canvas_width / 2
        pos_x = slot * half_w
        for i, sid in enumerate(existing[:n_items]):
            try:
                await self.request("SetSceneItemTransform", {
                    "sceneName": scene_name,
                    "sceneItemId": sid,
                    "sceneItemTransform": {
                        "positionX": float(pos_x),
                        "positionY": 0.0,
                        "boundsType": "OBS_BOUNDS_STRETCH",
                        "boundsWidth": float(half_w),
                        "boundsHeight": float(canvas_height),
                    },
                })
                # Hide tracker/timer/custom items by default (user enables
                # them via crop/template)
                await self.request("SetSceneItemEnabled", {
                    "sceneName": scene_name,
                    "sceneItemId": sid,
                    "sceneItemEnabled": (i == 0),  # only Game visible
                })
            except OBSRequestError as exc:
                log.warning("Transform setup failed for item %d: %s", sid, exc)

        # 8. Mute the input by default
        try:
            await self.mute_input(feed_name, True)
        except OBSRequestError:
            pass

        # 8b. Mute global audio (Desktop Audio / Mic) so the restream carries
        #     only racer/commentary audio — unmute in the mixer if wanted
        await self.mute_global_audio()

        # 9. Switch to the scene
        try:
            await self.set_scene(scene_name)
        except OBSRequestError:
            pass

        log.info("Full scene setup: scene='%s' slot=%d feed='%s' items=%s",
                 scene_name, slot, feed_name,
                 {n: self._scene_items.get((scene_name, n)) for n in logical_names})
        return {s.lower(): n for s, n in zip(suffixes, logical_names)}

    async def mute_global_audio(self) -> None:
        """Mute OBS's global audio inputs (Desktop Audio, Mic/Aux) if any."""
        try:
            specials = await self.request("GetSpecialInputs", {})
        except OBSRequestError:
            return
        for name in {v for v in specials.values() if v}:
            try:
                await self.mute_input(name, True)
                log.info("Muted global audio '%s' (unmute in the mixer if needed)", name)
            except OBSRequestError:
                pass

    async def ensure_source_in_scene(
        self, scene_name: str, source_name: str
    ) -> bool:
        """Check if a source exists as a scene item; return True if it does."""
        if (scene_name, source_name) in self._scene_items:
            return True
        try:
            await self.request("GetSceneItemId", {
                "sceneName": scene_name,
                "sourceName": source_name,
            })
            return True
        except OBSRequestError:
            return False

    async def create_image_source(
        self, scene_name: str, source_name: str, file_path: str,
    ) -> None:
        """Create or update an image source (for template backgrounds)."""
        settings = {"file": file_path, "unload": False}
        try:
            await self.request("GetInputSettings", {"inputName": source_name})
            await self.request("SetInputSettings", {
                "inputName": source_name,
                "inputSettings": settings,
            })
        except OBSRequestError:
            await self.request("CreateInput", {
                "sceneName": scene_name,
                "inputName": source_name,
                "inputKind": "image_source",
                "inputSettings": settings,
                "sceneItemEnabled": True,
            })
        log.info("Image source '%s' → %s", source_name, file_path)

    # Preferred text-source kinds, best first.  GDI+ variants exist only on
    # Windows; FreeType2 exists everywhere but is hidden on Windows builds.
    _TEXT_KIND_PREFERENCE = (
        "text_gdiplus_v3", "text_gdiplus_v2", "text_gdiplus",
        "text_ft2_source_v2", "text_ft2_source",
    )

    async def text_source_kind(self) -> str:
        """Resolve the best text-source input kind available in this OBS."""
        if self._text_kind:
            return self._text_kind
        try:
            resp = await self.request("GetInputKindList", {"unversioned": False})
            kinds = set(resp.get("inputKinds", []))
            for kind in self._TEXT_KIND_PREFERENCE:
                if kind in kinds:
                    self._text_kind = kind
                    break
        except OBSRequestError:
            pass
        if not self._text_kind:
            self._text_kind = ("text_gdiplus_v2" if self._obs_platform == "windows"
                               else "text_ft2_source_v2")
        log.info("Using text source kind: %s", self._text_kind)
        return self._text_kind

    @staticmethod
    def _color_to_obs(color_hex: str) -> int:
        """Convert '#rrggbb' to the ABGR-packed uint32 OBS text sources use."""
        hex_clean = color_hex.lstrip("#")
        if len(hex_clean) >= 6:
            r, g, b = int(hex_clean[0:2], 16), int(hex_clean[2:4], 16), int(hex_clean[4:6], 16)
        else:
            r, g, b = 255, 255, 255
        return 0xFF000000 | (b << 16) | (g << 8) | r

    @staticmethod
    def _text_settings(
        kind: str, text: str, font_size: int, color_hex: str,
        font_face: str = "Arial", align: str = "left",
    ) -> dict[str, Any]:
        """Build input settings for a text source of the given kind.

        The default face is Arial: present on Windows/macOS, and aliased to
        a metric-compatible font (Liberation Sans) by fontconfig on Linux —
        the same substitution browsers make, keeping the dashboard preview
        and the OBS render in step.  Multi-line text (\\n) is supported by
        both kinds; alignment is a GDI+-only setting (FreeType2 has none).
        """
        color_int = OBSController._color_to_obs(color_hex)
        settings: dict[str, Any] = {
            "text": text,
            "font": {"face": font_face or "Arial", "size": font_size, "flags": 0},
        }
        if kind.startswith("text_gdiplus"):
            settings["color"] = color_int
            settings["align"] = align if align in ("left", "center", "right") else "left"
            settings["valign"] = "top"
        else:  # FreeType2
            settings["color1"] = color_int
            settings["color2"] = color_int
            settings["custom_width"] = 0
        return settings

    async def create_text_source(
        self, scene_name: str, source_name: str, text: str,
        *, font_size: int = 36, color_hex: str = "#ffffff",
        font_face: str = "Arial", align: str = "left",
    ) -> None:
        """Create or update a text source using the platform's native text kind.

        If a source with this name exists but is of a different (possibly
        unsupported) text kind, it is recreated so Windows scenes recover
        from previously created FreeType2 sources.
        """
        kind = await self.text_source_kind()
        settings = self._text_settings(kind, text, font_size, color_hex, font_face, align)
        recreate = False
        try:
            existing = await self.request("GetInputSettings", {"inputName": source_name})
            if existing.get("inputKind") != kind:
                await self.remove_input_if_exists(source_name)
                recreate = True
            else:
                await self.request("SetInputSettings", {
                    "inputName": source_name,
                    "inputSettings": settings,
                })
        except OBSRequestError:
            recreate = True
        if recreate:
            await self.request("CreateInput", {
                "sceneName": scene_name,
                "inputName": source_name,
                "inputKind": kind,
                "inputSettings": settings,
                "sceneItemEnabled": True,
            })
        log.info("Text source '%s' (%s) → '%s'", source_name, kind, text[:50])

    async def move_source_to_bottom(
        self, scene_name: str, source_name: str,
    ) -> None:
        """Move a source to the bottom of the scene (behind all others)."""
        try:
            resp = await self.request("GetSceneItemId", {
                "sceneName": scene_name,
                "sourceName": source_name,
            })
            item_id = resp["sceneItemId"]
            # Index 0 = bottom of the stack in OBS
            await self.request("SetSceneItemIndex", {
                "sceneName": scene_name,
                "sceneItemId": item_id,
                "sceneItemIndex": 0,
            })
        except OBSRequestError as exc:
            log.warning("Failed to reorder '%s': %s", source_name, exc)

    async def remove_input_if_exists(self, input_name: str) -> bool:
        """Remove an input by name; return True if it existed."""
        try:
            await self.request("RemoveInput", {"inputName": input_name})
            return True
        except OBSRequestError:
            return False

    async def apply_template_layout(
        self,
        scene_name: str,
        image_path: str | None,
        slot_regions: dict[str, dict[str, dict[str, int]]],
        text_entries: list[dict[str, Any]] | None = None,
        template_size: tuple[int, int] | None = None,
        region_images: dict[str, dict[str, str]] | None = None,
    ) -> list[str]:
        """Apply a full template layout to the OBS scene.

        *image_path*: absolute path to the background image (set as an OBS
            image source). Pass ``None`` for image-less templates — any
            previously applied background is removed.
        *slot_regions*: mapping of  slot_index → {source_type → {x,y,width,height}}
            e.g. {"0": {"game": {"x":50,"y":50,"width":800,"height":600}, ...}}
        *text_entries*: optional list of text source definitions, each with
            id, text, x, y, font_size, color, font, align.
        *template_size*: (width, height) of the coordinate space the regions
            were drawn in.  Everything is rescaled from that space to the OBS
            canvas so templates of any resolution land correctly.
        *region_images*: slot → {region_key → image path} — static images
            shown INSTEAD of the corresponding feed region (placeholders).
            The feed item for that region is hidden; stale ``TplImg_*``
            sources from earlier applies are removed.

        Returns list of source names that were positioned.
        """
        await self.ensure_scene(scene_name)
        applied: list[str] = []

        # Scale factors: template coordinate space → OBS canvas
        try:
            vs = await self.get_video_settings()
            canvas_w = float(vs.get("baseWidth", 1920))
            canvas_h = float(vs.get("baseHeight", 1080))
        except OBSRequestError:
            canvas_w, canvas_h = 1920.0, 1080.0
        tpl_w, tpl_h = template_size if template_size else (canvas_w, canvas_h)
        sx = canvas_w / tpl_w if tpl_w else 1.0
        sy = canvas_h / tpl_h if tpl_h else 1.0

        # 1. Background image: stretch to fill the canvas (or remove it for
        #    image-less templates)
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
                log.warning("Failed to size background: %s", exc)
            applied.append(bg_name)
        else:
            await self.remove_input_if_exists(bg_name)

        # 2. Position every defined source
        for slot_str, regions in slot_regions.items():
            slot = int(slot_str)
            for source_type, rect in regions.items():
                if not rect:
                    continue
                src_name = f"Racer{slot + 1}_{source_type.capitalize()}"
                try:
                    # Ensure the source exists in the scene
                    exists = await self.ensure_source_in_scene(scene_name, src_name)
                    if not exists:
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
                    log.info("Placed '%s' at (%s,%s) %sx%s",
                             src_name, rect["x"], rect["y"], rect["width"], rect["height"])
                except OBSRequestError as exc:
                    log.warning("Failed to place '%s': %s", src_name, exc)

        # 3. Create and position text sources.  Text is NOT bounds-stretched:
        #    the source renders at its natural size for the scaled font, which
        #    matches how the dashboard editor previews it.
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
                    # Clear any stale bounds from older versions that
                    # stretched text to a box
                    "boundsType": "OBS_BOUNDS_NONE",
                    "scaleX": 1.0, "scaleY": 1.0,
                })
                applied.append(txt_name)
                log.info("Placed text '%s' at (%s,%s)",
                         txt_name, entry.get("x"), entry.get("y"))
            except OBSRequestError as exc:
                log.warning("Failed to create/place text '%s': %s", txt_name, exc)

        # 4. Region images: shown in place of the corresponding feed region
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
                    # The image replaces the live region → hide the feed item
                    try:
                        await self.set_scene_item_enabled(
                            scene_name, f"Racer{slot + 1}_{region.capitalize()}", False)
                    except OBSRequestError:
                        pass
                    applied.append(img_name)
                    log.info("Placed region image '%s'", img_name)
                except OBSRequestError as exc:
                    log.warning("Failed to place region image '%s': %s", img_name, exc)

        # 5. Remove app-managed text/image sources no longer in the template
        try:
            resp = await self.request("GetInputList", {})
            for inp in resp.get("inputs", []):
                name = inp.get("inputName", "")
                if re.match(r'^Text_txt\d+$', name) and name not in wanted_texts:
                    await self.remove_input_if_exists(name)
                    log.info("Removed stale text source '%s'", name)
                if re.match(r'^TplImg_R\d+_', name) and name not in wanted_imgs:
                    await self.remove_input_if_exists(name)
                    log.info("Removed stale region image '%s'", name)
        except OBSRequestError as exc:
            log.warning("Stale source cleanup failed: %s", exc)

        return applied

    @staticmethod
    def _make_qt_geometry(x: int, y: int, w: int, h: int) -> str:
        """Build a base64-encoded Qt window-geometry blob (format v2.0).

        Compatible with ``QWidget::restoreGeometry()`` which OBS uses
        to position the projector window.  QDataStream serialises QRect
        as (left, top, RIGHT, BOTTOM) inclusive — not width/height — and
        format 2.0 (Qt ≥5.4, still parsed by Qt6) appends the screen
        width as a trailing qint32.
        """
        magic = 0x01D9D0CB
        major, minor = 2, 0
        right, bottom = x + w - 1, y + h - 1
        data = struct.pack(
            ">I HH iiii iiii i BB i",
            magic, major, minor,
            x, y, right, bottom,   # frame geometry (l, t, r, b)
            x, y, right, bottom,   # normal geometry (l, t, r, b)
            0,                     # screen number
            0, 0,                  # maximised, fullscreen
            w,                     # v2.0: screen width at save time
        )
        return base64.b64encode(data).decode()

    async def get_video_settings(self) -> dict[str, Any]:
        """Return OBS video settings (base/output resolution, FPS)."""
        resp = await self.request("GetVideoSettings", {})
        return resp

    async def open_projector(
        self, scene_name: str, monitor: int = -1,
        width: int = 0, height: int = 0,
    ) -> None:
        """Open a windowed projector for *scene_name*.

        *monitor* = -1 → windowed projector (user can move/resize).
        A positive value opens full-screen on that monitor index.
        *width*/*height* — if both > 0, set the projector window size.
        """
        req: dict[str, Any] = {
            "sourceName": scene_name,
            "monitorIndex": monitor,
        }
        if width > 0 and height > 0:
            # Centre on screen (offset 100,100 as reasonable default)
            req["projectorGeometry"] = self._make_qt_geometry(100, 100, width, height)
        await self.request("OpenSourceProjector", req)
        log.info("Opened projector for scene '%s' (%dx%d)", scene_name, width, height)

    async def set_audio_monitor_type(
        self, input_name: str, monitor_type: str = "OBS_MONITORING_TYPE_MONITOR_ONLY",
    ) -> None:
        """Set the audio monitoring type for an input.

        Types:
            OBS_MONITORING_TYPE_NONE          – no monitoring
            OBS_MONITORING_TYPE_MONITOR_ONLY  – monitoring only (muted in stream)
            OBS_MONITORING_TYPE_MONITOR_AND_OUTPUT – both
        """
        await self.request("SetInputAudioMonitorType", {
            "inputName": input_name,
            "monitorType": monitor_type,
        })
        log.info("Audio monitor for '%s' set to %s", input_name, monitor_type)

    async def get_scene_screenshot(
        self, scene_name: str, width: int = 1280, height: int = 720,
        fmt: str = "jpg", quality: int = 75,
    ) -> str:
        """Get a base64-encoded screenshot of a scene.

        Returns the raw base64 string (no data-URI prefix).
        """
        resp = await self.request("GetSourceScreenshot", {
            "sourceName": scene_name,
            "imageFormat": fmt,
            "imageWidth": width,
            "imageHeight": height,
            "imageCompressionQuality": quality,
        })
        # OBS returns "data:image/jpeg;base64,XXXX..."
        data_uri: str = resp.get("imageData", "")
        # Strip the data-URI prefix if present
        if "," in data_uri:
            return data_uri.split(",", 1)[1]
        return data_uri

    # ------------------------------------------------------------------
    # Low-level request/response
    # ------------------------------------------------------------------

    async def request(self, request_type: str, request_data: dict[str, Any]) -> dict[str, Any]:
        if not self._connected or self._ws is None:
            raise OBSRequestError(request_type, -1, "OBS not connected")
        self._msg_id += 1
        rid = str(self._msg_id)
        msg = {
            "op": 6,  # Request
            "d": {
                "requestType": request_type,
                "requestId": rid,
                "requestData": request_data,
            },
        }
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[rid] = future
        try:
            await self._send(msg)
            result = await asyncio.wait_for(future, timeout=10)
        except asyncio.TimeoutError:
            raise OBSRequestError(request_type, -1, "request timed out after 10s")
        except websockets.WebSocketException as exc:
            self._connected = False
            raise OBSRequestError(request_type, -1, f"connection error: {exc}")
        finally:
            self._pending.pop(rid, None)
        status = result.get("requestStatus", {})
        if not status.get("result", False):
            code = status.get("code", -1)
            comment = status.get("comment", "unknown error")
            raise OBSRequestError(request_type, code, comment)
        return result.get("responseData", {})

    # ------------------------------------------------------------------
    # Internal transport
    # ------------------------------------------------------------------

    async def _send(self, data: dict[str, Any]) -> None:
        assert self._ws is not None
        await self._ws.send(json.dumps(data))

    async def _read_message(self) -> dict[str, Any]:
        assert self._ws is not None
        raw = await self._ws.recv()
        return json.loads(raw)  # type: ignore[return-value]

    async def _recv_loop(self) -> None:
        """Background task that dispatches incoming messages."""
        assert self._ws is not None
        try:
            async for raw in self._ws:
                msg = json.loads(raw)
                op = msg.get("op")
                if op == 7:  # RequestResponse
                    rid = msg["d"].get("requestId")
                    future = self._pending.pop(rid, None)
                    if future and not future.done():
                        future.set_result(msg["d"])
                # op 5 = Event – could be extended for monitoring
        except websockets.ConnectionClosed:
            log.warning("OBS WebSocket connection closed")
            self._connected = False
            self._fail_pending("OBS connection closed")


class OBSRequestError(Exception):
    def __init__(self, request_type: str, code: int, comment: str):
        self.request_type = request_type
        self.code = code
        self.comment = comment
        super().__init__(f"OBS '{request_type}' failed ({code}): {comment}")

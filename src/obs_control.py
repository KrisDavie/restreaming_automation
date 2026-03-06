"""Control Layer – OBS WebSocket client for remote scene/source manipulation.

Wraps the obs-websocket-py library (v5 protocol) behind a clean async API
used by the rest of the system to:
  • Set crop filters on sources
  • Adjust sync offsets (network buffer)
  • Switch scenes
  • Start / stop streaming
  • Create and configure media sources for ingest feeds
"""

from __future__ import annotations

import asyncio
import json
import logging
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
        self._scene_items: dict[str, int] = {}  # logical_name → sceneItemId

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        url = self._config.obs_ws_url
        log.info("Connecting to OBS WebSocket at %s …", url)
        self._ws = await websockets.connect(
            url, max_size=2**22,
            ping_interval=None,   # disable lib-level pings; our recv_loop handles the connection
            ping_timeout=None,
        )

        # The OBS-ws v5 handshake: wait for Hello, send Identify
        # NOTE: _recv_loop must NOT run yet — it would steal these messages.
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
            import hashlib, base64 as b64mod
            secret = b64mod.b64encode(
                hashlib.sha256(
                    (password + auth["salt"]).encode()
                ).digest()
            ).decode()
            auth_response = b64mod.b64encode(
                hashlib.sha256(
                    (secret + auth["challenge"]).encode()
                ).digest()
            ).decode()
            identify_payload["authentication"] = auth_response

        await self._send({"op": 1, "d": identify_payload})
        identified = await self._read_message()
        if identified.get("op") != 2:
            raise RuntimeError(f"OBS Identify failed: {identified}")

        # Handshake done — now start the background recv loop for requests
        self._recv_task = asyncio.create_task(self._recv_loop())
        self._connected = True
        log.info("Connected to OBS WebSocket (negotiated rpcVersion 1)")

    async def disconnect(self) -> None:
        self._connected = False
        self._scene_items.clear()
        if self._recv_task:
            self._recv_task.cancel()
        if self._ws:
            await self._ws.close()
        log.info("Disconnected from OBS WebSocket")

    @property
    def connected(self) -> bool:
        return self._connected

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
        if logical_name in self._scene_items:
            return self._scene_items[logical_name]
        # Direct lookup (works for legacy single-input sources)
        try:
            resp = await self.request("GetSceneItemId", {
                "sceneName": scene_name,
                "sourceName": logical_name,
            })
            item_id = resp["sceneItemId"]
            self._scene_items[logical_name] = item_id
            return item_id
        except OBSRequestError:
            pass
        # Not found – try to rebuild the cache from the scene
        feed_input = self._input_name_for(logical_name)
        if feed_input != logical_name:
            await self._rebuild_scene_cache(scene_name)
            if logical_name in self._scene_items:
                return self._scene_items[logical_name]
        raise OBSRequestError("GetSceneItemId", 600,
                              f"No cached or discoverable item for '{logical_name}'")

    @staticmethod
    def _input_name_for(logical_name: str) -> str:
        """Translate a logical name (Racer1_Game) to the real OBS input (Racer1_Feed)."""
        import re as _re
        m = _re.match(r'^(Racer\d+)_(Game|Tracker|Timer)$', logical_name)
        if m:
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
        stale = [k for k in self._scene_items if k.startswith(prefix)]
        for k in stale:
            del self._scene_items[k]

    async def _rebuild_scene_cache(self, scene_name: str) -> None:
        """Scan the scene and rebuild _scene_items for all Feed-based items.

        Each Racer{N}_Feed input has up to 3 scene items.  We assign them to
        Game / Tracker / Timer in the order they appear (lowest index first).

        Also removes legacy separate inputs (Racer{N}_Game/Tracker/Timer as
        standalone ``ffmpeg_source`` inputs) that conflict with the new
        single-Feed architecture.
        """
        import re as _re

        # --- Remove legacy separate-input sources that are actual inputs ---
        try:
            input_resp = await self.request("GetInputList", {})
            for inp in input_resp.get("inputs", []):
                name = inp.get("inputName", "")
                if _re.match(r'^Racer\d+_(Game|Tracker|Timer)$', name):
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
            if _re.match(r'^Racer\d+_Feed$', name):
                feeds.setdefault(name, []).append(item["sceneItemId"])
        # Assign logical names
        suffixes = ["Game", "Tracker", "Timer"]
        for feed_name, ids in feeds.items():
            prefix = feed_name.replace("_Feed", "")
            ids.sort()  # deterministic order
            for i, sid in enumerate(ids[:3]):
                logical = f"{prefix}_{suffixes[i]}"
                self._scene_items[logical] = sid
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
            item_id = await self._resolve_item_id(scene_name, source_name)
            await self.request("SetSceneItemTransform", {
                "sceneName": scene_name,
                "sceneItemId": item_id,
                "sceneItemTransform": {
                    "cropTop": crop.top,
                    "cropBottom": crop.bottom,
                    "cropLeft": crop.left,
                    "cropRight": crop.right,
                },
            })
            # Make sure the source is visible after cropping
            await self.request("SetSceneItemEnabled", {
                "sceneName": scene_name,
                "sceneItemId": item_id,
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

    async def set_sync_offset(self, source_name: str, offset_ms: int) -> int:
        """Set the sync delay (video + audio) for a source.

        Uses an OBS *Video Delay (Async)* source filter for video and
        ``SetInputAudioSyncOffset`` for audio so both are delayed
        uniformly without restarting the media source.

        Returns the new offset in ms.
        """
        input_name = self._input_name_for(source_name)
        delay = max(0, offset_ms)
        filter_name = f"{input_name}_Delay"

        # Video delay via async_delay_filter
        try:
            await self.request("GetSourceFilter", {
                "sourceName": input_name,
                "filterName": filter_name,
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

        # Audio delay to match (nanoseconds)
        await self.request("SetInputAudioSyncOffset", {
            "inputName": input_name,
            "inputAudioSyncOffset": delay * 1_000_000,
        })
        log.info("Sync delay for '%s' set to %d ms", source_name, delay)
        return delay

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
        item_id = await self._resolve_item_id(scene_name, source_name)
        await self.request("SetSceneItemTransform", {
            "sceneName": scene_name,
            "sceneItemId": item_id,
            "sceneItemTransform": transform,
        })
        log.info("Transform set for '%s' in '%s'", source_name, scene_name)

    async def set_scene_item_enabled(
        self, scene_name: str, source_name: str, enabled: bool
    ) -> None:
        """Show or hide a scene item."""
        item_id = await self._resolve_item_id(scene_name, source_name)
        await self.request("SetSceneItemEnabled", {
            "sceneName": scene_name,
            "sceneItemId": item_id,
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

        Creates a *single* media input (``Racer{N}_Feed``) and adds **three**
        scene-item references to it (Game / Tracker / Timer).  Each scene item
        can have its own independent transform & crop, but all three display
        the same decoded video feed — no UDP port conflicts.

        The method is idempotent: calling it again for the same slot updates
        the feed URL and ensures exactly 3 scene items exist without tearing
        down the connection (preserves the UDP stream).
        """
        await self.ensure_scene(scene_name)

        feed_name = f"Racer{slot + 1}_Feed"
        game_name = f"Racer{slot + 1}_Game"
        tracker_name = f"Racer{slot + 1}_Tracker"
        timer_name = f"Racer{slot + 1}_Timer"
        logical_names = [game_name, tracker_name, timer_name]

        # 1. Clean up legacy separate-input sources from old architecture
        for legacy in logical_names:
            try:
                await self.request("RemoveInput", {"inputName": legacy})
                log.info("Removed legacy input '%s'", legacy)
            except OBSRequestError:
                pass

        # 2. Create or update the single media input
        result = await self.create_media_source(scene_name, feed_name, input_url)

        # 3. Count existing scene items for this feed
        existing = await self._get_feed_scene_items(scene_name, feed_name)

        # 4. Trim excess items (keep at most 3)
        while len(existing) > 3:
            try:
                await self.request("RemoveSceneItem", {
                    "sceneName": scene_name,
                    "sceneItemId": existing.pop(),
                })
            except OBSRequestError:
                existing.pop()  # drop from list anyway

        # 5. Add items until we have exactly 3
        while len(existing) < 3:
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
            self._scene_items.pop(name, None)
        for i, sid in enumerate(existing[:3]):
            self._scene_items[logical_names[i]] = sid

        # 7. Set default transforms (side-by-side halves)
        half_w = canvas_width / 2
        pos_x = slot * half_w
        for i, sid in enumerate(existing[:3]):
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
                # Hide tracker/timer by default (user enables via crop/template)
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

        # 9. Switch to the scene
        try:
            await self.set_scene(scene_name)
        except OBSRequestError:
            pass

        log.info("Full scene setup: scene='%s' slot=%d feed='%s' items=%s",
                 scene_name, slot, feed_name,
                 {n: self._scene_items.get(n) for n in logical_names})
        return {"game": game_name, "tracker": tracker_name, "timer": timer_name}

    async def ensure_source_in_scene(
        self, scene_name: str, source_name: str
    ) -> bool:
        """Check if a source exists as a scene item; return True if it does."""
        if source_name in self._scene_items:
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

    async def create_text_source(
        self, scene_name: str, source_name: str, text: str,
        *, font_size: int = 36, color_hex: str = "#ffffff",
    ) -> None:
        """Create or update a FreeType2 text source."""
        hex_clean = color_hex.lstrip("#")
        if len(hex_clean) >= 6:
            r, g, b = int(hex_clean[0:2], 16), int(hex_clean[2:4], 16), int(hex_clean[4:6], 16)
        else:
            r, g, b = 255, 255, 255
        # OBS text_ft2_source_v2 colour is ABGR packed uint32
        color_int = 0xFF000000 | (b << 16) | (g << 8) | r
        settings: dict[str, Any] = {
            "text": text,
            "font": {"face": "Sans Serif", "size": font_size, "flags": 0},
            "color1": color_int,
            "color2": color_int,
        }
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
                "inputKind": "text_ft2_source_v2",
                "inputSettings": settings,
                "sceneItemEnabled": True,
            })
        log.info("Text source '%s' → '%s'", source_name, text[:50])

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

    async def apply_template_layout(
        self,
        scene_name: str,
        image_path: str | None,
        slot_regions: dict[str, dict[str, dict[str, int]]],
        text_entries: list[dict[str, Any]] | None = None,
    ) -> list[str]:
        """Apply a full template layout to the OBS scene.

        *image_path*: absolute path to the background image (set as an OBS
            image source). Pass ``None`` to skip.
        *slot_regions*: mapping of  slot_index → {source_type → {x,y,width,height}}
            e.g. {"0": {"game": {"x":50,"y":50,"width":800,"height":600}, ...}}
        *text_entries*: optional list of text source definitions, each with
            id, text, x, y, width, height, font_size, color.

        Returns list of source names that were positioned.
        """
        await self.ensure_scene(scene_name)
        applied: list[str] = []

        # 1. Set background image
        if image_path:
            bg_name = "Template_Background"
            await self.create_image_source(scene_name, bg_name, image_path)
            await self.move_source_to_bottom(scene_name, bg_name)
            applied.append(bg_name)

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
                        "positionX": float(rect["x"]),
                        "positionY": float(rect["y"]),
                        "boundsType": "OBS_BOUNDS_STRETCH",
                        "boundsWidth": float(rect["width"]),
                        "boundsHeight": float(rect["height"]),
                    })
                    await self.set_scene_item_enabled(scene_name, src_name, True)
                    applied.append(src_name)
                    log.info("Placed '%s' at (%s,%s) %sx%s",
                             src_name, rect["x"], rect["y"], rect["width"], rect["height"])
                except OBSRequestError as exc:
                    log.warning("Failed to place '%s': %s", src_name, exc)

        # 3. Create and position text sources
        for entry in (text_entries or []):
            txt_name = f"Text_{entry.get('id', 'unknown')}"
            try:
                await self.create_text_source(
                    scene_name, txt_name,
                    entry.get("text", ""),
                    font_size=entry.get("font_size", 36),
                    color_hex=entry.get("color", "#ffffff"),
                )
                resp = await self.request("GetSceneItemId", {
                    "sceneName": scene_name,
                    "sourceName": txt_name,
                })
                item_id = resp["sceneItemId"]
                transform: dict[str, Any] = {
                    "positionX": float(entry.get("x", 0)),
                    "positionY": float(entry.get("y", 0)),
                }
                if entry.get("width") and entry.get("height"):
                    transform["boundsType"] = "OBS_BOUNDS_STRETCH"
                    transform["boundsWidth"] = float(entry["width"])
                    transform["boundsHeight"] = float(entry["height"])
                await self.request("SetSceneItemTransform", {
                    "sceneName": scene_name,
                    "sceneItemId": item_id,
                    "sceneItemTransform": transform,
                })
                applied.append(txt_name)
                log.info("Placed text '%s' at (%s,%s)",
                         txt_name, entry.get("x"), entry.get("y"))
            except OBSRequestError as exc:
                log.warning("Failed to create/place text '%s': %s", txt_name, exc)

        return applied

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
        future: asyncio.Future[dict[str, Any]] = asyncio.get_event_loop().create_future()
        self._pending[rid] = future
        await self._send(msg)
        result = await asyncio.wait_for(future, timeout=10)
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


class OBSRequestError(Exception):
    def __init__(self, request_type: str, code: int, comment: str):
        self.request_type = request_type
        self.code = code
        self.comment = comment
        super().__init__(f"OBS '{request_type}' failed ({code}): {comment}")

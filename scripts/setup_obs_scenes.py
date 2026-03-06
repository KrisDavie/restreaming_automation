"""Create the standard OBS scene layout for ALttP 2-racer restreaming.

Requires the Python backend to be running (it proxies OBS-ws commands),
OR run standalone with OBS WebSocket directly.
"""

from __future__ import annotations

import asyncio
import platform
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.config import load_config
from src.obs_control import OBSController, SourceCrop

# OBS uses different text source plugins per platform
_IS_LINUX = platform.system() == "Linux"
_TEXT_SOURCE_KIND = "text_ft2_source_v2" if _IS_LINUX else "text_gdiplus_v3"
_TEXT_FONT_FACE = "Sans" if _IS_LINUX else "Segoe UI"


async def main() -> None:
    cfg = load_config()
    obs = OBSController(cfg)

    print("Connecting to OBS…")
    await obs.connect()
    print("Connected!")

    scene_name = "Race Scene"

    # Create main race scene
    try:
        await obs.request("CreateScene", {"sceneName": scene_name})
        print(f"Created scene: {scene_name}")
    except Exception:
        print(f"Scene '{scene_name}' already exists")

    # Create media sources for 2 racers
    for slot in range(2):
        racer_num = slot + 1
        port = cfg.ingest_base_port + slot
        input_url = f"udp://127.0.0.1:{port}"

        game_source = f"Racer{racer_num}_Game"
        tracker_source = f"Racer{racer_num}_Tracker"
        name_source = f"Racer{racer_num}_Name"

        # Game feed (Media Source)
        await obs.create_media_source(scene_name, game_source, input_url)
        print(f"  Created media source: {game_source} → {input_url}")

        # Tracker feed (same source, different crop – we create a second instance)
        await obs.create_media_source(scene_name, tracker_source, input_url)
        print(f"  Created media source: {tracker_source} → {input_url}")

        # Name text source (platform-aware: GDI+ on Windows, FreeType2 on Linux)
        text_settings: dict = {
            "text": f"Racer {racer_num}",
        }
        if _IS_LINUX:
            text_settings["font"] = {
                "face": _TEXT_FONT_FACE, "size": 36, "flags": 1,  # 1 = Bold
            }
        else:
            text_settings["font"] = {
                "face": _TEXT_FONT_FACE, "size": 36, "style": "Bold",
            }
            text_settings["color"] = 0xFFFFFFFF

        try:
            await obs.request("CreateInput", {
                "sceneName": scene_name,
                "inputName": name_source,
                "inputKind": _TEXT_SOURCE_KIND,
                "inputSettings": text_settings,
            })
            print(f"  Created text source: {name_source}")
        except Exception:
            print(f"  Text source '{name_source}' already exists")

    # Position sources (basic 2-up layout: 960px each)
    scene_items = await obs.request("GetSceneItemList", {"sceneName": scene_name})
    items = scene_items.get("sceneItems", [])
    for item in items:
        name = item.get("sourceName", "")
        item_id = item.get("sceneItemId")
        if "Racer1" in name:
            # Left half
            await obs.request("SetSceneItemTransform", {
                "sceneName": scene_name,
                "sceneItemId": item_id,
                "sceneItemTransform": {
                    "positionX": 0,
                    "positionY": 0 if "Name" not in name else 940,
                    "boundsType": "OBS_BOUNDS_SCALE_INNER",
                    "boundsWidth": 960 if "Game" in name else 300,
                    "boundsHeight": 720 if "Game" in name else 300,
                },
            })
        elif "Racer2" in name:
            # Right half
            await obs.request("SetSceneItemTransform", {
                "sceneName": scene_name,
                "sceneItemId": item_id,
                "sceneItemTransform": {
                    "positionX": 960,
                    "positionY": 0 if "Name" not in name else 940,
                    "boundsType": "OBS_BOUNDS_SCALE_INNER",
                    "boundsWidth": 960 if "Game" in name else 300,
                    "boundsHeight": 720 if "Game" in name else 300,
                },
            })

    print(f"\nScene '{scene_name}' is ready!")
    print("  Layout: 2-up side-by-side (960×720 each)")
    print("  Sources: Game feed, Tracker, Name text per racer")

    await obs.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

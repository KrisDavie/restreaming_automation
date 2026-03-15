"""Create the standard OBS scene layout for ALttP restreaming.

Uses the single-Feed, multi-item architecture: each racer gets one
``Racer{N}_Feed`` media source with three scene-item duplicates
(Game / Tracker / Timer) that are independently cropped and positioned.

Requires OBS to be running with WebSocket enabled.
"""

from __future__ import annotations

import asyncio
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.config import load_config
from src.obs_control import OBSController


async def main() -> None:
    cfg = load_config()
    obs = OBSController(cfg)

    print("Connecting to OBS…")
    await obs.connect()
    print("Connected!")

    scene_name = "Race Scene"
    num_slots = 2

    # Create main race scene
    try:
        await obs.request("CreateScene", {"sceneName": scene_name})
        print(f"Created scene: {scene_name}")
    except Exception:
        print(f"Scene '{scene_name}' already exists")

    # Use the controller's init_scene method which handles the single-Feed
    # multi-item architecture correctly (creates Feed + Game/Tracker/Timer items)
    await obs.init_scene(scene_name, num_slots)
    print(f"\nScene '{scene_name}' initialised with {num_slots} racer slots")
    print("  Each racer has: Feed (media source) → Game / Tracker / Timer (scene items)")
    print("\nUse the web dashboard to upload a template and position sources.")

    await obs.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

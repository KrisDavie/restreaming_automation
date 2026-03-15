"""Crop presets stored in SQLite — per-channel, multiple presets per channel."""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "presets.db"


@dataclass
class CropPreset:
    id: int
    channel: str
    name: str
    # Each region is {x, y, width, height} or null
    game_crop: dict[str, int] | None
    tracker_crop: dict[str, int] | None
    timer_crop: dict[str, int] | None
    created_at: str  # ISO timestamp

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "channel": self.channel,
            "name": self.name,
            "game_crop": self.game_crop,
            "tracker_crop": self.tracker_crop,
            "timer_crop": self.timer_crop,
            "created_at": self.created_at,
        }


class PresetStore:
    """SQLite-backed crop preset storage."""

    def __init__(self, db_path: Path | None = None) -> None:
        self._db_path = db_path or _DB_PATH
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path))
        self._conn.row_factory = sqlite3.Row
        self._init_db()
        log.info("Preset store opened: %s", self._db_path)

    def _init_db(self) -> None:
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS presets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                name TEXT NOT NULL,
                game_crop TEXT,
                tracker_crop TEXT,
                timer_crop TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                image_path TEXT NOT NULL,
                regions TEXT NOT NULL DEFAULT '{}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        self._conn.commit()

    def _row_to_preset(self, row: sqlite3.Row) -> CropPreset:
        return CropPreset(
            id=row["id"],
            channel=row["channel"],
            name=row["name"],
            game_crop=json.loads(row["game_crop"]) if row["game_crop"] else None,
            tracker_crop=json.loads(row["tracker_crop"]) if row["tracker_crop"] else None,
            timer_crop=json.loads(row["timer_crop"]) if row["timer_crop"] else None,
            created_at=row["created_at"],
        )

    # ---- Presets CRUD ----

    def save_preset(
        self,
        channel: str,
        name: str,
        game_crop: dict[str, int] | None = None,
        tracker_crop: dict[str, int] | None = None,
        timer_crop: dict[str, int] | None = None,
    ) -> CropPreset:
        cur = self._conn.execute(
            """INSERT INTO presets (channel, name, game_crop, tracker_crop, timer_crop)
               VALUES (?, ?, ?, ?, ?)""",
            (
                channel.lower().strip(),
                name.strip(),
                json.dumps(game_crop) if game_crop else None,
                json.dumps(tracker_crop) if tracker_crop else None,
                json.dumps(timer_crop) if timer_crop else None,
            ),
        )
        self._conn.commit()
        return self.get_preset(cur.lastrowid)  # type: ignore[arg-type]

    def get_preset(self, preset_id: int) -> CropPreset:
        row = self._conn.execute(
            "SELECT * FROM presets WHERE id = ?", (preset_id,)
        ).fetchone()
        if not row:
            raise KeyError(f"Preset {preset_id} not found")
        return self._row_to_preset(row)

    def list_presets(self, channel: str | None = None) -> list[CropPreset]:
        if channel:
            rows = self._conn.execute(
                "SELECT * FROM presets WHERE channel = ? ORDER BY created_at DESC",
                (channel.lower().strip(),),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM presets ORDER BY channel, created_at DESC"
            ).fetchall()
        return [self._row_to_preset(r) for r in rows]

    def delete_preset(self, preset_id: int) -> bool:
        cur = self._conn.execute("DELETE FROM presets WHERE id = ?", (preset_id,))
        self._conn.commit()
        return cur.rowcount > 0

    # ---- Templates CRUD ----

    def save_template(
        self, name: str, image_path: str, regions: dict[str, Any],
    ) -> dict[str, Any]:
        cur = self._conn.execute(
            """INSERT INTO templates (name, image_path, regions) VALUES (?, ?, ?)""",
            (name.strip(), image_path, json.dumps(regions)),
        )
        self._conn.commit()
        return self.get_template(cur.lastrowid)  # type: ignore[arg-type]

    def get_template(self, template_id: int) -> dict[str, Any]:
        row = self._conn.execute(
            "SELECT * FROM templates WHERE id = ?", (template_id,)
        ).fetchone()
        if not row:
            raise KeyError(f"Template {template_id} not found")
        return {
            "id": row["id"],
            "name": row["name"],
            "image_path": row["image_path"],
            "regions": json.loads(row["regions"]),
            "created_at": row["created_at"],
        }

    def list_templates(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM templates ORDER BY created_at DESC"
        ).fetchall()
        return [
            {
                "id": r["id"],
                "name": r["name"],
                "image_path": r["image_path"],
                "regions": json.loads(r["regions"]),
                "created_at": r["created_at"],
            }
            for r in rows
        ]

    def update_template_regions(
        self, template_id: int, regions: dict[str, Any],
    ) -> dict[str, Any]:
        self._conn.execute(
            "UPDATE templates SET regions = ? WHERE id = ?",
            (json.dumps(regions), template_id),
        )
        self._conn.commit()
        return self.get_template(template_id)

    def delete_template(self, template_id: int) -> bool:
        cur = self._conn.execute("DELETE FROM templates WHERE id = ?", (template_id,))
        self._conn.commit()
        return cur.rowcount > 0

    # ---- Settings (key-value) ----

    def get_setting(self, key: str, default: str | None = None) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM settings WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else default

    def set_setting(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (key, value),
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

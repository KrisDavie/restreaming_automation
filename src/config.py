"""Shared configuration loaded from environment / .env file."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent  # repo root (src/../)


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


@dataclass(frozen=True)
class Config:
    # OBS WebSocket
    obs_ws_host: str = field(default_factory=lambda: _env("OBS_WS_HOST", "127.0.0.1"))
    obs_ws_port: int = field(default_factory=lambda: int(_env("OBS_WS_PORT", "4455")))
    obs_ws_password: str = field(default_factory=lambda: _env("OBS_WS_PASSWORD", ""))

    # Ingest
    ingest_base_port: int = field(default_factory=lambda: int(_env("INGEST_BASE_PORT", "1234")))
    ingest_protocol: str = field(default_factory=lambda: _env("INGEST_PROTOCOL", "udp"))

    # API server
    api_host: str = field(default_factory=lambda: _env("API_HOST", "0.0.0.0"))
    api_port: int = field(default_factory=lambda: int(_env("API_PORT", "8008")))

    # Detection
    detect_confidence: float = field(
        default_factory=lambda: float(_env("DETECT_CONFIDENCE", "0.75"))
    )
    templates_dir: Path = field(
        default_factory=lambda: Path(_env("TEMPLATES_DIR", str(_ROOT / "templates")))
    )

    # Host-accessible data directory (for OBS running outside Docker)
    obs_data_dir: str = field(
        default_factory=lambda: _env("OBS_DATA_DIR", "")
    )

    @property
    def obs_ws_url(self) -> str:
        return f"ws://{self.obs_ws_host}:{self.obs_ws_port}"


def load_config() -> Config:
    """Load config, reading .env file if present."""
    env_path = _ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())
    return Config()

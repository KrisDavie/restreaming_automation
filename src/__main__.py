"""Entry point – ``python -m src`` starts the API server."""

import logging
import uvicorn

from .config import load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)

cfg = load_config()
uvicorn.run(
    "src.server:app",
    host=cfg.api_host,
    port=cfg.api_port,
    reload=False,
    log_level="info",
)

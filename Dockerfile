FROM python:3.12-slim AS base

# System deps: ffmpeg, streamlink, OpenCV support libs, curl (health check)
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg \
        streamlink \
        libgl1 \
        libglib2.0-0 \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps (editable so src/ can be bind-mounted later)
COPY pyproject.toml .
RUN pip install --no-cache-dir watchfiles python-multipart \
    && pip install --no-cache-dir -e . 2>/dev/null || pip install --no-cache-dir .

# Data volume for SQLite + template uploads
RUN mkdir -p /app/data
VOLUME /app/data

EXPOSE 8008

ENV PYTHONUNBUFFERED=1
ENV API_HOST=0.0.0.0
ENV API_PORT=8008
ENV OBS_WS_HOST=host.docker.internal
ENV OBS_WS_PORT=4455
ENV OBS_WS_PASSWORD=

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8008/api/health || exit 1

# ── Production stage ────────────────────────────────────────────
FROM base AS production
COPY src/ src/
COPY scripts/ scripts/
CMD ["python", "-m", "src"]

# ── Development stage ───────────────────────────────────────────
# src/ is bind-mounted from the host; uvicorn --reload watches for changes.
FROM base AS development
CMD ["uvicorn", "src.server:app", "--host", "0.0.0.0", "--port", "8008", "--reload", "--reload-dir", "src"]

# Deployment

Ways to run the API server beyond the simple `scripts/start.*` quickstart.

## Docker

The project ships with a multi-stage `Dockerfile` and a `docker-compose.yml` that supports
both **production** and **development** workflows.

### Production

```bash
docker compose up -d --build
docker compose logs -f restream-api
```

### Development (live-reload)

```bash
docker compose --profile dev up --build restream-dev
# Edit src/ locally → changes apply instantly via --reload
```

### Connecting to OBS on the host

Set in your `.env`:

```env
OBS_WS_HOST=host.docker.internal
OBS_WS_PORT=4455
OBS_WS_PASSWORD=your_password_here
```

`OBS_DATA_DIR` should point at the host path of the `data/` directory so OBS (running on
the host) can load images the containerized server saved, e.g.
`OBS_DATA_DIR=C:\restreaming_automation\data` on a Windows host.

### Data persistence

The `data/` directory stores `presets.db` (SQLite) plus uploaded template/preset images,
and persists across container restarts via the volume mount.

> **Mixed Docker + local runs:** the container runs as root, so files it creates under
> `data/` are root-owned. If you later run the server directly as your user and see
> `attempt to write a readonly database` or image-upload errors, fix ownership with
> `sudo chown -R $USER data/`.

## Linux (systemd)

```bash
sudo ./scripts/install-systemd.sh your_username
sudo systemctl enable --now restream-api@your_username
```

View logs:

```bash
journalctl -u restream-api@your_username -f
```

## VM notes

- Use a **GPU-enabled VM** for OBS compositing
- Open port **4455** (OBS-WS) and **8008** (dashboard) for your IP only — the API has no
  authentication, so never expose it to the open internet
- The dashboard serves from port **8008**

### Headless OBS on Linux

OBS can run without a display using a virtual framebuffer:

```bash
sudo pacman -S xorg-server-xvfb
Xvfb :99 -screen 0 1920x1080x24 &
export DISPLAY=:99
obs --startstreaming &
```

## Environment variables

All settings come from the environment or a `.env` file in the repo root
(see `.env.example`):

| Variable | Default | Purpose |
|---|---|---|
| `OBS_WS_HOST` | `127.0.0.1` | OBS WebSocket host |
| `OBS_WS_PORT` | `4455` | OBS WebSocket port |
| `OBS_WS_PASSWORD` | *(empty)* | OBS WebSocket password |
| `API_HOST` | `0.0.0.0` | API bind address |
| `API_PORT` | `8008` | API port |
| `INGEST_BASE_PORT` | `1234` | First local UDP port for feeds (slot N uses base+N) |
| `INGEST_PROTOCOL` | `udp` | `udp` or `srt` |
| `TWITCH_OAUTH_TOKEN` | *(empty)* | Twitch OAuth token for ad-free ingest (can also be set in the dashboard) |
| `OBS_DATA_DIR` | *(empty)* | Host path of `data/` when the server runs in Docker but OBS runs on the host |

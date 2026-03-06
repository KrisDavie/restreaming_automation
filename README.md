# ALttP Restreaming Automation

A **headless production stack** for automating ALttP community race restreams. Replaces manual cropping, stream-delay sync, and RDP-based OBS control with a browser-accessible dashboard.

## Architecture

```
┌─────────────┐    ┌──────────────┐    ┌──────────────┐    ┌───────────┐
│  Twitch /    │    │  Python API  │    │   NodeCG     │    │    OBS    │
│  YouTube     │───▶│  (FastAPI)   │◀──▶│  Dashboard   │    │  Studio   │
│  streams     │    │              │    │  :9090       │    │  (VM)     │
└─────────────┘    │  • Ingest    │    └──────┬───────┘    └─────┬─────┘
                   │  • Detection │           │                  │
                   │  • OBS ctrl  │───────────┼──────────────────┘
                   │  :8008       │     OBS-WebSocket :4455
                   └──────────────┘
```

| Layer | Technology | Purpose |
|---|---|---|
| **Ingest** | Streamlink + FFmpeg | Pipe live streams to local UDP/SRT ports |
| **Detection** | Python + OpenCV | Auto-detect game regions via template matching |
| **Control** | NodeCG + FastAPI | Web dashboard for cropping, sync, scene control |
| **Compositing** | OBS Studio | Assemble and encode the final broadcast |

## Prerequisites

| | Windows | Linux (Arch / CachyOS) |
|---|---|---|
| **Python** | 3.10+ from [python.org](https://python.org) | `sudo pacman -S python` |
| **Node.js** | 18+ from [nodejs.org](https://nodejs.org) | `sudo pacman -S nodejs npm` |
| **FFmpeg** | [ffmpeg.org](https://ffmpeg.org) | `sudo pacman -S ffmpeg` |
| **Streamlink** | `pip install streamlink` | `sudo pacman -S streamlink` |
| **OBS Studio** | [obsproject.com](https://obsproject.com) | `sudo pacman -S obs-studio` |

OBS must have **WebSocket Server** enabled (Settings → WebSocket Server).

## Quick Start

### Windows (PowerShell)

```powershell
git clone <this-repo>
cd restreaming_automation
.\scripts\setup.ps1       # installs venv, npm deps, NodeCG
# Edit .env with your OBS WebSocket password
# Place hearts.png in ./templates/
.\scripts\start.ps1       # launches API + NodeCG
```

### Linux (Bash)

```bash
git clone <this-repo>
cd restreaming_automation
chmod +x scripts/*.sh
./scripts/setup.sh         # installs packages, venv, npm deps, NodeCG
# Edit .env with your OBS WebSocket password
# Place hearts.png in ./templates/
./scripts/start.sh         # launches API + NodeCG (Ctrl+C stops both)
```

Services will be available at:
- **API Backend**: http://localhost:8008 (Swagger docs at `/docs`)
- **Standalone Dashboard**: http://localhost:8008/dashboard
- **NodeCG Dashboard**: http://localhost:9090

## Project Structure

```
restreaming_automation/
├── src/                          # Python backend
│   ├── __init__.py
│   ├── __main__.py               # Entry point (python -m src)
│   ├── config.py                 # Environment configuration
│   ├── ingest.py                 # Streamlink/FFmpeg pipeline manager
│   ├── detector.py               # OpenCV auto-crop detection
│   ├── obs_control.py            # OBS WebSocket v5 client
│   └── server.py                 # FastAPI REST + WebSocket API
├── nodecg/
│   └── bundles/
│       └── alttp-restream/       # NodeCG dashboard bundle
│           ├── dashboard/        # Dashboard panels (HTML)
│           │   ├── ingest.html   # Stream ingest controls
│           │   ├── cropper.html  # Auto/manual crop UI
│           │   ├── sync.html     # Sync offset nudger
│           │   └── obs.html      # OBS connection & scenes
│           ├── graphics/
│           │   └── race-overlay.html  # 1920×1080 race overlay
│           └── extension/
│               └── index.js      # Server-side NodeCG logic
├── templates/                    # Template images for detection
├── scripts/
│   ├── setup.ps1                 # One-time setup (Windows)
│   ├── setup.sh                  # One-time setup (Linux)
│   ├── start.ps1                 # Start all services (Windows)
│   ├── start.sh                  # Start all services (Linux)
│   ├── install-systemd.sh        # Install systemd units (Linux)
│   ├── systemd/                  # systemd service files
│   │   ├── restream-api@.service
│   │   └── restream-nodecg@.service
│   └── setup_obs_scenes.py       # Auto-create OBS scene layout
├── .env.example                  # Environment template
├── pyproject.toml                # Python project config
└── package.json                  # Node project config
```

## API Reference

### Ingest

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/ingest/start` | Start a feed `{ slot, url, quality }` |
| POST | `/api/ingest/stop` | Stop a feed `{ slot }` |
| GET | `/api/ingest/status` | List all active feeds |

### Detection

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/detect/{slot}` | Run auto-crop on a slot's feed |
| POST | `/api/detect/manual` | Submit manual crop coordinates |

### OBS Control

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/obs/connect` | Connect to OBS WebSocket |
| POST | `/api/obs/disconnect` | Disconnect |
| GET | `/api/obs/status` | Connection status |
| POST | `/api/obs/crop` | Apply crop filter `{ source_name, top, bottom, left, right }` |
| POST | `/api/obs/sync` | Nudge sync offset `{ source_name, delta_ms }` |
| POST | `/api/obs/scene` | Switch scene `{ scene_name }` |
| POST | `/api/obs/stream/start` | Start streaming |
| POST | `/api/obs/stream/stop` | Stop streaming |
| GET | `/api/obs/scenes` | List available scenes |
| GET | `/api/obs/sources` | List input sources |

### WebSocket

Connect to `ws://localhost:8008/ws` for real-time events:

```json
{ "event": "ingest:started", "data": { "slot": 0, "url": "...", "local_url": "..." } }
{ "event": "detect:applied", "data": { "slot": 0, "crop": { "x": 0, "y": 0, "width": 960, "height": 720 } } }
{ "event": "sync:nudged", "data": { "source": "Racer1_Game", "delta_ms": 100 } }
```

## Production Workflow

1. **Input URLs** → Enter racer Twitch URLs in the Ingest panel
2. **Start Feeds** → Dashboard triggers Streamlink pipelines on the VM
3. **Auto-Detect** → Click "Auto-Detect Crop" – OpenCV finds the game window
4. **Sync Streams** → Watch the VDO.Ninja preview, nudge offsets with ±buttons
5. **Go Live** → Hit "Start Streaming" from the OBS panel

## OBS Scene Setup

Run the scene setup script after connecting to OBS:

```bash
# Linux
python scripts/setup_obs_scenes.py

# Windows (PowerShell)
python scripts\setup_obs_scenes.py
```

The script auto-detects the platform and uses the correct OBS text source
plugin (`text_ft2_source_v2` on Linux, `text_gdiplus_v3` on Windows).

This creates a "Race Scene" with:
- 2× Media Sources (Racer1_Game, Racer2_Game) pointed at ingest UDP ports
- 2× Tracker Sources (Racer1_Tracker, Racer2_Tracker)
- 2× Text Sources (Racer1_Name, Racer2_Name)
- Side-by-side 960×720 layout

## Template Images

Place PNG template images in `./templates/` for auto-detection:

- **`hearts.png`** – Cropped screenshot of the ALttP health bar (green hearts row). ~50×20 pixels, taken from a clean SNES output.
- Additional templates can be added for different anchor points.

The detector uses `cv2.matchTemplate()` with normalised cross-correlation. Adjust `DETECT_CONFIDENCE` in `.env` if you get false positives/negatives.

## Docker Deployment

The project ships with a multi-stage `Dockerfile` and a `docker-compose.yml` that supports both **production** and **development** workflows.

### Prerequisites

- Docker Engine 20.10+ (or Docker Desktop)
- Docker Compose v2
- OBS Studio running on the **host** (or another machine reachable from the container)

### Production

Builds the image with source baked in and runs normally:

```bash
# Build & start (detached)
docker compose up -d --build

# View logs
docker compose logs -f restream-api

# Stop
docker compose down
```

### Development (live-reload)

The `dev` profile bind-mounts `src/`, `scripts/`, and `templates/` from your host into the container. Uvicorn runs with `--reload` so any file change on the host is picked up automatically — no rebuild needed.

```bash
# Start the dev container (note: the prod service won't start)
docker compose --profile dev up --build restream-dev

# Edit src/ or src/static/dashboard.html locally → changes apply instantly

# Stop
docker compose --profile dev down
```

> **Tip:** The production and dev services bind to the same host port (`8008` by default). Stop one before starting the other, or override `API_PORT` in your `.env`.

### Connecting to OBS on the Host

The container uses `host.docker.internal` to reach OBS on the host machine.
This is mapped automatically via `extra_hosts` on Linux and natively available on Docker Desktop.

Set in your `.env`:

```env
OBS_WS_HOST=host.docker.internal
OBS_WS_PORT=4455
OBS_WS_PASSWORD=your_password_here
```

If OBS runs on a different machine, replace `host.docker.internal` with its IP.

### Data Persistence

The `data/` directory is mounted as a volume and stores:
- `presets.db` — SQLite database for crop presets and templates
- `templates_upload/` — uploaded template images

This persists across container restarts and rebuilds.

---

## VM Deployment Notes

- Use a **GPU-enabled VM** (Azure NV-series / AWS G-series) for OBS compositing
- Open ports **9090** (NodeCG) and **4455** (OBS-WS) for your IP only
- Use **VDO.Ninja** for sub-second latency monitoring without RDP
- The API server runs on port **8008** – open it if accessing from a different machine

### Linux / CachyOS Production Deployment

For always-on operation, install the systemd services:

```bash
sudo ./scripts/install-systemd.sh your_username
sudo systemctl enable --now restream-api@your_username
sudo systemctl enable --now restream-nodecg@your_username
```

Both services auto-restart on failure. View logs with:

```bash
journalctl -u restream-api@your_username -f
journalctl -u restream-nodecg@your_username -f
```

#### Firewall (CachyOS / Arch with firewalld)

```bash
# Allow dashboard + OBS-WS from your IP only
sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv4" source address="YOUR_IP" port port="8008" protocol="tcp" accept'
sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv4" source address="YOUR_IP" port port="9090" protocol="tcp" accept'
sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv4" source address="YOUR_IP" port port="4455" protocol="tcp" accept'
sudo firewall-cmd --reload
```

Or with iptables:

```bash
sudo iptables -A INPUT -p tcp -s YOUR_IP --dport 8008 -j ACCEPT
sudo iptables -A INPUT -p tcp -s YOUR_IP --dport 9090 -j ACCEPT
sudo iptables -A INPUT -p tcp -s YOUR_IP --dport 4455 -j ACCEPT
```

#### Headless OBS on Linux

OBS can run without a display using a virtual framebuffer:

```bash
sudo pacman -S xorg-server-xvfb
Xvfb :99 -screen 0 1920x1080x24 &
export DISPLAY=:99
obs --startstreaming &
```

Or use `wlroots`-based headless Wayland compositor if preferred.

# ALttP Restreaming Automation

A **headless production stack** for automating ALttP community race restreams. Replaces manual cropping, stream-delay sync, and RDP-based OBS control with a browser-accessible dashboard.

## Architecture

```
┌─────────────┐    ┌──────────────┐    ┌───────────┐
│  Twitch /   │    │  Python API  │    │    OBS    │
│  YouTube    │───>│  (FastAPI)   │<──>│  Studio   │
│  streams    │    │              │    │  (host)   │
└─────────────┘    │  • Ingest    │    └─────┬─────┘
                   │  • OBS ctrl  │──────────┘
                   │  :8008       │  OBS-WebSocket :4455
                   └──────────────┘
```

| Layer | Technology | Purpose |
|---|---|---|
| **Ingest** | Streamlink + FFmpeg | Pipe live streams to local UDP/SRT ports |
| **Control** | FastAPI + HTML dashboard | Web UI for cropping, sync, scene control, templates |
| **OBS Link** | OBS WebSocket v5 (async) | Remote scene/source manipulation via raw websockets |
| **Compositing** | OBS Studio | Assemble and encode the final broadcast |
| **Discord** | User's own account + OBS projector | Share the OBS scene to Discord voice channels |

## Prerequisites

| | Windows | Linux (Arch / CachyOS) |
|---|---|---|
| **Python** | 3.10+ from [python.org](https://python.org) | `sudo pacman -S python` |
| **FFmpeg** | [ffmpeg.org](https://ffmpeg.org) | `sudo pacman -S ffmpeg` |
| **Streamlink** | `pip install streamlink` | `sudo pacman -S streamlink` |
| **OBS Studio** | [obsproject.com](https://obsproject.com) | `sudo pacman -S obs-studio` |

OBS must have **WebSocket Server** enabled (Settings → WebSocket Server).

## Quick Start

### Windows (PowerShell)

```powershell
git clone <this-repo>
cd restreaming_automation
.\scripts\setup.ps1       # installs Python venv
# Edit .env with your OBS WebSocket password
.\scripts\start.ps1       # launches API server
```

### Linux (Bash)

```bash
git clone <this-repo>
cd restreaming_automation
chmod +x scripts/*.sh
./scripts/setup.sh         # installs Python venv
# Edit .env with your OBS WebSocket password
./scripts/start.sh         # launches API server (Ctrl+C stops)
```

Services will be available at:
- **Dashboard**: http://localhost:8008/dashboard
- **API Docs**: http://localhost:8008/docs

## Discord Integration

The dashboard supports sharing your OBS output to Discord using your own account:

1. Click **Open Projector** in the Audio Mixer panel — OBS opens a resizable window of your scene (resolution scales dynamically with your OBS canvas).
2. In Discord, join a voice channel and click **Screen → Window**, then pick the OBS projector window.
3. To capture Discord commentary audio into OBS:
   - **Windows**: Enter the application name (e.g. `Discord.exe`) and click **Add Source** — uses Application Audio Capture to grab only Discord's audio.
   - **Linux/macOS**: Click **Scan Devices**, select the audio output device Discord uses, then click **Add Source**.
4. Set monitoring to **Monitor Only** so commentary goes into the restream but doesn't echo in your headphones.

## Project Structure

```
restreaming_automation/
├── src/                          # Python backend
│   ├── __init__.py
│   ├── __main__.py               # Entry point (python -m src)
│   ├── config.py                 # Environment configuration
│   ├── ingest.py                 # Streamlink/FFmpeg pipeline manager
│   ├── obs_control.py            # OBS WebSocket v5 client (async, raw websockets)
│   ├── presets.py                # SQLite-backed preset/template storage
│   ├── server.py                 # FastAPI REST + WebSocket API
│   └── static/
│       └── dashboard.html        # Single-page control dashboard
├── data/                         # SQLite DB + template uploads (auto-created)
├── scripts/
│   ├── setup.ps1 / setup.sh      # One-time setup
│   ├── start.ps1 / start.sh      # Start API server
│   ├── setup_obs_scenes.py        # Bootstrap OBS scene layout
│   ├── install-systemd.sh         # Install systemd units (Linux)
│   └── systemd/                   # systemd service files
├── .env.example                   # Environment template
├── Dockerfile                     # Multi-stage Docker build
├── docker-compose.yml             # Docker deployment (prod + dev)
└── pyproject.toml                 # Python project config
```

## API Reference

### Ingest

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/ingest/start` | Start a feed `{ slot, url, quality, start_offset }` |
| POST | `/api/ingest/stop` | Stop a feed `{ slot }` |
| GET | `/api/ingest/status` | List all active feeds |
| GET | `/api/ingest/qualities?url=` | Query available stream qualities |
| GET | `/api/ingest/preview/{slot}` | Capture a JPEG preview frame |
| GET | `/api/ingest/token` | Check if Twitch OAuth token is set |
| POST | `/api/ingest/token` | Set/clear Twitch OAuth token |

### Crop

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/detect/manual` | Submit manual crop coordinates |

### OBS Control

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/obs/connect` | Connect to OBS WebSocket |
| POST | `/api/obs/disconnect` | Disconnect |
| GET | `/api/obs/status` | Connection status + platform info |
| GET | `/api/obs/video-settings` | Get OBS canvas resolution/FPS |
| GET | `/api/obs/scenes` | List available scenes (+ current program scene) |
| GET | `/api/obs/screenshot` | Capture current scene preview |
| POST | `/api/obs/init` | Re-provision Race Scene sources |
| POST | `/api/obs/crop` | Apply crop filter |
| POST | `/api/obs/sync` | Nudge sync offset |
| POST | `/api/obs/scene` | Switch scene |
| POST | `/api/obs/stream/start` | Start streaming |
| POST | `/api/obs/stream/stop` | Stop streaming |
| GET | `/api/obs/stream/status` | Live-stream state (active, timecode, dropped frames) |
| POST | `/api/obs/projector` | Open OBS projector window |

### Audio

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/obs/audio` | Switch active audio slot |
| POST | `/api/obs/audio/volume` | Set input volume (dB) |
| POST | `/api/obs/audio/mute` | Mute/unmute input |
| POST | `/api/obs/audio/discord` | Create commentary audio capture (device or app) |
| POST | `/api/obs/audio/monitor` | Set audio monitoring type |
| GET | `/api/obs/audio/devices` | List audio capture devices |
| GET | `/api/obs/audio/mixer` | Get mixer strip states (`?scope=scene` limits to the current scene, `?scope=all` for everything) |

### Templates & Presets

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/templates` | List templates |
| POST | `/api/templates/upload` | Upload template image |
| POST | `/api/templates/blank` | Create an image-less template `{ name, width, height }` |
| GET | `/api/templates/{id}` | Get template details + image |
| PUT | `/api/templates/{id}/regions` | Update template region layout |
| POST | `/api/templates/{id}/apply` | Apply template to OBS |
| DELETE | `/api/templates/{id}` | Delete template |
| GET | `/api/active-template` | Get currently active template |
| GET | `/api/presets` | List crop presets |
| POST | `/api/presets` | Save crop preset (incl. `extra_crops` for custom regions) |
| POST | `/api/presets/{id}/apply` | Apply preset crops + attached images |
| POST | `/api/presets/{id}/image?region=` | Attach an image to a preset (e.g. tracker placeholder) |
| DELETE | `/api/presets/{id}/image?region=` | Remove an attached image |
| DELETE | `/api/presets/{id}` | Delete preset |
| GET/POST | `/api/custom-regions` | List / add custom crop regions (shared by all racers) |
| DELETE | `/api/custom-regions/{name}` | Remove a custom region |
| GET | `/api/obs/audio/apps` | List capturable app windows (Windows, for commentary capture) |

### Health

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Health check (OBS status, feed count) |

### WebSocket

Connect to `ws://localhost:8008/ws` for real-time events:

```json
{ "event": "ingest:started", "data": { "slot": 0, "url": "...", "local_url": "..." } }
{ "event": "ingest:stopped", "data": { "slot": 0 } }
{ "event": "ingest:reconnecting", "data": { "slot": 0, "attempt": 1, "delay": 3 } }
{ "event": "ingest:reconnected", "data": { "slot": 0, "attempt": 1, "url": "...", "local_url": "..." } }
{ "event": "ingest:reconnect_failed", "data": { "slot": 0, "attempts": 10 } }
{ "event": "template:applied", "data": { "template_id": 1, "template_name": "2-player", "applied": [...] } }
```

## Production Workflow

The dashboard is laid out in the same order (steps 1–6 in the header):

1. **Input URLs** → Enter racer Twitch URLs in the Ingest panel (use **+ / −** in the panel header to change the number of racer slots)
2. **Start Feeds** → Dashboard triggers Streamlink pipelines
3. **Layout Template** → Upload a background image **or click ➕ Blank for a layout without artwork**, draw per-racer regions and text, then Apply. Regions and text are rescaled to your OBS canvas resolution, and text renders in OBS exactly as previewed (same font size and position — no stretching)
4. **Crop Feeds** → Drag-to-crop on the preview to isolate game/tracker regions
5. **Sync Streams** → Nudge offsets with ±buttons until audio/video aligns
6. **Share to Discord** → Open Projector, screen-share the window in Discord
7. **Go Live** → Hit "Start Stream" — the header shows a pulsing **LIVE** badge with timecode while streaming

Notes:
- The **Audio Mixer** lists only the sources in the current OBS scene (plus global audio) by default — untick *Current scene only* to see everything.
- Text overlays are created with the platform's native OBS text source (GDI+ on Windows, FreeType2 elsewhere). Text supports fonts, multi-line (Shift+Enter) and left/center/right alignment, and the preview compensates for GDI's cell-height font sizing so the dashboard matches the OBS output.
- **Custom regions**: add extra named crop regions (e.g. `deaths`) with *＋ Region* in the Crop Tool — they apply to every racer, can be drawn in templates, and are saved in presets.
- **Region images**: in the Layout editor, attach an image to any region per racer (🖼 bar) — it previews in the editor and, on apply, is shown in OBS instead of that racer's live region (e.g. a tracker placeholder). Presets can carry per-channel images too (🖼 in the preset list).
- **Fonts**: the text editor's font list can load your real installed fonts via *🔤 System Fonts* (Chrome/Edge). Fonts must also exist on the OBS machine; a ⚠ marks fonts this browser can't render, where the preview is approximate.

## Docker Deployment

The project ships with a multi-stage `Dockerfile` and a `docker-compose.yml` that supports both **production** and **development** workflows.

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

### Connecting to OBS on the Host

Set in your `.env`:

```env
OBS_WS_HOST=host.docker.internal
OBS_WS_PORT=4455
OBS_WS_PASSWORD=your_password_here
```

### Data Persistence

The `data/` directory stores `presets.db` (SQLite) and uploaded template images.
This persists across container restarts via the Docker volume mount.

## VM Deployment Notes

- Use a **GPU-enabled VM** for OBS compositing
- Open port **4455** (OBS-WS) and **8008** (dashboard) for your IP only
- The dashboard serves from port **8008**

### Linux Production Deployment

```bash
sudo ./scripts/install-systemd.sh your_username
sudo systemctl enable --now restream-api@your_username
```

View logs:

```bash
journalctl -u restream-api@your_username -f
```

#### Headless OBS on Linux

OBS can run without a display using a virtual framebuffer:

```bash
sudo pacman -S xorg-server-xvfb
Xvfb :99 -screen 0 1920x1080x24 &
export DISPLAY=:99
obs --startstreaming &
```

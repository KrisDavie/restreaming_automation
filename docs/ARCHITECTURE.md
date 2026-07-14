# Architecture

## Overview

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
| **App Link** | OBS WebSocket v5 *or* Streamlabs JSON-RPC | Remote scene/source manipulation via raw websockets |
| **Compositing** | OBS Studio or Streamlabs Desktop | Assemble and encode the final broadcast |
| **Discord** | User's own account + projector window | Share the scene to Discord voice channels |

## Project structure

```
restreaming_automation/
├── src/                          # Python backend
│   ├── __init__.py
│   ├── __main__.py               # Entry point (python -m src)
│   ├── config.py                 # Environment configuration (.env)
│   ├── ingest.py                 # Streamlink/FFmpeg pipeline manager
│   ├── obs_control.py            # OBS WebSocket v5 client (async, raw websockets)
│   ├── slobs_control.py          # Streamlabs Desktop JSON-RPC client (same interface)
│   ├── presets.py                # SQLite-backed preset/template storage
│   ├── server.py                 # FastAPI REST + WebSocket API
│   └── static/
│       └── dashboard.html        # Single-page control dashboard (no build step)
├── data/                         # SQLite DB + uploaded images (auto-created)
├── docs/                         # This documentation
├── scripts/                      # Setup/start scripts, systemd units
├── setup.bat / start.bat         # Double-click wrappers for the Windows scripts
├── .env.example                  # Environment template
├── Dockerfile                    # Multi-stage Docker build
├── docker-compose.yml            # Docker deployment (prod + dev)
└── pyproject.toml                # Python project config
```

## Key design decisions

### Ingest pipeline

Each racer slot runs `streamlink <url> --stdout | ffmpeg -c copy -f mpegts udp://127.0.0.1:<port>`.
The stream is **remuxed, never re-encoded**, so latency and CPU cost stay minimal. OBS reads the
UDP port with a Media Source.

On POSIX the stream is duplicated with `tee` into a second, throw-away FFmpeg that decodes
0.5 fps JPEG snapshots for the dashboard previews — decoding is isolated so it can never
back-pressure the real-time copy. On Windows (no `/dev/fd`) a single dual-output FFmpeg
produces both the copy stream and the snapshots.

Feeds auto-reconnect with exponential back-off when they die unexpectedly. Back-off only
resets after a pipeline has stayed alive for 30 s, so an offline channel keeps backing off
instead of hammering Twitch. After 10 consecutive failures the feed is dropped and the
dashboard is notified.

### One input, many scene items

Each racer has a **single** OBS media input (`Racer{N}_Feed`) — two processes reading the
same UDP port would conflict. That input is referenced by multiple scene items, one per
region: `Racer{N}_Game`, `Racer{N}_Tracker`, `Racer{N}_Timer`, plus one per user-defined
custom region. Each item gets its own crop + transform, so one decoded stream feeds every
cut-out. The mapping from logical names to scene-item ids is cached per scene and rebuilt
from OBS when it goes stale (e.g. after manual edits in OBS).

### Templates and coordinate spaces

Template regions, text and images are stored in the template's own coordinate space (the
background image's pixel size, or an explicit canvas size for image-less templates). On
apply, everything is rescaled to the actual OBS canvas resolution, so templates keep
working when the OBS canvas changes.

### Text rendering (WYSIWYG)

Text sources use the platform's native kind, resolved at runtime from `GetInputKindList`
(GDI+ on Windows, FreeType2 elsewhere). GDI sizes fonts by *cell height* (ascent+descent)
while browsers use the *em* size — the dashboard measures the chosen font's cell/em ratio
via canvas metrics and compensates, so the preview matches the OBS render. Text items are
positioned without bounds-stretching (no glyph distortion); alignment is applied within
the text block, matching OBS GDI+ semantics.

### Storage

`data/presets.db` (SQLite) holds crop presets, templates (regions as JSON), and settings
(active template, custom region names). Uploaded images live under `data/template_images/`
and `data/preset_images/`. Schema migrations run automatically at startup.

### Two streaming-app backends

The server holds one active controller behind a common method surface:
`OBSController` (obs-websocket v5) or `SlobsController` (Streamlabs Desktop's
JSON-RPC API on port 59650, SockJS raw-websocket endpoint, token auth).
`POST /api/app` swaps them at runtime; every endpoint is backend-agnostic and
each controller reports `capabilities` so the dashboard can hide what the
active app can't do (Streamlabs: no screenshots, no projector geometry).

Streamlabs specifics worth knowing:

- Scenes/sources are id-based there; the controller keeps name→id caches.
- Scene items have no "bounds": stretch-to-rect is emulated as
  `scale = target / (source_size − crop)`, with target rects remembered per
  item so later crop changes keep the on-screen size.
- Sync (async_delay_filter + audio `syncOffset`), audio monitoring and the
  projector ride Streamlabs' *internal* API — reachable because its remote
  API falls back to internal services. Undocumented, hence every such call
  degrades into a clear error if a future Streamlabs build blocks it.
- "Expensive" calls (`getPropertiesFormData`) are rate-limited by Streamlabs;
  the controller serializes them with a minimum interval.

### The dashboard

A single static HTML file (`src/static/dashboard.html`) with vanilla JS — no build step.
It talks to the REST API and subscribes to `ws://…/ws` for live events (feed state,
reconnects, template applies). Everything the dashboard does is available via the
[API](API.md) for scripting.

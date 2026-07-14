# API Reference

The dashboard is a thin client over this REST + WebSocket API — anything the UI does can be scripted.

- **Base URL**: `http://localhost:8008`
- **Interactive docs** (Swagger UI, generated from the code): `http://localhost:8008/docs`
- All request/response bodies are JSON unless noted. Endpoints that talk to OBS return
  `{"status": "error", "error": "..."}` or an HTTP 502 with a `detail` field when OBS is
  unreachable or rejects a request.

## Ingest

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/ingest/start` | Start a feed `{ slot, url, quality, start_offset }` |
| POST | `/api/ingest/stop` | Stop a feed `{ slot }` |
| POST | `/api/ingest/reconnect` | Restart a feed with its existing settings `{ slot }` |
| GET | `/api/ingest/status` | List all active feeds |
| GET | `/api/ingest/qualities?url=` | Query available stream qualities via streamlink |
| GET | `/api/ingest/preview/{slot}` | Latest preview frame as base64 JPEG |
| GET | `/api/ingest/token` | Check if a Twitch OAuth token is set |
| POST | `/api/ingest/token` | Set/clear the Twitch OAuth token `{ token }` |

## Crop

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/detect/manual` | Apply a crop `{ source_name, x, y, width, height, source_width, source_height }` |
| GET/POST | `/api/custom-regions` | List / add custom crop regions (shared by all racers), `{ name }` |
| DELETE | `/api/custom-regions/{name}` | Remove a custom region |

## Streaming app selection

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/app` | Active app (`obs`/`streamlabs`), connection state, capabilities, saved Streamlabs host/port/token presence |
| POST | `/api/app` | Switch app and save Streamlabs connection settings `{ app, host?, port?, token? }`; reconnects |

`capabilities` (also on `/api/obs/status`) tells clients what the active app supports:
`{ screenshot, projector_geometry, app_audio_capture }` — e.g. Streamlabs Desktop has no
screenshot API, so the dashboard hides the Scene Preview panel when `screenshot` is false.

## OBS Control

The `/api/obs/*` endpoints drive **whichever app is active** (the path prefix is historical).

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/obs/connect` | Connect to the active streaming app |
| POST | `/api/obs/disconnect` | Disconnect |
| POST | `/api/obs/launch` | Launch OBS Studio as a detached process |
| GET | `/api/obs/status` | Connection status, platform, resolved text-source kind |
| GET | `/api/obs/video-settings` | OBS canvas (base) and output resolution |
| GET | `/api/obs/scenes` | List scenes + current program scene |
| POST | `/api/obs/scene` | Switch scene `{ scene_name }` |
| GET | `/api/obs/screenshot` | Base64 JPEG screenshot of the current scene |
| POST | `/api/obs/init` | Re-provision Race Scene sources for all running feeds |
| POST | `/api/obs/crop` | Apply crop values directly `{ source_name, top, bottom, left, right }` |
| POST | `/api/obs/sync` | Nudge sync delay `{ source_name, delta_ms }` |
| GET | `/api/obs/sync?num_slots=` | Current sync delay per racer |
| POST | `/api/obs/stream/start` | Start streaming |
| POST | `/api/obs/stream/stop` | Stop streaming |
| GET | `/api/obs/stream/status` | Live-stream state (active, timecode, dropped frames) |
| POST | `/api/obs/projector` | Open a projector window `{ scene_name, monitor, width, height }` |
| POST | `/api/obs/text` | Create/update a standalone text source |

## Audio

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/obs/audio` | Solo a racer's audio `{ active_slot, num_slots }` (−1 = mute all) |
| GET | `/api/obs/audio?num_slots=` | Mute state per racer |
| POST | `/api/obs/audio/volume` | Set input volume `{ input_name, volume_db }` |
| POST | `/api/obs/audio/mute` | Mute/unmute an input `{ input_name, muted }` |
| GET | `/api/obs/audio/mixer` | Mixer strip states; `?scope=scene` (default) limits to the current scene, `?scope=all` returns everything |
| POST | `/api/obs/audio/discord` | Create the Commentary capture source `{ device_id }` or `{ window }` |
| POST | `/api/obs/audio/monitor` | Set monitoring type `{ input_name, monitor_type }` |
| GET | `/api/obs/audio/devices` | List audio output-capture devices |
| GET | `/api/obs/audio/apps` | List capturable app windows (Windows Application Audio Capture) |

## Templates

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/templates` | List templates |
| POST | `/api/templates/upload?name=` | Create a template from an image (multipart `file`) |
| POST | `/api/templates/blank` | Create an image-less template `{ name, width, height }` |
| GET | `/api/templates/{id}` | Template details incl. base64 background image |
| PUT | `/api/templates/{id}/regions` | Update the region layout `{ regions }` |
| POST | `/api/templates/{id}/apply` | Apply the template to OBS |
| POST | `/api/templates/{id}/region-image?slot=&region=` | Attach an image shown instead of a region (multipart `file`) |
| GET | `/api/templates/{id}/region-image?slot=&region=` | Serve a region image file |
| DELETE | `/api/templates/{id}/region-image?slot=&region=` | Remove a region image |
| DELETE | `/api/templates/{id}` | Delete template (cleans up image files) |
| GET | `/api/active-template` | Currently active template id + slot count |

### Template regions format

Stored per template as JSON:

```json
{
  "num_slots": 2,
  "canvas": { "width": 1920, "height": 1080 },
  "slots": {
    "0": { "game": { "x": 48, "y": 42, "width": 1120, "height": 837 },
           "tracker": null, "timer": null, "deaths": null }
  },
  "texts": [
    { "id": "txt0", "text": "Racer 1", "x": 580, "y": 930,
      "font_size": 48, "color": "#ffffff", "font": "Arial", "align": "left" }
  ],
  "images": {
    "0": { "tracker": { "path": "…", "original_name": "placeholder.png" } }
  }
}
```

All coordinates are in the template's own space (`canvas`, or the background image's pixel
size); the backend rescales everything to the OBS canvas resolution on apply.

## Presets

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/presets?channel=` | List crop presets (optionally filtered by channel) |
| POST | `/api/presets` | Save a preset `{ channel, name, game_crop, tracker_crop, timer_crop, extra_crops }` |
| POST | `/api/presets/{id}/apply?slot=` | Apply a preset's crops + attached images to a racer slot |
| POST | `/api/presets/{id}/image?region=` | Attach an image for a region (multipart `file`) |
| DELETE | `/api/presets/{id}/image?region=` | Remove an attached image |
| DELETE | `/api/presets/{id}` | Delete preset (cleans up image files) |

Crop rectangles carry the resolution they were drawn at (`source_width`/`source_height`)
and are rescaled to the running feed's resolution on apply.

## Health

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Health check (OBS status, feed count) — used by Docker HEALTHCHECK |

## WebSocket events

Connect to `ws://localhost:8008/ws` for real-time events:

```json
{ "event": "ingest:started", "data": { "slot": 0, "url": "...", "local_url": "..." } }
{ "event": "ingest:stopped", "data": { "slot": 0 } }
{ "event": "ingest:reconnecting", "data": { "slot": 0, "attempt": 1, "delay": 3 } }
{ "event": "ingest:reconnected", "data": { "slot": 0, "attempt": 1, "url": "...", "local_url": "..." } }
{ "event": "ingest:reconnect_failed", "data": { "slot": 0, "attempts": 10 } }
{ "event": "obs:connected" } / { "event": "obs:disconnected" }
{ "event": "scene:changed", "data": { "scene": "Race Scene" } }
{ "event": "stream:started" } / { "event": "stream:stopped" }
{ "event": "audio:switched", "data": { "active_slot": 0 } }
{ "event": "audio:volume" } / { "event": "audio:mute" }
{ "event": "regions:changed", "data": { "regions": ["deaths"] } }
{ "event": "template:applied", "data": { "template_id": 1, "template_name": "2-player", "num_slots": 2, "applied": ["..."] } }
```

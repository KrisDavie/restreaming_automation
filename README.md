# ALttP Restreaming Automation

A browser dashboard that makes restreaming races (A Link to the Past Randomizer, or any other game) as simple as possible. It pulls the racers' Twitch streams onto your machine, builds the OBS scene for you — layout, crops, timers, text, audio — and gives you one page to run the whole broadcast from. No more manual cropping, no more fiddling in OBS mid-race.

- **Dashboard**: http://localhost:8008/dashboard (after starting the server)
- Docs: [API reference](docs/API.md) · [Architecture](docs/ARCHITECTURE.md) · [Deployment (Docker, systemd, VMs)](docs/DEPLOYMENT.md)

---

## Quickstart

No git or programming knowledge needed — just install four programs, download the app, and run one script.

### 1. Download the app

**[⬇ Download the latest version (ZIP)](https://github.com/KrisDavie/restreaming_automation/archive/refs/heads/main.zip)**

Extract the ZIP somewhere easy to find (e.g. `C:\restreaming_automation` or your home folder). The extracted folder is called `restreaming_automation-main` — everything below happens inside it.

*(If you use git: `git clone https://github.com/KrisDavie/restreaming_automation.git` works too. To update a ZIP install later, download the new ZIP and copy your old `data/` folder into it — that's where all your layouts and presets live.)*

### 2. Install the prerequisites

You need **OBS Studio**, **Python**, **FFmpeg** and **Streamlink**.

**Windows — easiest way** (Windows 10/11 comes with `winget`): open PowerShell and run

```powershell
winget install OBSProject.OBSStudio Python.Python.3.12 Gyan.FFmpeg Streamlink.Streamlink
```

then close and reopen PowerShell so the new programs are found.

**Windows — using installers instead:**

| Program | Where | Notes |
|---|---|---|
| **OBS Studio** | [obsproject.com](https://obsproject.com/download) | |
| **Python 3.10+** | [python.org](https://www.python.org/downloads/) | ⚠ On the first installer screen, tick **"Add python.exe to PATH"** |
| **FFmpeg** | [gyan.dev builds](https://www.gyan.dev/ffmpeg/builds/) → "release full" | Extract, then add its `bin` folder to PATH — or just use winget for this one, it's much easier |
| **Streamlink** | [streamlink installer](https://streamlink.github.io/install.html#windows-binaries) | The official Windows installer is all you need — this app runs the `streamlink` program directly, so no `pip install` required. (Note: Streamlink's bundled FFmpeg is internal-only; you still need FFmpeg from the row above.) |

**Linux** (Arch/CachyOS shown; the setup script also understands apt/dnf):

```bash
sudo pacman -S obs-studio python ffmpeg streamlink
```

**Finally, in OBS**: *Tools → WebSocket Server Settings* → tick **Enable WebSocket server**, and note/set the **Server Password** — you'll put it in the app's settings next.

### 3. Set up and start

**Windows:**

1. In the app folder, **double-click `setup.bat`**. It checks all prerequisites (and tells you exactly what's missing), sets up the app, and creates a settings file called `.env`.
2. Open `.env` in Notepad and set `OBS_WS_PASSWORD=` to your OBS WebSocket password.
3. **Double-click `start.bat`** to launch the server. Keep that window open while you use the app; close it (or press Ctrl+C in it) to stop.

*(PowerShell users can run `scripts\setup.ps1` / `scripts\start.ps1` directly instead — the `.bat` files are just double-click wrappers that bypass the "running scripts is disabled" policy.)*

**Linux:**

```bash
cd restreaming_automation-main
chmod +x scripts/*.sh
./scripts/setup.sh         # checks prerequisites, sets everything up
# Edit .env — set OBS_WS_PASSWORD to your OBS WebSocket password
./scripts/start.sh         # launches the server (Ctrl+C stops)
```

Now open **http://localhost:8008/dashboard** in your browser (Chrome/Edge recommended). Leave the server window open while you use the app. (Prefer Docker or a server install? See [Deployment](docs/DEPLOYMENT.md).)

### 2. Your first race

The dashboard is laid out in the order you'll use it — the numbered links in the header jump to each step.

1. **Connect OBS** — start OBS (or click *🚀 Launch OBS*), then click **Connect** in the *OBS Control* panel. The OBS dot in the header turns green.
2. **Start the feeds** — in *Stream Ingest*, enter each racer's Twitch URL and click **▶ Start**. A preview thumbnail appears when the feed is up. The app creates all OBS sources for you in a scene called **Race Scene**.
3. **Pick a layout** — in *Layout Template*, upload a background image (or click **➕ Blank** for a plain one), draw where each racer's game/tracker/timer goes, add text labels, then **✓ Apply Layout to OBS**.
4. **Crop the streams** — in the *Crop Tool*, drag a box around each racer's game (and tracker/timer) on their preview frame, then **✓ Apply Crops to OBS**.
5. **Set audio** — in *Audio Mixer*, click **🔊 Solo Racer 1** (or whoever should be heard).
6. **Sync the racers** — if one feed is ahead, delay it in *Sync Nudger* until they match.
7. **Go live** — click **▶ Start Stream**. A pulsing **LIVE** badge with the stream time appears in the header. Stopping asks for confirmation, so no accidental clicks.

Everything you set up (layouts, crops, custom regions) is saved and comes back after a restart.

---

## The dashboard, panel by panel

### Header

Always visible: quick links to each panel, a **LIVE** badge with timecode and dropped-frame count while streaming, and connection dots for OBS and the API server. If the API dot goes red the page reconnects by itself.

### 1 · Stream Ingest

Pulls each racer's stream onto your machine so OBS can use it.

- **Racer slots** — use the **+ / −** buttons in the panel header to set how many racers you have (1–8). Each racer gets a tab; the dot on the tab shows whether their feed is running.
- **URL** — anything streamlink understands: `twitch.tv/racer1`, a full URL, or a **VOD** like `twitch.tv/videos/123?t=1h0m28s` (practice mode!). The `t=` start time is picked up automatically, or set it explicitly in *VOD Start Time* (`1h5m30s`, `00:05:30`, or plain seconds).
- **Quality** — click **🔍 Fetch** to list the actual qualities for that channel, or leave `best`.
- **▶ Start / ■ Stop / ↻ Reconnect** — start creates the pipeline *and* the OBS sources automatically. Reconnect restarts a stuttering feed without retyping anything.
- **📷 Preview** — grabs a fresh frame (updates every ~2 s while the feed runs).
- **Twitch OAuth Token** *(optional but recommended)* — paste your Twitch OAuth token and click **💾 Set** so ingest sessions are authenticated: with Turbo/subs this removes ads. Takes effect on the next start/reconnect.
- **Auto-reconnect** — if a feed drops mid-race the app reconnects it automatically with increasing delays, and tells you in the log. After 10 straight failures it gives up and shows an alert.

### 2 · OBS Control

- **🚀 Launch OBS / Connect / Disconnect** — manage the OBS WebSocket link. Connecting also (re)builds the internal map of the Race Scene.
- **🔄 Re-provision** — rebuilds all Race Scene sources for the currently running feeds. Use it if the scene got messed up in OBS.
- **Scene** — switch the live OBS scene (e.g. to an intermission scene). The dropdown follows the actual current scene.
- **▶ Start Stream / ■ Stop Stream** — the buttons swap depending on whether you're live; stopping asks for confirmation.
- **Scene Preview** — a screenshot of the current OBS output; enable *Auto-refresh* to keep it updating every 5 s.

### 3 · Layout Template

Design where everything sits on the final stream — what you see here is what OBS shows.

- **Create** — *📤 Upload* a background image (JPG/PNG/WebP), or *➕ Blank* for a layout without artwork (a fine grid helps with positioning; the canvas matches your OBS resolution). Templates of any resolution work — everything is rescaled to your OBS canvas on apply.
- **Slots** — *+ Add Slot / − Remove* to change the racer count; the whole dashboard follows the template's slot count when it's applied.
- **Regions** — pick a racer and a region type (🎮 Game / 📊 Tracker / ⏱ Timer / your custom regions), then drag a rectangle on the canvas. Drag to move, drag corners to resize. Every change saves automatically.
- **🖼 Region images** — with a region selected, the image bar lets you **Attach** a picture that is shown *instead of* that racer's live region — e.g. a placeholder for a racer who doesn't run a tracker, or personal art. It previews right on the canvas and hides that racer's feed region in OBS when applied.
- **📝 Text** — add labels (racer names, round names, …). Text renders in the editor exactly as OBS will show it:
  - **Enter** applies, **Shift+Enter** makes a new line.
  - **Font** — click *🔤 System Fonts* once to load your real installed fonts (Chrome/Edge). Fonts must exist on the OBS machine; a ⚠ marks fonts your browser can't preview. *Other…* lets you type any family name.
  - **Align** left/center/right (multi-line text; alignment needs Windows OBS).
  - Size, X/Y and color are editable as numbers, or just drag the text on the canvas.
- **✓ Apply Layout to OBS** — positions everything in the Race Scene. The active template also re-applies automatically whenever a feed starts, so late-starting feeds land in the right place.

### 4 · Crop Tool

Cut the game/tracker/timer out of each racer's full stream. Requires a running feed (for the preview frame).

- Pick the **racer**, pick the **region type**, then drag a box on the preview. Corner handles resize; the X/Y/W/H fields accept exact numbers; the **magnifier** helps with pixel-perfect edges.
- **＋ Region** — add your own region types (e.g. `deaths`, `webcam`). Custom regions apply to **every racer**, get their own color, can be placed in templates, and are saved in presets. **－ Region** removes one.
- **✓ Apply Crops to OBS** — applies *all* racers' drawn regions at once (each against the preview it was drawn on). It re-applies the active template first so positioning and cropping always agree.
- **💾 Crop Presets** — save the current racer's regions under a channel name (`racer_name`) and reuse them next race: **Apply** loads them onto whichever racer is selected, rescaling automatically if the stream quality/resolution changed.
- **🖼 Preset images** — attach images to a preset per region (like template region images, but tied to the racer's channel — their personal placeholder follows them into any race).

### 5 · Audio Mixer

- **Solo buttons** — one click unmutes a racer and mutes everyone else; **🔇 Mute All** silences everything. The highlighted button reflects the actual OBS state.
- **Per-Source Volume** — sliders and mute toggles for each audio source. By default only sources in the **current scene** (plus global audio like Desktop/Mic) are listed so the mixer stays small — untick *Current scene only* to see every input OBS has.
- **🖥️ Discord Screen Share** — pick a resolution and click **📺 Open Projector**: OBS opens a clean window of your scene that you screen-share into the Discord voice channel (guide included in the panel).
- **🎙️ Commentary Audio Capture** — get Discord commentary *into* the stream:
  - **Windows**: click **↻ Apps**, pick Discord from the dropdown of running apps, **Add App Source** — captures only Discord's audio.
  - **Any platform**: **Scan Devices**, pick the output device Discord plays to, **Add Device Source**.
  - Set monitoring to **Monitor Only** so you hear commentary without it echoing back into the stream twice.

### 6 · Sync Nudger

Racers' streams arrive with different delays; this aligns them.

- Pick the racer that is **ahead**, then add delay with the **+100 ms … +5 s** buttons until both feeds show the same moment (use a shared visual cue — a chest opening, a menu). The − buttons take delay back out, **Reset** returns to zero, and the custom field nudges by any amount.
- Delay applies to video *and* audio together, live, without restarting the feed.

---

## Tips & troubleshooting

- **OBS won't connect** — is OBS running, WebSocket server enabled (Tools → WebSocket Server Settings), and does `OBS_WS_PASSWORD` in `.env` match? The error message in the OBS panel says what failed.
- **No preview frame** — the feed must actually be running (green dot). Offline channels retry automatically; check the Ingest log.
- **Ads on the ingested streams** — set your Twitch OAuth token in the Ingest panel.
- **Text looks different in OBS than in the editor** — almost always a font that isn't installed on one of the two machines (⚠ in the font list). Use *🔤 System Fonts* and pick something both machines have.
- **A racer's layout is broken mid-race** — *🔄 Re-provision* in OBS Control rebuilds the scene sources; *✓ Apply Layout* + *✓ Apply Crops* puts everything back.
- **Feed keeps dropping** — the app auto-reconnects with backoff; if the channel itself is unstable, try a lower fixed quality instead of `best`.
- **Where is my data?** — everything lives in `data/` (SQLite + images). Back that folder up and you keep your templates and presets.
- **Security note** — the dashboard has no login. Keep port 8008 on your LAN / firewalled; don't expose it to the internet.

## More documentation

- [docs/API.md](docs/API.md) — full REST + WebSocket reference (the dashboard is just a client; everything is scriptable)
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — how the pieces fit together and why
- [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) — Docker, systemd, VM notes, headless OBS, environment variables

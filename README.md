# ALttP Restreaming Automation

A browser dashboard that makes restreaming races (A Link to the Past Randomizer, or any other game) as simple as possible. It pulls the racers' Twitch streams onto your machine, builds the scene for you — layout, crops, timers, text, audio — and gives you one page to run the whole broadcast from. No more manual cropping, no more fiddling in your streaming app mid-race.

Works with **OBS Studio** and **Streamlabs Desktop** (see [Using Streamlabs Desktop](#using-streamlabs-desktop)).

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

### 2 · OBS / Streamlabs Control

- **Streaming App** — choose **OBS Studio** or **Streamlabs Desktop** and click **Use**. The dashboard reconnects and adapts to the selected app (see [Using Streamlabs Desktop](#using-streamlabs-desktop) for the token setup). Your choice is remembered.
- **🚀 Launch / Connect / Disconnect** — manage the connection to the selected app. Connecting also (re)builds the internal map of the Race Scene.
- **🔄 Re-provision** — rebuilds all Race Scene sources for the currently running feeds. Use it if the scene got messed up.
- **Scene** — switch the live scene (e.g. to an intermission scene). The dropdown follows the actual current scene.
- **▶ Start Stream / ■ Stop Stream** — the buttons swap depending on whether you're live; stopping asks for confirmation.
- **Scene Preview** — a screenshot of the current output; enable *Auto-refresh* to keep it updating every 5 s. *(OBS Studio only — Streamlabs has no screenshot API, so this panel hides itself.)*

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

## Using Streamlabs Desktop

The app can drive Streamlabs Desktop (formerly Streamlabs OBS) instead of OBS Studio:

1. In Streamlabs Desktop, open **Settings → Mobile** and find **Third Party Connections**.
2. Enable **Allow third party connections**, then copy the **API Token** shown there (the *IP Addresses* box lists the addresses Streamlabs accepts connections on; the *Port* is normally 59650).
3. In the dashboard's *Control* panel, set **Streaming App** to *Streamlabs Desktop*, paste the token, and click **Use**. The dashboard tries `127.0.0.1` and this machine's LAN address automatically; if Streamlabs runs on a different machine, set its IP (one from the *IP Addresses* box) as Host.
4. If the connection is refused, restart Streamlabs Desktop after enabling the toggle and try again.

Everything works the same as with OBS — layouts, crops, custom regions, text (GDI+ on Windows, same WYSIWYG preview), region images, audio mixing, sync, going live — with these differences:

- **Scene Preview is unavailable** (Streamlabs has no screenshot API); the panel hides itself.
- **Projector size can't be preset** — *Open Projector* still works, just resize the window it opens by hand before sharing it to Discord.
- Sync, audio monitoring and the projector use Streamlabs' *internal* API, which Streamlabs doesn't officially document. It works today (the same mechanism Stream Deck-style tools rely on), but a future Streamlabs update could restrict it — if that happens the dashboard shows a clear "your Streamlabs version may block it" error rather than breaking.
- Streamlabs Desktop runs on **Windows and macOS only**. If your dashboard server runs on another machine, use one of the addresses from Streamlabs' *IP Addresses* box as Host.

### Windows port conflict ("connection refused" even though everything is enabled)

Windows' NAT service (**winnat**, used by WSL2 / Hyper-V / Docker Desktop) hands out ports from a dynamic pool (49152+) that **includes Streamlabs' ports 59650/59651**. If Windows reserves them before Streamlabs starts, Streamlabs silently can't listen and every connection is refused.

**Diagnose** (any terminal):

```
netsh int ipv4 show excludedportrange protocol=tcp
```

If 59650 falls inside one of the listed ranges, that's the conflict.

**Fix permanently** — open *Command Prompt as Administrator* and run:

```
net stop winnat
netsh int ipv4 add excludedportrange protocol=tcp startport=59650 numberofports=2
net start winnat
```

then restart Streamlabs Desktop. This reserves 59650–59651 so Windows never hands them to WSL/Hyper-V again; the exclusion survives reboots (it shows with a `*` in the diagnose listing).

**Consequences, so you know what you're trading:** stopping `winnat` briefly drops WSL2/Hyper-V/Docker networking (do it while those aren't busy — they recover when it restarts), and those two ports are permanently withheld from the dynamic pool — harmless unless some other software is explicitly configured to use exactly 59650/59651. Without the `netsh` exclusion, a plain `net stop winnat` + `net start winnat` also frees the port, but only until some future boot wins the race again.

## Tips & troubleshooting

- **OBS won't connect** — is OBS running, WebSocket server enabled (Tools → WebSocket Server Settings), and does `OBS_WS_PASSWORD` in `.env` match? The error message in the panel says what failed.
- **Streamlabs won't connect** — is *Allow third party connections* enabled (Settings → Mobile), and did you paste the current API Token (it changes if you click *Generate new*)? A "connection refused" error usually means Streamlabs isn't listening: on Windows this is often the **winnat port conflict** — see [Windows port conflict](#windows-port-conflict-connection-refused-even-though-everything-is-enabled). Otherwise restart Streamlabs after enabling the toggle, and/or set Host to one of the addresses from its *IP Addresses* box.
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

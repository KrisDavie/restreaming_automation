"""Ingest Layer – manage Streamlink→FFmpeg pipelines for each racer feed."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import signal
import shutil
import sys
import tempfile
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Coroutine, Optional

from .config import Config

log = logging.getLogger(__name__)

# Auto-reconnect constants
_RECONNECT_DELAY_INITIAL = 3     # seconds before first retry
_RECONNECT_DELAY_MAX = 30        # max back-off
_RECONNECT_MAX_ATTEMPTS = 10     # give up after this many consecutive failures
_RECONNECT_STABLE_SECS = 30      # pipeline must survive this long to reset back-off


def parse_vod_offset(url: str, explicit_offset: str = "") -> int:
    """Parse a VOD start offset from a URL ?t= parameter or explicit value.

    Supports formats:
        - Plain seconds: "3628"
        - Timestamp: "1:00:28", "00:05:30"
        - Twitch-style: "1h0m28s", "5m30s"
    Returns offset in seconds.
    """
    raw = explicit_offset.strip()

    # If no explicit offset, try to extract ?t= from the URL
    if not raw:
        m = re.search(r'[?&]t=([^&]+)', url)
        if m:
            raw = m.group(1)

    if not raw:
        return 0

    # Twitch-style: 1h0m28s
    twitch = re.match(r'^(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$', raw)
    if twitch and any(twitch.groups()):
        h = int(twitch.group(1) or 0)
        m = int(twitch.group(2) or 0)
        s = int(twitch.group(3) or 0)
        return h * 3600 + m * 60 + s

    # Colon-separated: HH:MM:SS or MM:SS
    colon = re.match(r'^(\d+):(\d+)(?::(\d+))?$', raw)
    if colon:
        parts = [int(x) for x in raw.split(":")]
        if len(parts) == 3:
            return parts[0] * 3600 + parts[1] * 60 + parts[2]
        elif len(parts) == 2:
            return parts[0] * 60 + parts[1]

    # Plain seconds
    try:
        return int(raw)
    except ValueError:
        return 0


class IngestProtocol(str, Enum):
    UDP = "udp"
    SRT = "srt"


@dataclass
class IngestFeed:
    """Represents a single racer's stream ingest pipeline."""

    slot: int  # 0-based slot index
    url: str  # e.g. "twitch.tv/racer1"
    quality: str = "best"
    local_port: int = 0
    protocol: IngestProtocol = IngestProtocol.UDP
    start_offset: int = 0  # VOD start offset in seconds
    started_at: float = 0.0  # wall-clock time of the last (re)spawn
    process: Optional[asyncio.subprocess.Process] = field(default=None, repr=False)
    _sl_proc: Optional[asyncio.subprocess.Process] = field(default=None, repr=False)
    _tee_proc: Optional[asyncio.subprocess.Process] = field(default=None, repr=False)
    _snap_proc: Optional[asyncio.subprocess.Process] = field(default=None, repr=False)
    _monitor_task: Optional[asyncio.Task[None]] = field(default=None, repr=False)

    @property
    def local_url(self) -> str:
        proto = self.protocol.value
        if self.protocol == IngestProtocol.SRT:
            return f"srt://127.0.0.1:{self.local_port}?mode=listener"
        return f"{proto}://127.0.0.1:{self.local_port}?pkt_size=1316"

    @property
    def obs_input_url(self) -> str:
        """URL that OBS Media Source should point to."""
        if self.protocol == IngestProtocol.SRT:
            return f"srt://127.0.0.1:{self.local_port}?mode=caller"
        return f"udp://127.0.0.1:{self.local_port}?buffer_size=1048576&fifo_size=1000000"

    @property
    def snapshot_path(self) -> Path:
        """Path to the periodic snapshot JPEG written by FFmpeg."""
        return Path(tempfile.gettempdir()) / f"restream_slot{self.slot}_preview.jpg"


class IngestManager:
    """Spawns and manages Streamlink + FFmpeg processes for all racer feeds."""

    # Type alias for the async event callback
    EventCallback = Callable[[str, dict[str, Any]], Coroutine[Any, Any, None]]

    def __init__(self, config: Config, on_event: EventCallback | None = None) -> None:
        self._config = config
        self._feeds: dict[int, IngestFeed] = {}
        self._lock = asyncio.Lock()
        self._on_event = on_event
        # Twitch OAuth token — can be set at runtime too
        self._twitch_token: str = config.twitch_oauth_token
        # Per-slot flag to disable auto-reconnect (set on explicit stop)
        self._reconnect_enabled: dict[int, bool] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def feeds(self) -> dict[int, IngestFeed]:
        return dict(self._feeds)

    @property
    def twitch_token(self) -> str:
        return self._twitch_token

    @twitch_token.setter
    def twitch_token(self, value: str) -> None:
        self._twitch_token = value.strip()

    async def _emit(self, event: str, data: dict[str, Any]) -> None:
        """Fire the event callback if one was registered."""
        if self._on_event:
            try:
                await self._on_event(event, data)
            except Exception:
                log.debug("Event callback error for %s", event, exc_info=True)

    async def start_feed(
        self, slot: int, url: str, quality: str = "best", start_offset: str = "",
    ) -> IngestFeed:
        """Start an ingest pipeline for the given slot."""
        async with self._lock:
            if slot in self._feeds and self._feeds[slot].process is not None:
                log.info("Stopping existing feed on slot %d before starting new one", slot)
                await self._stop_feed_unlocked(slot)

            port = self._config.ingest_base_port + slot
            protocol = IngestProtocol(self._config.ingest_protocol)
            offset_secs = parse_vod_offset(url, start_offset)
            feed = IngestFeed(
                slot=slot,
                url=url,
                quality=quality,
                local_port=port,
                protocol=protocol,
                start_offset=offset_secs,
            )
            try:
                await self._spawn_pipeline(feed)
            except BaseException:
                # Reap anything that did get spawned before the failure
                await self._kill_feed_procs(feed)
                raise
            self._feeds[slot] = feed
            self._reconnect_enabled[slot] = True
            # Launch a background task that watches for process exit
            feed._monitor_task = asyncio.create_task(
                self._monitor_feed(slot), name=f"monitor-slot-{slot}",
            )
            log.info("Started ingest slot=%d  url=%s  → %s", slot, url, feed.local_url)
            return feed

    async def stop_feed(self, slot: int) -> None:
        """Stop a specific feed (disables auto-reconnect)."""
        async with self._lock:
            self._reconnect_enabled[slot] = False
            await self._stop_feed_unlocked(slot)

    async def stop_all(self) -> None:
        """Terminate all running feeds."""
        async with self._lock:
            for slot in list(self._feeds):
                self._reconnect_enabled[slot] = False
                await self._stop_feed_unlocked(slot)

    def get_feed(self, slot: int) -> IngestFeed | None:
        return self._feeds.get(slot)

    async def query_qualities(self, url: str) -> list[str]:
        """Ask streamlink which stream qualities are available for *url*.

        Returns a list of quality names sorted best-first, e.g.
        ``['1080p60', '1080p', '720p60', '720p', '480p', '360p', '160p', 'audio_only']``.
        The special names ``best`` and ``worst`` are prepended.
        """
        streamlink_bin = shutil.which("streamlink")
        if not streamlink_bin:
            raise RuntimeError("streamlink not found on PATH")

        proc = await asyncio.create_subprocess_exec(
            streamlink_bin, "--json", url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
        except asyncio.TimeoutError:
            await self._kill_proc(proc)
            raise RuntimeError("streamlink took too long to respond (30s)")

        import json
        try:
            data = json.loads(stdout)
        except (json.JSONDecodeError, ValueError):
            return ["best", "worst"]

        streams = data.get("streams", {})
        if not streams:
            return ["best", "worst"]

        # Build ordered list: resolution qualities first (descending), then specials
        resolution_order: list[str] = []
        specials: list[str] = []
        for name in streams:
            if name in ("best", "worst"):
                continue
            # Prioritize by approximate pixel height for sorting
            m = re.match(r'(\d+)p', name)
            if m:
                resolution_order.append(name)
            else:
                specials.append(name)

        # Sort resolution streams descending by height then fps
        def _sort_key(n: str) -> tuple[int, int]:
            m = re.match(r'(\d+)p(\d+)?', n)
            if m:
                return (-int(m.group(1)), -int(m.group(2) or 0))
            return (0, 0)

        resolution_order.sort(key=_sort_key)

        return ["best"] + resolution_order + specials + ["worst"]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _kill_feed_procs(self, feed: IngestFeed) -> None:
        """Terminate every process belonging to a feed's pipeline."""
        await self._kill_proc(feed.process)
        await self._kill_proc(feed._snap_proc)
        await self._kill_proc(feed._tee_proc)
        await self._kill_proc(feed._sl_proc)

    async def _stop_feed_unlocked(self, slot: int) -> None:
        feed = self._feeds.pop(slot, None)
        if feed is None:
            return
        # Cancel the monitor task first so it doesn't try to reconnect
        if feed._monitor_task and not feed._monitor_task.done():
            feed._monitor_task.cancel()
            try:
                await feed._monitor_task
            except asyncio.CancelledError:
                pass
        await self._kill_feed_procs(feed)
        # Clean up snapshot file
        try:
            feed.snapshot_path.unlink(missing_ok=True)
        except OSError:
            pass
        log.info("Stopped ingest slot=%d", slot)

    @staticmethod
    async def _kill_proc(proc: asyncio.subprocess.Process | None) -> None:
        """Terminate a subprocess, escalating to kill on timeout."""
        if proc is None or proc.returncode is not None:
            return
        try:
            if sys.platform == "win32":
                proc.kill()
            else:
                proc.send_signal(signal.SIGTERM)
            await asyncio.wait_for(proc.wait(), timeout=5)
        except (ProcessLookupError, asyncio.TimeoutError):
            try:
                proc.kill()
            except ProcessLookupError:
                pass

    _tee_flags_cache: list[str] | None = None

    @classmethod
    def _tee_flags(cls, tee_bin: str) -> list[str]:
        """Return extra flags for tee (probed once, then cached).

        GNU tee needs ``-p`` so that the snapshot FFmpeg dying does not
        SIGPIPE tee and take the main copy stream down with it.
        """
        if cls._tee_flags_cache is None:
            flags: list[str] = []
            try:
                import subprocess
                out = subprocess.run(
                    [tee_bin, "--version"], capture_output=True, timeout=5,
                ).stdout.decode(errors="replace")
                if "GNU coreutils" in out:
                    flags = ["-p"]
            except Exception:
                pass
            cls._tee_flags_cache = flags
        return cls._tee_flags_cache

    async def _spawn_pipeline(self, feed: IngestFeed) -> None:
        """Spawn `streamlink ... --stdout | ffmpeg ... <protocol>://...`.

        On POSIX with tee available, the stream is duplicated to a separate
        lightweight FFmpeg for periodic dashboard preview snapshots so that
        video decoding for thumbnails cannot create back-pressure on the
        real-time copy stream.  Elsewhere (Windows, or no tee) a single
        FFmpeg writes both the copy stream and the snapshots — tee's
        /dev/fd + pass_fds tricks do not exist on Windows.

        Processes are assigned to *feed* (``_sl_proc``, ``_tee_proc``,
        ``_snap_proc``, ``process``) as soon as they are spawned so a
        failure part-way can always be cleaned up via _kill_feed_procs.
        """
        streamlink_bin = shutil.which("streamlink")
        ffmpeg_bin = shutil.which("ffmpeg")
        if not streamlink_bin:
            raise RuntimeError("streamlink not found on PATH")
        if not ffmpeg_bin:
            raise RuntimeError("ffmpeg not found on PATH")

        # Detect whether this is a VOD URL (Twitch /videos/ID or similar)
        is_vod = "/videos/" in feed.url or "/video/" in feed.url

        # We chain two processes via an OS pipe: streamlink stdout → ffmpeg stdin.
        # Using os.pipe() instead of asyncio.subprocess.PIPE avoids the
        # StreamReader/fileno incompatibility with uvloop.
        streamlink_cmd = [
            streamlink_bin,
            feed.url,
            feed.quality,
            "--stdout",
        ]

        # Twitch OAuth token — authenticates the session to bypass ads
        if self._twitch_token:
            streamlink_cmd += [
                "--twitch-api-header",
                f"Authorization=OAuth {self._twitch_token}",
            ]

        # Live-stream-only flags – skip for VODs
        if not is_vod:
            streamlink_cmd += [
                "--twitch-low-latency",
                "--twitch-disable-ads",
            ]
        else:
            # VOD: apply start offset if provided
            if feed.start_offset > 0:
                streamlink_cmd += [
                    "--hls-start-offset",
                    str(feed.start_offset),
                ]

        ffmpeg_output = feed.local_url
        snapshot = str(feed.snapshot_path)
        copy_args = [
            "-fflags", "+genpts+discardcorrupt",
            "-i", "pipe:0",
            # Pure passthrough – no decode, no filtering
            "-map", "0",
            "-c", "copy",
            "-avoid_negative_ts", "make_zero",
            "-flush_packets", "1",
            "-f", "mpegts",
            ffmpeg_output,
        ]
        snap_args = [
            "-map", "0:v:0",
            "-vf", "fps=0.5",
            "-q:v", "2",
            "-update", "1",
            "-y",
            snapshot,
        ]

        feed.process = None
        feed._sl_proc = None
        feed._tee_proc = None
        feed._snap_proc = None

        # A snapshot left over from a previous run (unclean shutdown) would
        # otherwise be served as this feed's preview — remove it and record
        # the spawn time so the API can reject stale frames.
        try:
            feed.snapshot_path.unlink(missing_ok=True)
        except OSError:
            pass
        import time
        feed.started_at = time.time()

        tee_bin = shutil.which("tee") if sys.platform != "win32" else None

        # Track pipe fds so none leak if a spawn throws part-way through
        open_fds: set[int] = set()

        def _pipe() -> tuple[int, int]:
            r, w = os.pipe()
            open_fds.update((r, w))
            return r, w

        def _close(fd: int) -> None:
            os.close(fd)
            open_fds.discard(fd)

        try:
            # Start streamlink – writes into sl_w
            sl_r, sl_w = _pipe()
            feed._sl_proc = await asyncio.create_subprocess_exec(
                *streamlink_cmd,
                stdout=sl_w,
                stderr=asyncio.subprocess.DEVNULL,
            )
            _close(sl_w)

            if tee_bin:
                # POSIX pipe architecture:
                #   streamlink stdout → tee stdin
                #   tee stdout        → main FFmpeg stdin  (pure -c copy)
                #   tee /dev/fd/N     → snap FFmpeg stdin   (decode for thumbnails)
                #
                # tee(1) keeps the two FFmpeg processes fully independent so
                # video decoding for snapshots cannot stall the copy stream.
                # GNU tee gets -p so a dead snapshot FFmpeg can't SIGPIPE tee.
                ff_r, ff_w = _pipe()   # tee stdout   → main ffmpeg stdin
                sn_r, sn_w = _pipe()   # tee write-fd → snapshot ffmpeg stdin
                feed._tee_proc = await asyncio.create_subprocess_exec(
                    tee_bin, *self._tee_flags(tee_bin), f"/dev/fd/{sn_w}",
                    stdin=sl_r,
                    stdout=ff_w,
                    stderr=asyncio.subprocess.DEVNULL,
                    pass_fds=(sn_w,),
                )
                _close(sl_r)
                _close(ff_w)
                _close(sn_w)

                feed.process = await asyncio.create_subprocess_exec(
                    ffmpeg_bin, "-hide_banner", "-loglevel", "warning", *copy_args,
                    stdin=ff_r,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                _close(ff_r)

                feed._snap_proc = await asyncio.create_subprocess_exec(
                    ffmpeg_bin, "-hide_banner", "-loglevel", "error",
                    "-fflags", "+discardcorrupt", "-i", "pipe:0", *snap_args,
                    stdin=sn_r,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                _close(sn_r)
            else:
                # Windows (no /dev/fd, no pass_fds) or minimal systems without
                # tee: one FFmpeg produces both the copy stream and snapshots.
                feed.process = await asyncio.create_subprocess_exec(
                    ffmpeg_bin, "-hide_banner", "-loglevel", "warning",
                    *copy_args, *snap_args,
                    stdin=sl_r,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                _close(sl_r)
        finally:
            for fd in list(open_fds):
                try:
                    os.close(fd)
                except OSError:
                    pass

    # ------------------------------------------------------------------
    # Auto-reconnect monitor
    # ------------------------------------------------------------------

    async def _monitor_feed(self, slot: int) -> None:
        """Watch for unexpected feed exit and attempt to reconnect.

        Runs as a background task for each active feed.  If the ffmpeg
        (or streamlink) process exits while reconnect is still enabled,
        the pipeline is respawned with exponential back-off.
        """
        delay = _RECONNECT_DELAY_INITIAL
        attempts = 0

        while True:
            feed = self._feeds.get(slot)
            if feed is None:
                return

            if feed.process is not None:
                # Wait for the ffmpeg process to exit
                started = asyncio.get_running_loop().time()
                try:
                    await feed.process.wait()
                except asyncio.CancelledError:
                    return
                uptime = asyncio.get_running_loop().time() - started

                # Only a pipeline that ran for a while counts as a working
                # stream.  A spawn that dies immediately (offline channel,
                # bad URL) must keep backing off instead of respawning every
                # few seconds forever.
                if uptime >= _RECONNECT_STABLE_SECS:
                    attempts = 0
                    delay = _RECONNECT_DELAY_INITIAL

            # Process has exited (or never spawned) — should we reconnect?
            if not self._reconnect_enabled.get(slot, False):
                return

            # Ensure the rest of the pipeline is dead before respawning
            await self._kill_feed_procs(feed)

            attempts += 1
            if attempts > _RECONNECT_MAX_ATTEMPTS:
                log.warning("Slot %d: giving up after %d reconnect attempts", slot, attempts)
                async with self._lock:
                    if self._feeds.get(slot) is feed:
                        self._feeds.pop(slot, None)
                try:
                    feed.snapshot_path.unlink(missing_ok=True)
                except OSError:
                    pass
                await self._emit("ingest:reconnect_failed", {
                    "slot": slot, "attempts": attempts,
                })
                return

            log.warning(
                "Slot %d: feed exited (rc=%s), reconnecting in %ds (attempt %d/%d)",
                slot, getattr(feed.process, "returncode", "?"), delay,
                attempts, _RECONNECT_MAX_ATTEMPTS,
            )
            await self._emit("ingest:reconnecting", {
                "slot": slot, "attempt": attempts, "delay": delay,
            })

            await asyncio.sleep(delay)
            delay = min(delay * 2, _RECONNECT_DELAY_MAX)

            # Respawn under the manager lock so a concurrent stop_feed /
            # start_feed cannot interleave and orphan a fresh pipeline.
            async with self._lock:
                if not self._reconnect_enabled.get(slot, False):
                    return
                try:
                    await self._spawn_pipeline(feed)
                    self._feeds[slot] = feed
                    log.info("Slot %d: reconnected (attempt %d)", slot, attempts)
                except asyncio.CancelledError:
                    await self._kill_feed_procs(feed)
                    raise
                except Exception as exc:
                    log.warning("Slot %d: reconnect spawn failed: %s", slot, exc)
                    await self._kill_feed_procs(feed)
                    continue
            await self._emit("ingest:reconnected", {
                "slot": slot, "attempt": attempts,
                "url": feed.url, "local_url": feed.obs_input_url,
            })
            # Back-off is reset only after the pipeline proves stable
            # (see uptime check above), not merely because it spawned.

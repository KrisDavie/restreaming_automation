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
from typing import Optional

from .config import Config

log = logging.getLogger(__name__)


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
    process: Optional[asyncio.subprocess.Process] = field(default=None, repr=False)

    @property
    def local_url(self) -> str:
        proto = self.protocol.value
        if self.protocol == IngestProtocol.SRT:
            return f"srt://127.0.0.1:{self.local_port}?mode=listener"
        return f"{proto}://127.0.0.1:{self.local_port}"

    @property
    def obs_input_url(self) -> str:
        """URL that OBS Media Source should point to."""
        if self.protocol == IngestProtocol.SRT:
            return f"srt://127.0.0.1:{self.local_port}?mode=caller"
        return f"udp://127.0.0.1:{self.local_port}"

    @property
    def snapshot_path(self) -> Path:
        """Path to the periodic snapshot JPEG written by FFmpeg."""
        return Path(tempfile.gettempdir()) / f"restream_slot{self.slot}_preview.jpg"


class IngestManager:
    """Spawns and manages Streamlink + FFmpeg processes for all racer feeds."""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._feeds: dict[int, IngestFeed] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def feeds(self) -> dict[int, IngestFeed]:
        return dict(self._feeds)

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
            proc = await self._spawn_pipeline(feed)
            feed.process = proc
            self._feeds[slot] = feed
            log.info("Started ingest slot=%d  url=%s  → %s", slot, url, feed.local_url)
            return feed

    async def stop_feed(self, slot: int) -> None:
        """Stop a specific feed."""
        async with self._lock:
            await self._stop_feed_unlocked(slot)

    async def stop_all(self) -> None:
        """Terminate all running feeds."""
        async with self._lock:
            for slot in list(self._feeds):
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
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30)

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

    async def _stop_feed_unlocked(self, slot: int) -> None:
        feed = self._feeds.pop(slot, None)
        if feed is None or feed.process is None:
            return
        try:
            if sys.platform == "win32":
                # Windows doesn't support SIGTERM on subprocesses reliably
                feed.process.kill()
            else:
                feed.process.send_signal(signal.SIGTERM)
            await asyncio.wait_for(feed.process.wait(), timeout=5)
        except (ProcessLookupError, asyncio.TimeoutError):
            feed.process.kill()
        # Clean up snapshot file
        try:
            feed.snapshot_path.unlink(missing_ok=True)
        except OSError:
            pass
        log.info("Stopped ingest slot=%d", slot)

    async def _spawn_pipeline(self, feed: IngestFeed) -> asyncio.subprocess.Process:
        """Spawn `streamlink ... --stdout | ffmpeg ... <protocol>://...`."""
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
        ffmpeg_cmd = [
            ffmpeg_bin,
            "-hide_banner",
            "-loglevel", "warning",
            # No -re: streamlink already delivers at real-time rate.
            # Adding -re causes buffering issues and stuttery playback.
            "-i", "pipe:0",
            # Output 1: MPEG-TS stream to OBS (passthrough, no decode)
            "-map", "0",
            "-c", "copy",
            "-f", "mpegts",
            ffmpeg_output,
            # Output 2: periodic snapshot JPEG for dashboard preview
            "-map", "0:v:0",
            "-vf", "fps=0.5",
            "-q:v", "4",
            "-update", "1",
            "-y",
            snapshot,
        ]

        log.debug("Streamlink cmd: %s", streamlink_cmd)
        log.debug("FFmpeg cmd: %s", ffmpeg_cmd)

        # Create a real OS pipe so both subprocesses share a kernel‐level
        # file descriptor.  This works with uvloop (which rejects asyncio
        # StreamReader objects that lack fileno()).
        r_fd, w_fd = os.pipe()

        # Start streamlink – writes into the pipe
        sl_proc = await asyncio.create_subprocess_exec(
            *streamlink_cmd,
            stdout=w_fd,
            stderr=asyncio.subprocess.DEVNULL,
        )

        # Parent no longer needs the write end
        os.close(w_fd)

        # Start ffmpeg – reads from the pipe
        ff_proc = await asyncio.create_subprocess_exec(
            *ffmpeg_cmd,
            stdin=r_fd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )

        # Parent no longer needs the read end
        os.close(r_fd)

        # We track the ffmpeg process as the "main" process; killing it
        # will cause streamlink to get a broken pipe and exit.
        return ff_proc

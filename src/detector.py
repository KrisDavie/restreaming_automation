"""Analysis Layer – OpenCV-based auto-detection of ALttP game regions.

Given a screenshot of a raw ingest feed, this module locates the gameplay area
and item tracker by matching template images (e.g. the green health bar, the
item box border) against the frame.  It returns normalised crop coordinates
that can be forwarded to OBS via the control layer.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import cv2
import numpy as np
from numpy.typing import NDArray

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CropRect:
    """Pixel-level crop rectangle (top-left origin)."""

    x: int
    y: int
    width: int
    height: int

    def to_dict(self) -> dict[str, int]:
        return {"x": self.x, "y": self.y, "width": self.width, "height": self.height}

    def to_obs_crop(self, source_w: int, source_h: int) -> dict[str, int]:
        """Convert to OBS Crop/Pad filter values (top/bottom/left/right)."""
        return {
            "left": self.x,
            "top": self.y,
            "right": max(0, source_w - (self.x + self.width)),
            "bottom": max(0, source_h - (self.y + self.height)),
        }


@dataclass(frozen=True)
class DetectionResult:
    """Result of an auto-detect pass on one frame."""

    success: bool
    game_crop: CropRect | None = None
    tracker_crop: CropRect | None = None
    confidence: float = 0.0
    debug_frame: NDArray[np.uint8] | None = None


# ---------------------------------------------------------------------------
# Template cache
# ---------------------------------------------------------------------------

class TemplateStore:
    """Load and cache template images from the templates directory."""

    def __init__(self, templates_dir: Path) -> None:
        self._dir = templates_dir
        self._cache: dict[str, NDArray[np.uint8]] = {}

    def get(self, name: str) -> NDArray[np.uint8]:
        if name not in self._cache:
            path = self._dir / name
            if not path.exists():
                raise FileNotFoundError(f"Template not found: {path}")
            img = cv2.imread(str(path), cv2.IMREAD_COLOR)
            if img is None:
                raise ValueError(f"Failed to read template image: {path}")
            self._cache[name] = img
            log.debug("Loaded template '%s'  shape=%s", name, img.shape)
        return self._cache[name]

    def list_templates(self) -> list[str]:
        if not self._dir.exists():
            return []
        return [p.name for p in self._dir.iterdir() if p.suffix in (".png", ".jpg", ".bmp")]


# ---------------------------------------------------------------------------
# Detector
# ---------------------------------------------------------------------------

# Standard ALttP aspect ratio
_GAME_ASPECT = 4 / 3


class GameDetector:
    """Detect the ALttP game window inside an arbitrary stream frame.

    Strategy
    --------
    1.  Match a provided anchor template (e.g. hearts / magic bar) inside the
        frame using ``cv2.matchTemplate``.
    2.  From the anchor location, infer the full 4:3 game bounding box using
        known positional offsets (anchor is at a fixed position in the SNES
        output).
    3.  Optionally detect the item tracker region in a second pass.
    """

    # Anchor offsets – these describe *where* in the 256×224 SNES output the
    # anchor template normally sits.  Values are fractions of the full game
    # area.  Adjust once you calibrate your template.
    #
    # Default: HUD hearts row is roughly at x=65%, y=0%, spanning ~30% width
    #          and ~6% height of the 256×224 area.
    ANCHOR_OFFSET_X_FRAC: float = 0.65
    ANCHOR_OFFSET_Y_FRAC: float = 0.0

    # Item tracker is usually placed by the overlay somewhere to the right of
    # the game area.  We define a search region relative to the game box.
    TRACKER_SEARCH_RIGHT_FRAC: float = 1.05  # 5 % right of game right edge
    TRACKER_SEARCH_WIDTH_FRAC: float = 0.40  # up to 40 % of game width

    def __init__(
        self,
        templates: TemplateStore,
        confidence_threshold: float = 0.75,
        anchor_template: str = "hearts.png",
    ) -> None:
        self._templates = templates
        self._threshold = confidence_threshold
        self._anchor_name = anchor_template

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def detect(self, frame: NDArray[np.uint8], *, debug: bool = False) -> DetectionResult:
        """Run detection on a BGR frame.  Returns a ``DetectionResult``."""
        try:
            anchor = self._templates.get(self._anchor_name)
        except FileNotFoundError:
            log.warning("Anchor template '%s' not found – skipping detection", self._anchor_name)
            return DetectionResult(success=False)

        match_loc, confidence = self._match_template(frame, anchor)
        if confidence < self._threshold:
            log.info(
                "Anchor match below threshold (%.2f < %.2f)", confidence, self._threshold
            )
            return DetectionResult(success=False, confidence=confidence)

        game_crop = self._infer_game_rect(frame, anchor, match_loc)
        tracker_crop = self._find_tracker(frame, game_crop)

        debug_frame = None
        if debug:
            debug_frame = self._draw_debug(frame, game_crop, tracker_crop)

        return DetectionResult(
            success=True,
            game_crop=game_crop,
            tracker_crop=tracker_crop,
            confidence=confidence,
            debug_frame=debug_frame,
        )

    def detect_from_file(self, path: str | Path, *, debug: bool = False) -> DetectionResult:
        """Convenience: load an image file and run detection."""
        frame = cv2.imread(str(path), cv2.IMREAD_COLOR)
        if frame is None:
            raise ValueError(f"Cannot read image: {path}")
        return self.detect(frame, debug=debug)

    # ------------------------------------------------------------------
    # Template matching
    # ------------------------------------------------------------------

    @staticmethod
    def _match_template(
        frame: NDArray[np.uint8], template: NDArray[np.uint8]
    ) -> tuple[tuple[int, int], float]:
        """Return (top-left x,y) and confidence of the best match."""
        result = cv2.matchTemplate(frame, template, cv2.TM_CCOEFF_NORMED)
        _, max_val, _, max_loc = cv2.minMaxLoc(result)
        return (int(max_loc[0]), int(max_loc[1])), float(max_val)

    # ------------------------------------------------------------------
    # Geometry inference
    # ------------------------------------------------------------------

    def _infer_game_rect(
        self,
        frame: NDArray[np.uint8],
        anchor: NDArray[np.uint8],
        anchor_loc: tuple[int, int],
    ) -> CropRect:
        """From the anchor position, compute the full 4:3 game bounding box."""
        ah, aw = anchor.shape[:2]
        fh, fw = frame.shape[:2]

        # Estimate the game area width from how big the anchor appears
        # compared to its expected fractional width.
        # anchor real width ≈ game_width * (anchor_template_native_w / 256)
        # For a first pass we assume the anchor occupies ~30% of game width:
        est_anchor_frac_w = aw  # pixel width of the matched anchor
        game_width_est = int(est_anchor_frac_w / 0.30)

        # Height from 4:3
        game_height_est = int(game_width_est / _GAME_ASPECT)

        # Game top-left from anchor offset
        game_x = max(0, anchor_loc[0] - int(self.ANCHOR_OFFSET_X_FRAC * game_width_est))
        game_y = max(0, anchor_loc[1] - int(self.ANCHOR_OFFSET_Y_FRAC * game_height_est))

        # Clamp to frame bounds
        game_width_est = min(game_width_est, fw - game_x)
        game_height_est = min(game_height_est, fh - game_y)

        return CropRect(x=game_x, y=game_y, width=game_width_est, height=game_height_est)

    def _find_tracker(
        self, frame: NDArray[np.uint8], game: CropRect
    ) -> CropRect | None:
        """Attempt to locate the item tracker region to the right of the game.

        This is a heuristic: we look for a high-contrast rectangular region
        in the expected tracker area.  Returns ``None`` if not found.
        """
        fh, fw = frame.shape[:2]
        search_x = int(game.x + game.width * self.TRACKER_SEARCH_RIGHT_FRAC)
        search_w = int(game.width * self.TRACKER_SEARCH_WIDTH_FRAC)
        if search_x + search_w > fw:
            search_w = fw - search_x
        if search_w < 20:
            return None

        roi = frame[game.y : game.y + game.height, search_x : search_x + search_w]
        gray = cv2.cvtColor(roi, cv2.COLOR_BGR2GRAY)
        edges = cv2.Canny(gray, 50, 150)

        contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        if not contours:
            return None

        # Pick the largest rectangular contour
        best: CropRect | None = None
        best_area = 0
        for cnt in contours:
            rx, ry, rw, rh = cv2.boundingRect(cnt)
            area = rw * rh
            if area > best_area and rw > 20 and rh > 20:
                best_area = area
                best = CropRect(
                    x=search_x + rx,
                    y=game.y + ry,
                    width=rw,
                    height=rh,
                )
        return best

    # ------------------------------------------------------------------
    # Debug visualisation
    # ------------------------------------------------------------------

    @staticmethod
    def _draw_debug(
        frame: NDArray[np.uint8],
        game: CropRect,
        tracker: CropRect | None,
    ) -> NDArray[np.uint8]:
        vis = frame.copy()
        cv2.rectangle(
            vis,
            (game.x, game.y),
            (game.x + game.width, game.y + game.height),
            (0, 255, 0),
            2,
        )
        cv2.putText(vis, "GAME", (game.x, game.y - 8), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2)
        if tracker:
            cv2.rectangle(
                vis,
                (tracker.x, tracker.y),
                (tracker.x + tracker.width, tracker.y + tracker.height),
                (255, 0, 0),
                2,
            )
            cv2.putText(
                vis, "TRACKER", (tracker.x, tracker.y - 8), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 0, 0), 2
            )
        return vis


# ---------------------------------------------------------------------------
# Quick-capture helpers (used by the API to grab a frame from a live feed)
# ---------------------------------------------------------------------------

def capture_frame_from_url(url: str, timeout_s: int = 15) -> NDArray[np.uint8]:
    """Grab a single decoded frame from a video URL using FFmpeg subprocess.

    Unlike ``cv2.VideoCapture``, this approach uses the full FFmpeg demuxer
    which properly waits for H.264 PPS/SPS + IDR keyframes before decoding.
    This eliminates the ``non-existing PPS 0 referenced`` errors that occur
    when joining MPEG-TS/UDP streams mid-GOP with OpenCV.
    """
    ffmpeg_bin = shutil.which("ffmpeg")
    if not ffmpeg_bin:
        raise RuntimeError("ffmpeg not found on PATH")

    cmd = [
        ffmpeg_bin,
        "-hide_banner",
        "-loglevel", "error",
        # For UDP streams, give FFmpeg time to receive a keyframe
        "-analyzeduration", "10000000",   # 10 s
        "-probesize", "10000000",         # 10 MB
        "-i", url,
        "-frames:v", "1",
        "-f", "image2pipe",
        "-vcodec", "mjpeg",
        "-q:v", "2",
        "pipe:1",
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, timeout=timeout_s)
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"FFmpeg timed out after {timeout_s}s capturing from {url}")

    if not result.stdout:
        stderr_snippet = result.stderr.decode(errors="replace")[:300]
        raise RuntimeError(f"FFmpeg produced no frame from {url}: {stderr_snippet}")

    arr = np.frombuffer(result.stdout, dtype=np.uint8)
    frame = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if frame is None:
        raise RuntimeError(f"Failed to decode captured JPEG from {url}")
    return frame

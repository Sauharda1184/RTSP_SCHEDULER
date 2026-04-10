"""FFmpeg subprocess wrapper for RTSP recording."""

from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path


logger = logging.getLogger(__name__)


def ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None


def build_output_filename(camera_name: str, scheduled_at) -> str:
    """cameraName_YYYY-MM-DD_HH-MM.mp4 (camera name sanitized)."""
    safe = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in camera_name.strip())
    safe = safe.strip("_") or "camera"
    ts = scheduled_at.strftime("%Y-%m-%d_%H-%M")
    return f"{safe}_{ts}.mp4"


def record_rtsp(
    *,
    rtsp_url: str,
    duration_seconds: int,
    output_file: Path,
    timeout_margin_seconds: int = 30,
) -> subprocess.CompletedProcess[str]:
    """
    Record RTSP with stream copy. Uses TCP transport per requirement.
    """
    output_file.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "warning",
        "-rtsp_transport",
        "tcp",
        "-i",
        rtsp_url,
        "-t",
        str(duration_seconds),
        "-c",
        "copy",
        "-avoid_negative_ts",
        "make_zero",
        "-y",
        str(output_file),
    ]
    logger.info("Starting FFmpeg: %s", " ".join(cmd[:8]) + " ... " + " ".join(cmd[-4:]))
    # FFmpeg may need extra time to connect; add margin to subprocess timeout
    timeout = duration_seconds + max(60, timeout_margin_seconds)
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )

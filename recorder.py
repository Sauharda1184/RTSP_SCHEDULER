"""FFmpeg subprocess wrapper for RTSP recording."""

from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path


logger = logging.getLogger(__name__)


def ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None


# CRF 23 ≈ visually lossless; higher = smaller files (typical range 18–28).
DEFAULT_H264_CRF = "26"
DEFAULT_H264_PRESET = "veryfast"


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
    compress: bool = False,
) -> subprocess.CompletedProcess[str]:
    """
    Record RTSP over TCP. With compress=False, stream copy (fast, large files).
    With compress=True, re-encode to H.264 (smaller files, heavy CPU; must keep up in real time).
    """
    output_file.parent.mkdir(parents=True, exist_ok=True)
    base = [
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
    ]
    if compress:
        cmd = [
            *base,
            "-map",
            "0:v:0",
            "-c:v",
            "libx264",
            "-crf",
            DEFAULT_H264_CRF,
            "-preset",
            DEFAULT_H264_PRESET,
            "-pix_fmt",
            "yuv420p",
            "-an",
            "-movflags",
            "+faststart",
            "-avoid_negative_ts",
            "make_zero",
            "-y",
            str(output_file),
        ]
        logger.info(
            "Starting FFmpeg (H.264 transcode, crf=%s preset=%s)",
            DEFAULT_H264_CRF,
            DEFAULT_H264_PRESET,
        )
    else:
        cmd = [
            *base,
            "-c",
            "copy",
            "-avoid_negative_ts",
            "make_zero",
            "-y",
            str(output_file),
        ]
        logger.info("Starting FFmpeg (stream copy)")
    timeout = duration_seconds + max(60, timeout_margin_seconds)
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )

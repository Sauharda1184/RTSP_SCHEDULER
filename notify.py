"""Optional native desktop notifications (macOS and Linux; Windows is best-effort)."""

from __future__ import annotations

import logging
import platform
import subprocess

logger = logging.getLogger(__name__)


def _escape_apple(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def desktop_notify(title: str, message: str) -> None:
    title = title.strip() or "RTSP Scheduler"
    message = message.strip() or ""
    system = platform.system()
    try:
        if system == "Darwin":
            cmd = (
                f'display notification "{_escape_apple(message)}" '
                f'with title "{_escape_apple(title)}"'
            )
            subprocess.run(
                ["osascript", "-e", cmd],
                check=False,
                capture_output=True,
                timeout=10,
            )
        elif system == "Windows":
            pass
        else:
            subprocess.run(
                ["notify-send", title, message],
                check=False,
                capture_output=True,
                timeout=10,
            )
    except FileNotFoundError:
        logger.debug("Notification helper not found for this OS")
    except Exception:
        logger.debug("desktop_notify failed", exc_info=True)

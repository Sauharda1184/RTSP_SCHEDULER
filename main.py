"""
RTSP Recording Scheduler — desktop tool to record traffic camera streams on a schedule.

Requires FFmpeg on PATH. Uses APScheduler, SQLite, and Tkinter.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from scheduler_service import SchedulerService
from storage import RecordingStore
from ui import SchedulerApp


def _setup_logging(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "rtsp_scheduler.log"
    fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter(fmt, datefmt))
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(fmt, datefmt))
    root.handlers.clear()
    root.addHandler(fh)
    root.addHandler(ch)
    logging.info("Logging to %s", log_file)


def main() -> None:
    data_dir = Path.home() / ".rtsp_scheduler"
    _setup_logging(data_dir)
    db_path = data_dir / "recordings.db"

    store = RecordingStore(db_path)
    app_holder: list[SchedulerApp | None] = [None]

    def refresh_ui(recording_id: int) -> None:
        app = app_holder[0]
        if app is not None:
            app.on_job_finished(recording_id)

    scheduler = SchedulerService(
        store,
        on_job_started=refresh_ui,
        on_job_finished=refresh_ui,
    )
    scheduler.start()

    app = SchedulerApp(store, scheduler)
    app_holder[0] = app
    try:
        app.mainloop()
    finally:
        scheduler.shutdown(wait=True)


if __name__ == "__main__":
    main()

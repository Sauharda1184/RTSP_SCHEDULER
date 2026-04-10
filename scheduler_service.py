"""APScheduler integration and recording job execution."""

from __future__ import annotations

import logging
import subprocess
import threading
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger

from recorder import build_output_filename, ffmpeg_available, record_rtsp
from storage import RecordingStore

if TYPE_CHECKING:
    from storage import RecordingRow

logger = logging.getLogger(__name__)

JobCallback = Callable[[int], None]


class SchedulerService:
    """Registers one-shot date jobs and persists state via RecordingStore."""

    def __init__(
        self,
        store: RecordingStore,
        *,
        on_job_finished: JobCallback | None = None,
    ) -> None:
        self._store = store
        self._on_job_finished = on_job_finished
        self._scheduler = BackgroundScheduler()
        self._lock = threading.Lock()

    def start(self) -> None:
        self._scheduler.start()
        self.restore_pending_jobs()

    def shutdown(self, *, wait: bool = True) -> None:
        self._scheduler.shutdown(wait=wait)

    def job_id_for(self, recording_id: int) -> str:
        return f"recording_{recording_id}"

    def schedule_recording(self, recording_id: int, run_at: datetime) -> None:
        jid = self.job_id_for(recording_id)
        with self._lock:
            if self._scheduler.get_job(jid):
                self._scheduler.remove_job(jid)
            self._scheduler.add_job(
                self._run_recording,
                DateTrigger(run_date=run_at),
                id=jid,
                args=[recording_id],
                replace_existing=True,
            )
        logger.info("Scheduled job %s at %s", jid, run_at.isoformat())

    def cancel_scheduled_job(self, recording_id: int) -> None:
        jid = self.job_id_for(recording_id)
        with self._lock:
            job = self._scheduler.get_job(jid)
            if job:
                self._scheduler.remove_job(jid)
                logger.info("Removed scheduler job %s", jid)

    def restore_pending_jobs(self) -> None:
        now = datetime.now().astimezone()
        n = self._store.expire_stale_pending(now)
        if n:
            logger.info("Marked %d overdue pending recording(s) as missed", n)
        pending = self._store.list_pending_future(now)
        for row in pending:
            if row.scheduled_at <= now:
                continue
            self.schedule_recording(row.id, row.scheduled_at)
        logger.info("Restored %d pending recording job(s)", len(pending))

    def _run_recording(self, recording_id: int) -> None:
        row = self._store.get(recording_id)
        if not row or row.status != "pending":
            logger.warning("Skip recording %s: not pending or missing", recording_id)
            return

        if not ffmpeg_available():
            msg = "ffmpeg not found in PATH"
            logger.error("%s", msg)
            self._store.update_status(recording_id, "failed", error_message=msg)
            self._notify_finished(recording_id)
            return

        self._store.update_status(recording_id, "running")
        out_name = build_output_filename(row.camera_name, row.scheduled_at)
        out_path = Path(row.output_folder) / out_name

        try:
            proc = record_rtsp(
                rtsp_url=row.rtsp_url,
                duration_seconds=row.duration_seconds,
                output_file=out_path,
            )
        except subprocess.TimeoutExpired as e:
            err = f"FFmpeg timeout: {e}"
            logger.exception("%s", err)
            self._store.update_status(recording_id, "failed", error_message=err)
            self._notify_finished(recording_id)
            return

        if proc.returncode == 0 and out_path.exists() and out_path.stat().st_size > 0:
            self._store.update_status(
                recording_id,
                "completed",
                output_path=str(out_path.resolve()),
            )
            logger.info("Recording %s completed: %s", recording_id, out_path)
        else:
            stderr = (proc.stderr or "").strip()
            stdout = (proc.stdout or "").strip()
            detail = stderr or stdout or f"exit code {proc.returncode}"
            err = f"FFmpeg failed: {detail[:2000]}"
            logger.error("Recording %s failed: %s", recording_id, err)
            self._store.update_status(recording_id, "failed", error_message=err)

        self._notify_finished(recording_id)

    def _notify_finished(self, recording_id: int) -> None:
        if self._on_job_finished:
            try:
                self._on_job_finished(recording_id)
            except Exception:
                logger.exception("on_job_finished callback failed")

"""APScheduler integration and recording job execution."""

from __future__ import annotations

import logging
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Callable

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger

from notify import desktop_notify
from recorder import build_output_filename, ffmpeg_available, record_rtsp
from storage import RecordingRow, RecordingStore, StreamRow

logger = logging.getLogger(__name__)

JobCallback = Callable[[int], None]

MAX_CONCURRENT_STREAMS = 6


class SchedulerService:
    """Registers one-shot date jobs and persists state via RecordingStore."""

    def __init__(
        self,
        store: RecordingStore,
        *,
        on_job_finished: JobCallback | None = None,
        on_job_started: JobCallback | None = None,
    ) -> None:
        self._store = store
        self._on_job_finished = on_job_finished
        self._on_job_started = on_job_started
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

    def _capture_stream(
        self,
        stream: StreamRow,
        row: RecordingRow,
        *,
        multi_stream: bool,
    ) -> tuple[bool, str | None, str | None]:
        """Run FFmpeg for one stream. Returns (ok, output_path, error_message)."""
        out_name = build_output_filename(
            stream.camera_name,
            row.scheduled_at,
            multi_stream=multi_stream,
            stream_index=stream.sort_order,
        )
        out_path = Path(row.output_folder) / out_name
        try:
            proc = record_rtsp(
                rtsp_url=stream.rtsp_url,
                duration_seconds=row.duration_seconds,
                output_file=out_path,
                compress=row.compress,
            )
        except subprocess.TimeoutExpired as e:
            err = f"FFmpeg timeout: {e}"
            logger.exception("%s", err)
            self._store.update_stream_result(stream.id, error_message=err)
            return False, None, err

        if proc.returncode == 0 and out_path.exists() and out_path.stat().st_size > 0:
            resolved = str(out_path.resolve())
            self._store.update_stream_result(stream.id, output_path=resolved)
            logger.info("Stream %s saved: %s", stream.id, resolved)
            return True, resolved, None

        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        detail = stderr or stdout or f"exit code {proc.returncode}"
        err = f"{stream.camera_name}: {detail[:800]}"
        self._store.update_stream_result(stream.id, error_message=err)
        logger.error("Stream %s failed: %s", stream.id, err)
        return False, None, err

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
            desktop_notify("Recording failed", f"Job #{recording_id}: {msg}")
            return

        streams = self._store.get_streams(recording_id)
        if not streams:
            msg = "No streams configured for this job"
            self._store.update_status(recording_id, "failed", error_message=msg)
            self._notify_finished(recording_id)
            desktop_notify("Recording failed", f"Job #{recording_id}: {msg}")
            return

        self._store.update_status(recording_id, "recording")
        self._notify_started(recording_id)

        multi = len(streams) > 1
        workers = min(MAX_CONCURRENT_STREAMS, len(streams))
        results: list[tuple[bool, str | None, str | None]] = []

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [
                pool.submit(self._capture_stream, s, row, multi_stream=multi)
                for s in streams
            ]
            for fut in as_completed(futures):
                results.append(fut.result())

        ok_paths = [p for ok, p, _ in results if ok and p]
        errors = [e for ok, _, e in results if not ok and e]
        n_ok = sum(1 for ok, _, _ in results if ok)

        if n_ok == len(streams):
            summary = "; ".join(ok_paths)
            self._store.update_status(
                recording_id,
                "completed",
                output_path=summary,
            )
            logger.info("Recording %s completed (%d streams)", recording_id, len(streams))
            desktop_notify(
                "Recording finished",
                f"Job #{recording_id}: {len(streams)} stream(s) saved.",
            )
        else:
            err_text = " | ".join(errors) if errors else "Unknown error"
            self._store.update_status(
                recording_id,
                "failed",
                error_message=err_text[:4000],
                output_path="; ".join(ok_paths) if ok_paths else None,
            )
            logger.error("Recording %s partial/fail: %s", recording_id, err_text)
            desktop_notify(
                "Recording failed",
                f"Job #{recording_id}: {n_ok}/{len(streams)} succeeded.",
            )

        self._notify_finished(recording_id)

    def _notify_started(self, recording_id: int) -> None:
        if self._on_job_started:
            try:
                self._on_job_started(recording_id)
            except Exception:
                logger.exception("on_job_started callback failed")

    def _notify_finished(self, recording_id: int) -> None:
        if self._on_job_finished:
            try:
                self._on_job_finished(recording_id)
            except Exception:
                logger.exception("on_job_finished callback failed")

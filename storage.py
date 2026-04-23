"""SQLite persistence for scheduled recordings."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class RecordingRow:
    id: int
    camera_name: str
    rtsp_url: str
    scheduled_at: datetime
    duration_seconds: int
    output_folder: str
    status: str
    created_at: datetime
    output_path: str | None
    error_message: str | None
    compress: bool


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


class RecordingStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS recordings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    camera_name TEXT NOT NULL,
                    rtsp_url TEXT NOT NULL,
                    scheduled_at TEXT NOT NULL,
                    duration_seconds INTEGER NOT NULL,
                    output_folder TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    output_path TEXT,
                    error_message TEXT
                )
                """
            )
            conn.execute(
                "UPDATE recordings SET status = 'recording' WHERE status = 'running'"
            )
            cols = {r[1] for r in conn.execute("PRAGMA table_info(recordings)").fetchall()}
            if "compress" not in cols:
                conn.execute(
                    "ALTER TABLE recordings ADD COLUMN compress INTEGER NOT NULL DEFAULT 0"
                )

    def add_recording(
        self,
        *,
        camera_name: str,
        rtsp_url: str,
        scheduled_at: datetime,
        duration_seconds: int,
        output_folder: str,
        compress: bool = False,
    ) -> int:
        now = datetime.now().astimezone()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO recordings (
                    camera_name, rtsp_url, scheduled_at, duration_seconds,
                    output_folder, status, created_at, compress
                ) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
                """,
                (
                    camera_name.strip(),
                    rtsp_url.strip(),
                    scheduled_at.isoformat(),
                    duration_seconds,
                    output_folder,
                    now.isoformat(),
                    1 if compress else 0,
                ),
            )
            return int(cur.lastrowid)

    def get(self, recording_id: int) -> RecordingRow | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM recordings WHERE id = ?", (recording_id,)
            ).fetchone()
        return self._row_to_model(row) if row else None

    def list_pending_future(self, now: datetime) -> list[RecordingRow]:
        """Pending jobs whose scheduled time is still in the future (for reschedule on startup)."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM recordings
                WHERE status = 'pending' AND scheduled_at > ?
                ORDER BY scheduled_at
                """,
                (now.isoformat(),),
            ).fetchall()
        return [self._row_to_model(r) for r in rows]

    def list_recent(self, limit: int = 100) -> list[RecordingRow]:
        """Active jobs (pending/recording) first, soonest first; then others by latest scheduled."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM recordings
                ORDER BY
                  CASE status
                    WHEN 'pending' THEN 0
                    WHEN 'recording' THEN 0
                    WHEN 'running' THEN 0
                    WHEN 'completed' THEN 1
                    WHEN 'failed' THEN 1
                    WHEN 'missed' THEN 2
                    WHEN 'cancelled' THEN 2
                    ELSE 3
                  END,
                  CASE
                    WHEN status IN ('pending', 'recording', 'running')
                    THEN scheduled_at
                  END ASC,
                  scheduled_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._row_to_model(r) for r in rows]

    def update_status(
        self,
        recording_id: int,
        status: str,
        *,
        output_path: str | None = None,
        error_message: str | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE recordings
                SET status = ?, output_path = COALESCE(?, output_path),
                    error_message = COALESCE(?, error_message)
                WHERE id = ?
                """,
                (status, output_path, error_message, recording_id),
            )

    def expire_stale_pending(self, now: datetime) -> int:
        """Mark pending rows whose scheduled time passed (e.g. app was off) as missed."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE recordings
                SET status = 'missed',
                    error_message = 'Scheduled time passed while scheduler was not running'
                WHERE status = 'pending' AND scheduled_at <= ?
                """,
                (now.isoformat(),),
            )
            return cur.rowcount

    def cancel(self, recording_id: int) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE recordings SET status = 'cancelled'
                WHERE id = ? AND status = 'pending'
                """,
                (recording_id,),
            )
            return cur.rowcount > 0

    def delete(self, recording_id: int) -> bool:
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM recordings WHERE id = ?", (recording_id,))
            return cur.rowcount > 0

    def _row_to_model(self, row: sqlite3.Row) -> RecordingRow:
        d: dict[str, Any] = dict(row)
        return RecordingRow(
            id=int(d["id"]),
            camera_name=d["camera_name"],
            rtsp_url=d["rtsp_url"],
            scheduled_at=_parse_dt(d["scheduled_at"]),
            duration_seconds=int(d["duration_seconds"]),
            output_folder=d["output_folder"],
            status=d["status"],
            created_at=_parse_dt(d["created_at"]),
            output_path=d["output_path"],
            error_message=d["error_message"],
            compress=bool(d.get("compress", 0)),
        )

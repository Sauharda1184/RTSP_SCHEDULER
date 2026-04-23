"""SQLite persistence for scheduled recordings."""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
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
    display_cameras: str | None = None


@dataclass
class StreamRow:
    id: int
    recording_id: int
    sort_order: int
    rtsp_url: str
    camera_name: str
    output_path: str | None
    error_message: str | None


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
        conn.execute("PRAGMA foreign_keys = ON")
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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS recording_streams (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recording_id INTEGER NOT NULL,
                    sort_order INTEGER NOT NULL,
                    rtsp_url TEXT NOT NULL,
                    camera_name TEXT NOT NULL,
                    output_path TEXT,
                    error_message TEXT,
                    FOREIGN KEY (recording_id) REFERENCES recordings(id) ON DELETE CASCADE
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS presets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    streams_json TEXT NOT NULL,
                    output_folder TEXT NOT NULL,
                    compress INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                INSERT INTO recording_streams (recording_id, sort_order, rtsp_url, camera_name)
                SELECT r.id, 0, r.rtsp_url, r.camera_name
                FROM recordings r
                WHERE NOT EXISTS (
                    SELECT 1 FROM recording_streams s WHERE s.recording_id = r.id
                )
                """
            )

    def add_recording(
        self,
        *,
        streams: list[tuple[str, str]],
        scheduled_at: datetime,
        duration_seconds: int,
        output_folder: str,
        compress: bool = False,
    ) -> int:
        if not streams:
            raise ValueError("at least one stream is required")
        cleaned: list[tuple[str, str]] = []
        for url, name in streams:
            u, n = url.strip(), name.strip() or "camera"
            if not u:
                raise ValueError("empty RTSP URL")
            cleaned.append((u, n))

        labels = [c[1] for c in cleaned]
        summary = ", ".join(labels[:4])
        if len(labels) > 4:
            summary += f", +{len(labels) - 4} more"
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
                    summary,
                    cleaned[0][0],
                    scheduled_at.isoformat(),
                    duration_seconds,
                    output_folder,
                    now.isoformat(),
                    1 if compress else 0,
                ),
            )
            rid = int(cur.lastrowid)
            for i, (url, cam) in enumerate(cleaned):
                conn.execute(
                    """
                    INSERT INTO recording_streams
                    (recording_id, sort_order, rtsp_url, camera_name)
                    VALUES (?, ?, ?, ?)
                    """,
                    (rid, i, url, cam),
                )
            return rid

    def get(self, recording_id: int) -> RecordingRow | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM recordings WHERE id = ?", (recording_id,)
            ).fetchone()
        return self._row_to_model(row) if row else None

    def get_streams(self, recording_id: int) -> list[StreamRow]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM recording_streams
                WHERE recording_id = ?
                ORDER BY sort_order
                """,
                (recording_id,),
            ).fetchall()
        return [self._stream_to_model(r) for r in rows]

    def update_stream_result(
        self,
        stream_id: int,
        *,
        output_path: str | None = None,
        error_message: str | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE recording_streams
                SET output_path = COALESCE(?, output_path),
                    error_message = COALESCE(?, error_message)
                WHERE id = ?
                """,
                (output_path, error_message, stream_id),
            )

    def list_pending_future(self, now: datetime) -> list[RecordingRow]:
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

    def _camera_displays(self, conn: sqlite3.Connection, ids: list[int]) -> dict[int, str]:
        if not ids:
            return {}
        placeholders = ",".join("?" * len(ids))
        rows = conn.execute(
            f"""
            SELECT recording_id, camera_name, sort_order
            FROM recording_streams
            WHERE recording_id IN ({placeholders})
            ORDER BY recording_id, sort_order
            """,
            ids,
        ).fetchall()
        by_rec: dict[int, list[str]] = defaultdict(list)
        for r in rows:
            by_rec[int(r["recording_id"])].append(r["camera_name"])
        return {rid: ", ".join(names) for rid, names in by_rec.items()}

    def list_recent(self, limit: int = 100) -> list[RecordingRow]:
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
            models = [self._row_to_model(r) for r in rows]
            ids = [m.id for m in models]
            displays = self._camera_displays(conn, ids)
            for m in models:
                m.display_cameras = displays.get(m.id, m.camera_name)
        return models

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

    def list_preset_names(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT name FROM presets ORDER BY name COLLATE NOCASE"
            ).fetchall()
        return [r[0] for r in rows]

    def get_preset(self, name: str) -> tuple[list[tuple[str, str]], str, bool] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT streams_json, output_folder, compress FROM presets WHERE name = ?",
                (name,),
            ).fetchone()
        if not row:
            return None
        raw = json.loads(row["streams_json"])
        streams = [(str(p[0]), str(p[1])) for p in raw]
        return streams, row["output_folder"], bool(row["compress"])

    def save_preset(
        self,
        name: str,
        streams: list[tuple[str, str]],
        output_folder: str,
        compress: bool,
    ) -> None:
        now = datetime.now().astimezone().isoformat()
        payload = json.dumps([[u, n] for u, n in streams])
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO presets (name, streams_json, output_folder, compress, created_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    streams_json = excluded.streams_json,
                    output_folder = excluded.output_folder,
                    compress = excluded.compress,
                    created_at = excluded.created_at
                """,
                (name.strip(), payload, output_folder, 1 if compress else 0, now),
            )

    def delete_preset(self, name: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM presets WHERE name = ?", (name,))
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
            display_cameras=None,
        )

    def _stream_to_model(self, row: sqlite3.Row) -> StreamRow:
        d: dict[str, Any] = dict(row)
        return StreamRow(
            id=int(d["id"]),
            recording_id=int(d["recording_id"]),
            sort_order=int(d["sort_order"]),
            rtsp_url=d["rtsp_url"],
            camera_name=d["camera_name"],
            output_path=d["output_path"],
            error_message=d["error_message"],
        )

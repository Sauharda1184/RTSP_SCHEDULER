"""Tkinter GUI for scheduling RTSP recordings."""

from __future__ import annotations

import logging
import tkinter as tk
from datetime import datetime, timedelta
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from zoneinfo import ZoneInfo

from recorder import ffmpeg_available
from scheduler_service import SchedulerService
from storage import RecordingStore

logger = logging.getLogger(__name__)


def _local_tz():
    tz = datetime.now().astimezone().tzinfo
    return tz if tz is not None else ZoneInfo("UTC")


def _to_24h(hour12: int, minute: int, ampm: str) -> tuple[int, int]:
    """Convert 12-hour clock to 24-hour hour and minute."""
    ap = ampm.strip().upper()
    if ap == "AM":
        h24 = 0 if hour12 == 12 else hour12
    else:
        h24 = 12 if hour12 == 12 else hour12 + 12
    return h24, minute


def _datetime_from_date_and_12h(date_str: str, hour12: int, minute: int, ampm: str) -> datetime:
    """Combine local date with 12h time into timezone-aware datetime."""
    h24, m = _to_24h(hour12, minute, ampm)
    ds = date_str.strip()
    naive = datetime.strptime(f"{ds} {h24:02d}:{m:02d}", "%Y-%m-%d %H:%M")
    return naive.replace(tzinfo=_local_tz())


def _end_datetime_from_start(
    date_str: str,
    start: datetime,
    hour12: int,
    minute: int,
    ampm: str,
) -> datetime:
    """End time on the given calendar date; if not after start, roll end to the next day."""
    end_same_day = _datetime_from_date_and_12h(date_str, hour12, minute, ampm)
    if end_same_day <= start:
        end_same_day = end_same_day + timedelta(days=1)
    return end_same_day


class SchedulerApp(tk.Tk):
    def __init__(
        self,
        store: RecordingStore,
        scheduler: SchedulerService,
    ) -> None:
        super().__init__()
        self.title("RTSP Recording Scheduler")
        self.geometry("980x620")
        self.minsize(800, 480)

        self._store = store
        self._scheduler = scheduler

        self._build_form()
        self._build_list()
        self._build_actions()
        self._maybe_warn_ffmpeg()
        self.refresh_list()

    def _build_form(self) -> None:
        frm = ttk.LabelFrame(self, text="New recording", padding=8)
        frm.pack(fill=tk.X, padx=8, pady=8)

        ttk.Label(frm, text="RTSP URL").grid(row=0, column=0, sticky=tk.W, pady=2)
        self.var_url = tk.StringVar()
        ttk.Entry(frm, textvariable=self.var_url, width=72).grid(
            row=0, column=1, columnspan=3, sticky=tk.EW, pady=2
        )

        ttk.Label(frm, text="Camera name").grid(row=1, column=0, sticky=tk.W, pady=2)
        self.var_camera = tk.StringVar(value="traffic_cam")
        ttk.Entry(frm, textvariable=self.var_camera, width=32).grid(
            row=1, column=1, sticky=tk.W, pady=2
        )

        ttk.Label(frm, text="Date (YYYY-MM-DD)").grid(row=2, column=0, sticky=tk.W, pady=2)
        self.var_date = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d"))
        ttk.Entry(frm, textvariable=self.var_date, width=14).grid(
            row=2, column=1, columnspan=3, sticky=tk.W, pady=2
        )

        ttk.Label(frm, text="Start time").grid(row=3, column=0, sticky=tk.NW, pady=2)
        start_row = ttk.Frame(frm)
        start_row.grid(row=3, column=1, columnspan=3, sticky=tk.W, pady=2)
        self.var_start_h = tk.StringVar(value="9")
        self.var_start_m = tk.StringVar(value="0")
        self.var_start_ampm = tk.StringVar(value="AM")
        tk.Spinbox(
            start_row,
            from_=1,
            to=12,
            textvariable=self.var_start_h,
            width=4,
            wrap=True,
        ).pack(side=tk.LEFT)
        ttk.Label(start_row, text=" : ").pack(side=tk.LEFT)
        tk.Spinbox(
            start_row,
            from_=0,
            to=59,
            textvariable=self.var_start_m,
            width=4,
            wrap=True,
        ).pack(side=tk.LEFT)
        ttk.Label(start_row, text=" ").pack(side=tk.LEFT)
        ttk.Combobox(
            start_row,
            textvariable=self.var_start_ampm,
            values=("AM", "PM"),
            width=5,
            state="readonly",
        ).pack(side=tk.LEFT)

        ttk.Label(frm, text="End time").grid(row=4, column=0, sticky=tk.NW, pady=2)
        end_row = ttk.Frame(frm)
        end_row.grid(row=4, column=1, columnspan=3, sticky=tk.W, pady=2)
        self.var_end_h = tk.StringVar(value="10")
        self.var_end_m = tk.StringVar(value="0")
        self.var_end_ampm = tk.StringVar(value="AM")
        tk.Spinbox(
            end_row,
            from_=1,
            to=12,
            textvariable=self.var_end_h,
            width=4,
            wrap=True,
        ).pack(side=tk.LEFT)
        ttk.Label(end_row, text=" : ").pack(side=tk.LEFT)
        tk.Spinbox(
            end_row,
            from_=0,
            to=59,
            textvariable=self.var_end_m,
            width=4,
            wrap=True,
        ).pack(side=tk.LEFT)
        ttk.Label(end_row, text=" ").pack(side=tk.LEFT)
        ttk.Combobox(
            end_row,
            textvariable=self.var_end_ampm,
            values=("AM", "PM"),
            width=5,
            state="readonly",
        ).pack(side=tk.LEFT)

        ttk.Label(frm, text="Output folder").grid(row=5, column=0, sticky=tk.NW, pady=2)
        out_row = ttk.Frame(frm)
        out_row.grid(row=5, column=1, columnspan=3, sticky=tk.EW, pady=2)
        self.var_folder = tk.StringVar(value=str(Path.home() / "Videos" / "rtsp_recordings"))
        ttk.Entry(out_row, textvariable=self.var_folder, width=60).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(out_row, text="Browse…", command=self._browse_folder).pack(side=tk.LEFT, padx=(8, 0))

        self.var_compress = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            frm,
            text="Compress to H.264 (smaller files, uses more CPU — must encode in real time)",
            variable=self.var_compress,
        ).grid(row=6, column=1, columnspan=3, sticky=tk.W, pady=(4, 0))

        ttk.Button(frm, text="Schedule recording", command=self._schedule).grid(
            row=7, column=1, sticky=tk.W, pady=(8, 0)
        )

        for c in range(4):
            frm.columnconfigure(c, weight=1 if c >= 1 else 0)

    def _browse_folder(self) -> None:
        path = filedialog.askdirectory(initialdir=self.var_folder.get() or str(Path.home()))
        if path:
            self.var_folder.set(path)

    def _maybe_warn_ffmpeg(self) -> None:
        if not ffmpeg_available():
            messagebox.showwarning(
                "FFmpeg not found",
                "ffmpeg was not found in PATH. Install FFmpeg and restart this app, "
                "or recordings will fail when they run.",
            )

    def _schedule(self) -> None:
        url = self.var_url.get().strip()
        if not url:
            messagebox.showerror("Validation", "RTSP URL is required.")
            return
        camera = self.var_camera.get().strip() or "camera"
        folder = self.var_folder.get().strip()
        if not folder:
            messagebox.showerror("Validation", "Output folder is required.")
            return
        try:
            sh = int(self.var_start_h.get().strip())
            sm = int(self.var_start_m.get().strip())
            eh = int(self.var_end_h.get().strip())
            em = int(self.var_end_m.get().strip())
        except ValueError:
            messagebox.showerror("Validation", "Start and end times need valid hour and minute numbers.")
            return
        if not (1 <= sh <= 12 and 0 <= sm <= 59 and 1 <= eh <= 12 and 0 <= em <= 59):
            messagebox.showerror(
                "Validation",
                "Hour must be 1–12 and minutes 0–59.",
            )
            return

        start_ampm = self.var_start_ampm.get().strip()
        end_ampm = self.var_end_ampm.get().strip()
        if start_ampm not in ("AM", "PM") or end_ampm not in ("AM", "PM"):
            messagebox.showerror("Validation", "Choose AM or PM for start and end.")
            return

        try:
            when = _datetime_from_date_and_12h(self.var_date.get(), sh, sm, start_ampm)
            end_dt = _end_datetime_from_start(self.var_date.get(), when, eh, em, end_ampm)
        except ValueError:
            messagebox.showerror("Validation", "Invalid date. Use YYYY-MM-DD.")
            return

        duration_sec = int((end_dt - when).total_seconds())
        if duration_sec < 1:
            messagebox.showerror("Validation", "End time must be after start time.")
            return

        now = datetime.now().astimezone()
        if when <= now:
            messagebox.showerror("Validation", "Start time must be in the future.")
            return

        out_path = Path(folder)
        try:
            out_path.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            messagebox.showerror("Output folder", f"Cannot create output folder: {e}")
            return

        compress = self.var_compress.get()
        rid = self._store.add_recording(
            camera_name=camera,
            rtsp_url=url,
            scheduled_at=when,
            duration_seconds=duration_sec,
            output_folder=str(out_path.resolve()),
            compress=compress,
        )
        self._scheduler.schedule_recording(rid, when)
        logger.info(
            "Scheduled recording id=%s camera=%s at=%s duration=%ss",
            rid,
            camera,
            when.isoformat(),
            duration_sec,
        )
        self.refresh_list()
        enc_note = "\nOutput: H.264 re-encode (smaller file)." if compress else "\nOutput: stream copy (original size)."
        messagebox.showinfo(
            "Scheduled",
            f"Recording #{rid} scheduled.\nStart: {when.strftime('%Y-%m-%d %I:%M %p')}\n"
            f"End: {end_dt.strftime('%Y-%m-%d %I:%M %p')}\n"
            f"Duration: {duration_sec // 3600}h {(duration_sec % 3600) // 60}m"
            f"{enc_note}",
        )

    def _build_list(self) -> None:
        outer = ttk.LabelFrame(self, text="Recordings", padding=8)
        outer.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

        cols = ("id", "camera", "scheduled", "duration", "compress", "status", "detail")
        self.tree = ttk.Treeview(outer, columns=cols, show="headings", height=14)
        headings = {
            "id": "ID",
            "camera": "Camera",
            "scheduled": "Scheduled (local)",
            "duration": "Duration",
            "compress": "H.264",
            "status": "Status",
            "detail": "Output / error",
        }
        widths = {
            "id": 48,
            "camera": 120,
            "scheduled": 180,
            "duration": 90,
            "compress": 52,
            "status": 100,
            "detail": 280,
        }
        for c in cols:
            self.tree.heading(c, text=headings[c])
            self.tree.column(c, width=widths[c], stretch=c == "detail")

        scroll = ttk.Scrollbar(outer, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scroll.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scroll.pack(side=tk.RIGHT, fill=tk.Y)

    def _build_actions(self) -> None:
        bar = ttk.Frame(self)
        bar.pack(fill=tk.X, padx=8, pady=(0, 8))
        ttk.Button(bar, text="Refresh", command=self.refresh_list).pack(side=tk.LEFT)
        ttk.Button(bar, text="Cancel selected (pending)", command=self._cancel_selected).pack(
            side=tk.LEFT, padx=(8, 0)
        )
        ttk.Button(bar, text="Delete selected", command=self._delete_selected).pack(side=tk.LEFT, padx=(8, 0))

    def _selected_id(self) -> int | None:
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Selection", "Select a row first.")
            return None
        values = self.tree.item(sel[0], "values")
        if not values:
            return None
        return int(values[0])

    def _cancel_selected(self) -> None:
        rid = self._selected_id()
        if rid is None:
            return
        row = self._store.get(rid)
        if not row or row.status != "pending":
            messagebox.showinfo("Cancel", "Only pending jobs can be cancelled.")
            return
        if not messagebox.askyesno("Cancel", f"Cancel recording #{rid}?"):
            return
        if self._store.cancel(rid):
            self._scheduler.cancel_scheduled_job(rid)
            logger.info("Cancelled recording id=%s", rid)
            self.refresh_list()
        else:
            messagebox.showerror("Cancel", "Could not cancel (already started or missing).")

    def _delete_selected(self) -> None:
        rid = self._selected_id()
        if rid is None:
            return
        row = self._store.get(rid)
        if not row:
            return
        msg = (
            f"Remove scheduled job #{rid} from the database?"
            if row.status == "pending"
            else f"Delete recording row #{rid} from the database?"
        )
        if not messagebox.askyesno("Delete", msg):
            return
        self._scheduler.cancel_scheduled_job(rid)
        if self._store.delete(rid):
            logger.info("Deleted recording id=%s", rid)
            self.refresh_list()

    def refresh_list(self) -> None:
        for i in self.tree.get_children():
            self.tree.delete(i)
        for row in self._store.list_recent(200):
            sched = row.scheduled_at.strftime("%Y-%m-%d %H:%M %Z")
            ds = row.duration_seconds
            if ds >= 3600:
                dur = f"{ds // 3600}h {(ds % 3600) // 60}m"
            elif ds % 60 == 0:
                dur = f"{ds // 60}m"
            else:
                dur = f"{ds}s"
            enc = "Yes" if row.compress else ""
            detail = row.output_path or (row.error_message or "") or ""
            if len(detail) > 80:
                detail = detail[:77] + "..."
            status_display = row.status.replace("_", " ").title()
            self.tree.insert(
                "",
                tk.END,
                values=(row.id, row.camera_name, sched, dur, enc, status_display, detail),
            )

    def on_job_finished(self, recording_id: int) -> None:
        """Called from scheduler thread; marshal to UI thread."""
        self.after(0, self._on_job_finished_ui, recording_id)

    def _on_job_finished_ui(self, recording_id: int) -> None:
        self.refresh_list()
        row = self._store.get(recording_id)
        if row:
            logger.info("UI refresh after job %s status=%s", recording_id, row.status)

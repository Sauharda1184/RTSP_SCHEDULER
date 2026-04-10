"""Tkinter GUI for scheduling RTSP recordings."""

from __future__ import annotations

import logging
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from zoneinfo import ZoneInfo

from recorder import ffmpeg_available
from scheduler_service import SchedulerService
from storage import RecordingStore

logger = logging.getLogger(__name__)


def _parse_schedule_datetime(date_str: str, time_str: str) -> datetime:
    """Combine local date (YYYY-MM-DD) and time (HH:MM) into timezone-aware datetime."""
    ds = date_str.strip()
    ts = time_str.strip()
    naive = datetime.strptime(f"{ds} {ts}", "%Y-%m-%d %H:%M")
    local_tz = datetime.now().astimezone().tzinfo
    if local_tz is None:
        local_tz = ZoneInfo("UTC")
    return naive.replace(tzinfo=local_tz)


class SchedulerApp(tk.Tk):
    def __init__(
        self,
        store: RecordingStore,
        scheduler: SchedulerService,
    ) -> None:
        super().__init__()
        self.title("RTSP Recording Scheduler")
        self.geometry("920x560")
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
            row=2, column=1, sticky=tk.W, pady=2
        )

        ttk.Label(frm, text="Start time (HH:MM)").grid(row=2, column=2, sticky=tk.W, padx=(16, 0))
        self.var_time = tk.StringVar(value=datetime.now().strftime("%H:%M"))
        ttk.Entry(frm, textvariable=self.var_time, width=8).grid(row=2, column=3, sticky=tk.W)

        ttk.Label(frm, text="Duration (minutes)").grid(row=3, column=0, sticky=tk.W, pady=2)
        self.var_duration_min = tk.StringVar(value="30")
        ttk.Entry(frm, textvariable=self.var_duration_min, width=8).grid(
            row=3, column=1, sticky=tk.W, pady=2
        )

        ttk.Label(frm, text="Output folder").grid(row=4, column=0, sticky=tk.NW, pady=2)
        out_row = ttk.Frame(frm)
        out_row.grid(row=4, column=1, columnspan=3, sticky=tk.EW, pady=2)
        self.var_folder = tk.StringVar(value=str(Path.home() / "Videos" / "rtsp_recordings"))
        ttk.Entry(out_row, textvariable=self.var_folder, width=60).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(out_row, text="Browse…", command=self._browse_folder).pack(side=tk.LEFT, padx=(8, 0))

        ttk.Button(frm, text="Schedule recording", command=self._schedule).grid(
            row=5, column=1, sticky=tk.W, pady=(8, 0)
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
            duration_min = int(self.var_duration_min.get().strip())
        except ValueError:
            messagebox.showerror("Validation", "Duration must be a whole number of minutes.")
            return
        if duration_min <= 0:
            messagebox.showerror("Validation", "Duration must be positive.")
            return
        duration_sec = duration_min * 60

        try:
            when = _parse_schedule_datetime(self.var_date.get(), self.var_time.get())
        except ValueError:
            messagebox.showerror(
                "Validation",
                "Invalid date or time. Use YYYY-MM-DD and HH:MM (24h).",
            )
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

        rid = self._store.add_recording(
            camera_name=camera,
            rtsp_url=url,
            scheduled_at=when,
            duration_seconds=duration_sec,
            output_folder=str(out_path.resolve()),
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
        messagebox.showinfo("Scheduled", f"Recording #{rid} scheduled for {when.isoformat()}.")

    def _build_list(self) -> None:
        outer = ttk.LabelFrame(self, text="Recordings", padding=8)
        outer.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

        cols = ("id", "camera", "scheduled", "duration", "status", "detail")
        self.tree = ttk.Treeview(outer, columns=cols, show="headings", height=14)
        headings = {
            "id": "ID",
            "camera": "Camera",
            "scheduled": "Scheduled (local)",
            "duration": "Duration",
            "status": "Status",
            "detail": "Output / error",
        }
        widths = {"id": 48, "camera": 120, "scheduled": 180, "duration": 90, "status": 100, "detail": 320}
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
            dur = f"{row.duration_seconds // 60}m" if row.duration_seconds % 60 == 0 else f"{row.duration_seconds}s"
            detail = row.output_path or (row.error_message or "") or ""
            if len(detail) > 80:
                detail = detail[:77] + "..."
            self.tree.insert(
                "",
                tk.END,
                values=(row.id, row.camera_name, sched, dur, row.status, detail),
            )

    def on_job_finished(self, recording_id: int) -> None:
        """Called from scheduler thread; marshal to UI thread."""
        self.after(0, self._on_job_finished_ui, recording_id)

    def _on_job_finished_ui(self, recording_id: int) -> None:
        self.refresh_list()
        row = self._store.get(recording_id)
        if row:
            logger.info("UI refresh after job %s status=%s", recording_id, row.status)

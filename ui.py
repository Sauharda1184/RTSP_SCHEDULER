"""Tkinter GUI for scheduling RTSP recordings."""

from __future__ import annotations

import logging
import tkinter as tk
from datetime import datetime, timedelta
from pathlib import Path
from tkinter import filedialog, messagebox, simpledialog, ttk
from zoneinfo import ZoneInfo

from recorder import ffmpeg_available
from scheduler_service import SchedulerService
from storage import RecordingStore

logger = logging.getLogger(__name__)

try:
    import sv_ttk

    _HAS_SV_TTK = True
except ImportError:
    _HAS_SV_TTK = False

try:
    from tkcalendar import DateEntry

    _HAS_TKCALENDAR = True
except ImportError:
    _HAS_TKCALENDAR = False
    DateEntry = None  # type: ignore[misc, assignment]


def _local_tz():
    tz = datetime.now().astimezone().tzinfo
    return tz if tz is not None else ZoneInfo("UTC")


def _to_24h(hour12: int, minute: int, ampm: str) -> tuple[int, int]:
    ap = ampm.strip().upper()
    if ap == "AM":
        h24 = 0 if hour12 == 12 else hour12
    else:
        h24 = 12 if hour12 == 12 else hour12 + 12
    return h24, minute


def _datetime_from_date_and_12h(date_str: str, hour12: int, minute: int, ampm: str) -> datetime:
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
        if _HAS_SV_TTK:
            sv_ttk.set_theme("dark")
        self.title("RTSP Recording Scheduler")
        self.geometry("1020x720")
        self.minsize(880, 600)

        self._store = store
        self._scheduler = scheduler
        self._stream_rows: list[dict[str, tk.Widget | tk.StringVar]] = []
        self._date_entry: tk.Widget | None = None

        self._build_presets_bar()
        self._build_form()
        self._build_list()
        self._build_actions()
        self._maybe_warn_ffmpeg()
        self.refresh_list()

    def _build_presets_bar(self) -> None:
        pf = ttk.LabelFrame(self, text="Presets", padding=6)
        pf.pack(fill=tk.X, padx=8, pady=(8, 0))
        ttk.Label(pf, text="Saved layout").pack(side=tk.LEFT, padx=(0, 6))
        self.var_preset_pick = tk.StringVar()
        self.preset_combo = ttk.Combobox(
            pf,
            textvariable=self.var_preset_pick,
            width=28,
            state="readonly",
        )
        self.preset_combo.pack(side=tk.LEFT, padx=4)
        ttk.Button(pf, text="Load", command=self._preset_load).pack(side=tk.LEFT, padx=2)
        ttk.Button(pf, text="Save as…", command=self._preset_save).pack(side=tk.LEFT, padx=2)
        ttk.Button(pf, text="Delete", command=self._preset_delete).pack(side=tk.LEFT, padx=2)
        self._refresh_preset_names()

    def _refresh_preset_names(self) -> None:
        names = self._store.list_preset_names()
        self.preset_combo["values"] = names
        if names and self.var_preset_pick.get() not in names:
            self.var_preset_pick.set("")

    def _preset_load(self) -> None:
        name = self.var_preset_pick.get().strip()
        if not name:
            messagebox.showinfo("Presets", "Choose a preset from the list.")
            return
        data = self._store.get_preset(name)
        if not data:
            messagebox.showerror("Presets", f"Preset “{name}” not found.")
            self._refresh_preset_names()
            return
        streams, folder, compress = data
        self._clear_stream_rows()
        for url, cam in streams:
            self._add_stream_row(url, cam)
        if not self._stream_rows:
            self._add_stream_row("", "traffic_cam")
        self.var_folder.set(folder)
        self.var_compress.set(compress)
        messagebox.showinfo("Presets", f"Loaded preset “{name}”.")

    def _preset_save(self) -> None:
        try:
            streams = self._collect_stream_pairs()
        except ValueError as e:
            messagebox.showerror("Presets", str(e))
            return
        name = simpledialog.askstring("Save preset", "Preset name:", parent=self)
        if not name or not name.strip():
            return
        name = name.strip()
        self._store.save_preset(
            name,
            streams,
            self.var_folder.get().strip(),
            self.var_compress.get(),
        )
        self._refresh_preset_names()
        self.var_preset_pick.set(name)
        messagebox.showinfo("Presets", f"Saved preset “{name}”.")

    def _preset_delete(self) -> None:
        name = self.var_preset_pick.get().strip()
        if not name:
            messagebox.showinfo("Presets", "Choose a preset to delete.")
            return
        if not messagebox.askyesno("Delete preset", f"Delete preset “{name}”?"):
            return
        if self._store.delete_preset(name):
            self._refresh_preset_names()
            self.var_preset_pick.set("")
            messagebox.showinfo("Presets", "Preset deleted.")
        else:
            messagebox.showerror("Presets", "Could not delete preset.")

    def _clear_stream_rows(self) -> None:
        for item in self._stream_rows:
            item["frame"].destroy()
        self._stream_rows.clear()

    def _add_stream_row(self, url: str = "", camera: str = "traffic_cam") -> None:
        host = self._streams_host
        rowf = ttk.Frame(host)
        rowf.pack(fill=tk.X, pady=2)
        var_u = tk.StringVar(value=url)
        var_n = tk.StringVar(value=camera)
        ttk.Entry(rowf, textvariable=var_u, width=62).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Label(rowf, text="Name").pack(side=tk.LEFT)
        ttk.Entry(rowf, textvariable=var_n, width=16).pack(side=tk.LEFT, padx=6)

        def remove() -> None:
            if len(self._stream_rows) <= 1:
                messagebox.showinfo("Cameras", "At least one camera row is required.")
                return
            rowf.destroy()
            self._stream_rows[:] = [x for x in self._stream_rows if x["frame"] is not rowf]

        ttk.Button(rowf, text="Remove", width=8, command=remove).pack(side=tk.LEFT, padx=4)
        self._stream_rows.append({"frame": rowf, "url": var_u, "name": var_n})

    def _collect_stream_pairs(self) -> list[tuple[str, str]]:
        pairs: list[tuple[str, str]] = []
        for item in self._stream_rows:
            u = item["url"].get().strip()  # type: ignore[union-attr]
            n = item["name"].get().strip() or "camera"  # type: ignore[union-attr]
            if u:
                pairs.append((u, n))
        if not pairs:
            raise ValueError("At least one RTSP URL is required.")
        return pairs

    def _schedule_date_str(self) -> str:
        if self._date_entry is not None and _HAS_TKCALENDAR:
            d = self._date_entry.get_date()  # type: ignore[union-attr]
            return d.strftime("%Y-%m-%d")
        return self.var_date.get().strip()

    def _sync_calendar_from_var(self) -> None:
        if self._date_entry is None or not _HAS_TKCALENDAR:
            return
        try:
            d = datetime.strptime(self.var_date.get().strip(), "%Y-%m-%d").date()
            self._date_entry.set_date(d)  # type: ignore[union-attr]
        except ValueError:
            pass

    def _build_form(self) -> None:
        frm = ttk.LabelFrame(self, text="New recording", padding=8)
        frm.pack(fill=tk.X, padx=8, pady=8)

        cam_lf = ttk.LabelFrame(frm, text="Cameras (recorded concurrently, max parallel capped)", padding=6)
        cam_lf.grid(row=0, column=0, columnspan=4, sticky=tk.EW, pady=(0, 6))
        self._streams_host = ttk.Frame(cam_lf)
        self._streams_host.pack(fill=tk.X)
        ttk.Button(cam_lf, text="+ Add camera", command=self._add_stream_row).pack(anchor=tk.W, pady=4)
        self._add_stream_row("", "traffic_cam")

        ttk.Label(frm, text="Date").grid(row=1, column=0, sticky=tk.W, pady=2)
        date_row = ttk.Frame(frm)
        date_row.grid(row=1, column=1, columnspan=3, sticky=tk.W, pady=2)
        self.var_date = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d"))
        if _HAS_TKCALENDAR and DateEntry is not None:
            self._date_entry = DateEntry(
                date_row,
                width=12,
                date_pattern="yyyy-MM-dd",
            )
            self._date_entry.pack(side=tk.LEFT)  # type: ignore[union-attr]
            self._sync_calendar_from_var()

            def _on_cal(_event=None) -> None:
                self.var_date.set(self._schedule_date_str())

            self._date_entry.bind("<<DateEntrySelected>>", _on_cal)  # type: ignore[union-attr]
        else:
            ttk.Entry(date_row, textvariable=self.var_date, width=14).pack(side=tk.LEFT)
            ttk.Label(
                date_row,
                text="(YYYY-MM-DD — install tkcalendar for a date picker)",
                foreground="gray",
            ).pack(side=tk.LEFT, padx=8)

        ttk.Label(frm, text="Start time").grid(row=2, column=0, sticky=tk.NW, pady=2)
        start_row = ttk.Frame(frm)
        start_row.grid(row=2, column=1, columnspan=3, sticky=tk.W, pady=2)
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

        ttk.Label(frm, text="End time").grid(row=3, column=0, sticky=tk.NW, pady=2)
        end_row = ttk.Frame(frm)
        end_row.grid(row=3, column=1, columnspan=3, sticky=tk.W, pady=2)
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

        ttk.Label(
            frm,
            text=(
                "Tip: If end clock time is earlier than start on the same calendar date, "
                "the end is treated as the next day (overnight recording)."
            ),
            wraplength=760,
            foreground="gray",
        ).grid(row=4, column=1, columnspan=3, sticky=tk.W, pady=(0, 4))

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
        try:
            stream_pairs = self._collect_stream_pairs()
        except ValueError as e:
            messagebox.showerror("Validation", str(e))
            return
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

        date_part = self._schedule_date_str()
        try:
            when = _datetime_from_date_and_12h(date_part, sh, sm, start_ampm)
            end_dt = _end_datetime_from_start(date_part, when, eh, em, end_ampm)
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
            streams=stream_pairs,
            scheduled_at=when,
            duration_seconds=duration_sec,
            output_folder=str(out_path.resolve()),
            compress=compress,
        )
        self._scheduler.schedule_recording(rid, when)
        logger.info(
            "Scheduled recording id=%s streams=%s at=%s duration=%ss",
            rid,
            len(stream_pairs),
            when.isoformat(),
            duration_sec,
        )
        self.refresh_list()
        enc_note = "\nOutput: H.264 re-encode (smaller file)." if compress else "\nOutput: stream copy (original size)."
        multi_note = f"\n{len(stream_pairs)} cameras will record in parallel (up to the app limit)."
        messagebox.showinfo(
            "Scheduled",
            f"Recording #{rid} scheduled.\nStart: {when.strftime('%Y-%m-%d %I:%M %p')}\n"
            f"End: {end_dt.strftime('%Y-%m-%d %I:%M %p')}\n"
            f"Duration: {duration_sec // 3600}h {(duration_sec % 3600) // 60}m"
            f"{enc_note}{multi_note}",
        )

    def _build_list(self) -> None:
        outer = ttk.LabelFrame(self, text="Recordings", padding=8)
        outer.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

        cols = ("id", "camera", "scheduled", "duration", "compress", "status", "detail")
        self.tree = ttk.Treeview(outer, columns=cols, show="headings", height=12)
        headings = {
            "id": "ID",
            "camera": "Camera(s)",
            "scheduled": "Scheduled (local)",
            "duration": "Duration",
            "compress": "H.264",
            "status": "Status",
            "detail": "Output / error",
        }
        widths = {
            "id": 44,
            "camera": 160,
            "scheduled": 160,
            "duration": 80,
            "compress": 48,
            "status": 96,
            "detail": 260,
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
            cam = row.display_cameras or row.camera_name
            detail = row.output_path or (row.error_message or "") or ""
            if len(detail) > 80:
                detail = detail[:77] + "..."
            status_display = row.status.replace("_", " ").title()
            self.tree.insert(
                "",
                tk.END,
                values=(row.id, cam, sched, dur, enc, status_display, detail),
            )

    def on_job_finished(self, recording_id: int) -> None:
        self.after(0, self._on_job_finished_ui, recording_id)

    def _on_job_finished_ui(self, recording_id: int) -> None:
        self.refresh_list()
        row = self._store.get(recording_id)
        if row:
            logger.info("UI refresh after job %s status=%s", recording_id, row.status)

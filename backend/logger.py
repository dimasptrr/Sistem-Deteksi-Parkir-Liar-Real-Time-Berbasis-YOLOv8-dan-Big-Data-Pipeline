from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict

import pandas as pd


CSV_HEADERS = [
    "timestamp_entry",
    "timestamp_violation",
    "duration_seconds",
    "vehicle_type",
    "track_id",
    "zone_name",
    "screenshot_path",
    "camera_id",
]


@dataclass
class ViolationLogger:
    csv_path: Path
    screenshot_dir: Path

    def __post_init__(self) -> None:
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        self.screenshot_dir.mkdir(parents=True, exist_ok=True)
        
        # Check and repair CSV schema/rows on startup to prevent Pandas C errors
        if self.csv_path.exists():
            repaired_rows = []
            needs_rewrite = False
            try:
                with self.csv_path.open("r", encoding="utf-8") as handle:
                    reader = csv.reader(handle)
                    rows = list(reader)
                
                if rows:
                    header = rows[0]
                    if len(header) != len(CSV_HEADERS) or header != CSV_HEADERS:
                        needs_rewrite = True
                    
                    for r in rows[1:]:
                        if not r:
                            continue
                        if len(r) < len(CSV_HEADERS):
                            # Pad missing columns with default camera 'cam1'
                            new_row = r + ["cam1"] * (len(CSV_HEADERS) - len(r))
                            repaired_rows.append(new_row)
                            needs_rewrite = True
                        elif len(r) > len(CSV_HEADERS):
                            # Truncate duplicate/extra columns to prevent parsing error
                            new_row = r[:len(CSV_HEADERS)]
                            repaired_rows.append(new_row)
                            needs_rewrite = True
                        else:
                            repaired_rows.append(r)
                else:
                    needs_rewrite = True
            except Exception:
                needs_rewrite = True
                
            if needs_rewrite:
                try:
                    with self.csv_path.open("w", newline="", encoding="utf-8") as handle:
                        writer = csv.writer(handle)
                        writer.writerow(CSV_HEADERS)
                        writer.writerows(repaired_rows)
                except Exception:
                    pass
        else:
            with self.csv_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(CSV_HEADERS)

        # Count total violations logged today globally on init
        self._today_violation_count = 0
        try:
            df = self.load_today()
            if not df.empty:
                self._today_violation_count = df.shape[0]
        except Exception:
            pass

    def log_violation(
        self,
        *,
        timestamp_entry: datetime,
        timestamp_violation: datetime,
        duration_seconds: float,
        vehicle_type: str,
        track_id: int,
        zone_name: str,
        screenshot_path: str,
        camera_id: str = "cam1",
    ) -> Dict[str, str]:
        row = {
            "timestamp_entry": timestamp_entry.isoformat(timespec="seconds"),
            "timestamp_violation": timestamp_violation.isoformat(timespec="seconds"),
            "duration_seconds": f"{duration_seconds:.1f}",
            "vehicle_type": vehicle_type,
            "track_id": str(track_id),
            "zone_name": zone_name,
            "screenshot_path": screenshot_path,
            "camera_id": camera_id,
        }
        with self.csv_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_HEADERS)
            writer.writerow(row)
        if timestamp_violation.date() == date.today():
            self._today_violation_count += 1
        return row

    def load_dataframe(self) -> pd.DataFrame:
        if not self.csv_path.exists():
            return pd.DataFrame(columns=CSV_HEADERS)
        try:
            df = pd.read_csv(self.csv_path)
        except Exception:
            return pd.DataFrame(columns=CSV_HEADERS)
        if df.empty:
            return df
        if "camera_id" not in df.columns:
            df["camera_id"] = "cam1"
        df["timestamp_violation"] = pd.to_datetime(df["timestamp_violation"], errors="coerce")
        df["timestamp_entry"] = pd.to_datetime(df["timestamp_entry"], errors="coerce")
        return df

    def load_today(self, camera_id: str | None = None) -> pd.DataFrame:
        df = self.load_dataframe()
        if df.empty:
            return df
        today = date.today()
        mask = df["timestamp_violation"].dt.date == today
        df_today = df.loc[mask].copy()
        if camera_id is not None and "camera_id" in df_today.columns:
            df_today = df_today[df_today["camera_id"] == camera_id]
        return df_today

    def load_recent(self, limit: int = 50, camera_id: str | None = None) -> pd.DataFrame:
        df = self.load_dataframe()
        if df.empty:
            return df
        if camera_id is not None and "camera_id" in df.columns:
            df = df[df["camera_id"] == camera_id]
        return df.sort_values("timestamp_violation", ascending=False).head(limit).copy()

    def today_count(self, camera_id: str | None = None) -> int:
        df = self.load_today(camera_id)
        return int(df.shape[0])

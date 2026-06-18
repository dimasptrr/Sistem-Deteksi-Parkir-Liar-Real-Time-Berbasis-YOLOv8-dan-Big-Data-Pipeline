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
]


@dataclass
class ViolationLogger:
    csv_path: Path
    screenshot_dir: Path

    def __post_init__(self) -> None:
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        self.screenshot_dir.mkdir(parents=True, exist_ok=True)
        if not self.csv_path.exists():
            with self.csv_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(CSV_HEADERS)
        self._today_violation_count = self.load_today().shape[0]

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
    ) -> Dict[str, str]:
        row = {
            "timestamp_entry": timestamp_entry.isoformat(timespec="seconds"),
            "timestamp_violation": timestamp_violation.isoformat(timespec="seconds"),
            "duration_seconds": f"{duration_seconds:.1f}",
            "vehicle_type": vehicle_type,
            "track_id": str(track_id),
            "zone_name": zone_name,
            "screenshot_path": screenshot_path,
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
        df = pd.read_csv(self.csv_path)
        if df.empty:
            return df
        df["timestamp_violation"] = pd.to_datetime(df["timestamp_violation"], errors="coerce")
        df["timestamp_entry"] = pd.to_datetime(df["timestamp_entry"], errors="coerce")
        return df

    def load_today(self) -> pd.DataFrame:
        df = self.load_dataframe()
        if df.empty:
            return df
        today = date.today()
        mask = df["timestamp_violation"].dt.date == today
        return df.loc[mask].copy()

    def load_recent(self, limit: int = 50) -> pd.DataFrame:
        df = self.load_dataframe()
        if df.empty:
            return df
        return df.sort_values("timestamp_violation", ascending=False).head(limit).copy()

    def today_count(self) -> int:
        return self._today_violation_count

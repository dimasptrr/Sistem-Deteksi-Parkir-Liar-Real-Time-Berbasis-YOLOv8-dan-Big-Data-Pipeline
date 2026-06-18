import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
csv_path = PROJECT_ROOT / "backend" / "violations_log.csv"

# Predefined headers
headers = [
    "timestamp_entry",
    "timestamp_violation",
    "duration_seconds",
    "vehicle_type",
    "track_id",
    "zone_name",
    "screenshot_path"
]

# Keep the original 5 records
original_rows = [
    ["2026-06-15T18:51:18", "2026-06-15T18:53:18", "120.1", "mobil", "1", "right", r"D:\Kuliah\Semester 4\Big Data Dan Data Lakehouse\EAS BIG DATA\parkir-liar-detector\backend\violations\violation_20260615_185318_id1.jpg"],
    ["2026-06-15T18:51:31", "2026-06-15T18:53:31", "120.1", "mobil", "26", "right", r"D:\Kuliah\Semester 4\Big Data Dan Data Lakehouse\EAS BIG DATA\parkir-liar-detector\backend\violations\violation_20260615_185331_id26.jpg"],
    ["2026-06-17T16:30:15", "2026-06-17T16:32:15", "120.0", "mobil", "1", "right", r"D:\Kuliah\Semester 4\Big Data Dan Data Lakehouse\EAS BIG DATA\parkir-liar-detector\backend\violations\violation_20260617_163215_id1.jpg"],
    ["2026-06-17T20:55:23", "2026-06-17T20:57:23", "120.1", "mobil", "129", "right", r"D:\Kuliah\Semester 4\Big Data Dan Data Lakehouse\EAS BIG DATA\parkir-liar-detector\backend\violations\violation_20260617_205723_id129.jpg"],
    ["2026-06-17T21:11:54", "2026-06-17T21:14:00", "125.9", "mobil", "1", "right", r"D:\Kuliah\Semester 4\Big Data Dan Data Lakehouse\EAS BIG DATA\parkir-liar-detector\backend\violations\violation_20260617_211400_id1.jpg"]
]

mock_rows = list(original_rows)

# Generate 50 realistic historical entries for the last 5 days (June 13 to June 17)
today = datetime(2026, 6, 17)
vehicles = ["mobil", "mobil", "mobil", "motor", "motor", "bus", "truk"]
zones = ["left", "right"]

random.seed(42)  # For reproducible mock data

for i in range(50):
    day_offset = random.randint(0, 4)
    target_date = today - timedelta(days=day_offset)
    
    # Peak hours logic: more likelihood at 8-10 AM, 12-2 PM, and 5-8 PM
    hour_pool = [8, 9, 10, 12, 13, 14, 17, 18, 19, 20] * 3 + list(range(24))
    hour = random.choice(hour_pool)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    
    entry_time = datetime(target_date.year, target_date.month, target_date.day, hour, minute, second)
    duration = random.randint(120, 1200)  # 2m to 20m
    violation_time = entry_time + timedelta(seconds=duration)
    
    vehicle = random.choice(vehicles)
    track_id = random.randint(10, 500)
    zone = random.choice(zones)
    
    # Left zone might have slightly higher duration, right zone might have more counts
    if zone == "left" and vehicle == "mobil":
        duration = random.randint(300, 1800)  # Left zone: average longer stops
        violation_time = entry_time + timedelta(seconds=duration)
        
    screenshot = f"{PROJECT_ROOT}\\backend\\violations\\violation_{violation_time.strftime('%Y%m%d_%H%M%S')}_id{track_id}.jpg"
    
    mock_rows.append([
        entry_time.isoformat(timespec="seconds"),
        violation_time.isoformat(timespec="seconds"),
        f"{duration:.1f}",
        vehicle,
        str(track_id),
        zone,
        screenshot
    ])

# Sort by violation time so it is in chronological order
mock_rows.sort(key=lambda x: x[1])

with open(csv_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(headers)
    writer.writerows(mock_rows)

print(f"Successfully generated {len(mock_rows)} total records in violations_log.csv")

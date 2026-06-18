import json
import random
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
bronze_path = PROJECT_ROOT / "data" / "bronze" / "raw_detections.json"
bronze_path.parent.mkdir(parents=True, exist_ok=True)

# FMNoto CCTV stream URL
STREAM_URL = "https://cctv.jogjaprov.go.id/cctv-proxy/atcs-kota/FMNoto.stream/playlist.m3u8"

# Vehicle classes
VEHICLES = ["mobil", "mobil", "mobil", "motor", "motor", "bus", "truk"]
ZONES = ["left", "right"]

def main():
    print("Membuat data tiruan Bronze Layer (raw_detections.json)...")
    frames = []

    random.seed(42)
    today = datetime.now()

    # Generate 60 violation events, and 30 noise events
    track_counter = 100

    # Generate violations (stationary >= 120 seconds in forbidden zones)
    for i in range(60):
        day_offset = random.randint(0, 4)
        target_date = today - timedelta(days=day_offset)
        
        # Peak hours logic: more likelihood at 8-10 AM, 12-2 PM, and 5-8 PM
        hour_pool = [8, 9, 10, 12, 13, 14, 17, 18, 19, 20] * 3 + list(range(24))
        hour = random.choice(hour_pool)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        start_time = datetime(target_date.year, target_date.month, target_date.day, hour, minute, second)
        duration = random.randint(125, 1200) # 2m 5s to 20m (violators)
        
        vehicle = random.choice(VEHICLES)
        track_id = track_counter
        track_counter += 1
        zone = random.choice(ZONES)
        
        # Left zone might have slightly higher duration
        if zone == "left" and vehicle == "mobil":
            duration = random.randint(300, 1800)
        
        # Generate frame data at 5-second intervals during vehicle stay
        num_frames = duration // 5
        for f in range(num_frames):
            ts = start_time + timedelta(seconds=f * 5)
            # Randomize coordinates slightly to represent typical tracker coordinate jitter
            x1 = 100 + random.randint(-2, 2)
            y1 = 150 + random.randint(-2, 2)
            x2 = 250 + random.randint(-2, 2)
            y2 = 300 + random.randint(-2, 2)
            
            frames.append({
                "timestamp": ts.isoformat(),
                "stream_url": STREAM_URL,
                "frame_width": 960,
                "frame_height": 540,
                "detections": [{
                    "track_id": track_id,
                    "class_id": 2 if vehicle == "mobil" else (3 if vehicle == "motor" else (5 if vehicle == "bus" else 7)),
                    "class_name": vehicle,
                    "bbox": [x1, y1, x2, y2],
                    "confidence": round(random.uniform(0.75, 0.95), 2),
                    "center": [float((x1+x2)/2.0), float((y1+y2)/2.0)],
                    "zone_name": zone
                }]
            })

    # Generate noise events (vehicles passing by, staying < 120 seconds or not in a zone)
    for i in range(30):
        day_offset = random.randint(0, 4)
        target_date = today - timedelta(days=day_offset)
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        start_time = datetime(target_date.year, target_date.month, target_date.day, hour, minute, second)
        duration = random.randint(5, 45) # 5s to 45s (noise/non-violators)
        
        vehicle = random.choice(VEHICLES)
        track_id = track_counter
        track_counter += 1
        zone = random.choice(ZONES + [None]) # some outside zones
        
        num_frames = max(1, duration // 5)
        for f in range(num_frames):
            ts = start_time + timedelta(seconds=f * 5)
            x1 = 300 + f * 10
            y1 = 200 + f * 5
            x2 = 450 + f * 10
            y2 = 350 + f * 5
            
            frames.append({
                "timestamp": ts.isoformat(),
                "stream_url": STREAM_URL,
                "frame_width": 960,
                "frame_height": 540,
                "detections": [{
                    "track_id": track_id,
                    "class_id": 2 if vehicle == "mobil" else (3 if vehicle == "motor" else (5 if vehicle == "bus" else 7)),
                    "class_name": vehicle,
                    "bbox": [x1, y1, x2, y2],
                    "confidence": round(random.uniform(0.80, 0.95), 2),
                    "center": [float((x1+x2)/2.0), float((y1+y2)/2.0)],
                    "zone_name": zone
                }]
            })

    # Sort all frames chronologically to simulate a real continuous stream
    frames.sort(key=lambda x: x["timestamp"])

    # Overwrite the bronze JSON file
    with open(bronze_path, "w", encoding="utf-8") as f:
        for frame in frames:
            f.write(json.dumps(frame) + "\n")

    print(f"[SUCCESS] Berhasil menghasilkan {len(frames)} record frame deteksi raw di: {bronze_path}")

if __name__ == "__main__":
    main()

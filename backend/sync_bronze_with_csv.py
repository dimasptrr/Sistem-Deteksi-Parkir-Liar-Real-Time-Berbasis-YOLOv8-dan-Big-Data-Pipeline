import csv
import json
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
csv_path = PROJECT_ROOT / "backend" / "violations_log.csv"
bronze_path = PROJECT_ROOT / "data" / "bronze" / "raw_detections.json"

STREAM_URL = "https://cctv.jogjaprov.go.id/cctv-proxy/atcs-kota/FMNoto.stream/playlist.m3u8"

def main():
    print("=====================================================================")
    print("Singkronisasi Bronze Layer (raw_detections.json) dengan violations_log.csv")
    print("=====================================================================")

    if not csv_path.exists():
        print(f"[ERROR] violations_log.csv tidak ditemukan di: {csv_path}")
        return

    # 1. Baca data yang sudah ada di raw_detections.json untuk menghindari duplikasi
    existing_sessions = set() # set of (camera_id, track_id, date_str)
    if bronze_path.exists():
        print("[*] Membaca data Bronze yang sudah ada...")
        try:
            with open(bronze_path, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    data = json.loads(line)
                    cam_id = data.get("camera_id", "cam1")
                    ts_str = data.get("timestamp", "")[:10] # YYYY-MM-DD
                    for det in data.get("detections", []):
                        tid = det.get("track_id")
                        existing_sessions.add((cam_id, tid, ts_str))
        except Exception as e:
            print(f"[WARNING] Gagal membaca data Bronze: {e}")

    # 2. Baca data dari CSV dan buat frame deteksi jika belum ada di Bronze
    new_frames = []
    skipped_count = 0
    added_count = 0

    print("[*] Memproses violations_log.csv...")
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                cam_id = row.get("camera_id", "cam1")
                track_id = int(row["track_id"])
                
                # Parsing timestamp_entry dan timestamp_violation
                ts_entry_str = row["timestamp_entry"]
                ts_viol_str = row["timestamp_violation"]
                
                # Format isoformat parsing (e.g. 2026-06-19T17:32:03)
                dt_entry = datetime.fromisoformat(ts_entry_str)
                dt_viol = datetime.fromisoformat(ts_viol_str)
                date_str = ts_entry_str[:10]
                
                # Cek jika sesi ini sudah ada di Bronze
                if (cam_id, track_id, date_str) in existing_sessions:
                    skipped_count += 1
                    continue
                
                # Re-generate raw detections frames at 5-second intervals during vehicle stay
                duration = int(float(row.get("duration_seconds", 120)))
                num_frames = (duration // 5) + 1
                
                vehicle_type = row.get("vehicle_type", "mobil")
                zone_name = row.get("zone_name", "right")
                
                class_id = 2
                if vehicle_type == "mobil":
                    class_id = 2
                elif vehicle_type == "motor":
                    class_id = 3
                elif vehicle_type == "bus":
                    class_id = 5
                elif vehicle_type == "truk":
                    class_id = 7
                
                for f_idx in range(num_frames):
                    # Distribute frames from dt_entry to dt_viol
                    ts = dt_entry + timedelta(seconds=min(duration, f_idx * 5))
                    new_frames.append({
                        "timestamp": ts.isoformat(),
                        "stream_url": STREAM_URL,
                        "frame_width": 960,
                        "frame_height": 540,
                        "camera_id": cam_id,
                        "detections": [{
                            "track_id": track_id,
                            "class_id": class_id,
                            "class_name": vehicle_type,
                            "bbox": [100, 150, 250, 300],
                            "confidence": 0.9,
                            "center": [175.0, 225.0],
                            "zone_name": zone_name
                        }]
                    })
                
                # Tambah ke existing set agar tidak duplikat untuk entri serupa
                existing_sessions.add((cam_id, track_id, date_str))
                added_count += 1
            except Exception as e:
                # Lewati jika ada error baris kosong atau corrupt
                continue

    print(f"[*] Sesi terlewati (sudah ada di Bronze): {skipped_count}")
    print(f"[*] Sesi baru yang ditambahkan ke Bronze: {added_count}")

    if not new_frames:
        print("[SUCCESS] Bronze Layer sudah sinkron sepenuhnya dengan CSV.")
        return

    # 3. Baca semua data Bronze lama, gabungkan dengan data baru, lalu urutkan kronologis
    all_frames = []
    if bronze_path.exists():
        try:
            with open(bronze_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        all_frames.append(json.loads(line))
        except Exception:
            pass

    all_frames.extend(new_frames)
    
    # Urutkan berdasarkan timestamp secara kronologis
    print("[*] Mengurutkan seluruh data Bronze secara kronologis...")
    all_frames.sort(key=lambda x: x["timestamp"])

    # Tulis kembali ke raw_detections.json
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    with open(bronze_path, "w", encoding="utf-8") as f:
        for frame in all_frames:
            f.write(json.dumps(frame) + "\n")

    print(f"[SUCCESS] Sinkronisasi selesai! Total data Bronze: {len(all_frames)} record frame.")

if __name__ == "__main__":
    main()

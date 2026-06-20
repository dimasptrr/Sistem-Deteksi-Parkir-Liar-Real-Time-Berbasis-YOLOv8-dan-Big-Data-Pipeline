from __future__ import annotations

import argparse
import base64
import json
import logging
import sys
import time
from pathlib import Path

import cv2
import numpy as np

# Menambahkan root project ke sys.path untuk mengimpor backend
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.bronze_detector import ParkingDetector
from backend.kafka_config import KAFKA_BROKER, KAFKA_TOPIC, KAFKA_GROUP_ID

# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
LOGGER = logging.getLogger("kafka_consumer")


def main():
    parser = argparse.ArgumentParser(description="Kafka Consumer untuk Sistem Deteksi Parkir Liar")
    parser.add_argument(
        "--broker",
        type=str,
        default=KAFKA_BROKER,
        help="Alamat broker Kafka (host:port)"
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=KAFKA_TOPIC,
        help="Nama Kafka topic (bisa dipisah koma untuk multiple)"
    )
    parser.add_argument(
        "--group_id",
        type=str,
        default=KAFKA_GROUP_ID,
        help="Kafka Consumer Group ID"
    )
    parser.add_argument(
        "--no_gui",
        action="store_true",
        help="Jangan tampilkan window GUI OpenCV (jalankan headless)"
    )
    args = parser.parse_args()

    # Tentukan topic yang akan di-subscribe (mendukung 2 CCTV sekaligus)
    topics = [t.strip() for t in args.topic.split(",")]
    if len(topics) == 1 and topics[0] == KAFKA_TOPIC:
        topics = ["cctv-frames-cam1", "cctv-frames-cam2"]

    LOGGER.info(f"Memulai Kafka Consumer dengan Broker: {args.broker}, Topics: {topics}")

    # Impor confluent_kafka di sini
    try:
        from confluent_kafka import Consumer, KafkaError
    except ImportError:
        LOGGER.error("Library 'confluent-kafka' belum diinstal. Jalankan 'pip install -r requirements.txt'")
        sys.exit(1)

    # Inisialisasi ParkingDetector dengan 1 model YOLO untuk hemat RAM
    LOGGER.info("Menginisialisasi ParkingDetector (YOLOv8 + ByteTrack)...")
    try:
        detector = ParkingDetector(
            stream_url="cctv-kafka-stream",  # Penanda sumber umum
            model_name=str(PROJECT_ROOT / "yolov8n.pt"),
            confidence=0.35,
            display_width=960,
            stationary_speed_threshold=12.0,
            stationary_grace_seconds=5.0,
            violation_seconds=15 * 60,
        )
        detector._ensure_default_zones(960, 540)
    except Exception as e:
        LOGGER.error(f"Gagal menginisialisasi detector: {e}")
        sys.exit(1)

    # Konfigurasi Consumer Kafka
    conf = {
        'bootstrap.servers': args.broker,
        'group.id': args.group_id,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True,
        # Batasan ukuran pesan 2MB agar aman untuk frame gambar
        'message.max.bytes': 2097152
    }

    try:
        consumer = Consumer(conf)
        consumer.subscribe(topics)
        LOGGER.info(f"Berhasil berlangganan ke Kafka topics {topics}")
    except Exception as e:
        LOGGER.error(f"Gagal membuat Kafka Consumer: {e}")
        sys.exit(1)

    try:
        while True:
            # Poll pesan dari Kafka broker
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    LOGGER.error(f"Kafka Error: {msg.error()}")
                    break

            try:
                # Tentukan camera_id asal frame dari topic pesan
                topic_name = msg.topic()
                camera_id = "cam2" if "cam2" in topic_name else "cam1"

                # Decode JSON payload
                payload = json.loads(msg.value().decode('utf-8'))
                
                timestamp = payload.get("timestamp")
                width = payload.get("width")
                height = payload.get("height")
                stream_url = payload.get("stream_url")
                frame_data_b64 = payload.get("frame_data")

                if not frame_data_b64:
                    LOGGER.warning(f"Menerima pesan tanpa data frame gambar dari {topic_name}.")
                    continue

                # Dekode Base64 kembali ke bytes JPEG
                jpeg_bytes = base64.b64decode(frame_data_b64)
                
                # Konversi bytes ke numpy array
                nparr = np.frombuffer(jpeg_bytes, np.uint8)
                
                # Dekode data JPEG ke frame BGR OpenCV
                frame_bgr = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                if frame_bgr is None:
                    LOGGER.error("Gagal mendekode bytes JPEG ke frame BGR.")
                    continue

                # Jalankan logika deteksi parkir liar YOLOv8 + ByteTrack per camera_id
                annotated_frame = detector.process_frame(frame_bgr, camera_id=camera_id)

                # Dapatkan status statistik terbaru
                stats = detector.latest_stats_dict.get(camera_id, {})
                alert = detector.latest_alert_dict.get(camera_id)
                
                # Print status ringkas ke konsol
                alert_status = f" | [ALERT: {alert}]" if alert else ""
                LOGGER.info(
                    f"Frame {camera_id.upper()} dari {stream_url} | "
                    f"Dipantau: {stats.get('total_tracked', 0)} | "
                    f"Diam: {stats.get('stationary_count', 0)} | "
                    f"Pelanggaran Hari Ini: {stats.get('violations_today', 0)}{alert_status}"
                )

                # Tulis annotated frame ke file JPG agar server.py bisa baca & stream ke web UI
                frame_path = PROJECT_ROOT / "backend" / f"latest_frame_{camera_id}.jpg"
                try:
                    cv2.imwrite(str(frame_path), annotated_frame)
                except Exception as e:
                    LOGGER.error(f"Gagal menulis file visual frame {camera_id}: {e}")

                # Update status stats terpusat di latest_stats.json untuk UI
                stats_path = PROJECT_ROOT / "backend" / "latest_stats.json"
                existing_stats = {}
                if stats_path.exists():
                    try:
                        with open(stats_path, "r", encoding="utf-8") as f:
                            existing_stats = json.load(f)
                    except Exception:
                        pass
                
                existing_stats[camera_id] = {
                    "stats": stats,
                    "alert": alert,
                    "custom_zones": detector.zone_manager.is_custom(),
                    "zone_json": detector.zone_manager.to_json()
                }
                
                try:
                    with open(stats_path, "w", encoding="utf-8") as f:
                        json.dump(existing_stats, f, indent=2)
                except Exception as e:
                    LOGGER.error(f"Gagal menulis stats file: {e}")

                # Tampilkan visualisasi jika tidak dalam mode headless (no_gui)
                if not args.no_gui:
                    cv2.imshow("Deteksi Parkir Liar (Kafka Consumer)", annotated_frame)
                    if cv2.waitKey(1) & 0xFF == ord('q'):
                        LOGGER.info("Tombol 'q' ditekan. Keluar dari consumer...")
                        break

            except json.JSONDecodeError:
                LOGGER.error("Gagal mendekode format JSON pada pesan.")
            except Exception as e:
                LOGGER.error(f"Error saat memproses pesan Kafka: {e}")

    except KeyboardInterrupt:
        LOGGER.info("Consumer dihentikan secara manual oleh pengguna.")
    finally:
        LOGGER.info("Menutup Kafka Consumer...")
        consumer.close()
        if not args.no_gui:
            cv2.destroyAllWindows()
        LOGGER.info("Kafka Consumer selesai.")


if __name__ == "__main__":
    main()

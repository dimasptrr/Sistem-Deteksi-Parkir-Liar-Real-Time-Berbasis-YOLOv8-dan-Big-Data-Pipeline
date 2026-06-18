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
        help="Nama Kafka topic"
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

    LOGGER.info(f"Memulai Kafka Consumer dengan Broker: {args.broker}, Topic: {args.topic}")

    # Impor confluent_kafka di sini
    try:
        from confluent_kafka import Consumer, KafkaError
    except ImportError:
        LOGGER.error("Library 'confluent-kafka' belum diinstal. Jalankan 'pip install -r requirements.txt'")
        sys.exit(1)

    # Inisialisasi ParkingDetector
    # Kita tidak memanggil detector.start() karena capture thread-nya tidak kita butuhkan;
    # kita akan menyuapi frame ke detector.process_frame() secara manual dari Kafka Consumer.
    LOGGER.info("Menginisialisasi ParkingDetector (YOLOv8 + ByteTrack)...")
    try:
        detector = ParkingDetector(
            stream_url=args.topic,  # Nama topic sebagai penanda sumber
            model_name=str(PROJECT_ROOT / "yolov8n.pt"),
            confidence=0.35,
            display_width=960,
            stationary_speed_threshold=12.0,
            stationary_grace_seconds=5.0,
            violation_seconds=15 * 60,
        )
        # Panggil ensuransi zona default dengan dimensi frame standar
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
        consumer.subscribe([args.topic])
        LOGGER.info(f"Berhasil berlangganan ke Kafka topic '{args.topic}'")
    except Exception as e:
        LOGGER.error(f"Gagal membuat Kafka Consumer: {e}")
        sys.exit(1)

    try:
        while True:
            # Poll pesan dari Kafka broker
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Timeout poll, terus loop
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Akhir partisi tercapai, lanjutkan
                    continue
                else:
                    LOGGER.error(f"Kafka Error: {msg.error()}")
                    break

            try:
                # Decode JSON payload
                payload = json.loads(msg.value().decode('utf-8'))
                
                # Mendapatkan metadata dari payload
                timestamp = payload.get("timestamp")
                width = payload.get("width")
                height = payload.get("height")
                stream_url = payload.get("stream_url")
                frame_data_b64 = payload.get("frame_data")

                if not frame_data_b64:
                    LOGGER.warning("Menerima pesan tanpa data frame gambar.")
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

                # Jalankan logika deteksi parkir liar YOLOv8 + ByteTrack
                annotated_frame = detector.process_frame(frame_bgr)

                # Dapatkan status statistik terbaru
                stats = detector.latest_stats
                alert = detector.latest_alert
                
                # Print status ringkas ke konsol
                alert_status = f" | [ALERT: {alert}]" if alert else ""
                LOGGER.info(
                    f"Frame dari {stream_url} | "
                    f"Dipantau: {stats['total_tracked']} | "
                    f"Diam: {stats['stationary_count']} | "
                    f"Pelanggaran Hari Ini: {stats['violations_today']}{alert_status}"
                )

                # Tampilkan visualisasi jika tidak dalam mode headless (no_gui)
                if not args.no_gui:
                    cv2.imshow("Deteksi Parkir Liar (Kafka Consumer)", annotated_frame)
                    # Menunggu tombol 'q' ditekan untuk keluar
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

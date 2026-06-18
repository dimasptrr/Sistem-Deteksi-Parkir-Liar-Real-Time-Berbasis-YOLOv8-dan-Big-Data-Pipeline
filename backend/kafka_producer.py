from __future__ import annotations

import argparse
import base64
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import cv2

# Menambahkan root project ke sys.path untuk mengimpor backend
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.ffmpeg_capture import FFmpegPipeCapture
from backend.kafka_config import KAFKA_BROKER, KAFKA_TOPIC, DEFAULT_STREAM_URL

# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
LOGGER = logging.getLogger("kafka_producer")


def delivery_report(err, msg):
    """Callback untuk menerima status pengiriman pesan dari Kafka broker."""
    if err is not None:
        LOGGER.error(f"Gagal mengirim pesan ke Kafka: {err}")
    else:
        # Menghindari logging berlebih, cetak hanya log ringkas
        pass


def main():
    parser = argparse.ArgumentParser(description="Kafka Producer untuk Stream CCTV")
    parser.add_argument(
        "--stream_url",
        type=str,
        default=DEFAULT_STREAM_URL,
        help="URL HLS stream CCTV (.m3u8)"
    )
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
    args = parser.parse_args()

    LOGGER.info(f"Memulai Kafka Producer dengan Broker: {args.broker}, Topic: {args.topic}")
    LOGGER.info(f"Membuka stream CCTV: {args.stream_url}")

    # Mengimpor confluent_kafka di sini agar tidak gagal jika dependency belum diinstal
    try:
        from confluent_kafka import Producer
    except ImportError:
        LOGGER.error("Library 'confluent-kafka' belum diinstal. Jalankan 'pip install -r requirements.txt'")
        sys.exit(1)

    # Inisialisasi Producer Kafka
    conf = {
        'bootstrap.servers': args.broker,
        # Batasan ukuran pesan 2MB agar aman untuk frame gambar
        'message.max.bytes': 2097152
    }
    try:
        producer = Producer(conf)
    except Exception as e:
        LOGGER.error(f"Gagal membuat Kafka Producer: {e}")
        sys.exit(1)

    # Memulai capture stream menggunakan FFmpeg
    try:
        capture = FFmpegPipeCapture(args.stream_url, max_width=960)
    except Exception as e:
        LOGGER.error(f"Gagal membuka capture stream: {e}")
        sys.exit(1)

    frame_count = 0
    try:
        while capture.isOpened():
            start_time = time.time()
            ok, frame = capture.read()
            if not ok or frame is None:
                LOGGER.warning("Gagal membaca frame dari stream, mencoba membaca ulang...")
                time.sleep(0.5)
                continue

            frame_count += 1
            
            # Kompresi frame BGR ke format JPEG untuk menghemat bandwidth
            success, encoded_img = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), 80])
            if not success:
                LOGGER.error("Gagal mengompresi frame ke JPEG")
                continue

            # Encode data JPEG biner ke string Base64
            frame_base64 = base64.b64encode(encoded_img.tobytes()).decode('utf-8')

            # Dapatkan metadata frame
            timestamp_str = datetime.now().isoformat()
            height, width = frame.shape[:2]

            # Membuat payload JSON
            payload = {
                "timestamp": timestamp_str,
                "width": width,
                "height": height,
                "stream_url": args.stream_url,
                "frame_data": frame_base64
            }

            payload_bytes = json.dumps(payload).encode('utf-8')

            try:
                # Mengirim pesan ke Kafka topic
                producer.produce(
                    args.topic,
                    value=payload_bytes,
                    callback=delivery_report
                )
                # Memicu pemanggilan callback pengiriman secara asinkron
                producer.poll(0)
            except BufferError:
                LOGGER.warning("Antrean pesan Kafka penuh, melakukan flushing...")
                producer.flush()
                # Coba kirim ulang setelah flush
                producer.produce(
                    args.topic,
                    value=payload_bytes,
                    callback=delivery_report
                )
            except Exception as e:
                LOGGER.error(f"Error saat mengirim pesan Kafka: {e}")

            if frame_count % 50 == 0:
                LOGGER.info(f"Berhasil mempublikasikan {frame_count} frame ke topic '{args.topic}'")

            # Batasi FPS pengiriman (~25 FPS -> 40ms interval antar frame)
            elapsed = time.time() - start_time
            sleep_time = max(0, 0.04 - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        LOGGER.info("Producer dihentikan secara manual oleh pengguna.")
    finally:
        LOGGER.info("Melakukan flushing sisa pesan Kafka...")
        producer.flush()
        LOGGER.info("Menutup capture stream...")
        capture.release()
        LOGGER.info("Kafka Producer selesai.")


if __name__ == "__main__":
    main()

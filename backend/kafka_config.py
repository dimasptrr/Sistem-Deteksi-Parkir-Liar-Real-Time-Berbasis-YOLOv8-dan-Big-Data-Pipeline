# Configuration file untuk Apache Kafka

# Alamat broker Kafka default
KAFKA_BROKER = "localhost:9092"

# Nama topic untuk stream frame CCTV
KAFKA_TOPIC = "cctv-frames"

# Group ID untuk consumer Kafka
KAFKA_GROUP_ID = "parkir-detector-group"

# URL stream CCTV default (menggunakan stream Jati Pulo yang sudah ada)
DEFAULT_STREAM_URL = "https://cctv.jogjaprov.go.id/cctv-proxy/atcs-kota/FMNoto.stream/playlist.m3u8"


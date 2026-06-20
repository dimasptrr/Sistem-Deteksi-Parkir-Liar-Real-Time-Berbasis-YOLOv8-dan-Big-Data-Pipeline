# Configuration file untuk Apache Kafka

# Alamat broker Kafka default
KAFKA_BROKER = "localhost:9092"

# Nama topic untuk stream frame CCTV
KAFKA_TOPIC = "cctv-frames"
KAFKA_TOPIC_CAM1 = "cctv-frames-cam1"
KAFKA_TOPIC_CAM2 = "cctv-frames-cam2"

# Group ID untuk consumer Kafka
KAFKA_GROUP_ID = "parkir-detector-group"

# URL stream CCTV default (menggunakan stream Jati Pulo / Jogja yang sudah ada)
DEFAULT_STREAM_URL = "https://cctv.jogjaprov.go.id/cctv-proxy/atcs-kota/FMNoto.stream/playlist.m3u8"
DEFAULT_STREAM_URL_CAM2 = "https://cctv.jogjaprov.go.id/cctv-proxy/cctv-kominfosleman/SimpangTantular1.stream/chunklist_w836255020.m3u8"


# 🚫 Parkir Liar Detector — Real-Time IoT & Big Data Lakehouse System

Sistem pemantauan dan deteksi pelanggaran parkir liar secara *real-time* berbasis kecerdasan buatan (*computer vision*) dengan arsitektur **Event-Driven Streaming** menggunakan **Apache Kafka**, penyimpanan data terstruktur **Data Lakehouse** (Bronze, Silver, Gold), pengolahan analitik data besar (**Big Data**) menggunakan **Apache Spark**, serta visualisasi antarmuka interaktif menggunakan **FastAPI, Streamlit, dan Chart.js**.

---

## 📝 Rubrik 1: Identifikasi Masalah & Latar Belakang

1. Masalah parkir liar di Yogyakarta terus memburuk akibat ketimpangan ekstrem antara volume kendaraan (lebih dari 1 juta saat musim liburan berdasarkan detik.com) dengan kapasitas ruang parkir resmi yang sangat terbatas. Akibatnya, Menumpuknya parkir ilegal terjadi di lebih dari 130 ruas jalan(berdasarkan artikel pandangan jogja).

Pendekatan konvensional yang ada saat ini terbukti gagal menyelesaikan masalah karena beberapa gap operasional:

- Penindakan bergantung pada laporan manual masyarakat dengan waktu respons 1 hingga 3 jam.
- Jeda waktu penanganan mengakibatkan >50% pelanggar sudah kabur sebelum petugas tiba di lokasi.
- Ketiadaan bukti digital membuat sanksi denda maksimal Rp 50.000.000 (Perda No. 2 Tahun 2019) tidak efektif memberikan efek jera.

Sistem kami dibangun untuk menutup celah tersebut. Dengan memanfaatkan stream dari API CCTV yang diintegrasikan dengan deteksi Computer Vision, platform web ini secara otonom memantau titik rawan, menangkap snapshot bukti pelanggaran secara real-time, dan menyediakan dashboard statistik proaktif untuk mengarahkan operasi petugas secara presisi.

2. Mengapa Big Data Diperlukan? (Kerangka 5V)
Sistem web yang menarik umpan video langsung (Live Cam) dari API CCTV kota (seperti ATCS Dishub) dan memprosesnya dengan AI akan menghasilkan lalu lintas data berskala masif. 

- Volume: Mengambil data (fetching) dari  API CCTV di seluruh Jogja berarti menampung gambar setiap harinya. Skala data video mentah dan snapshot ini lumayan besar dan membutuhkan manajemen penyimpanan yang terukur di backend.

- Velocity: Data dari API CCTV masuk dalam bentuk streaming secara konstan (sekian frame per second). Model AI harus melakukan inferencing (mendeteksi kendaraan dan menghitung durasi berhenti) secara real-time (kecepatan tinggi) agar statistik di web dashboard selalu mutakhir tanpa penundaan.

- Variety: Sistem ini harus sanggup menelan tipe data yang sangat bervariasi, aliran video tak terstruktur (RTSP/HTTP stream), gambar statis (snapshot pelanggaran), log semi-terstruktur (berkas JSON), hingga data terstruktur (statistik jumlah pelanggaran per jalan).

- Veracity: Di jalanan, AI akan menghadapi noise yang tinggi—seperti pantulan cahaya malam hari, hujan, atau kendaraan yang tertutup (occluded) kendaraan lain. Big Data dibutuhkan untuk melatih dan menyaring data ini agar insight yang dihasilkan akurat, memastikan sistem bisa membedakan mobil yang terjebak macet dengan mobil yang sengaja parkir liar.

- Value: Aliran data yang besar tidak ada gunanya tanpa visualisasi yang tepat. Melalui web dashboard, data itu diubah menjadi Value, seperti statistik live, grafik tren jam rawan, dan rekomendasi otomatis bagi aparat untuk melakukan penertiban secara presisi.

Mengapa Sistem Saat Ini Belum Menyelesaikan Masalah?
Sistem web berbasis AI yang kami rancang ini menutup celah besar (gap) dari metode penanganan yang saat ini dijalankan oleh pemerintah daerah:

**Gap Pengumpulan Bukti** (Manual vs Bukti Digital Otomatis)
- Kondisi Saat Ini: Penindakan hukum terhambat karena kendaraan sudah pergi saat petugas tiba
- Solusi Sistem Web AI: AI secara otomatis menangkap (capture) plat nomor/kendaraan dari stream CCTV saat terdeteksi berhenti melampaui batas waktu yang ditentukan. Snapshot beserta timestamp-nya otomatis tersimpan di database web sebagai bukti mutlak.

**Gap Pengambilan Keputusan** (Reaktif Berdasarkan Aduan vs Proaktif Berdasarkan Statistik)

- Kondisi Saat Ini: Dishub dan Polisi seringkali bergerak berdasarkan laporan viral di media sosial atau E-Lapor (yang memakan waktu verifikasi), sehingga kemacetan terlanjur parah.
- Solusi Sistem Web AI: Dashboard web menyajikan live footage cctv di lapangan. Petugas tidak perlu menunggu laporan; mereka bisa memantau tren yang muncul di monitor web dan mengirim personel sebelum kemacetan akibat parkir liar mengular.
## 📝 Rubrik 2: Desain Infrastruktur & Arsitektur Terdistribusi (Event-Driven)

Sistem ini dirancang menggunakan paradigma **Event-Driven Architecture (EDA)** berlatensi rendah untuk memastikan pemrosesan data berjalan secara terdistribusi dan *scalable* (siap menangani ratusan kamera secara simultan).

### 2.1 Alur Aliran Data (Data Pipeline Flow)

```text
 [ CCTV HLS Streams (Cam 1: FM Noto & Cam 2: Simpang Tantular) ]
                     │
                     ▼ (Low-latency capture)
              [ FFmpeg Capture ]
                     │
                     ▼ (Raw video frame bytes)
             [ Kafka Producer ] ───► Publikasi Base64 frame & metadata ke broker
                     │
                     ▼ (Topic: "cctv-frames")
                 [ Apache Kafka ]
                     │
                     ▼ (Subscriber polls frames)
              [ Kafka Consumer ]
                     │
                     ├─► [ YOLOv8 Object Detection ]  (Deteksi kendaraan)
                     ├─► [ ByteTrack Multi-Object ]   (Pelacakan track ID unik per Cam)
                     ├─► [ Zone Manager Geofence ]    (Pengecekan Polygon merah per Cam)
                     └─► [ Real-Time Detector State ] (Buffered writing ke Bronze)
                                 │
                                 ├─► [ Alarm Pelanggaran ] ──► Update UI real-time
                                 ├─► [ Foto Screenshot ]   ──► Simpan ke backend/violations/
                                 └─► [ Bronze Layer ]      ──► data/bronze/raw_detections.json
                                             │
                                             ▼ (Spark Batch ETL)
                                   [ Apache Spark Engine ]
                                             │
                                             ├─► [ Silver Layer ] ──► data/silver/violations_clean.parquet
                                             └─► [ Gold Layer ]   ──► data/gold/*.parquet
                                                                            │
                                                                            ▼ (Query Parquet langsung per camera_id)
                                                                    [ Dashboard Web UI ]
                                                              (FastAPI + Streamlit + Chart.js)
```

### 2.2 Peran Komponen Utama
- **FFmpeg Capture**: Mengambil video streaming berformat HLS (`.m3u8`) dengan latensi sangat rendah, mengubah frame gambar BGR mentah menjadi buffer byte.
- **Apache Kafka (Broker)**: Bertindak sebagai bus data terdistribusi. Producer mengirimkan data frame terkompresi Base64 ke Kafka Topic `cctv-frames`. Hal ini mencegah kehilangan data jika subsistem deteksi (YOLO) sedang mengalami perlambatan (*backpressure*).
- **YOLOv8 & ByteTrack**: Membaca frame dari Kafka secara asinkron, mengenali jenis kendaraan, mengunci identitas unik (*track ID*) kendaraan, dan melacak pergerakannya.
- **FastAPI Backend**: Sebagai gerbang API terpadu yang memanajemeni status detektor, menyajikan stream visual teranotasi MJPEG, dan menyediakan data analitik ke frontend.

### 2.3 Desain Skalabilitas Multi-Kamera (Multi-Camera Ingestion)
Sistem dirancang untuk mendukung skalabilitas multi-kamera secara efisien dengan strategi berikut:
1. **Pemisahan Data via Metadata (`camera_id`)**: Setiap frame yang dikirim oleh Kafka Producer membawa payload terstruktur berisi `camera_id` (misalnya: `cam1` atau `cam2`), `timestamp`, `stream_url`, dan `frame_bytes`.
2. **Optimasi Resource Memory (RAM 8GB)**: Alih-alih membuat satu instance consumer & model YOLOv8 per kamera (yang memakan banyak RAM), sistem ini menggunakan **Single Consumer multi-camera model**. Consumer mendengarkan frame dari semua kamera secara round-robin, lalu memprosesnya menggunakan model YOLOv8 yang dimuat sekali di memory. State pelacakan (ByteTrack) dipisahkan menggunakan dictionary terisolasi per `camera_id` guna mencegah pencampuran tracking objek.
3. **Pemisahan Output Analytics**: Seluruh hasil deteksi dicatat ke dalam satu file Bronze Layer (`raw_detections.json`) dengan pembeda kolom `camera_id`. Spark Batch kemudian memproses data ini secara tersegregasi untuk menghasilkan file Gold Parquet terpisah atau terindeks berdasarkan `camera_id`, sehingga visualisasi dashboard di frontend bersifat mandiri untuk masing-masing kamera.

---

## 📝 Rubrik 3: Implementasi Data Lakehouse (Bronze, Silver, Gold Layer)

Untuk menyusun sistem pengolahan data yang rapi dan meminimalkan beban komputasi real-time, kami mengadopsi konsep **Data Lakehouse** berbasis penyimpanan file teroptimasi (**Parquet**).

| Layer Lakehouse | Format Data | Lokasi File | Deskripsi & Fungsi |
| :--- | :--- | :--- | :--- |
| **🥉 Bronze Layer** <br>(Raw Data) | JSON Lines (`.json`) | `data/bronze/raw_detections.json` | Menyimpan seluruh metadata frame dan deteksi YOLO mentah apa adanya (koordinat bounding box, confidence score, nama kelas, timestamp, dan zona) pada setiap detik. |
| **🥈 Silver Layer** <br>(Clean Data) | Parquet (`.parquet`) | `data/silver/violations_clean.parquet` | Data hasil pembersihan oleh Spark: Hanya menyimpan kendaraan yang terbukti berhenti di zona larangan parkir (`left`/`right`) dengan durasi diam $\ge 2$ menit (120 detik), serta membersihkan duplikasi track ID. |
| **🥇 Gold Layer** <br>(Aggregated Data) | Parquet (`.parquet`) | `data/gold/*.parquet` | Hasil agregasi data Silver oleh Apache Spark yang siap dikueri secara instan oleh dashboard grafik untuk efisiensi performa tinggi. |

### Mengapa Menggunakan Parquet untuk Silver & Gold?
Parquet adalah format file penyimpanan berbasis kolom (*columnar storage*) yang sangat terkompresi. Membaca data Parquet berkali-kali lebih cepat daripada CSV atau JSON konvensional karena sistem hanya memuat kolom yang dibutuhkan (misal: hanya kolom `timestamp` dan `duration_seconds`) tanpa perlu memindai seluruh baris file.

---

## 📝 Rubrik 4: Teknis Analisis & Kualitas Output (Apache Spark)

Pemrosesan batch dan agregasi analitik dilakukan oleh **Apache Spark (PySpark)** secara terdistribusi. Spark memproses raw JSON dari Bronze Layer menjadi bentuk agregat terstruktur di Gold Layer.

### 4.1 Logika Analisis Spark (ETL Pipeline)
1. **Pembersihan Data (Bronze -> Silver)**:
   - Spark membaca `raw_detections.json`, men-flatten struktur data array deteksi YOLO, dan menyaring record yang memiliki koordinat di dalam zona merah (`left` atau `right`).
   - Melakukan pengelompokan (*grouping*) data berdasarkan `track_id` dan `zone_name`.
   - Menghitung waktu masuk awal (`timestamp_entry` = min timestamp) dan waktu deteksi terakhir (`timestamp_violation` = max timestamp).
   - Menghitung `duration_seconds` (`timestamp_violation - timestamp_entry`).
   - Menyaring data guna membuang noise lalu lintas (kendaraan lewat yang diam kurang dari 120 detik dibuang).
   - Menyimpan ke `violations_clean.parquet`.

2. **Perhitungan Metrik Kebijakan (Silver -> Gold)**:
   - **Illegal Parking Index (IPI)**: Spark mengkalkulasi indeks tingkat keparahan pelanggaran per zona menggunakan formula:
     $$\text{IPI} = \min\left(10, (\text{Jumlah Pelanggaran} \times 0.4) + \left(\frac{\text{Durasi Rata-Rata (detik)}}{300}\right)\right)$$
   - **Analisis Dampak Kemacetan**: Mengelompokkan zona ke dalam tingkat prioritas tindakan (RENDAH, SEDANG, TINGGI) berdasarkan nilai IPI beserta rekomendasi kebijakan (misal: derek paksa atau pemasangan rambu tambahan).
   - **Temporal & Sektoral Analysis**: Mengagregasi pelanggaran per jam (`hourly_stats.parquet`), tren harian (`daily_trend.parquet`), dan distribusi kelas kendaraan (`vehicle_stats.parquet`).

### 4.2 Konsistensi Waktu & Manajemen Timezone (Timezone Consistency)
Untuk memastikan keandalan analisis tren harian dan jam rawan kemacetan, sistem ini menjamin konsistensi zona waktu (*timezone*) melalui pengaturan berikut:
1. **Konfigurasi Spark Session**: Spark dikonfigurasi secara eksplisit menggunakan `.config("spark.sql.session.timeZone", "Asia/Jakarta")` pada file [spark_silver_gold.py](file:///d:/Kuliah/Semester%204/Big%20Data%20Dan%20Data%20Lakehouse/EAS%20BIG%20DATA/parkir-liar-detector/backend/spark_silver_gold.py). Hal ini memaksa seluruh pengolahan fungsi tanggal dan waktu di PySpark (seperti `hour()`, `date_format()`, dan fungsi casting timestamp) menggunakan zona waktu **WIB (Waktu Indonesia Barat / UTC+07:00)** secara konsisten.
2. **Pencegahan Pergeseran Jam (Time-Shift)**: Langkah ini mencegah data bergeser ke UTC+00:00 saat diproses di Spark (terutama jika Spark dijalankan di VM/container dengan setelan default UTC), yang dapat merusak analisis jam sibuk pelanggaran (jam rawan) di dashboard analitik.
3. **Sinkronisasi Detektor & Database**: Timestamp mentah yang dicatat oleh Kafka Producer menggunakan ISO format lokal (`datetime.now().isoformat()`), sehingga ketika diurai oleh Spark dan disimpan ke format Parquet, nilainya tetap sinkron dengan waktu nyata di lapangan (Yogyakarta).

---

## 📝 Rubrik 5: Inovasi Solusi & Sistem Pencegah False Alarm

Aplikasi ini menghadirkan inovasi berupa **Multi-Level Filtering System** untuk menghindari alarm palsu (*false alarm*), seperti mendeteksi mobil macet atau pejalan kaki sebagai pelanggar parkir.

1. **Inovasi 1: Spasial Filter (Geofencing Polygon)**
   Menggunakan algoritma *Point-in-Polygon* (PIP) di [zone_manager.py](file:///d:/Kuliah/Semester%204/Big%20Data%20Dan%20Data%20Lakehouse/EAS%20BIG%20DATA/parkir-liar-detector/backend/zone_manager.py) untuk memastikan koordinat centroid kendaraan benar-benar berada di dalam batas bahu jalan dilarang parkir yang digambar secara kustom.
2. **Inovasi 2: Temporal Filter (Double Threshold Duration)**
   Membedakan status kendaraan berdasarkan durasi berhenti:
   - `< 5 detik`: Kendaraan bergerak normal (Abaikan).
   - `5 - 120 detik` (Toleransi): Status visual berwarna **Kuning** (Peringatan awal).
   - `> 120 detik` (Pelanggaran): Status berwarna **Merah** (Pelanggaran resmi, otomatis mengambil screenshot bukti dan dicatat ke Lakehouse).
3. **Inovasi 3: Tracking Filter (ByteTrack)**
   Mencegah hilangnya jejak identitas kendaraan akibat oklusi (terhalang pohon atau kendaraan lain melintas). ID kendaraan dikunci secara konsisten untuk mencegah pendeteksian berulang (*double logging*).
4. **Inovasi 4: Semantik Filter (YOLOv8 Target)**
   Menyaring kelas deteksi. Sistem hanya mengidentifikasi objek bertipe kendaraan (`mobil`, `motor`, `bus`, `truk`) dan mengabaikan objek lain seperti pejalan kaki atau bayangan.

---

## 📝 Rubrik 6: Demo Sistem & Panduan Langkah-Langkah

### 6.1 Peta Fungsi Kode Program & Logika Krusial (Codebase Mapping)

Berikut adalah peran dari masing-masing file kode program di dalam sistem beserta **logika penting (krusial)** yang diimplementasikan:

*   **[bronze_detector.py](file:///d:/Kuliah/Semester%204/Big%20Data%20Dan%20Data%20Lakehouse/EAS%20BIG%20DATA/parkir-liar-detector/backend/bronze_detector.py)**
    *   *Peran*: Core Engine Deteksi Real-Time.
    *   *Logika Krusial*: Mengintegrasikan model **YOLOv8** untuk klasifikasi kendaraan dengan **ByteTrack** untuk pelacakan objek unik (*track ID*) secara terpisah per kamera. Status durasi kendaraan diam dikelola oleh class `VehicleState`. Jika centroid bounding box bergeser di bawah nilai toleransi kecepatan piksel, kendaraan dianggap diam dan variabel `stationary_since` diaktifkan. Jika durasi diam melampaui `stationary_grace_seconds` (5 detik), status visual berubah menjadi **Kuning** (Peringatan). Jika terus diam hingga melampaui `violation_seconds` (120 detik), status berubah menjadi **Merah** (Pelanggaran), yang secara otomatis memicu penyimpanan screenshot bukti fisik (`.jpg`) dan menulis record event deteksi mentah secara konstan ke **Bronze Layer** (`raw_detections.json`).

*   **[zone_manager.py](file:///d:/Kuliah/Semester%204/Big%20Data%20Dan%20Data%20Lakehouse/EAS%20BIG%20DATA/parkir-liar-detector/backend/zone_manager.py)**
    *   *Peran*: Manajemen Bahu Jalan Dilarang Parkir (Geofencing).
    *   *Logika Krusial*: Mengimplementasikan algoritma **Point-in-Polygon (PIP)** menggunakan library `Shapely`. Script ini menentukan apakah titik tengah (*centroid*) bawah dari bounding box kendaraan (koordinat X,Y) berada di dalam batas area polygon zona larangan (`left` atau `right`). Zona koordinat ini juga dapat diperbarui secara dinamis oleh pengguna melalui web UI FastAPI.

*   **[ffmpeg_capture.py](file:///d:/Kuliah/Semester%204/Big%20Data%20Dan%20Data%20Lakehouse/EAS%20BIG%20DATA/parkir-liar-detector/backend/ffmpeg_capture.py)**
    *   *Peran*: Ingestion CCTV Stream Berlatensi Rendah.
    *   *Logika Krusial*: Menjalankan program `FFmpeg` eksternal di latar belakang sebagai subprocess Python. Frame didecode dari stream HLS (`.m3u8`) CCTV Yogyakarta langsung dari `stdout` pipa (pipe) subprocess menjadi buffer bytes mentah (format BGR/RGB) tanpa menulis ke disk. Proses ini berjalan secara *non-blocking* di thread terpisah untuk meminimalkan jeda waktu (*network lag*) video.

*   **[kafka_producer.py](file:///d:/Kuliah/Semester%204/Big%20Data%20Dan%20Data%20Lakehouse/EAS%20BIG%20DATA/parkir-liar-detector/backend/kafka_producer.py)**
    *   *Peran*: Ingestion Pipeline Data Terdistribusi (Producer).
    *   *Logika Krusial*: Berjalan di thread terpisah untuk menangkap frame dari HLS stream, mengompresnya ke dalam format JPEG untuk meminimalkan bandwidth, melakukan encoding byte tersebut ke format **Base64 String**, dan mempublikasikan data terstruktur (berisi `camera_id`, `timestamp`, `stream_url`, dan `frame_data`) ke Kafka Topic `cctv-frames-<camera_id>` secara asinkron.

*   **[kafka_consumer.py](file:///d:/Kuliah/Semester%204/Big%20Data%20Dan%20Data%20Lakehouse/EAS%20BIG%20DATA/parkir-liar-detector/backend/kafka_consumer.py)**
    *   *Peran*: Sub-sistem Pemrosesan Terdistribusi (Consumer & YOLO).
    *   *Logika Krusial*: Bertindak sebagai subsistem terdistribusi yang mendengarkan frame dari broker Kafka untuk beberapa topik kamera secara round-robin. Untuk menghemat RAM, consumer ini memuat model YOLOv8 sekali saja ke memori. Byte gambar didekode kembali menjadi matriks BGR, lalu diproses oleh detektor. State pelacakan dipisahkan secara terisolasi per kamera menggunakan dictionary. Setelah diproses, visual frame beranotasi disimpan ke disk (`latest_frame_<camera_id>.jpg`) dan metrik real-time diekspor ke file terpusat `latest_stats.json` agar dapat dikonsumsi oleh API server.

*   **[spark_silver_gold.py](file:///d:/Kuliah/Semester%204/Big%20Data%20Dan%20Data%20Lakehouse/EAS%20BIG%20DATA/parkir-liar-detector/backend/spark_silver_gold.py)**
    *   *Peran*: Engine Batch Processing Big Data (Apache Spark).
    *   *Logika Krusial*:
        *   **Bronze -> Silver**: Spark membaca file JSON besar, mem-flatten data deteksi YOLO, mengonversi string timestamp menjadi tipe `Timestamp` Spark. Menggunakan **Window Partitioning** (berdasarkan `camera_id`, `track_id`, dan `zone_name` terurut kronologis) untuk menghitung selisih waktu (`gap_seconds`) antar-frame. Jika gap terdeteksi > 120 detik, Spark mengklasifikasikannya sebagai sesi parkir baru (`session_id`). Data dikelompokkan berdasarkan sesi untuk menghitung durasi berhenti bersih (`duration_seconds`). Data yang berhenti $\ge 120$ detik disimpan ke format **Parquet** (Silver Layer) untuk mengeliminasi duplikasi track ID dan data noise.
        *   **Silver -> Gold**: Spark mengagregasi data Silver untuk menghitung metrik kebijakan seperti **Illegal Parking Index (IPI)** per zona, menentukan dampak kemacetan dan rekomendasi kebijakan secara otomatis, serta mengagregasi statistik jam rawan (`hourly_stats.parquet`), tren harian (`daily_trend.parquet`), dan distribusi kendaraan (`vehicle_stats.parquet`).

*   **[sync_bronze_with_csv.py](file:///d:/Kuliah/Semester%204/Big%20Data%20Dan%20Data%20Lakehouse/EAS%20BIG%20DATA/parkir-liar-detector/backend/sync_bronze_with_csv.py)**
    *   *Peran*: Sinkronisasi Data Manual/CSV ke Lakehouse.
    *   *Logika Krusial*: Berfungsi menyinkronkan data dari berkas log CSV (`violations_log.csv`) hasil input penindakan manual ke dalam format raw JSON di Bronze Layer. Logika pentingnya adalah membaca data CSV, menyaring entri yang belum terdaftar di Bronze, meregenerasi baris-baris frame deteksi tiruan dengan interval 5 detik sepanjang masa durasi berhenti kendaraan tersebut, dan menulisnya kembali ke `raw_detections.json` dengan urutan kronologis yang rapi sehingga dapat diolah secara utuh oleh Spark.

*   **[generate_mock_bronze.py](file:///d:/Kuliah/Semester%204/Big%20Data%20Dan%20Data%20Lakehouse/EAS%20BIG%20DATA/parkir-liar-detector/backend/generate_mock_bronze.py)**
    *   *Peran*: Generator Data Simulasi Skala Besar.
    *   *Logika Krusial*: Mensimulasikan data deteksi YOLO historis ribuan baris dengan variasi hari, jam sibuk (pagi/sore), dan jenis kendaraan untuk menguji performa engine Apache Spark PySpark dalam mengolah jutaan baris data secara lokal.

*   **[server.py](file:///d:/Kuliah/Semester%204/Big%20Data%20Dan%20Data%20Lakehouse/EAS%20BIG%20DATA/parkir-liar-detector/frontend/server.py)**
    *   *Peran*: API Gateway & Backend FastAPI.
    *   *Logika Krusial*: Menyediakan endpoint `/video_feed` yang menyajikan stream visual MJPEG real-time dengan membaca frame beranotasi terbaru dari disk secara asinkron. Menyediakan endpoint analitik `/api/analytics` yang **membaca langsung file Gold Parquet** menggunakan library `Pandas`/`PyArrow` (memanfaatkan kecepatan columnar format Parquet tanpa query database yang berat) serta memiliki mekanisme fallback ke CSV/JSON jika database Parquet belum siap.

*   **[app.py](file:///d:/Kuliah/Semester%204/Big%20Data%20Dan%20Data%20Lakehouse/EAS%20BIG%20DATA/parkir-liar-detector/frontend/app.py)**
    *   *Peran*: Streamlit Entrypoint & Process Wrapper.
    *   *Logika Krusial*: Mengontrol inisialisasi aplikasi. Menggunakan dekorator `@st.cache_resource` untuk menjalankan server FastAPI (Uvicorn) dalam thread latar belakang secara *singleton* (tepat sekali saja) selama session Streamlit berlangsung. Ini mencegah konflik kegagalan pengikatan port (port 8080) akibat reload antarmuka Streamlit.

*   **[index.html](file:///d:/Kuliah/Semester%204/Big%20Data%20Dan%20Data%20Lakehouse/EAS%20BIG%20DATA/parkir-liar-detector/frontend/index.html)**
    *   *Peran*: Antarmuka Pengguna Dashboard Interaktif.
    *   *Logika Krusial*: Halaman dashboard utama berbasis *glassmorphism UI*. Menggunakan **Chart.js** untuk merender tren analitik, *real-time polling* untuk memperbarui statistik live tanpa refresh halaman, modal pop-up untuk memvalidasi tangkapan bukti foto pelanggaran, dan menyediakan SOP penindakan parkir liar bagi petugas Dinas Perhubungan Yogyakarta.

### 6.2 Cara Menjalankan Aplikasi

#### Langkah 1: Persiapan Environment
Instal pustaka Python yang diperlukan termasuk PySpark dan PyArrow (untuk Parquet):
```bash
pip install -r requirements.txt
pip install pyspark pyarrow
```

#### Langkah 2: Jalankan Apache Kafka (Docker)
Buka Docker Desktop, kemudian jalankan Kafka Broker pada terminal di root direktori proyek Anda:
```bash
docker-compose up -d
```
*Gunakan `docker-compose ps` untuk memastikan broker Kafka telah aktif.*

#### Langkah 3: Inisialisasi Data Lakehouse & Jalankan Spark ETL
Buat data historis tiruan ke dalam Bronze Layer agar grafik dashboard langsung terisi, lalu jalankan Spark Job untuk memprosesnya ke Silver dan Gold Parquet:
1.  **Generate data Bronze**:
    ```bash
    python backend/generate_mock_bronze.py
    ```
2.  **Sinkronisasi dengan violations_log.csv**:
    Jika Anda memiliki rekaman deteksi real-time dari log CSV yang ingin dimasukkan ke dalam visualisasi analitik Spark, jalankan:
    ```bash
    python backend/sync_bronze_with_csv.py
    ```
3.  **Jalankan Spark Job (Bronze -> Silver -> Gold)**:
    ```bash
    python backend/spark_silver_gold.py
    ```

#### Langkah 4: Jalankan Real-Time Streaming Pipeline
Buka tiga terminal terpisah untuk mengalirkan video HLS CCTV Cam 1 dan Cam 2 secara *live*:
*   **Terminal 1 (Kafka Producer - Cam 1)**:
    ```bash
    python backend/kafka_producer.py --camera_id cam1
    ```
*   **Terminal 2 (Kafka Producer - Cam 2)**:
    ```bash
    python backend/kafka_producer.py --camera_id cam2
    ```
*   **Terminal 3 (Kafka Consumer + YOLO)**:
    ```bash
    python backend/kafka_consumer.py
    ```
    *Catatan RAM 8GB Optimization: Sistem ini menggunakan 1 proses consumer tunggal dengan 1 model YOLOv8 yang di-load sekali di memory. Consumer ini mendengarkan frame dari kedua kamera sekaligus secara round-robin, lalu memisahkan tracking state menggunakan dictionary per camera_id dan membagikannya ke FastAPI server via file disk. Hal ini mencegah memory leak atau pemakaian RAM berlebih karena tidak memuat model YOLO dua kali.*

#### Langkah 5: Jalankan Dashboard Web
Buka terminal baru dan jalankan Streamlit:
```bash
streamlit run frontend/app.py
```
Akses dashboard pada peramban Anda di: `http://localhost:8501`

### 6.3 Skenario Demonstrasi di Depan Penguji
1.  **Alur CCTV Real-Time**: Tunjukkan video CCTV FMNoto langsung. Ketika mobil berhenti di zona merah, kotak YOLO berubah kuning (toleransi), lalu setelah melewati batas waktu menjadi merah (pelanggaran), memicu alarm, menyimpan screenshot bukti, dan data mentahnya masuk ke **Bronze Layer**.
2.  **Peran Apache Spark (Lakehouse Batch)**: Jelaskan bahwa data Bronze yang sangat kotor disaring, dibersihkan dari duplikat, dan dihitung durasinya oleh **Apache Spark** menjadi **Silver Parquet**, kemudian diagregasi ke **Gold Parquet**. Tab dashboard Analytics memuat Parquet secara langsung sehingga performa dashboard sangat responsif dan ringan.
3.  **Akurasi Sistem (Anti-False Alarm)**: Jelaskan 4 tingkat penyaringan sistem (Spasial Geofencing, Temporal Grace Period, Vector Tracking ByteTrack, dan Semantik YOLOv8) yang menjamin sistem tidak salah mendeteksi kemacetan biasa sebagai parkir liar.


Sumber Artikel :
https://www.detik.com/jogja/berita/d-8292990/4-juta-kendaraan-diprediksi-masuk-diy-saat-nataru-ternyata
https://www.instagram.com/p/DSXBh1dE4bp/
https://peraturan.bpk.go.id/Details/108354/perda-kota-yogyakarta-no-2-tahun-2019


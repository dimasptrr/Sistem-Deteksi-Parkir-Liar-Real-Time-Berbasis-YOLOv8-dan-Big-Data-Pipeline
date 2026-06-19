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

---

## 📝 Rubrik 2: Desain Infrastruktur & Arsitektur Terdistribusi (Event-Driven)

Sistem ini dirancang menggunakan paradigma **Event-Driven Architecture (EDA)** berlatensi rendah untuk memastikan pemrosesan data berjalan secara terdistribusi dan *scalable* (siap menangani ratusan kamera secara simultan).

### 2.1 Alur Aliran Data (Data Pipeline Flow)

```text
  [ CCTV HLS Stream (ATCS Kota Yogyakarta - FMNoto) ]
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
                     ├─► [ ByteTrack Multi-Object ]   (Pelacakan track ID unik)
                     ├─► [ Zone Manager Geofence ]    (Pengecekan Polygon merah)
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
                                                                            ▼ (Query Parquet langsung)
                                                                    [ Dashboard Web UI ]
                                                              (FastAPI + Streamlit + Chart.js)
```

### 2.2 Peran Komponen Utama
- **FFmpeg Capture**: Mengambil video streaming berformat HLS (`.m3u8`) dengan latensi sangat rendah, mengubah frame gambar BGR mentah menjadi buffer byte.
- **Apache Kafka (Broker)**: Bertindak sebagai bus data terdistribusi. Producer mengirimkan data frame terkompresi Base64 ke Kafka Topic `cctv-frames`. Hal ini mencegah kehilangan data jika subsistem deteksi (YOLO) sedang mengalami perlambatan (*backpressure*).
- **YOLOv8 & ByteTrack**: Membaca frame dari Kafka secara asinkron, mengenali jenis kendaraan, mengunci identitas unik (*track ID*) kendaraan, dan melacak pergerakannya.
- **FastAPI Backend**: Sebagai gerbang API terpadu yang memanajemeni status detektor, menyajikan stream visual teranotasi MJPEG, dan menyediakan data analitik ke frontend.

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

### 6.1 Peta Fungsi Kode Program (Codebase Mapping)
*   **[bronze_detector.py](file:///d:/Kuliah/Semester%204/Big%20Data%20Dan%20Data%20Lakehouse/EAS%20BIG%20DATA/parkir-liar-detector/backend/bronze_detector.py)**: Mengelola deteksi objek (YOLOv8), pelacakan (ByteTrack), logika durasi diam, pemanggilan screenshot bukti, dan penyimpanan data mentah **Bronze Layer** (`raw_detections.json`).
*   **[zone_manager.py](file:///d:/Kuliah/Semester%204/Big%20Data%20Dan%20Data%20Lakehouse/EAS%20BIG%20DATA/parkir-liar-detector/backend/zone_manager.py)**: Mengatur batas geofencing bahu jalan dilarang parkir (polygon).
*   **[ffmpeg_capture.py](file:///d:/Kuliah/Semester%204/Big%20Data%20Dan%20Data%20Lakehouse/EAS%20BIG%20DATA/parkir-liar-detector/backend/ffmpeg_capture.py)**: Menangkap stream HLS video CCTV Yogyakarta secara *real-time* berlatensi rendah menggunakan FFmpeg.
*   **[spark_silver_gold.py](file:///d:/Kuliah/Semester%204/Big%20Data%20Dan%20Data%20Lakehouse/EAS%20BIG%20DATA/parkir-liar-detector/backend/spark_silver_gold.py)**: Engine Apache Spark yang mengimplementasikan ETL Lakehouse (Bronze -> Silver -> Gold).
*   **[generate_mock_bronze.py](file:///d:/Kuliah/Semester%204/Big%20Data%20Dan%20Data%20Lakehouse/EAS%20BIG%20DATA/parkir-liar-detector/backend/generate_mock_bronze.py)**: Membuat data tiruan raw frame YOLO historis berukuran besar ke dalam Bronze Layer untuk pengujian Spark.
*   **[server.py](file:///d:/Kuliah/Semester%204/Big%20Data%20Dan%20Data%20Lakehouse/EAS%20BIG%20DATA/parkir-liar-detector/frontend/server.py)**: API Gateway FastAPI. Endpoint `/api/analytics` diubah untuk membaca **Gold Parquet** secara langsung untuk efisiensi visualisasi grafik.
*   **[app.py](file:///d:/Kuliah/Semester%204/Big%20Data%20Dan%20Data%20Lakehouse/EAS%20BIG%20DATA/parkir-liar-detector/frontend/app.py)**: Entrypoint Streamlit pembungkus HTML.
*   **[index.html](file:///d:/Kuliah/Semester%204/Big%20Data%20Dan%20Data%20Lakehouse/EAS%20BIG%20DATA/parkir-liar-detector/frontend/index.html)**: UI Dashboard web interaktif (glassmorphism UI, grafik Chart.js, feed video langsung, modal bukti foto, SOP Validasi).

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
2.  **Jalankan Spark Job (Bronze -> Silver -> Gold)**:
    ```bash
    python backend/spark_silver_gold.py
    ```

#### Langkah 4: Jalankan Real-Time Streaming Pipeline
Buka dua terminal terpisah untuk mengalirkan video HLS CCTV FMNoto secara *live*:
*   **Terminal 1 (Kafka Producer)**:
    ```bash
    python backend/kafka_producer.py
    ```
*   **Terminal 2 (Kafka Consumer + YOLO)**:
    ```bash
    python backend/kafka_consumer.py
    ```

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


from __future__ import annotations

import os
import sys
import json
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

def main():
    print("=====================================================================")
    print("Memulai Job Batch Spark: Memproses Data Lakehouse (Bronze->Silver->Gold)")
    print("=====================================================================")

    # Set environment variables to avoid python version mismatch on Windows workers
    import sys
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, explode, min, max, avg, count, round as spark_round, when, to_timestamp, hour, date_format, lit, concat
    except ImportError:
        print("\n[ERROR] Library 'pyspark' tidak ditemukan.")
        print("Silakan instal dengan: pip install pyspark")
        sys.exit(1)

    bronze_path = PROJECT_ROOT / "data" / "bronze" / "raw_detections.json"
    silver_path = PROJECT_ROOT / "data" / "silver" / "violations_clean.parquet"
    gold_dir = PROJECT_ROOT / "data" / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)
    
    if not bronze_path.exists():
        print(f"[ERROR] Data mentah (Bronze Layer) tidak ditemukan di: {bronze_path}")
        print("Jalankan detektor CCTV atau generate_mock_bronze.py terlebih dahulu.")
        sys.exit(1)

    # Inisialisasi SparkSession dengan alokasi resource lokal (ram dibatasi karena ram laptop 8GB)
    spark = SparkSession.builder \
        .appName("IllegalParkingLakehouseProcessor") \
        .master("local[*]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.session.timeZone", "Asia/Jakarta") \
        .getOrCreate()

    try:
        # ----------------------------------------------------
        # 1. BRONZE LAYER -> SILVER LAYER (Cleaning & Validation)
        # ----------------------------------------------------
        print("\n[*] Membaca data mentah dari Bronze Layer...")
        bronze_df = spark.read.json(str(bronze_path))
        
        if bronze_df.count() == 0:
            print("[WARNING] Bronze Layer kosong. Menghentikan proses.")
            spark.stop()
            return
            
        print("[*] Melakukan flattening dan penyaringan data...")
        # Check if camera_id exists in Bronze layer schema
        has_camera_id = "camera_id" in bronze_df.columns

        # Flatten array detections
        df_flat = bronze_df.select(
            col("timestamp"),
            col("stream_url"),
            col("frame_width"),
            col("frame_height"),
            (col("camera_id") if has_camera_id else lit("cam1")).alias("camera_id"),
            explode("detections").alias("det")
        ).select(
            col("timestamp"),
            col("stream_url"),
            col("camera_id"),
            col("det.track_id").alias("track_id"),
            col("det.class_id").alias("class_id"),
            col("det.class_name").alias("vehicle_type"),
            col("det.zone_name").alias("zone_name"),
            col("det.confidence").alias("confidence")
        )
        
        # Filter: Hanya kendaraan di zona larangan (left/right)
        df_flat = df_flat.filter(col("zone_name").isNotNull() & (col("zone_name") != "") & (col("zone_name") != "null") & (col("zone_name") != "None"))
        
        from pyspark.sql.window import Window
        from pyspark.sql.functions import lag, sum as spark_sum

        # Konversi timestamp string ke data type Timestamp
        df_flat = df_flat.withColumn("ts", to_timestamp(col("timestamp")))
        
        # Definisikan window partition untuk mendeteksi gap waktu per track_id
        window_spec = Window.partitionBy("camera_id", "track_id", "zone_name").orderBy("ts")
        
        # Hitung selisih waktu (dalam detik) dengan frame sebelumnya
        df_with_lag = df_flat.withColumn("prev_ts", lag("ts").over(window_spec))
        df_with_gap = df_with_lag.withColumn(
            "gap_seconds", 
            col("ts").cast("long") - col("prev_ts").cast("long")
        )
        
        # Jika gap > 120 detik (2 menit) atau data pertama, tandai sebagai sesi baru (is_new_session = 1)
        df_with_session_flag = df_with_gap.withColumn(
            "is_new_session",
            when(col("prev_ts").isNull() | (col("gap_seconds") > 120), 1).otherwise(0)
        )
        
        # Lakukan cumulative sum untuk membuat session_id yang unik per track_id
        session_window = Window.partitionBy("camera_id", "track_id", "zone_name").orderBy("ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        df_with_session_id = df_with_session_flag.withColumn(
            "session_id",
            spark_sum("is_new_session").over(session_window)
        )
        
        # Group by termasuk session_id agar pencatatan per sesi parkir terpisah
        silver_df = df_with_session_id.groupBy("track_id", "zone_name", "vehicle_type", "stream_url", "camera_id", "session_id") \
            .agg(
                min("ts").alias("timestamp_entry"),
                max("ts").alias("timestamp_violation"),
                (max("ts").cast("long") - min("ts").cast("long")).alias("duration_seconds")
            )
            
        # Filter: Hanya kendaraan yang diam/berhenti >= 120 detik (2 menit)
        silver_df = silver_df.filter(col("duration_seconds") >= 120)
        
        # Buat screenshot_path dinamis berdasarkan timestamp_entry, track_id, dan camera_id
        violations_dir_literal = str(PROJECT_ROOT / "backend" / "violations") + "\\"
        silver_df = silver_df.withColumn(
            "screenshot_path",
            concat(
                lit(violations_dir_literal),
                lit("violation_"),
                date_format(col("timestamp_entry"), "yyyyMMdd_HHmmss"),
                lit("_id"),
                col("track_id"),
                lit("_"),
                col("camera_id"),
                lit(".jpg")
            )
        )
        
        import pandas as pd

        print(f"[*] Menulis hasil bersih ke Silver Layer: {silver_path}")
        silver_pd = silver_df.toPandas()
        silver_pd.to_parquet(str(silver_path))

        total_violations = len(silver_pd)
        print(f"[SUCCESS] Silver Layer berhasil diperbarui. Jumlah pelanggaran bersih: {total_violations}")
        
        if total_violations == 0:
            print("[WARNING] Tidak ada pelanggaran yang memenuhi syarat (durasi >= 2 menit). Menghentikan proses pembuatan Gold Layer.")
            spark.stop()
            return

        # ----------------------------------------------------
        # 2. SILVER LAYER -> GOLD LAYER (Aggregation per camera_id)
        # ----------------------------------------------------
        print("\n[*] Menghitung agregasi Gold Layer...")
        
        # A. IPI PER ZONE (Grouped by camera_id and zone_name)
        zone_agg_pd = silver_pd.groupby(["camera_id", "zone_name"]).agg(
            jumlah_pelanggaran=('track_id', 'count'),
            durasi_rata_detik=('duration_seconds', 'mean')
        ).reset_index()
        
        zone_agg_pd["indeks_parkir_liar"] = (zone_agg_pd["jumlah_pelanggaran"] * 0.4 + zone_agg_pd["durasi_rata_detik"] / 300.0)
        zone_agg_pd["indeks_parkir_liar"] = zone_agg_pd["indeks_parkir_liar"].clip(upper=10.0).round(1)
        
        zone_agg_pd["dampak_kemacetan"] = zone_agg_pd["indeks_parkir_liar"].apply(
            lambda x: "RENDAH (LOW)" if x < 3.0 else ("SEDANG (MEDIUM)" if x < 7.0 else "TINGGI (HIGH)")
        )
        zone_agg_pd["dampak_deskripsi"] = zone_agg_pd["indeks_parkir_liar"].apply(
            lambda x: "Kendaraan berhenti tidak mengganggu arus lalu lintas secara signifikan." if x < 3.0 
            else ("Bahu jalan terhambat, menyebabkan perlambatan arus lalu lintas utama." if x < 7.0 
            else "Penyumbatan parah lajur jalan, memicu kemacetan ekor panjang di jam-jam sibuk.")
        )
        zone_agg_pd["prioritas_penanganan"] = zone_agg_pd["indeks_parkir_liar"].apply(
            lambda x: "RENDAH" if x < 3.0 else ("SEDANG" if x < 7.0 else "TINGGI")
        )
        zone_agg_pd["rekomendasi"] = zone_agg_pd["indeks_parkir_liar"].apply(
            lambda x: "Lakukan pemantauan rutin via kamera CCTV, pastikan marka jalan tetap bersih." if x < 3.0
            else ("Pasang rambu portabel 'Dilarang Parkir' dan lakukan patroli berkala oleh petugas perhubungan." if x < 7.0
            else "Lakukan penertiban/derek langsung, pasang pembatas fisik (bollard atau guardrail) untuk mencegah parkir.")
        )
        
        zone_agg_pd = zone_agg_pd.rename(columns={"zone_name": "zona"})
        zone_agg_pd["zona"] = zone_agg_pd["zona"].str.upper()
        
        # Ensure LEFT and RIGHT always exist for both cam1 and cam2
        default_rows = []
        for c in ["cam1", "cam2"]:
            for z in ["LEFT", "RIGHT"]:
                if zone_agg_pd[(zone_agg_pd["camera_id"] == c) & (zone_agg_pd["zona"] == z)].empty:
                    default_rows.append({
                        "camera_id": c,
                        "zona": z,
                        "jumlah_pelanggaran": 0,
                        "durasi_rata_detik": 0.0,
                        "indeks_parkir_liar": 0.0,
                        "dampak_kemacetan": "RENDAH (LOW)",
                        "dampak_deskripsi": "Kendaraan berhenti tidak mengganggu arus lalu lintas secara signifikan.",
                        "prioritas_penanganan": "RENDAH",
                        "rekomendasi": "Lakukan pemantauan rutin via kamera CCTV, pastikan marka jalan tetap bersih."
                    })
        if default_rows:
            zone_agg_pd = pd.concat([zone_agg_pd, pd.DataFrame(default_rows)], ignore_index=True)
            
        zone_agg_pd.to_parquet(str(gold_dir / "ipi_per_zone.parquet"))
        print("[*] Gold Layer: ipi_per_zone.parquet diperbarui.")

        # B. HOURLY STATS (Peak Hours per camera_id)
        silver_pd["timestamp_violation"] = pd.to_datetime(silver_pd["timestamp_violation"])
        silver_pd["hour"] = silver_pd["timestamp_violation"].dt.hour
        
        hourly_gold_pd = silver_pd.groupby(["camera_id", "hour"]).size().reset_index(name="count")
        hourly_gold_pd = hourly_gold_pd.sort_values(["camera_id", "hour"])
        hourly_gold_pd.to_parquet(str(gold_dir / "hourly_stats.parquet"))
        print("[*] Gold Layer: hourly_stats.parquet diperbarui.")

        # C. DAILY TREND (per camera_id)
        silver_pd["tanggal"] = silver_pd["timestamp_violation"].dt.strftime("%Y-%m-%d")
        daily_gold_pd = silver_pd.groupby(["camera_id", "tanggal"]).size().reset_index(name="jumlah")
        daily_gold_pd = daily_gold_pd.sort_values(["camera_id", "tanggal"])
        daily_gold_pd.to_parquet(str(gold_dir / "daily_trend.parquet"))
        print("[*] Gold Layer: daily_trend.parquet diperbarui.")

        # D. VEHICLE STATS (Vehicle Type Distribution per camera_id)
        vehicle_gold_pd = silver_pd.groupby(["camera_id", "vehicle_type"]).size().reset_index(name="count")
        vehicle_gold_pd.to_parquet(str(gold_dir / "vehicle_stats.parquet"))
        print("[*] Gold Layer: vehicle_stats.parquet diperbarui.")

        # ----------------------------------------------------
        # 3. EXPORT SUMMARY JSON (Grouped by camera_id)
        # ----------------------------------------------------
        print("\n[*] Membuat ringkasan JSON hasil analisis batch per camera...")
        
        results = {}
        for cam in ["cam1", "cam2"]:
            cam_silver = silver_pd[silver_pd["camera_id"] == cam]
            total_pelanggaran = len(cam_silver)
            
            durasi_rata_detik_total = float(cam_silver["duration_seconds"].mean()) if total_pelanggaran > 0 else 0.0
            
            # Hourly stats for this camera
            cam_hourly = hourly_gold_pd[hourly_gold_pd["camera_id"] == cam]
            distribusi_jam = [0] * 24
            for _, row in cam_hourly.iterrows():
                h = int(row["hour"])
                if 0 <= h < 24:
                    distribusi_jam[h] = int(row["count"])
                    
            peak_hour = 0
            max_count = -1
            for h, c in enumerate(distribusi_jam):
                if c > max_count:
                    max_count = c
                    peak_hour = h
            jam_rawan = f"{peak_hour:02d}:00 - {(peak_hour+1)%24:02d}:00"
            
            # Daily stats for this camera
            cam_daily = daily_gold_pd[daily_gold_pd["camera_id"] == cam]
            tren_pelanggaran = [{"tanggal": str(row["tanggal"]), "jumlah": int(row["jumlah"])} for _, row in cam_daily.iterrows()]
            
            # Vehicle stats for this camera
            cam_vehicle = vehicle_gold_pd[vehicle_gold_pd["camera_id"] == cam]
            distribusi_kendaraan = {str(row["vehicle_type"]): int(row["count"]) for _, row in cam_vehicle.iterrows()}
            
            # Zone stats for this camera
            cam_zone = zone_agg_pd[zone_agg_pd["camera_id"] == cam]
            analisis_zona = []
            for _, row in cam_zone.iterrows():
                analisis_zona.append({
                    "zona": str(row["zona"]),
                    "jumlah_pelanggaran": int(row["jumlah_pelanggaran"]),
                    "durasi_rata_detik": round(float(row["durasi_rata_detik"]), 1),
                    "indeks_parkir_liar": float(row["indeks_parkir_liar"]),
                    "dampak_kemacetan": str(row["dampak_kemacetan"]),
                    "dampak_deskripsi": str(row["dampak_deskripsi"]),
                    "prioritas_penanganan": str(row["prioritas_penanganan"]),
                    "rekomendasi": str(row["rekomendasi"])
                })
                
            results[cam] = {
                "status": "success",
                "spark_processed": True,
                "total_pelanggaran": total_pelanggaran,
                "durasi_rata_detik_total": round(durasi_rata_detik_total, 1),
                "jam_rawan": jam_rawan,
                "jam_rawan_jam": peak_hour,
                "distribusi_jam": distribusi_jam,
                "distribusi_kendaraan": distribusi_kendaraan,
                "tren_pelanggaran": tren_pelanggaran,
                "analisis_zona": analisis_zona
            }
        
        output_json_path = PROJECT_ROOT / "backend" / "spark_analytics_results.json"
        with open(output_json_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)
            
        print(f"[SUCCESS] Lakehouse Batch Job selesai! Hasil ringkasan diekspor ke: {output_json_path}")

    except Exception as e:
        print(f"[ERROR] Terjadi kesalahan saat memproses data: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

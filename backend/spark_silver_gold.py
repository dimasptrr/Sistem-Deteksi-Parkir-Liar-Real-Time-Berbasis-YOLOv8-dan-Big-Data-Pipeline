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
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    try:
        # ----------------------------------------------------
        # 1. BRONZE LAYER -> SILVER LAYER (Cleaning & Validation)
        # ----------------------------------------------------
        print("\n[*] Membaca data mentah dari Bronze Layer...")
        # PySpark membaca format line-delimited JSON secara otomatis
        bronze_df = spark.read.json(str(bronze_path))
        
        if bronze_df.count() == 0:
            print("[WARNING] Bronze Layer kosong. Menghentikan proses.")
            spark.stop()
            return
            
        print("[*] Melakukan flattening dan penyaringan data...")
        # Flatten array detections
        df_flat = bronze_df.select(
            col("timestamp"),
            col("stream_url"),
            col("frame_width"),
            col("frame_height"),
            explode("detections").alias("det")
        ).select(
            col("timestamp"),
            col("stream_url"),
            col("det.track_id").alias("track_id"),
            col("det.class_id").alias("class_id"),
            col("det.class_name").alias("vehicle_type"),
            col("det.zone_name").alias("zone_name"),
            col("det.confidence").alias("confidence")
        )
        
        # Filter: Hanya kendaraan di zona larangan (left/right)
        df_flat = df_flat.filter(col("zone_name").isNotNull() & (col("zone_name") != "") & (col("zone_name") != "null") & (col("zone_name") != "None"))
        
        # Konversi timestamp string ke data type Timestamp
        df_flat = df_flat.withColumn("ts", to_timestamp(col("timestamp")))
        
        # Group by track_id & zone_name untuk menghitung durasi diam kendaraan
        # track_id dianggap unik per session deteksi
        silver_df = df_flat.groupBy("track_id", "zone_name", "vehicle_type", "stream_url") \
            .agg(
                min("ts").alias("timestamp_entry"),
                max("ts").alias("timestamp_violation"),
                (max("ts").cast("long") - min("ts").cast("long")).alias("duration_seconds")
            )
            
        # Filter: Hanya kendaraan yang diam/berhenti >= 120 detik (2 menit)
        silver_df = silver_df.filter(col("duration_seconds") >= 120)
        
        # Buat screenshot_path dinamis berdasarkan timestamp_entry dan track_id
        violations_dir_literal = str(PROJECT_ROOT / "backend" / "violations") + "\\"
        silver_df = silver_df.withColumn(
            "screenshot_path",
            concat(
                lit(violations_dir_literal),
                lit("violation_"),
                date_format(col("timestamp_entry"), "yyyyMMdd_HHmmss"),
                lit("_id"),
                col("track_id"),
                lit(".jpg")
            )
        )
        
        print(f"[*] Menulis hasil bersih ke Silver Layer: {silver_path}")
        silver_df.toPandas().to_parquet(str(silver_path))

        
        total_violations = silver_df.count()
        print(f"[SUCCESS] Silver Layer berhasil diperbarui. Jumlah pelanggaran bersih: {total_violations}")
        
        if total_violations == 0:
            print("[WARNING] Tidak ada pelanggaran yang memenuhi syarat (durasi >= 2 menit). Menghentikan proses pembuatan Gold Layer.")
            spark.stop()
            return

        # ----------------------------------------------------
        # 2. SILVER LAYER -> GOLD LAYER (Aggregation)
        # ----------------------------------------------------
        print("\n[*] Menghitung agregasi Gold Layer...")
        
        # A. IPI PER ZONE
        # Rumus: min(10.0, count * 0.4 + avg_duration / 300)
        zone_agg = silver_df.groupBy("zone_name").agg(
            count("*").alias("jumlah_pelanggaran"),
            avg("duration_seconds").alias("durasi_rata_detik")
        )
        
        zone_gold = zone_agg.withColumn(
            "indeks_parkir_liar",
            spark_round(
                when((col("jumlah_pelanggaran") * 0.4 + col("durasi_rata_detik") / 300.0) > 10.0, 10.0)
                .otherwise(col("jumlah_pelanggaran") * 0.4 + col("durasi_rata_detik") / 300.0),
                1
            )
        ).withColumn(
            "dampak_kemacetan",
            when(col("indeks_parkir_liar") < 3.0, "RENDAH (LOW)")
            .when(col("indeks_parkir_liar") < 7.0, "SEDANG (MEDIUM)")
            .otherwise("TINGGI (HIGH)")
        ).withColumn(
            "dampak_deskripsi",
            when(col("indeks_parkir_liar") < 3.0, "Kendaraan berhenti tidak mengganggu arus lalu lintas secara signifikan.")
            .when(col("indeks_parkir_liar") < 7.0, "Bahu jalan terhambat, menyebabkan perlambatan arus lalu lintas utama.")
            .otherwise("Penyumbatan parah lajur jalan, memicu kemacetan ekor panjang di jam-jam sibuk.")
        ).withColumn(
            "prioritas_penanganan",
            when(col("indeks_parkir_liar") < 3.0, "RENDAH")
            .when(col("indeks_parkir_liar") < 7.0, "SEDANG")
            .otherwise("TINGGI")
        ).withColumn(
            "rekomendasi",
            when(col("indeks_parkir_liar") < 3.0, "Lakukan pemantauan rutin via kamera CCTV, pastikan marka jalan tetap bersih.")
            .when(col("indeks_parkir_liar") < 7.0, "Pasang rambu portabel 'Dilarang Parkir' dan lakukan patroli berkala oleh petugas perhubungan.")
            .otherwise("Lakukan penertiban/derek langsung, pasang pembatas fisik (bollard atau guardrail) untuk mencegah parkir.")
        ).withColumnRenamed("zone_name", "zona")
        
        # Ubah zona menjadi uppercase untuk keselarasan frontend
        from pyspark.sql.functions import upper
        zone_gold = zone_gold.withColumn("zona", upper(col("zona")))
        
        # Pastikan LEFT dan RIGHT selalu ada di hasil akhir
        default_data = [
            ("LEFT", 0, 0.0, 0.0, "RENDAH (LOW)", "Kendaraan berhenti tidak mengganggu arus lalu lintas secara signifikan.", "RENDAH", "Lakukan pemantauan rutin via kamera CCTV, pastikan marka jalan tetap bersih."),
            ("RIGHT", 0, 0.0, 0.0, "RENDAH (LOW)", "Kendaraan berhenti tidak mengganggu arus lalu lintas secara signifikan.", "RENDAH", "Lakukan pemantauan rutin via kamera CCTV, pastikan marka jalan tetap bersih.")
        ]
        default_df = spark.createDataFrame(default_data, schema=["zona", "jumlah_pelanggaran", "durasi_rata_detik", "indeks_parkir_liar", "dampak_kemacetan", "dampak_deskripsi", "prioritas_penanganan", "rekomendasi"])
        
        present_zones = [row["zona"] for row in zone_gold.select("zona").collect()]
        missing_df = default_df.filter(~col("zona").isin(present_zones))
        zone_gold = zone_gold.union(missing_df)
        
        zone_gold.toPandas().to_parquet(str(gold_dir / "ipi_per_zone.parquet"))
        print("[*] Gold Layer: ipi_per_zone.parquet diperbarui.")

        # B. HOURLY STATS (Peak Hours)
        hourly_gold = silver_df.withColumn("hour", hour(col("timestamp_violation"))) \
            .groupBy("hour") \
            .agg(count("*").alias("count")) \
            .orderBy("hour")
        hourly_gold.toPandas().to_parquet(str(gold_dir / "hourly_stats.parquet"))
        print("[*] Gold Layer: hourly_stats.parquet diperbarui.")

        # C. DAILY TREND
        daily_gold = silver_df.withColumn("tanggal", date_format(col("timestamp_violation"), "yyyy-MM-dd")) \
            .groupBy("tanggal") \
            .agg(count("*").alias("jumlah")) \
            .orderBy("tanggal")
        daily_gold.toPandas().to_parquet(str(gold_dir / "daily_trend.parquet"))
        print("[*] Gold Layer: daily_trend.parquet diperbarui.")

        # D. VEHICLE STATS (Vehicle Type Distribution)
        vehicle_gold = silver_df.groupBy("vehicle_type") \
            .agg(count("*").alias("count"))
        vehicle_gold.toPandas().to_parquet(str(gold_dir / "vehicle_stats.parquet"))
        print("[*] Gold Layer: vehicle_stats.parquet diperbarui.")

        # ----------------------------------------------------
        # 3. EXPORT SUMMARY JSON (For dashboard metadata backwards-compatibility)
        # ----------------------------------------------------
        print("\n[*] Membuat ringkasan JSON hasil analisis batch...")
        
        # Hitung ringkasan statistik
        stats_summary = silver_df.select(
            count("*").alias("total"),
            avg("duration_seconds").alias("avg_duration")
        ).collect()[0]
        
        total_pelanggaran = int(stats_summary["total"])
        durasi_rata_detik_total = float(stats_summary["avg_duration"]) if stats_summary["avg_duration"] is not None else 0.0
        
        # Ekstrak data jam rawan
        hourly_collected = hourly_gold.collect()
        distribusi_jam = [0] * 24
        for row in hourly_collected:
            h = int(row["hour"])
            if 0 <= h < 24:
                distribusi_jam[h] = int(row["count"])
                
        # Cari jam puncak kemacetan / parkir liar
        peak_hour = 0
        max_count = -1
        for h, c in enumerate(distribusi_jam):
            if c > max_count:
                max_count = c
                peak_hour = h
        jam_rawan = f"{peak_hour:02d}:00 - {(peak_hour+1)%24:02d}:00"
        
        # Ekstrak tren harian
        daily_collected = daily_gold.collect()
        tren_pelanggaran = [{"tanggal": str(row["tanggal"]), "jumlah": int(row["jumlah"])} for row in daily_collected]
        
        # Ekstrak distribusi kendaraan
        vehicle_collected = vehicle_gold.collect()
        distribusi_kendaraan = {row["vehicle_type"]: int(row["count"]) for row in vehicle_collected}
        
        # Ekstrak analisis zona
        zone_collected = zone_gold.collect()
        analisis_zona = []
        for row in zone_collected:
            analisis_zona.append({
                "zona": row["zona"],
                "jumlah_pelanggaran": int(row["jumlah_pelanggaran"]),
                "durasi_rata_detik": round(float(row["durasi_rata_detik"]), 1),
                "indeks_parkir_liar": float(row["indeks_parkir_liar"]),
                "dampak_kemacetan": row["dampak_kemacetan"],
                "dampak_deskripsi": row["dampak_deskripsi"],
                "prioritas_penanganan": row["prioritas_penanganan"],
                "rekomendasi": row["rekomendasi"]
            })
            
        results = {
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

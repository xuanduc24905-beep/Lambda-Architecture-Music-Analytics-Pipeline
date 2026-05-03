#!/bin/bash
# Lambda Architecture — Music Analytics Pipeline
# Dataset: Spotify 1921-2020 (~600K tracks)
set -e

SPARK="docker exec spark-master bash -c"
SPARK_SUBMIT="spark-submit --master spark://spark-master:7077"

echo "=============================================="
echo "  Lambda Architecture — Music Analytics"
echo "  Dataset: Spotify 1921-2020 (~600K tracks)"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "=============================================="
echo ""

# ── BATCH PHASE ─────────────────────────────────────────────
echo "[1/6] Processing Spotify CSV (schema mapping + decade column)..."
docker exec spark-master python /spark-jobs/00_load_csv.py

echo ""
echo "[2/6] Uploading CSV to HDFS..."
docker exec namenode bash -c \
  "hdfs dfs -mkdir -p /music/raw && hdfs dfs -put -f /data/spotify_tracks.csv /music/raw/"

echo ""
echo "[3/6] EDA + cleaning — Loudness War, Energy trend, Explicit trend..."
$SPARK "$SPARK_SUBMIT /spark-jobs/01_eda.py"

echo ""
echo "[4/6] KMeans clustering (era-based, k=3~8)..."
$SPARK "$SPARK_SUBMIT /spark-jobs/02_kmeans.py"

echo ""
echo "[5/6] Hive analytical queries — temporal analysis..."
$SPARK "$SPARK_SUBMIT /spark-jobs/04_hive_query.py"

echo ""
echo "[6/6] Exporting HDFS parquet → local /data/ ..."
$SPARK "$SPARK_SUBMIT /spark-jobs/05_export.py"

echo ""
echo "=============================================="
echo "  Batch complete! Starting streaming..."
echo "=============================================="
echo ""

# ── STREAMING PHASE ──────────────────────────────────────────
docker exec -d spark-master bash -c \
  "python /spark-jobs/03_kafka_producer.py > /tmp/producer.log 2>&1"
echo "  Kafka producer started  (log: /tmp/producer.log)"

docker exec -d spark-master bash -c \
  "spark-submit --master spark://spark-master:7077 \
   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
   /spark-jobs/03_spark_streaming.py > /tmp/streaming.log 2>&1"
echo "  Spark Streaming started  (log: /tmp/streaming.log)"

echo ""
echo "=============================================="
echo "  Pipeline đang chạy!"
echo ""
echo "  Dashboard    : http://localhost:8501"
echo "  Spark UI     : http://localhost:8080"
echo "  HDFS UI      : http://localhost:9870"
echo "  YARN UI      : http://localhost:8088"
echo "  HiveServer2  : http://localhost:10002"
echo ""
echo "  Xem log:"
echo "  docker exec spark-master tail -f /tmp/producer.log"
echo "  docker exec spark-master tail -f /tmp/streaming.log"
echo "=============================================="

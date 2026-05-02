#!/bin/bash
# Batch pipeline: fetch → HDFS → EDA → KMeans → Hive → export
set -e

SPARK="docker exec spark-master bash -c"
SPARK_SUBMIT="spark-submit --master spark://spark-master:7077"

echo "=============================================="
echo "  Real-time Music Analytics Pipeline"
echo "  Batch phase — $(date '+%Y-%m-%d %H:%M:%S')"
echo "=============================================="
echo ""

echo "[1/6] Fetching ~12K tracks from Deezer API..."
docker exec spark-master python /spark-jobs/00_deezer_batch.py

echo ""
echo "[2/6] Uploading CSV to HDFS..."
docker exec namenode bash -c "hdfs dfs -mkdir -p /music/raw && hdfs dfs -put -f /data/deezer_tracks.csv /music/raw/"

echo ""
echo "[3/6] Running EDA + cleaning (Spark)..."
$SPARK "$SPARK_SUBMIT /spark-jobs/01_eda.py"

echo ""
echo "[4/6] Running KMeans clustering (Spark MLlib)..."
$SPARK "$SPARK_SUBMIT /spark-jobs/02_kmeans.py"

echo ""
echo "[5/6] Running Hive analytical queries..."
$SPARK "$SPARK_SUBMIT /spark-jobs/04_hive_query.py"

echo ""
echo "[6/6] Exporting HDFS parquet → local /data/..."
$SPARK "$SPARK_SUBMIT /spark-jobs/05_export.py"

echo ""
echo "=============================================="
echo "  Batch pipeline complete!"
echo "=============================================="
echo ""
echo "Next: start real-time streaming in 2 terminals:"
echo ""
echo "  Terminal 1 — Kafka producer (polls Deezer every 2 min):"
echo "  docker exec -it spark-master python /spark-jobs/03_kafka_producer.py"
echo ""
echo "  Terminal 2 — Spark Structured Streaming → HDFS:"
echo "  docker exec -it spark-master bash -c \\"
echo "    'spark-submit --master spark://spark-master:7077 \\"
echo "     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\"
echo "     /spark-jobs/03_spark_streaming.py'"
echo ""
echo "  Dashboard: http://localhost:8501"
echo ""

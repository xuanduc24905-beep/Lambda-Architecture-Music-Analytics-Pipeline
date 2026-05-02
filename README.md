# Deezer Big Data Pipeline

Real-time music data pipeline using Deezer API, Apache Kafka, Spark, HDFS, Hive, and Streamlit.

## Architecture

```
Deezer API
    │
    ├── Batch (00_deezer_batch.py)
    │       └── CSV → HDFS → Spark EDA + KMeans → Parquet
    │
    └── Streaming (03_kafka_producer.py)
            └── Kafka → Spark Streaming → HDFS (append)
                                              │
                                         Streamlit Dashboard
```

## Stack

| Component | Technology |
|---|---|
| Data source | Deezer API (no auth required) |
| Message queue | Apache Kafka |
| Processing | Apache Spark (MLlib, Structured Streaming) |
| Storage | Hadoop HDFS |
| Data warehouse | Apache Hive |
| Dashboard | Streamlit |
| Infrastructure | Docker Compose |

## Setup

**Prerequisites:** Docker, Docker Compose

```bash
git clone https://github.com/xuanduc24905-beep/Real-time-Music-Analytics-Pipeline-.git
cd Real-time-Music-Analytics-Pipeline-
docker compose up -d
```

## Running the Pipeline

**Step 1 — Fetch batch data from Deezer:**
```bash
python spark-jobs/00_deezer_batch.py
```

**Step 2 — Upload to HDFS:**
```bash
docker exec -it namenode bash -c "hdfs dfs -mkdir -p /music/raw && hdfs dfs -put -f /data/deezer_tracks.csv /music/raw/"
```

**Step 3 — EDA + cleaning:**
```bash
docker exec -it spark-master bash -c "spark-submit --master spark://spark-master:7077 /spark-jobs/01_eda.py"
```

**Step 4 — KMeans clustering:**
```bash
docker exec -it spark-master bash -c "spark-submit --master spark://spark-master:7077 /spark-jobs/02_kmeans.py"
```

**Step 5 — Hive queries:**
```bash
docker exec -it spark-master bash -c "spark-submit --master spark://spark-master:7077 /spark-jobs/04_hive_query.py"
```

**Step 6 — Export to Streamlit:**
```bash
docker exec -it spark-master bash -c "spark-submit --master spark://spark-master:7077 /spark-jobs/05_export.py"
```

**Step 7 — Real-time streaming (2 terminals):**
```bash
# Terminal 1
docker exec -it spark-master bash -c "python /spark-jobs/03_kafka_producer.py"

# Terminal 2
docker exec -it spark-master bash -c "spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /spark-jobs/03_spark_streaming.py"
```

## Dashboard

Open `http://localhost:8501`

Pages:
- **Overview** — general stats, popularity distribution, scatter plots
- **Genre Analysis** — top genres, audio features comparison
- **Cluster Analysis** — KMeans results, audio profile per cluster
- **Top Tracks** — filter by genre, popularity ranking
- **Live Stream** — real-time data from HDFS streaming path

## Spark Jobs

| File | Description |
|---|---|
| `00_deezer_batch.py` | Fetch ~12K tracks from Deezer API by genre |
| `01_eda.py` | EDA, data cleaning, genre stats |
| `02_kmeans.py` | KMeans clustering (k=3~8, elbow method) |
| `03_kafka_producer.py` | Stream Deezer data to Kafka every 2 minutes |
| `03_spark_streaming.py` | Consume Kafka, write to HDFS |
| `04_hive_query.py` | Analytical queries via Hive |
| `05_export.py` | Export HDFS parquet to local for Streamlit |

## UIs

| Service | URL |
|---|---|
| Streamlit | http://localhost:8501 |
| Spark Master | http://localhost:8080 |
| HDFS Namenode | http://localhost:9870 |

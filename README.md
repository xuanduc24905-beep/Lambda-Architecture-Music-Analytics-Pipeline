# Real-time Music Analytics Pipeline

Pipeline xử lý và phân tích dữ liệu âm nhạc theo thời gian thực, sử dụng Deezer API, Apache Kafka, Spark Structured Streaming, HDFS, Hive và Streamlit.

## Kiến trúc tổng thể

```
Deezer API
    │
    ├── Batch (00_deezer_batch.py)
    │       └── CSV → HDFS → Spark EDA + KMeans → Parquet → Streamlit
    │
    └── Streaming (03_kafka_producer.py)
            └── CSV tracks → Kafka topic "music-stream"
                    └── Spark Structured Streaming → HDFS (append)
                                                        │
                                                   Streamlit Live Stream
```

## Stack

| Thành phần | Công nghệ |
|---|---|
| Nguồn dữ liệu | Deezer API (không cần xác thực) |
| Message queue | Apache Kafka + Zookeeper |
| Xử lý | Apache Spark 3.5 (MLlib, Structured Streaming) |
| Lưu trữ | Hadoop HDFS (1 namenode + 2 datanode) |
| Data warehouse | Apache Hive + PostgreSQL metastore |
| Dashboard | Streamlit (5 trang, tự động refresh theo giây) |
| Hạ tầng | Docker Compose (12 containers) |

## Cài đặt

**Yêu cầu:** Docker, Docker Compose

```bash
git clone https://github.com/xuanduc24905-beep/Real-time-Music-Analytics-Pipeline-.git
cd Real-time-Music-Analytics-Pipeline-
docker compose up -d
```

Chờ khoảng 2 phút để tất cả service khởi động xong.

## Chạy pipeline

### Cách 1 — Tự động (khuyến nghị)

```bash
./run_pipeline.sh
```

Script tự động chạy toàn bộ 6 bước batch, sau đó hiển thị hướng dẫn chạy streaming.

### Cách 2 — Thủ công từng bước

**Bước 1 — Cào dữ liệu từ Deezer API:**
```bash
docker exec spark-master python /spark-jobs/00_deezer_batch.py
```

**Bước 2 — Upload lên HDFS:**
```bash
docker exec namenode bash -c "hdfs dfs -mkdir -p /music/raw && hdfs dfs -put -f /data/deezer_tracks.csv /music/raw/"
```

**Bước 3 — EDA + làm sạch dữ liệu:**
```bash
docker exec spark-master bash -c "spark-submit --master spark://spark-master:7077 /spark-jobs/01_eda.py"
```

**Bước 4 — KMeans clustering:**
```bash
docker exec spark-master bash -c "spark-submit --master spark://spark-master:7077 /spark-jobs/02_kmeans.py"
```

**Bước 5 — Truy vấn Hive:**
```bash
docker exec spark-master bash -c "spark-submit --master spark://spark-master:7077 /spark-jobs/04_hive_query.py"
```

**Bước 6 — Export ra local:**
```bash
docker exec spark-master bash -c "spark-submit --master spark://spark-master:7077 /spark-jobs/05_export.py"
```

## Streaming thời gian thực

Mở 2 terminal sau khi batch pipeline chạy xong:

```bash
# Terminal 1 — Kafka producer (đẩy từng track liên tục từ CSV)
docker exec -it spark-master python /spark-jobs/03_kafka_producer.py

# Terminal 2 — Spark Structured Streaming ghi xuống HDFS
docker exec -it spark-master bash -c \
  "spark-submit --master spark://spark-master:7077 \
   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
   /spark-jobs/03_spark_streaming.py"
```

Producer đọc từ CSV và gửi ~20 track/giây vào Kafka. Spark tiêu thụ và ghi xuống HDFS mỗi giây. Dashboard trang Live Stream tự refresh mỗi giây.

## Dashboard

Truy cập `http://localhost:8501`

| Trang | Nội dung |
|---|---|
| Overview | Thống kê tổng quan, top genre, scatter plot theo cluster |
| Genre Analysis | Số lượng track và audio features theo từng genre |
| Cluster Analysis | Kết quả KMeans, hồ sơ âm thanh theo cluster |
| Top Tracks | Lọc theo genre, xếp hạng theo độ phổ biến |
| Live Stream | Dữ liệu thời gian thực từ HDFS, tự động cập nhật mỗi giây |

## Spark Jobs

| File | Mô tả |
|---|---|
| `00_deezer_batch.py` | Cào ~12K tracks từ Deezer API (12 thể loại) |
| `01_eda.py` | EDA, làm sạch dữ liệu, thống kê theo genre → HDFS Parquet |
| `02_kmeans.py` | KMeans clustering k=3~8 (elbow + silhouette) → HDFS Parquet |
| `03_kafka_producer.py` | Stream từng track từ CSV vào Kafka liên tục |
| `03_spark_streaming.py` | Tiêu thụ Kafka, enrich data, ghi append xuống HDFS |
| `04_hive_query.py` | Truy vấn phân tích qua Hive (top genre, cluster, explicit) |
| `05_export.py` | Export HDFS Parquet → local `/data/` cho Streamlit đọc |

## Giao diện web

| Service | URL |
|---|---|
| Streamlit Dashboard | http://localhost:8501 |
| Spark Master | http://localhost:8080 |
| HDFS Namenode | http://localhost:9870 |
| YARN Resource Manager | http://localhost:8088 |
| HiveServer2 Web UI | http://localhost:10002 |

import json
import time
import random
import pandas as pd
from kafka import KafkaProducer

KAFKA_TOPIC = "music-stream"
CSV_PATH = "/data/deezer_tracks.csv"

# Mỗi track được gửi cách nhau bao nhiêu giây (mô phỏng tốc độ stream)
DELAY_SECONDS = 0.01

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)
print("Kafka producer connected")


def load_tracks():
    df = pd.read_csv(CSV_PATH)
    df = df.drop(columns=["Unnamed: 0"], errors="ignore")
    df = df.dropna(subset=["track_id", "track_name"])
    tracks = []
    for _, row in df.iterrows():
        record = row.to_dict()
        record = {k: (None if pd.isna(v) else v) for k, v in record.items()}
        tracks.append(record)
    print(f"Loaded {len(tracks)} tracks from CSV")
    return tracks


tracks = load_tracks()
pass_num = 0

# Stream liên tục: mỗi vòng shuffle lại để thứ tự khác nhau
while True:
    pass_num += 1
    random.shuffle(tracks)
    print(f"\n--- Pass {pass_num}: streaming {len(tracks)} tracks ---")

    for i, track in enumerate(tracks):
        producer.send(
            topic=KAFKA_TOPIC,
            key=str(track.get("track_id", i)),
            value=track,
        )
        if (i + 1) % 100 == 0:
            producer.flush()
            print(f"  sent {i + 1}/{len(tracks)}")
        time.sleep(DELAY_SECONDS)

    producer.flush()
    print(f"Pass {pass_num} complete. Sleeping 10s before next pass...")
    time.sleep(10)

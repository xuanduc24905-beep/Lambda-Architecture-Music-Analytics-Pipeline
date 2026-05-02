import json
import time
import requests
import pandas as pd
from kafka import KafkaProducer

DEEZER_GENRES = {
    132: "pop",
    116: "hip-hop",
    152: "rock",
    113: "dance",
    165: "r-n-b",
    106: "electronic",
    85:  "indie",
    129: "jazz",
    98:  "classical",
    144: "metal",
    169: "soul",
    197: "latin",
}

genre_defaults = {
    "pop":        dict(danceability=0.65, energy=0.70, key=5, loudness=-6.0,  mode=1, speechiness=0.05, acousticness=0.15, instrumentalness=0.01, liveness=0.12, valence=0.60, tempo=120.0, time_signature=4),
    "rock":       dict(danceability=0.50, energy=0.85, key=7, loudness=-5.0,  mode=1, speechiness=0.06, acousticness=0.05, instrumentalness=0.05, liveness=0.18, valence=0.45, tempo=130.0, time_signature=4),
    "hip-hop":    dict(danceability=0.78, energy=0.65, key=2, loudness=-7.0,  mode=0, speechiness=0.22, acousticness=0.10, instrumentalness=0.01, liveness=0.10, valence=0.55, tempo=95.0,  time_signature=4),
    "electronic": dict(danceability=0.75, energy=0.88, key=9, loudness=-5.5,  mode=0, speechiness=0.07, acousticness=0.03, instrumentalness=0.40, liveness=0.08, valence=0.50, tempo=128.0, time_signature=4),
    "latin":      dict(danceability=0.80, energy=0.75, key=0, loudness=-6.5,  mode=1, speechiness=0.08, acousticness=0.20, instrumentalness=0.02, liveness=0.14, valence=0.72, tempo=105.0, time_signature=4),
    "indie":      dict(danceability=0.55, energy=0.60, key=4, loudness=-8.0,  mode=1, speechiness=0.04, acousticness=0.30, instrumentalness=0.08, liveness=0.12, valence=0.48, tempo=115.0, time_signature=4),
    "r-n-b":      dict(danceability=0.72, energy=0.60, key=1, loudness=-7.5,  mode=0, speechiness=0.12, acousticness=0.25, instrumentalness=0.02, liveness=0.10, valence=0.58, tempo=90.0,  time_signature=4),
    "jazz":       dict(danceability=0.45, energy=0.45, key=6, loudness=-10.0, mode=1, speechiness=0.04, acousticness=0.70, instrumentalness=0.25, liveness=0.20, valence=0.52, tempo=110.0, time_signature=4),
    "classical":  dict(danceability=0.30, energy=0.25, key=3, loudness=-15.0, mode=1, speechiness=0.03, acousticness=0.90, instrumentalness=0.85, liveness=0.10, valence=0.35, tempo=100.0, time_signature=4),
    "metal":      dict(danceability=0.40, energy=0.95, key=8, loudness=-3.0,  mode=0, speechiness=0.07, acousticness=0.02, instrumentalness=0.15, liveness=0.22, valence=0.30, tempo=150.0, time_signature=4),
    "soul":       dict(danceability=0.63, energy=0.55, key=2, loudness=-8.5,  mode=1, speechiness=0.06, acousticness=0.45, instrumentalness=0.03, liveness=0.12, valence=0.65, tempo=95.0,  time_signature=4),
    "dance":      dict(danceability=0.82, energy=0.85, key=5, loudness=-5.0,  mode=1, speechiness=0.06, acousticness=0.05, instrumentalness=0.20, liveness=0.10, valence=0.65, tempo=128.0, time_signature=4),
}

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)
print("Kafka producer connected")


def fetch_deezer_tracks():
    tracks = []
    seen_ids = set()

    for genre_id, genre_label in DEEZER_GENRES.items():
        try:
            url = f"https://api.deezer.com/chart/{genre_id}/tracks?limit=100"
            resp = requests.get(url, timeout=10)
            items = resp.json().get("data", [])
            print(f"  {genre_label}: {len(items)} tracks")

            for pos, item in enumerate(items):
                if item["id"] in seen_ids:
                    continue
                seen_ids.add(item["id"])

                f = genre_defaults.get(genre_label, genre_defaults["pop"])
                popularity = max(1, 100 - pos)

                track = {
                    "track_id":        str(item["id"]),
                    "track_name":      item["title"],
                    "artists":         item["artist"]["name"],
                    "album_name":      item["album"]["title"],
                    "popularity":      popularity,
                    "duration_ms":     item["duration"] * 1000,
                    "explicit":        str(item.get("explicit_lyrics", False)),
                    "danceability":    f["danceability"],
                    "energy":          f["energy"],
                    "key":             f["key"],
                    "loudness":        f["loudness"],
                    "mode":            f["mode"],
                    "speechiness":     f["speechiness"],
                    "acousticness":    f["acousticness"],
                    "instrumentalness":f["instrumentalness"],
                    "liveness":        f["liveness"],
                    "valence":         f["valence"],
                    "tempo":           f["tempo"],
                    "time_signature":  f["time_signature"],
                    "track_genre":     genre_label,
                    "source":          "deezer",
                }
                tracks.append(track)
            time.sleep(0.5)
        except Exception as e:
            print(f"  error {genre_label}: {e}")
            continue

    print(f"Fetched {len(tracks)} tracks from Deezer")
    return tracks


def get_csv_tracks():
    print("Loading from CSV...")
    df = pd.read_csv("/data/deezer_tracks.csv")
    df = df.drop(columns=["Unnamed: 0"], errors="ignore")
    tracks = []
    for _, row in df.iterrows():
        record = row.to_dict()
        record = {k: (None if pd.isna(v) else v) for k, v in record.items()}
        record["source"] = "csv_batch"
        tracks.append(record)
    print(f"Loaded {len(tracks)} tracks from CSV")
    return tracks


def stream_to_kafka(tracks, label):
    print(f"Streaming {len(tracks)} tracks [{label}]")
    count = 0
    for track in tracks:
        producer.send(
            topic="music-stream",
            key=str(track.get("track_id", count)),
            value=track
        )
        count += 1
        if count % 100 == 0:
            print(f"  sent {count}/{len(tracks)}")
            producer.flush()
            time.sleep(0.3)
    producer.flush()
    print(f"Done. Sent {count} tracks")


# Poll Deezer mỗi 2 phút, fallback về CSV nếu lỗi
while True:
    try:
        tracks = fetch_deezer_tracks()
        if tracks:
            stream_to_kafka(tracks, "Deezer")
        else:
            raise ValueError("empty response")
    except Exception as e:
        print(f"Deezer failed: {e}, using CSV")
        try:
            csv_tracks = get_csv_tracks()
            stream_to_kafka(csv_tracks, "CSV")
        except Exception as e2:
            print(f"CSV also failed: {e2}")
    time.sleep(120)

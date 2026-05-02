import json
import time
import os
import pandas as pd
from kafka import KafkaProducer
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

SPOTIFY_CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET", "")
USE_API = bool(SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET)

GENRES = [
    "pop", "hip-hop", "rock", "latin", "dance",
    "r-n-b", "electronic", "indie", "jazz", "classical",
    "country", "metal", "reggae", "soul", "funk"
]

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)
print("Kafka producer connected")

def get_spotify_tracks():
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET
    ))

    queries = [
        ("year:2024", "pop"),
        ("year:2023", "pop"),
        ("year:2022", "rock"),
        ("year:2021", "hip-hop"),
        ("year:2020", "electronic"),
        ("year:2019", "latin"),
        ("year:2018", "indie"),
        ("year:2017", "r-n-b"),
        ("year:2016", "jazz"),
        ("year:2015", "classical"),
        ("year:2014", "country"),
        ("year:2013", "metal"),
        ("year:2012", "soul"),
        ("year:2011", "reggae"),
        ("year:2010", "funk"),
    ]

    # genre-based defaults khi audio_features bị block (deprecated từ 27/11/2024)
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
        "country":    dict(danceability=0.58, energy=0.65, key=5, loudness=-6.0,  mode=1, speechiness=0.04, acousticness=0.40, instrumentalness=0.01, liveness=0.13, valence=0.62, tempo=120.0, time_signature=4),
        "metal":      dict(danceability=0.40, energy=0.95, key=8, loudness=-3.0,  mode=0, speechiness=0.07, acousticness=0.02, instrumentalness=0.15, liveness=0.22, valence=0.30, tempo=150.0, time_signature=4),
        "soul":       dict(danceability=0.63, energy=0.55, key=2, loudness=-8.5,  mode=1, speechiness=0.06, acousticness=0.45, instrumentalness=0.03, liveness=0.12, valence=0.65, tempo=95.0,  time_signature=4),
        "reggae":     dict(danceability=0.70, energy=0.55, key=0, loudness=-9.0,  mode=1, speechiness=0.08, acousticness=0.35, instrumentalness=0.05, liveness=0.15, valence=0.68, tempo=85.0,  time_signature=4),
        "funk":       dict(danceability=0.82, energy=0.72, key=3, loudness=-7.0,  mode=1, speechiness=0.09, acousticness=0.20, instrumentalness=0.10, liveness=0.16, valence=0.75, tempo=108.0, time_signature=4),
    }

    tracks = []
    seen_ids = set()

    for query, genre_label in queries:
        try:
            print(f"  searching: {query}")
            results = sp.search(q=query, type="track", limit=20)
            items = results["tracks"]["items"]
            print(f"    -> {len(items)} results")
            for item in items:
                if item["id"] in seen_ids:
                    continue
                seen_ids.add(item["id"])
                try:
                    features = sp.audio_features(item["id"])[0]
                    if features is None:
                        raise ValueError
                except Exception:
                    features = genre_defaults.get(genre_label, genre_defaults["pop"])
                track = {
                    "track_id": item["id"],
                    "track_name": item["name"],
                    "artists": ", ".join([a["name"] for a in item["artists"]]),
                    "album_name": item["album"]["name"],
                    "popularity": item["popularity"],
                    "duration_ms": item["duration_ms"],
                    "explicit": str(item["explicit"]),
                    "danceability": features["danceability"],
                    "energy": features["energy"],
                    "key": features["key"],
                    "loudness": features["loudness"],
                    "mode": features["mode"],
                    "speechiness": features["speechiness"],
                    "acousticness": features["acousticness"],
                    "instrumentalness": features["instrumentalness"],
                    "liveness": features["liveness"],
                    "valence": features["valence"],
                    "tempo": features["tempo"],
                    "time_signature": features["time_signature"],
                    "track_genre": genre_label,
                    "source": "spotify_api"
                }
                tracks.append(track)
            time.sleep(0.3)
        except Exception as e:
            print(f"  error: {e}")
            continue

    print(f"Fetched {len(tracks)} tracks from Spotify API")
    return tracks


def get_csv_tracks():
    print("Loading from CSV...")
    df = pd.read_csv("/data/spotify_tracks.csv")
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
            topic="spotify-stream",
            key=str(track.get("track_id", count)),
            value=track
        )
        count += 1
        if count % 100 == 0:
            print(f"  sent {count}/{len(tracks)}")
            producer.flush()
            time.sleep(0.5)
    producer.flush()
    print(f"Done. Sent {count} tracks")


if USE_API:
    try:
        api_tracks = get_spotify_tracks()
        stream_to_kafka(api_tracks, "Spotify API")
        csv_tracks = get_csv_tracks()
        while True:
            stream_to_kafka(csv_tracks, "CSV Loop")
            time.sleep(2)
    except Exception as e:
        print(f"API failed: {e}, falling back to CSV")
        USE_API = False

if not USE_API:
    csv_tracks = get_csv_tracks()
    while True:
        stream_to_kafka(csv_tracks, "CSV")
        time.sleep(2)

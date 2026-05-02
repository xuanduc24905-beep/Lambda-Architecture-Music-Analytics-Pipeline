import requests
import pandas as pd
import time

GENRES = [
    "pop", "rock", "hip hop", "electronic", "jazz",
    "classical", "latin", "r&b", "metal", "soul",
    "indie", "dance",
]

genre_defaults = {
    "pop":        dict(danceability=0.65, energy=0.70, key=5, loudness=-6.0,  mode=1, speechiness=0.05, acousticness=0.15, instrumentalness=0.01, liveness=0.12, valence=0.60, tempo=120.0, time_signature=4),
    "rock":       dict(danceability=0.50, energy=0.85, key=7, loudness=-5.0,  mode=1, speechiness=0.06, acousticness=0.05, instrumentalness=0.05, liveness=0.18, valence=0.45, tempo=130.0, time_signature=4),
    "hip hop":    dict(danceability=0.78, energy=0.65, key=2, loudness=-7.0,  mode=0, speechiness=0.22, acousticness=0.10, instrumentalness=0.01, liveness=0.10, valence=0.55, tempo=95.0,  time_signature=4),
    "electronic": dict(danceability=0.75, energy=0.88, key=9, loudness=-5.5,  mode=0, speechiness=0.07, acousticness=0.03, instrumentalness=0.40, liveness=0.08, valence=0.50, tempo=128.0, time_signature=4),
    "latin":      dict(danceability=0.80, energy=0.75, key=0, loudness=-6.5,  mode=1, speechiness=0.08, acousticness=0.20, instrumentalness=0.02, liveness=0.14, valence=0.72, tempo=105.0, time_signature=4),
    "indie":      dict(danceability=0.55, energy=0.60, key=4, loudness=-8.0,  mode=1, speechiness=0.04, acousticness=0.30, instrumentalness=0.08, liveness=0.12, valence=0.48, tempo=115.0, time_signature=4),
    "r&b":        dict(danceability=0.72, energy=0.60, key=1, loudness=-7.5,  mode=0, speechiness=0.12, acousticness=0.25, instrumentalness=0.02, liveness=0.10, valence=0.58, tempo=90.0,  time_signature=4),
    "jazz":       dict(danceability=0.45, energy=0.45, key=6, loudness=-10.0, mode=1, speechiness=0.04, acousticness=0.70, instrumentalness=0.25, liveness=0.20, valence=0.52, tempo=110.0, time_signature=4),
    "classical":  dict(danceability=0.30, energy=0.25, key=3, loudness=-15.0, mode=1, speechiness=0.03, acousticness=0.90, instrumentalness=0.85, liveness=0.10, valence=0.35, tempo=100.0, time_signature=4),
    "metal":      dict(danceability=0.40, energy=0.95, key=8, loudness=-3.0,  mode=0, speechiness=0.07, acousticness=0.02, instrumentalness=0.15, liveness=0.22, valence=0.30, tempo=150.0, time_signature=4),
    "soul":       dict(danceability=0.63, energy=0.55, key=2, loudness=-8.5,  mode=1, speechiness=0.06, acousticness=0.45, instrumentalness=0.03, liveness=0.12, valence=0.65, tempo=95.0,  time_signature=4),
    "dance":      dict(danceability=0.82, energy=0.85, key=5, loudness=-5.0,  mode=1, speechiness=0.06, acousticness=0.05, instrumentalness=0.20, liveness=0.10, valence=0.65, tempo=128.0, time_signature=4),
}

tracks = []
seen_ids = set()

for genre in GENRES:
    print(f"Fetching: {genre}")
    for index in range(0, 1000, 100):
        try:
            url = f"https://api.deezer.com/search?q={genre}&limit=100&index={index}"
            resp = requests.get(url, timeout=10)
            items = resp.json().get("data", [])
            if not items:
                break
            for item in items:
                if item["id"] in seen_ids:
                    continue
                seen_ids.add(item["id"])
                f = genre_defaults.get(genre, genre_defaults["pop"])
                tracks.append({
                    "track_id":         str(item["id"]),
                    "track_name":       item["title"],
                    "artists":          item["artist"]["name"],
                    "album_name":       item["album"]["title"],
                    "popularity":       item.get("rank", 50000) / 1000,
                    "duration_ms":      item["duration"] * 1000,
                    "explicit":         str(item.get("explicit_lyrics", False)),
                    "danceability":     f["danceability"],
                    "energy":           f["energy"],
                    "key":              f["key"],
                    "loudness":         f["loudness"],
                    "mode":             f["mode"],
                    "speechiness":      f["speechiness"],
                    "acousticness":     f["acousticness"],
                    "instrumentalness": f["instrumentalness"],
                    "liveness":         f["liveness"],
                    "valence":          f["valence"],
                    "tempo":            f["tempo"],
                    "time_signature":   f["time_signature"],
                    "track_genre":      genre,
                })
            time.sleep(0.3)
        except Exception as e:
            print(f"  error at index {index}: {e}")
            break

    print(f"  -> {len([t for t in tracks if t['track_genre'] == genre])} tracks")

df = pd.DataFrame(tracks)
df.to_csv("/data/deezer_tracks.csv", index=False)
print(f"\nSaved {len(df)} tracks to /data/deezer_tracks.csv")

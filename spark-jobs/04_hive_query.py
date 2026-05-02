from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Spotify Hive Query") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.hadoop.hive.metastore.schema.verification", "false") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE DATABASE IF NOT EXISTS spotify")
spark.sql("USE spotify")

cleaned = spark.read.parquet("hdfs://namenode:9000/spotify/processed/cleaned")
clustered = spark.read.parquet("hdfs://namenode:9000/spotify/processed/clustered")
genre_stats = spark.read.parquet("hdfs://namenode:9000/spotify/processed/genre_stats")

cleaned.createOrReplaceTempView("tracks_cleaned")
clustered.createOrReplaceTempView("tracks_clustered")
genre_stats.createOrReplaceTempView("genre_stats")

print("\n--- Top 10 Genres ---")
spark.sql("""
    SELECT track_genre, track_count, avg_popularity, avg_danceability, avg_energy
    FROM genre_stats
    ORDER BY avg_popularity DESC
    LIMIT 10
""").show()

print("\n--- Cluster Distribution ---")
spark.sql("""
    SELECT cluster,
           COUNT(*) as track_count,
           ROUND(AVG(popularity), 2) as avg_popularity,
           ROUND(AVG(danceability), 3) as avg_danceability,
           ROUND(AVG(energy), 3) as avg_energy,
           ROUND(AVG(valence), 3) as avg_valence
    FROM tracks_clustered
    GROUP BY cluster
    ORDER BY cluster
""").show()

print("\n--- Top 20 Popular Tracks ---")
spark.sql("""
    SELECT track_name, artists, track_genre, popularity, cluster
    FROM tracks_clustered
    ORDER BY popularity DESC
    LIMIT 20
""").show()

print("\n--- Explicit Tracks by Genre ---")
spark.sql("""
    SELECT track_genre,
           COUNT(*) as total,
           SUM(CASE WHEN explicit = 'True' THEN 1 ELSE 0 END) as explicit_count,
           ROUND(SUM(CASE WHEN explicit = 'True' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as explicit_pct
    FROM tracks_cleaned
    GROUP BY track_genre
    ORDER BY explicit_pct DESC
    LIMIT 10
""").show()

print("\n--- Top Danceable Tracks per Cluster ---")
spark.sql("""
    SELECT cluster, track_name, artists, danceability, energy, valence
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY cluster ORDER BY danceability DESC) as rn
        FROM tracks_clustered
    ) t
    WHERE rn <= 5
    ORDER BY cluster, danceability DESC
""").show(50)

print("All Hive queries done")
spark.stop()

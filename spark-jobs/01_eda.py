from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Music EDA") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.shuffle.partitions", "16") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("track_id", StringType(), True),
    StructField("artists", StringType(), True),
    StructField("album_name", StringType(), True),
    StructField("track_name", StringType(), True),
    StructField("popularity", FloatType(), True),
    StructField("duration_ms", FloatType(), True),
    StructField("explicit", StringType(), True),
    StructField("danceability", FloatType(), True),
    StructField("energy", FloatType(), True),
    StructField("key", FloatType(), True),
    StructField("loudness", FloatType(), True),
    StructField("mode", FloatType(), True),
    StructField("speechiness", FloatType(), True),
    StructField("acousticness", FloatType(), True),
    StructField("instrumentalness", FloatType(), True),
    StructField("liveness", FloatType(), True),
    StructField("valence", FloatType(), True),
    StructField("tempo", FloatType(), True),
    StructField("time_signature", FloatType(), True),
    StructField("track_genre", StringType(), True),
])

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv("hdfs://namenode:9000/music/raw/deezer_tracks.csv")

df = df.dropna().dropDuplicates()
print(f"Loaded {df.count()} rows after cleaning")

print("\n--- Basic Statistics ---")
df.describe(["popularity", "duration_ms", "danceability",
             "energy", "loudness", "tempo", "valence"]).show()

print("\n--- Top 10 Genres ---")
df.groupBy("track_genre").agg(
    F.count("*").alias("track_count"),
    F.round(F.avg("popularity"), 2).alias("avg_popularity"),
    F.round(F.avg("danceability"), 3).alias("avg_danceability"),
    F.round(F.avg("energy"), 3).alias("avg_energy"),
    F.round(F.avg("valence"), 3).alias("avg_valence")
).orderBy(F.desc("track_count")).show(10)

print("\n--- Popularity Distribution ---")
df.groupBy(
    F.when(F.col("popularity") >= 80, "Viral (80-100)")
     .when(F.col("popularity") >= 60, "Popular (60-79)")
     .when(F.col("popularity") >= 40, "Average (40-59)")
     .when(F.col("popularity") >= 20, "Low (20-39)")
     .otherwise("Unknown (0-19)").alias("popularity_tier")
).agg(
    F.count("*").alias("count"),
    F.round(F.avg("danceability"), 3).alias("avg_danceability"),
    F.round(F.avg("energy"), 3).alias("avg_energy")
).orderBy(F.desc("count")).show()

print("\n--- Explicit vs Non-Explicit ---")
df.groupBy("explicit").agg(
    F.count("*").alias("count"),
    F.round(F.avg("popularity"), 2).alias("avg_popularity"),
    F.round(F.avg("energy"), 3).alias("avg_energy"),
    F.round(F.avg("danceability"), 3).alias("avg_danceability")
).show()

print("\n--- Top 20 Artists ---")
df.groupBy("artists").agg(
    F.count("*").alias("track_count"),
    F.round(F.avg("popularity"), 2).alias("avg_popularity")
).filter(F.col("track_count") >= 5) \
 .orderBy(F.desc("avg_popularity")) \
 .show(20)

print("\n--- Feature Correlations with Popularity ---")
features = ["danceability", "energy", "loudness", "speechiness",
            "acousticness", "instrumentalness", "liveness", "valence", "tempo"]
for feature in features:
    corr = df.stat.corr("popularity", feature)
    print(f"  popularity vs {feature:20s}: {corr:.4f}")

print("\n--- Saving to HDFS ---")
df.write.mode("overwrite").parquet("hdfs://namenode:9000/music/processed/cleaned")

df.groupBy("track_genre").agg(
    F.count("*").alias("track_count"),
    F.round(F.avg("popularity"), 2).alias("avg_popularity"),
    F.round(F.avg("danceability"), 3).alias("avg_danceability"),
    F.round(F.avg("energy"), 3).alias("avg_energy"),
    F.round(F.avg("valence"), 3).alias("avg_valence")
).orderBy(F.desc("track_count")) \
 .write.mode("overwrite") \
 .parquet("hdfs://namenode:9000/music/processed/genre_stats")

print("EDA complete")
spark.stop()

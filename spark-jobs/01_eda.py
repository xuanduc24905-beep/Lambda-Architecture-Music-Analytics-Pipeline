from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Spotify EDA") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "10g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "32") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("track_id",          StringType(),  True),
    StructField("track_name",        StringType(),  True),
    StructField("artists",           StringType(),  True),
    StructField("release_date",      StringType(),  True),
    StructField("year",              IntegerType(), True),
    StructField("decade",            StringType(),  True),
    StructField("acousticness",      FloatType(),   True),
    StructField("danceability",      FloatType(),   True),
    StructField("duration_ms",       FloatType(),   True),
    StructField("energy",            FloatType(),   True),
    StructField("explicit",          StringType(),  True),
    StructField("instrumentalness",  FloatType(),   True),
    StructField("key",               FloatType(),   True),
    StructField("liveness",          FloatType(),   True),
    StructField("loudness",          FloatType(),   True),
    StructField("mode",              FloatType(),   True),
    StructField("popularity",        FloatType(),   True),
    StructField("speechiness",       FloatType(),   True),
    StructField("tempo",             FloatType(),   True),
    StructField("valence",           FloatType(),   True),
])

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv("hdfs://namenode:9000/music/raw/spotify_tracks.csv")

df = df.dropna(subset=["track_id", "track_name", "year"]).dropDuplicates(["track_id"])
print(f"Loaded {df.count()} rows after cleaning")

print("\n--- Basic Statistics ---")
df.describe(["popularity", "duration_ms", "danceability",
             "energy", "loudness", "tempo", "valence", "acousticness"]).show()

print("\n--- Tracks per Decade ---")
df.groupBy("decade").agg(
    F.count("*").alias("track_count"),
    F.round(F.avg("popularity"), 2).alias("avg_popularity"),
    F.round(F.avg("danceability"), 3).alias("avg_danceability"),
    F.round(F.avg("energy"), 3).alias("avg_energy"),
    F.round(F.avg("loudness"), 2).alias("avg_loudness"),
    F.round(F.avg("acousticness"), 3).alias("avg_acousticness"),
    F.round(F.avg("valence"), 3).alias("avg_valence"),
).orderBy("decade").show(20)

print("\n--- Loudness War (loudness trend by decade) ---")
df.groupBy("decade").agg(
    F.round(F.avg("loudness"), 3).alias("avg_loudness"),
    F.round(F.stddev("loudness"), 3).alias("std_loudness"),
    F.count("*").alias("track_count"),
).orderBy("decade").show(20)

print("\n--- Energy & Acousticness trend ---")
df.groupBy("decade").agg(
    F.round(F.avg("energy"), 3).alias("avg_energy"),
    F.round(F.avg("acousticness"), 3).alias("avg_acousticness"),
    F.round(F.avg("danceability"), 3).alias("avg_danceability"),
    F.round(F.avg("valence"), 3).alias("avg_valence"),
).orderBy("decade").show(20)

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
    F.round(F.avg("energy"), 3).alias("avg_energy"),
).orderBy(F.desc("count")).show()

print("\n--- Explicit Content by Decade ---")
df.groupBy("decade").agg(
    F.count("*").alias("total"),
    F.sum(F.when(F.col("explicit") == "True", 1).otherwise(0)).alias("explicit_count"),
    F.round(
        F.sum(F.when(F.col("explicit") == "True", 1).otherwise(0)) * 100.0 / F.count("*"), 2
    ).alias("explicit_pct"),
).orderBy("decade").show(20)

print("\n--- Feature Correlations with Popularity ---")
features = ["danceability", "energy", "loudness", "speechiness",
            "acousticness", "instrumentalness", "liveness", "valence", "tempo", "year"]
for feature in features:
    corr = df.stat.corr("popularity", feature)
    print(f"  popularity vs {feature:20s}: {corr:.4f}")

print("\n--- Top 20 Artists (by avg popularity) ---")
df.groupBy("artists").agg(
    F.count("*").alias("track_count"),
    F.round(F.avg("popularity"), 2).alias("avg_popularity"),
).filter(F.col("track_count") >= 5) \
 .orderBy(F.desc("avg_popularity")) \
 .show(20)

print("\n--- Saving to HDFS ---")
df.write.mode("overwrite").parquet("hdfs://namenode:9000/music/processed/cleaned")

df.groupBy("decade").agg(
    F.count("*").alias("track_count"),
    F.round(F.avg("popularity"), 2).alias("avg_popularity"),
    F.round(F.avg("danceability"), 3).alias("avg_danceability"),
    F.round(F.avg("energy"), 3).alias("avg_energy"),
    F.round(F.avg("loudness"), 2).alias("avg_loudness"),
    F.round(F.avg("acousticness"), 3).alias("avg_acousticness"),
    F.round(F.avg("valence"), 3).alias("avg_valence"),
    F.round(F.avg("tempo"), 1).alias("avg_tempo"),
    F.sum(F.when(F.col("explicit") == "True", 1).otherwise(0)).alias("explicit_count"),
    F.round(
        F.sum(F.when(F.col("explicit") == "True", 1).otherwise(0)) * 100.0 / F.count("*"), 2
    ).alias("explicit_pct"),
).orderBy("decade") \
 .write.mode("overwrite") \
 .parquet("hdfs://namenode:9000/music/processed/decade_stats")

print("EDA complete")
spark.stop()

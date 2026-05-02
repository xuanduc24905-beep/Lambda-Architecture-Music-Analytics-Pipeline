from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Music Kafka Streaming") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
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

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "music-stream") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

df_parsed = df_kafka.select(
    F.from_json(F.col("value").cast("string"), schema).alias("data"),
    F.col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")

df_processed = df_parsed \
    .withColumn("popularity_tier",
        F.when(F.col("popularity") >= 80, "Viral")
         .when(F.col("popularity") >= 60, "Popular")
         .when(F.col("popularity") >= 40, "Average")
         .otherwise("Low")
    ) \
    .withColumn("energy_level",
        F.when(F.col("energy") >= 0.7, "High")
         .when(F.col("energy") >= 0.4, "Medium")
         .otherwise("Low")
    ) \
    .withColumn("is_danceable",
        F.when(F.col("danceability") >= 0.7, True).otherwise(False)
    ) \
    .withColumn("ingestion_time", F.current_timestamp()) \
    .dropna(subset=["track_id", "track_name"])

query_hdfs = df_processed.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/music/streaming/tracks") \
    .option("checkpointLocation", "hdfs://namenode:9000/music/streaming/checkpoint") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

query_console = df_processed \
    .groupBy("track_genre", "popularity_tier") \
    .agg(
        F.count("*").alias("count"),
        F.round(F.avg("popularity"), 2).alias("avg_popularity"),
        F.round(F.avg("danceability"), 3).alias("avg_danceability")
    ) \
    .writeStream \
    .format("console") \
    .outputMode("complete") \
    .trigger(processingTime="10 seconds") \
    .start()

query_hdfs.awaitTermination(300)
query_console.awaitTermination(300)

print("Streaming job completed")
spark.stop()

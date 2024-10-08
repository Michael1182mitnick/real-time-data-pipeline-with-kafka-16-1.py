# Real Time Date Pipeline with Kafka
# Install Apache Spark.
# Set up a real-time data pipeline using Apache Kafka to ingest, process, and analyze streaming data from multiple sources.
# Spark Structured Streaming

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType, LongType

# Create Spark session
spark = SparkSession.builder \
    .appName("Kafka Stream Processing") \
    .getOrCreate()

# Define the schema of the incoming data
schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("temperature", FloatType()) \
    .add("timestamp", LongType())

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "streaming-data") \
    .load()

# Parse the Kafka message value (which is in JSON)
df = df.selectExpr("CAST(value AS STRING)").select(
    from_json(col("value"), schema).alias("data")).select("data.*")

# Process the data, for example, calculate average temperature over a time window
aggregated_df = df.groupBy("sensor_id").avg("temperature")

# Write the results to console
query = aggregated_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

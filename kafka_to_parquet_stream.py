from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Define required JARs
JARS = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    "org.apache.kafka:kafka-clients:3.9.0"
]

# Initialize Spark Session with Kafka support
spark = SparkSession.builder \
    .appName("DynamicKafkaToParquet") \
    .config("spark.jars.packages", ",".join(JARS)) \
    .getOrCreate()

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert Kafka value to string
json_df = kafka_df.select(F.col("value").cast("string").alias("json_string"))

# Sample a single record for schema inference
sample_df = json_df.limit(1).select("json_string").collect()
sample_json = sample_df[0]["json_string"] if sample_df else None

# Infer schema dynamically or use a fallback
if sample_json:
    schema = spark.read.json(spark.sparkContext.parallelize([sample_json])).schema
else:
    schema = StructType([StructField("data", StringType())])  # Default schema if no data

# Parse JSON with inferred schema
parsed_df = json_df.withColumn("data", F.from_json(F.col("json_string"), schema))

# Flatten JSON structure
output_df = parsed_df.select("data.*")

# Write stream to Parquet
query = output_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "/output/transactions") \
    .option("checkpointLocation", "/output/checkpoint") \
    .start()

# Await termination
query.awaitTermination()

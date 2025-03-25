from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import os
from os.path import expanduser

# Get user's home directory and set paths
home = expanduser("~")
output_base = os.path.join(home, "kafka_to_parquet_output")
checkpoint_path = os.path.join(output_base, "checkpoint")
parquet_path = os.path.join(output_base, "transactions_parquet")
error_path = os.path.join(output_base, "parse_errors")

# Create directories if they don't exist
os.makedirs(output_base, exist_ok=True)
os.makedirs(checkpoint_path, exist_ok=True)
os.makedirs(parquet_path, exist_ok=True)
os.makedirs(error_path, exist_ok=True)

# Correct JARs for Spark 2.4.7 (Scala 2.11)
KAFKA_JARS = [
    "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7",
    "org.apache.kafka:kafka-clients:2.0.0"
]

# Initialize Spark with Kafka support
spark = SparkSession.builder \
    .appName("KafkaToParquet_UserHome") \
    .config("spark.jars.packages", ",".join(KAFKA_JARS)) \
    .config("spark.sql.parquet.writeLegacyFormat", "true") \
    .getOrCreate()

# 1. Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# 2. Convert to JSON string with error handling
json_string_df = kafka_df.select(
    F.col("key").cast("string").alias("message_key"),
    F.col("value").cast("string").alias("json_string"),
    F.col("timestamp").alias("kafka_timestamp")
)

# 3. Define a fallback schema (modify according to your expected structure)
fallback_schema = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("payload", StringType(), True)
])

# 4. Parse JSON with error handling
def parse_json(df, epoch_id):
    if df.rdd.isEmpty():  # Correct way to check if DataFrame is empty
        return
    
    # Debug: Show incoming data
    print(f"\n=== Processing Batch {epoch_id} ===")
    df.show(truncate=False)
    
    # Try parsing JSON
    parsed_df = df.withColumn("parsed_data", 
                F.from_json(F.col("json_string"), fallback_schema))
    
    # Separate good and bad records
    good_df = parsed_df.filter(F.col("parsed_data").isNotNull())
    error_df = parsed_df.filter(F.col("parsed_data").isNull())
    
    # Process errors
    if not error_df.rdd.isEmpty():  # Correct empty check
        print(f"\n=== Parse Errors in Batch {epoch_id} ===")
        error_df.show(truncate=False)
        error_df.write \
            .mode("append") \
            .parquet(error_path)
    
    # Process good records
    if not good_df.rdd.isEmpty():  # Correct empty check
        final_df = good_df.select(
            "message_key",
            "kafka_timestamp",
            "parsed_data.*"
        )
        final_df.write \
            .mode("append") \
            .parquet(parquet_path)

# 5. Start processing
query = json_string_df.writeStream \
    .foreachBatch(parse_json) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

print(f"""
Streaming query started...
- Output Parquet: {parquet_path}
- Checkpoint: {checkpoint_path}
- Error logs: {error_path}
""")
query.awaitTermination()

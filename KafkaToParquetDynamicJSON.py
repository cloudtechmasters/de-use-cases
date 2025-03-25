from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from os.path import expanduser

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaToParquetDynamicJSON") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7") \
    .config("spark.sql.parquet.writeLegacyFormat", "true") \
    .getOrCreate()

# Setup Paths
home = expanduser("~")
output_base = os.path.join(home, "kafka_parquet_output")
output_path = os.path.join(output_base, "data")
checkpoint_path = os.path.join(output_base, "checkpoint")
error_path = os.path.join(output_base, "errors")

# Create directories if they don't exist
os.makedirs(output_path, exist_ok=True)
os.makedirs(checkpoint_path, exist_ok=True)
os.makedirs(error_path, exist_ok=True)

# 1. Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# 2. Extract Base Fields with null handling
base_df = kafka_df.select(
    F.coalesce(F.col("key").cast("string"), F.lit("no_key")).alias("message_key"),
    F.col("value").cast("string").alias("json_string"),
    F.col("timestamp").alias("kafka_timestamp"),
    F.col("topic").alias("kafka_topic"),
    F.col("partition").alias("kafka_partition"),
    F.col("offset").alias("kafka_offset")
)

# 3. Batch Processing Function
def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    
    try:
        # Parse JSON with schema inference
        parsed_df = spark.read.json(
            batch_df.rdd.map(lambda x: x.json_string),
            multiLine=True
        )
        
        # Join with base fields
        result_df = batch_df.select(
            "message_key",
            "kafka_timestamp",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset"
        ).crossJoin(parsed_df)
        
        # Write to Parquet
        result_df.write \
            .mode("append") \
            .parquet(output_path)
        
        # Debug output
        print(f"=== Successfully processed batch {batch_id} ===")
        print("Schema:")
        result_df.printSchema()
        print("Sample data:")
        result_df.limit(2).show(truncate=False)
        
    except Exception as e:
        print(f"!!! Error processing batch {batch_id}: {str(e)}")
        # Write failed batch to error location
        batch_df.write \
            .mode("append") \
            .parquet(error_path)

# 4. Start Streaming
query = base_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

print(f"""
=== Kafka to Parquet Streaming Started ===
Input Topic: test-topic
Output Path: {output_path}
Checkpoint: {checkpoint_path}
Error Path: {error_path}
""")

query.awaitTermination()

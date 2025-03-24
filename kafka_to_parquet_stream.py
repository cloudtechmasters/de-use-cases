from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType
import logging

# Initialize Spark with Kafka support
spark = SparkSession.builder \
    .appName("DynamicKafkaToParquet") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Kafka Configurations
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "test-topic"
STARTING_OFFSET = "earliest"

# Output Paths
OUTPUT_PATH = "/output/transactions"
CHECKPOINT_PATH = "/output/checkpoint"

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 1. Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", STARTING_OFFSET) \
    .load()

# 2. Convert Kafka value to string
json_df = kafka_df.select(col("value").cast("string").alias("json_string"))

# 3. Infer schema dynamically (sample multiple records)
try:
    sample_records = json_df.limit(5).select("json_string").collect()
    sample_json_list = [row["json_string"] for row in sample_records if row["json_string"]]

    if sample_json_list:
        # Infer schema from multiple JSON samples
        schema = spark.read.json(spark.sparkContext.parallelize(sample_json_list)).schema
        logger.info(f"Schema inferred dynamically: {schema.simpleString()}")
    else:
        raise ValueError("No sample data available.")
except Exception as e:
    logger.warning(f"Failed to infer schema dynamically. Using fallback schema. Error: {e}")
    schema = StructType().add("data", StringType())

# 4. Parse JSON with inferred schema
parsed_df = json_df.withColumn("data", from_json(col("json_string"), schema))

# 5. Extract nested fields
output_df = parsed_df.select("data.*")

# 6. Write to Parquet with checkpointing
query = output_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", OUTPUT_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()

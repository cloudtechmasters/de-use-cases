from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

# Define the schema for the JSON data
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("in_stock", BooleanType(), True)
])

# Initialize Spark Session with Kafka package
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.kafka:kafka-clients:3.9.0") \
    .getOrCreate()

# Read data from Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the JSON data from the Kafka value field
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Define the output directory for Parquet files
output_dir = "C:/output/parquet"  # Change this to your desired output directory

# Write the parsed data to Parquet files
query = parsed_df.writeStream.outputMode("append").format("parquet").option("path", output_dir).option("checkpointLocation", "C:/output/checkpoint").start()
# Wait for the streaming query to terminate
query.awaitTermination()
import json
import os
import logging
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from kafka_config import load_and_validate_config  # Reuse the shared config loader
import argparse

# Configuration
OUTPUT_DIR = "../jewellery_data_09042025"  # Main folder for Parquet files
KAFKA_GROUP_ID = "jewellery_consumer_group"
MAX_FILE_SIZE_MB = 128  # Max size per Parquet file in MB
DEFAULT_CONFIG_PATH = "client.properties"  # Default path to Kafka properties file
DEFAULT_TOPIC = "jewelry-client-topic"  # Default Kafka topic

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)


def infer_type(value):
    """Recursively infer PyArrow type from a value, optimized for Spark."""
    if value is None:
        return pa.null()
    elif isinstance(value, bool):
        return pa.bool_()
    elif isinstance(value, int):
        return pa.int64()
    elif isinstance(value, float):
        return pa.float64()
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
            return pa.timestamp('us') if 'T' in value else pa.date32()
        except ValueError:
            return pa.string()
    elif isinstance(value, list) and value:
        return pa.list_(infer_type(value[0]))
    elif isinstance(value, dict) and value:
        key_type = infer_type(next(iter(value.keys())))
        value_type = infer_type(next(iter(value.values())))
        return pa.map_(key_type, value_type)
    elif isinstance(value, list):
        return pa.list_(pa.string())
    elif isinstance(value, dict):
        return pa.map_(pa.string(), pa.string())
    return pa.string()


def infer_schema_from_single(record):
    """Infer schema from a single JSON record with nullable fields."""
    field_types = {
        key: pa.field(key, infer_type(value), nullable=True)
        for key, value in record.items()
    }
    return pa.schema(list(field_types.values()))


def load_existing_schema(directory):
    """Load schema from the first Parquet file in the directory, if any exist."""
    parquet_files = [f for f in os.listdir(directory) if f.endswith(".parquet")]
    if parquet_files:
        return pq.read_schema(os.path.join(directory, parquet_files[0]))
    return None


def merge_schemas(existing_schema, new_schema):
    """Merge schemas for evolution, ensuring Spark compatibility."""
    if not existing_schema:
        return new_schema

    merged_fields = {f.name: f for f in existing_schema}
    for field in new_schema:
        if field.name not in merged_fields:
            merged_fields[field.name] = field
        else:
            current_type = merged_fields[field.name].type
            new_type = field.type
            if not pa.types.is_null(new_type) and not pa.types.is_null(current_type):
                if pa.types.is_integer(current_type) and pa.types.is_floating(new_type):
                    merged_fields[field.name] = pa.field(field.name, pa.float64(), nullable=True)
                elif pa.types.is_timestamp(current_type) and pa.types.is_string(new_type):
                    merged_fields[field.name] = pa.field(field.name, pa.timestamp('us'), nullable=True)
                elif pa.types.is_string(current_type) or pa.types.is_string(new_type):
                    merged_fields[field.name] = pa.field(field.name, pa.string(), nullable=True)

    return pa.schema(list(merged_fields.values()))


def convert_to_array(value, target_type):
    """Convert a single value to a PyArrow array."""
    try:
        if value is None:
            return pa.array([None], type=target_type)
        if pa.types.is_timestamp(target_type) and isinstance(value, str):
            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
            return pa.array([dt], type=pa.timestamp('us'))
        elif pa.types.is_date(target_type) and isinstance(value, str):
            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
            return pa.array([dt.date()], type=pa.date32())
        elif pa.types.is_list(target_type):
            return pa.array([value], type=target_type)
        elif pa.types.is_map(target_type):
            return pa.array([list(value.items())] if value else [None], type=target_type)
        return pa.array([value], type=target_type)
    except Exception as e:
        logging.warning(f"Failed to convert {value} to {target_type}: {e}")
        return pa.array([str(value)], type=pa.string())


def get_current_parquet_file(directory):
    """Get the latest Parquet file and its index, or start a new one."""
    parquet_files = sorted([f for f in os.listdir(directory) if f.endswith(".parquet")])
    if not parquet_files:
        return os.path.join(directory, "part-00001.parquet"), 1

    latest_file = parquet_files[-1]  # e.g., "part-00003.parquet"
    index = int(latest_file.split("-")[1].split(".")[0])  # Extract number (e.g., 3)

    file_size_mb = os.path.getsize(os.path.join(directory, latest_file)) / (1024 * 1024)  # Size in MB
    if file_size_mb < MAX_FILE_SIZE_MB:
        return os.path.join(directory, latest_file), index
    else:
        new_index = index + 1
        return os.path.join(directory, f"part-{new_index:05d}.parquet"), new_index


def write_single_json_to_parquet(record, directory):
    """Write a single JSON record to a Parquet file, splitting at 128 MB."""
    new_schema = infer_schema_from_single(record)
    existing_schema = load_existing_schema(directory)
    schema = merge_schemas(existing_schema, new_schema)

    # Create arrays for the new record
    arrays = []
    for field in schema:
        value = record.get(field.name)
        arrays.append(convert_to_array(value, field.type))
    new_table = pa.Table.from_arrays(arrays, schema=schema)

    # Determine the current file to write to
    current_file, _ = get_current_parquet_file(directory)

    # Append or write new
    if os.path.exists(current_file):
        existing_table = pq.read_table(current_file)
        # Align existing table with merged schema
        existing_arrays = []
        for field in schema:
            if field.name in existing_table.schema.names:
                existing_arrays.append(existing_table[field.name])
            else:
                existing_arrays.append(pa.array([None] * existing_table.num_rows, type=field.type))
        aligned_existing_table = pa.Table.from_arrays(existing_arrays, schema=schema)

        # Concatenate and write
        combined = pa.concat_tables([aligned_existing_table, new_table])
        pq.write_table(
            combined,
            current_file,
            row_group_size=100000,
            compression='SNAPPY',
            version='2.6',
            flavor='spark'
        )
    else:
        pq.write_table(
            new_table,
            current_file,
            row_group_size=100000,
            compression='SNAPPY',
            version='2.6',
            flavor='spark'
        )
    logging.info(f"Appended 1 record to {current_file}")


def kafka_consumer_loop(config_path, topic_name):
    """Consume JSON messages from Kafka and write to Parquet."""
    conf = load_and_validate_config(config_path)  # Use the reusable config loader
    conf["group.id"] = "jewellery_consumer_group"
    conf["auto.offset.reset"] = "latest"

    consumer = Consumer(conf)
    consumer.subscribe([topic_name])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.info("Reached end of partition")
                else:
                    logging.error(f"Kafka error: {msg.error()}")
                continue

            try:
                json_data = json.loads(msg.value().decode('utf-8'))
                logging.info(f"Consumed message: {json_data}")
                write_single_json_to_parquet(json_data, OUTPUT_DIR)
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode JSON: {e}")
            except Exception as e:
                logging.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logging.info("Consumer interrupted by user")
    finally:
        consumer.close()
        logging.info("Kafka consumer closed")


if __name__ == "__main__":
    # Argument parser with config and topic options
    parser = argparse.ArgumentParser(description="Kafka Consumer for Parquet")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Path to Kafka .properties file")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="The Kafka topic to subscribe to")
    args = parser.parse_args()

    # Run the consumer loop with the provided config and topic
    kafka_consumer_loop(args.config, args.topic)

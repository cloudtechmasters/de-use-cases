# kafka_consumer_09042025.py
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import os
import logging
from typing import Dict, Any, Tuple, Optional
from confluent_kafka import Consumer, KafkaError
import json
import argparse
import sys

# Configuration
DEFAULT_OUTPUT_DIR = "../jewellery_data_09042025"
DEFAULT_CREDENTIALS_PATH = "client.properties"
DEFAULT_TOPIC = "input_topic"
MAX_FILE_SIZE_MB = 128

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("kafka_consumer.log")
    ]
)
logger = logging.getLogger("KafkaConsumer")

def parse_arguments():
    parser = argparse.ArgumentParser(description="Kafka Consumer to Parquet")
    parser.add_argument('--credentials', default=DEFAULT_CREDENTIALS_PATH,
                       help="Path to client.properties file")
    parser.add_argument('--topic', default=DEFAULT_TOPIC,
                       help="Kafka topic to consume from")
    parser.add_argument('--output-dir', default=DEFAULT_OUTPUT_DIR,
                       help="Directory to store Parquet files")
    parser.add_argument('--group-id', default='jewellery_consumer_group',
                       help="Kafka consumer group ID")
    return parser.parse_args()

def load_kafka_config(credentials_path: str, group_id: str) -> Dict[str, str]:
    config = {
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'group.id': group_id,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': 'false',
        'max.poll.interval.ms': '300000'
    }
    try:
        with open(credentials_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.strip()
        logger.info(f"Kafka config loaded from {credentials_path}, using bootstrap: {config.get('bootstrap.servers')}")
        return config
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        raise

def infer_type(value: Any) -> pa.DataType:
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
            normalized = value.replace('Z', '+00:00')
            datetime.fromisoformat(normalized)
            return pa.timestamp('us')
        except (ValueError, TypeError):
            return pa.string()
    elif isinstance(value, dict):
        # Handle nested dictionaries (like 'specs')
        fields = [pa.field(k, infer_type(v), nullable=True) for k, v in value.items()]
        return pa.struct(fields)
    return pa.string()

def infer_schema(record: Dict[str, Any]) -> pa.Schema:
    fields = []
    for key, value in record.items():
        try:
            field_type = infer_type(value)
            fields.append(pa.field(key, field_type, nullable=True))
        except Exception as e:
            logger.warning(f"Failed to infer type for '{key}': {e}")
            fields.append(pa.field(key, pa.string(), nullable=True))
    return pa.schema(fields)

def convert_value(value: Any, target_type: pa.DataType):
    try:
        if value is None:
            return None
        if pa.types.is_timestamp(target_type) and isinstance(value, str):
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        elif pa.types.is_struct(target_type) and isinstance(value, dict):
            return {k: convert_value(v, f.type) for k, f in zip(value.keys(), target_type)}
        return value
    except Exception as e:
        logger.warning(f"Conversion failed for {value}: {e}")
        return str(value)

def get_current_parquet_file(directory: str) -> str:
    os.makedirs(directory, exist_ok=True)
    parquet_files = [f for f in os.listdir(directory) if f.endswith(".parquet")]
    if not parquet_files:
        return os.path.join(directory, "part-00001.parquet")
    
    latest_file = os.path.join(directory, sorted(parquet_files)[-1])
    if os.path.getsize(latest_file) / (1024 * 1024) < MAX_FILE_SIZE_MB:
        return latest_file
    
    index = len(parquet_files) + 1
    return os.path.join(directory, f"part-{index:05d}.parquet")

def write_to_parquet(records: list, directory: str):
    try:
        if not records:
            return
        
        schema = infer_schema(records[0])
        data = {}
        for field in schema:
            data[field.name] = [convert_value(r.get(field.name), field.type) for r in records]
        
        table = pa.Table.from_pydict(data, schema=schema)
        output_file = get_current_parquet_file(directory)
        
        # Use version '1.0' for maximum compatibility
        pq.write_table(
            table,
            output_file,
            version='1.0',
            compression='snappy',
            use_dictionary=True,
            write_statistics=True
        )
        logger.info(f"Appended {len(records)} record(s) to {output_file}")
    except Exception as e:
        logger.error(f"Error writing to Parquet: {e}")
        raise

def create_consumer(credentials_path: str, group_id: str) -> Consumer:
    try:
        config = load_kafka_config(credentials_path, group_id)
        return Consumer(config)
    except Exception as e:
        logger.error(f"Failed to create consumer: {e}")
        raise

def consume_messages(consumer: Consumer, topic: str, output_dir: str):
    consumer.subscribe([topic])
    batch = []
    BATCH_SIZE = 100  # Process in batches for better performance
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if batch:  # Write remaining batch
                    write_to_parquet(batch, output_dir)
                    batch.clear()
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("Reached end of partition")
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                continue
                
            try:
                record = json.loads(msg.value().decode('utf-8'))
                logger.info(f"Consumed message: {record}")
                batch.append(record)
                
                if len(batch) >= BATCH_SIZE:
                    write_to_parquet(batch, output_dir)
                    consumer.commit(asynchronous=False)
                    batch.clear()
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                
    except KeyboardInterrupt:
        if batch:
            write_to_parquet(batch, output_dir)
        logger.info("Shutting down...")
    finally:
        consumer.close()

if __name__ == "__main__":
    args = parse_arguments()
    consumer = create_consumer(args.credentials, args.group_id)
    consume_messages(consumer, args.topic, args.output_dir)

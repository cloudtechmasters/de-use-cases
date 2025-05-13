import logging
from confluent_kafka import Consumer, KafkaException, TopicPartition
import os
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("KafkaTopicInspector")

def load_properties_file(file_path):
    """Loads a .properties file into a dictionary."""
    props = {}
    if not os.path.isfile(file_path):
        logger.error(f"Properties file not found: {file_path}")
        raise FileNotFoundError(f"No such file: {file_path}")
    
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                props[key.strip()] = value.strip()
    return props

def validate_kafka_config(config):
    """Validates Kafka consumer config."""
    required = ["bootstrap.servers", "group.id"]
    for key in required:
        if key not in config:
            raise ValueError(f"Missing required config: '{key}'")

def get_topic_info(config_path, topic_name):
    """
    Gets basic topic information without admin privileges:
    - Partitions
    - Latest offsets (message counts)
    """
    try:
        # Load and validate config
        config = load_properties_file(config_path)
        validate_kafka_config(config)
        
        # Add some default consumer configs if not present
        config.setdefault("auto.offset.reset", "earliest")
        config.setdefault("enable.auto.commit", "false")

        # Create consumer
        consumer = Consumer(config)
        
        # Get topic metadata (doesn't require admin)
        metadata = consumer.list_topics(topic_name, timeout=10)
        
        if topic_name not in metadata.topics:
            logger.error(f"Topic '{topic_name}' not found")
            return False

        topic_metadata = metadata.topics[topic_name]
        
        # Print basic topic info
        logger.info(f"\nTopic: {topic_name}")
        logger.info(f"Partitions: {len(topic_metadata.partitions)}")
        
        # Get latest offsets (message counts) for each partition
        partitions = [TopicPartition(topic_name, p) for p in topic_metadata.partitions]
        offsets = consumer.end_offsets(partitions)
        
        logger.info("\nPartition Information:")
        total_messages = 0
        for tp in partitions:
            partition = tp.partition
            end_offset = offsets[tp]
            logger.info(f"  Partition {partition}: Latest offset = {end_offset}")
            total_messages += end_offset
        
        logger.info(f"\nTotal messages in topic (approximate): {total_messages}")
        
        return True

    except KafkaException as e:
        logger.error(f"Kafka error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return False
    finally:
        if 'consumer' in locals():
            consumer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Topic Inspector")
    parser.add_argument("--config", default="client.properties", help="Path to .properties config file")
    parser.add_argument("--topic", required=True, help="Topic name to inspect")
    args = parser.parse_args()

    get_topic_info(args.config, args.topic)

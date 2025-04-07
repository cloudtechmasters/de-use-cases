import logging
from confluent_kafka.admin import AdminClient
import os
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("KafkaConnectionTest")

def load_properties_file(file_path):
    """
    Loads a .properties file into a dictionary.
    Ignores comments and empty lines.
    """
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
    """
    Validates config based on security mode.
    Raises ValueError if required fields are missing.
    """
    required_common = ["bootstrap.servers"]
    for key in required_common:
        if key not in config:
            raise ValueError(f"Missing required config: '{key}'")

    security_protocol = config.get("security.protocol", "").upper()

    if security_protocol in ("SASL_SSL", "SASL_PLAINTEXT"):
        required_secure = ["sasl.mechanisms", "sasl.username", "sasl.password"]
        for key in required_secure:
            if key not in config:
                raise ValueError(f"Missing required secure config for {security_protocol}: '{key}'")

def test_kafka_connection(config_path):
    """
    Tests Kafka connectivity using AdminClient.
    Works for both secure and local insecure setups.
    """
    try:
        # Load and validate config
        config = load_properties_file(config_path)
        validate_kafka_config(config)

        # Log the essential config info (without passwords)
        logger.info(f"Using config: bootstrap.servers={config.get('bootstrap.servers')}, "
                    f"security.protocol={config.get('security.protocol', 'PLAINTEXT')}")

        # Create Admin client and fetch metadata
        client = AdminClient(config)
        metadata = client.list_topics(timeout=10)

        logger.info("Kafka connection successful.")
        logger.info(f"Brokers: {[broker.id for broker in metadata.brokers.values()]}")
        logger.info(f"Topics: {list(metadata.topics.keys())}")

        return True

    except Exception as e:
        logger.error("Kafka connection failed.", exc_info=True)
        return False


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Kafka Connection Tester")
    parser.add_argument("--config", default="client.properties", help="Path to .properties config file")
    args = parser.parse_args()

    # Test Kafka connection
    test_kafka_connection(args.config)

import os
import logging

logger = logging.getLogger("KafkaConfig")


def load_properties_file(file_path):
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
    if "bootstrap.servers" not in config:
        raise ValueError("Missing required config: 'bootstrap.servers'")

    protocol = config.get("security.protocol", "PLAINTEXT").upper()
    if protocol in ("SASL_SSL", "SASL_PLAINTEXT"):
        required = ["sasl.mechanisms", "sasl.username", "sasl.password"]
        for key in required:
            if key not in config:
                raise ValueError(f"Missing required secure config for {protocol}: '{key}'")


def load_and_validate_config(config_path):
    config = load_properties_file(config_path)
    validate_kafka_config(config)
    logger.info(f"Kafka config loaded from {config_path}, using bootstrap: {config.get('bootstrap.servers')}")
    return config

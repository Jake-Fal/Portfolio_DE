"""Base Kafka producer with JSON serialization."""

import json
from kafka import KafkaProducer
from typing import Dict, Any, Optional
from .logger import setup_logger

logger = setup_logger(__name__)


class JsonKafkaProducer:
    """Base class for producing JSON-encoded messages to Kafka."""

    def __init__(self, bootstrap_servers: str, topic: str):
        """
        Initialize the Kafka producer with JSON serialization.

        Args:
            bootstrap_servers: Kafka broker addresses
            topic: Kafka topic name
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

        # Configure Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3
        )

        logger.info(f"Initialized producer for topic: {topic}")

    def send(self, record: Dict[str, Any], key: Optional[str] = None):
        """
        Send a record to Kafka.

        Args:
            record: Record to send (will be JSON-serialized)
            key: Optional message key
        """
        try:
            future = self.producer.send(
                self.topic,
                value=record,
                key=key
            )

            # Wait for send to complete (with timeout)
            record_metadata = future.get(timeout=10)

            logger.debug(
                f"Message sent to {record_metadata.topic} "
                f"[partition {record_metadata.partition}] at offset {record_metadata.offset}"
            )

        except Exception as e:
            logger.error(f"Error sending message: {e}")
            raise

    def flush(self, timeout: float = 10.0):
        """
        Flush pending messages.

        Args:
            timeout: Maximum time to wait for delivery (seconds)
        """
        self.producer.flush(timeout=timeout)
        logger.debug("All messages flushed successfully")

    def close(self):
        """Close the producer and flush remaining messages."""
        logger.info("Closing producer...")
        self.flush()
        self.producer.close()

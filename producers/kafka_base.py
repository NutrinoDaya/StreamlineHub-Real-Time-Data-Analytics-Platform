"""
Kafka Producer Configuration and Base Classes

This module provides the foundation for all Kafka producers in the StreamLineHub Analytics system.
It handles Kafka connection management, topic creation, and provides base producer functionality.
"""

import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import structlog

logger = structlog.get_logger()


class KafkaConfig:
    """Kafka configuration settings"""
    
    BOOTSTRAP_SERVERS = ['localhost:9093']
    
    # Topic configurations
    TOPICS = {
        'customer_events': {
            'name': 'customer-events',
            'partitions': 3,
            'replication_factor': 1
        },
        'transaction_events': {
            'name': 'transaction-events', 
            'partitions': 3,
            'replication_factor': 1
        },
        'analytics_events': {
            'name': 'analytics-events',
            'partitions': 2,
            'replication_factor': 1
        },
        'ml_events': {
            'name': 'ml-model-events',
            'partitions': 2,
            'replication_factor': 1
        }
    }
    
    # Producer configurations
    PRODUCER_CONFIG = {
        'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
        'key_serializer': lambda x: x.encode('utf-8') if x else None,
        'acks': 'all',  # Wait for all replicas to acknowledge
        'retries': 3,
        'max_in_flight_requests_per_connection': 1,
        'enable_idempotence': True,
        'compression_type': 'gzip'
    }


class BaseKafkaProducer:
    """
    Base Kafka producer class with common functionality.
    
    Provides connection management, error handling, and logging
    for all specific producer implementations.
    """
    
    def __init__(self, topic_name: str):
        """
        Initialize Kafka producer.
        
        Args:
            topic_name: Name of the Kafka topic to publish to
        """
        self.topic_name = topic_name
        self.producer = None
        self._setup_producer()
        
    def _setup_producer(self) -> None:
        """Initialize Kafka producer with configuration."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
                **KafkaConfig.PRODUCER_CONFIG
            )
            logger.info(f"Kafka producer initialized for topic: {self.topic_name}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
            
    def send_message(self, message: Dict[str, Any], key: Optional[str] = None) -> bool:
        """
        Send a message to Kafka topic.
        
        Args:
            message: Message payload to send
            key: Optional message key for partitioning
            
        Returns:
            bool: True if message sent successfully, False otherwise
        """
        try:
            # Add metadata to message
            enriched_message = {
                **message,
                'timestamp': datetime.now().isoformat(),
                'producer_id': self.__class__.__name__
            }
            
            # Send message
            future = self.producer.send(
                topic=self.topic_name,
                value=enriched_message,
                key=key
            )
            
            # Block for synchronous send (optional - can be made async)
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"Message sent successfully",
                topic=self.topic_name,
                partition=record_metadata.partition,
                offset=record_metadata.offset
            )
            return True
            
        except KafkaError as e:
            logger.error(f"Kafka error sending message: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending message: {e}")
            return False
            
    def send_batch(self, messages: list) -> int:
        """
        Send multiple messages in batch.
        
        Args:
            messages: List of message dictionaries
            
        Returns:
            int: Number of messages sent successfully
        """
        success_count = 0
        for message in messages:
            key = message.get('key')
            if self.send_message(message.get('payload', message), key):
                success_count += 1
        return success_count
        
    def flush_and_close(self) -> None:
        """Flush pending messages and close producer."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info(f"Kafka producer closed for topic: {self.topic_name}")
            
    def __enter__(self):
        """Context manager entry."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.flush_and_close()


class EventMetrics:
    """Utility class for generating event metrics and metadata."""
    
    @staticmethod
    def create_event_metadata(event_type: str, source: str) -> Dict[str, Any]:
        """Create standard event metadata."""
        return {
            'event_id': f"{event_type}_{datetime.now().timestamp()}",
            'event_type': event_type,
            'source': source,
            'created_at': datetime.now().isoformat(),
            'version': '1.0'
        }
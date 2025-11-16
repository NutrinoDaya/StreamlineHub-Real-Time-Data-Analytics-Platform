"""
Kafka Topic Management and Setup

This module handles Kafka topic creation, configuration, and management
for the StreamLineHub Analytics streaming data pipeline.
"""

import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
try:
    from producers.kafka_base import KafkaConfig
except ImportError:
    from kafka_base import KafkaConfig

logger = logging.getLogger(__name__)


class TopicManager:
    """
    Kafka topic management for StreamLineHub Analytics.
    
    Handles topic creation, configuration, and validation
    for all streaming data topics in the system.
    """
    
    def __init__(self):
        """Initialize Kafka admin client."""
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
            request_timeout_ms=30000,
            connections_max_idle_ms=60000
        )
        
    def create_topics(self) -> None:
        """
        Create all required Kafka topics.
        
        Creates topics with appropriate partitions and replication
        factors for optimal performance and fault tolerance.
        """
        topics_to_create = []
        
        for topic_key, config in KafkaConfig.TOPICS.items():
            topic = NewTopic(
                name=config['name'],
                num_partitions=config['partitions'],
                replication_factor=config['replication_factor'],
                topic_configs={
                    'retention.ms': '604800000',  # 7 days retention
                    'compression.type': 'gzip',
                    'cleanup.policy': 'delete'
                }
            )
            topics_to_create.append(topic)
            
        try:
            # Create topics
            fs = self.admin_client.create_topics(new_topics=topics_to_create)
            
            # Wait for each operation to finish
            for topic_name, future in fs.items():
                try:
                    future.result()  # The result itself is None
                    logger.info(f"Successfully created topic: {topic_name}")
                except TopicAlreadyExistsError:
                    logger.info(f"Topic already exists: {topic_name}")
                except Exception as e:
                    logger.error(f"Failed to create topic {topic_name}: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to create topics: {e}")
            
    def list_topics(self) -> dict:
        """
        List all existing topics.
        
        Returns:
            dict: Topic metadata
        """
        try:
            metadata = self.admin_client.list_topics()
            return metadata
        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            return {}
            
    def delete_topics(self, topic_names: list) -> None:
        """
        Delete specified topics.
        
        Args:
            topic_names: List of topic names to delete
        """
        try:
            fs = self.admin_client.delete_topics(topics=topic_names)
            for topic_name, future in fs.items():
                try:
                    future.result()
                    logger.info(f"Successfully deleted topic: {topic_name}")
                except Exception as e:
                    logger.error(f"Failed to delete topic {topic_name}: {e}")
        except Exception as e:
            logger.error(f"Failed to delete topics: {e}")


def setup_kafka_infrastructure():
    """
    Setup complete Kafka infrastructure for StreamLineHub Analytics.
    
    Creates all required topics and validates the Kafka setup
    for the streaming data pipeline.
    """
    print("Setting up Kafka infrastructure for StreamLineHub Analytics...")
    
    topic_manager = TopicManager()
    
    # Create topics
    print("Creating Kafka topics...")
    topic_manager.create_topics()
    
    # List topics to validate
    print("\nValidating topic creation...")
    topics = topic_manager.list_topics()
    
    expected_topics = [config['name'] for config in KafkaConfig.TOPICS.values()]
    
    for topic_name in expected_topics:
        if topic_name in topics:
            print(f"✓ Topic '{topic_name}' is ready")
        else:
            print(f"✗ Topic '{topic_name}' not found")
    
    print("\nKafka infrastructure setup complete!")
    return topic_manager


if __name__ == "__main__":
    # Setup Kafka infrastructure
    setup_kafka_infrastructure()
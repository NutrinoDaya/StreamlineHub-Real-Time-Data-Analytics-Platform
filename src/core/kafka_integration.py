"""
Real Kafka Integration for StreamLineHub Analytics
Implements actual Kafka producers and consumers for enterprise streaming
"""

import asyncio
import json
import logging
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import threading
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaEventProducer:
    """Production-ready Kafka event producer with high throughput."""
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.topics = {
            'customer_events': 'customer-events',
            'transaction_events': 'transaction-events',
            'analytics_events': 'analytics-events'
        }
        self.stats = {
            'sent': 0,
            'errors': 0,
            'start_time': None
        }
        
    def connect(self) -> bool:
        """Initialize Kafka producer connection."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                batch_size=32768,  # 32KB batches for high throughput
                linger_ms=10,      # Small delay to batch messages
                compression_type='snappy',  # Compress for better network usage
                acks='1',          # Wait for leader acknowledgment
                retries=3,         # Retry on failure
                max_in_flight_requests_per_connection=5,
                buffer_memory=67108864  # 64MB buffer
            )
            logger.info(f"✅ Connected to Kafka at {self.bootstrap_servers}")
            self.stats['start_time'] = time.time()
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            return False
    
    def get_topic_for_event(self, event_type: str) -> str:
        """Get appropriate Kafka topic for event type."""
        topic_mapping = {
            'customer_behavior': self.topics['customer_events'],
            'transaction': self.topics['transaction_events'],
            'analytics_metric': self.topics['analytics_events']
        }
        return topic_mapping.get(event_type, 'unknown-events')
    
    def send_event(self, event: Dict[str, Any]) -> bool:
        """Send single event to appropriate Kafka topic."""
        if not self.producer:
            return False
            
        try:
            topic = self.get_topic_for_event(event.get('event_type', 'unknown'))
            key = f"{event.get('customer_id', 'unknown')}_{event.get('timestamp', '')}"
            
            # Send to Kafka
            future = self.producer.send(topic, value=event, key=key)
            
            # Async callback for tracking
            def on_success(metadata):
                self.stats['sent'] += 1
                
            def on_error(exception):
                self.stats['errors'] += 1
                logger.error(f"Failed to send event: {exception}")
            
            future.add_callback(on_success)
            future.add_errback(on_error)
            
            return True
            
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Error sending event: {e}")
            return False
    
    def send_batch(self, events: List[Dict[str, Any]]) -> int:
        """Send batch of events to Kafka topics."""
        if not self.producer or not events:
            return 0
            
        sent_count = 0
        
        for event in events:
            if self.send_event(event):
                sent_count += 1
        
        # Flush to ensure delivery
        self.producer.flush(timeout=5)
        
        return sent_count
    
    def get_stats(self) -> Dict[str, Any]:
        """Get producer statistics."""
        elapsed = time.time() - (self.stats['start_time'] or time.time())
        rate = self.stats['sent'] / elapsed if elapsed > 0 else 0
        
        return {
            'events_sent': self.stats['sent'],
            'errors': self.stats['errors'],
            'rate_per_second': round(rate, 1),
            'elapsed_time': round(elapsed, 1)
        }
    
    def close(self):
        """Close Kafka producer connection."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("🔌 Kafka producer connection closed")


class KafkaEventConsumer:
    """Kafka consumer for processing events and updating WebSocket clients."""
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.running = False
        self.stats = {
            'consumed': 0,
            'processed': 0,
            'errors': 0,
            'start_time': None
        }
        self.message_callback = None
        
    def connect(self, topics: List[str], group_id: str = 'streamlinehub-analytics') -> bool:
        """Initialize Kafka consumer connection."""
        try:
            self.consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=group_id,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                key_deserializer=lambda x: x.decode('utf-8') if x else None,
                auto_offset_reset='latest',  # Start from latest messages
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                max_poll_records=500,  # Process up to 500 messages per poll
                fetch_min_bytes=1024,  # Minimum bytes to fetch
                fetch_max_wait_ms=100  # Maximum wait for fetch
            )
            logger.info(f"✅ Connected Kafka consumer to topics: {topics}")
            self.stats['start_time'] = time.time()
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect Kafka consumer: {e}")
            return False
    
    def set_message_callback(self, callback):
        """Set callback function for processing messages."""
        self.message_callback = callback
    
    def start_consuming(self):
        """Start consuming messages from Kafka topics."""
        if not self.consumer:
            logger.error("Consumer not connected")
            return
            
        self.running = True
        logger.info("🔄 Starting Kafka message consumption...")
        
        try:
            while self.running:
                # Poll for messages
                message_batch = self.consumer.poll(timeout_ms=100, max_records=100)
                
                if message_batch:
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            try:
                                self.stats['consumed'] += 1
                                
                                # Process message
                                event_data = {
                                    'topic': message.topic,
                                    'partition': message.partition,
                                    'offset': message.offset,
                                    'key': message.key,
                                    'value': message.value,
                                    'timestamp': message.timestamp,
                                    'consumed_at': datetime.now().isoformat()
                                }
                                
                                # Call callback if set
                                if self.message_callback:
                                    asyncio.create_task(self.message_callback(event_data))
                                
                                self.stats['processed'] += 1
                                
                            except Exception as e:
                                self.stats['errors'] += 1
                                logger.error(f"Error processing message: {e}")
                
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}")
        finally:
            self.running = False
    
    def stop_consuming(self):
        """Stop consuming messages."""
        self.running = False
        logger.info("🛑 Stopping Kafka message consumption...")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get consumer statistics."""
        elapsed = time.time() - (self.stats['start_time'] or time.time())
        consume_rate = self.stats['consumed'] / elapsed if elapsed > 0 else 0
        process_rate = self.stats['processed'] / elapsed if elapsed > 0 else 0
        
        return {
            'messages_consumed': self.stats['consumed'],
            'messages_processed': self.stats['processed'],
            'errors': self.stats['errors'],
            'consume_rate_per_second': round(consume_rate, 1),
            'process_rate_per_second': round(process_rate, 1),
            'elapsed_time': round(elapsed, 1)
        }
    
    def close(self):
        """Close Kafka consumer connection."""
        self.running = False
        if self.consumer:
            self.consumer.close()
            logger.info("🔌 Kafka consumer connection closed")


class KafkaManager:
    """Manages Kafka producers and consumers for the application."""
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.producer = KafkaEventProducer(bootstrap_servers)
        self.consumer = KafkaEventConsumer(bootstrap_servers)
        self.consumer_thread = None
        self.websocket_manager = None
        
    async def initialize(self, websocket_manager=None):
        """Initialize Kafka connections and start consumer."""
        self.websocket_manager = websocket_manager
        
        # Connect producer
        if not self.producer.connect():
            raise Exception("Failed to connect Kafka producer")
            
        # Connect consumer to all topics
        topics = list(self.producer.topics.values())
        if not self.consumer.connect(topics):
            raise Exception("Failed to connect Kafka consumer")
            
        # Set message callback for WebSocket broadcasting
        if websocket_manager:
            self.consumer.set_message_callback(self._handle_kafka_message)
            
        # Start consumer in background thread
        self.consumer_thread = threading.Thread(
            target=self.consumer.start_consuming,
            daemon=True
        )
        self.consumer_thread.start()
        
        logger.info("🚀 Kafka manager initialized successfully")
    
    async def _handle_kafka_message(self, event_data: Dict[str, Any]):
        """Handle messages consumed from Kafka and broadcast to WebSocket clients."""
        if self.websocket_manager:
            # Add Kafka metadata
            enhanced_event = {
                **event_data['value'],
                'kafka_metadata': {
                    'topic': event_data['topic'],
                    'partition': event_data['partition'],
                    'offset': event_data['offset'],
                    'consumed_at': event_data['consumed_at']
                }
            }
            
            # Broadcast to WebSocket clients
            await self.websocket_manager.broadcast_event(enhanced_event)
    
    def send_events_to_kafka(self, events: List[Dict[str, Any]]) -> int:
        """Send events to Kafka topics."""
        return self.producer.send_batch(events)
    
    def get_combined_stats(self) -> Dict[str, Any]:
        """Get combined producer and consumer statistics."""
        producer_stats = self.producer.get_stats()
        consumer_stats = self.consumer.get_stats()
        
        return {
            'producer': producer_stats,
            'consumer': consumer_stats,
            'kafka_bootstrap_servers': self.bootstrap_servers,
            'topics': list(self.producer.topics.values()),
            'status': 'connected' if self.producer.producer and self.consumer.consumer else 'disconnected'
        }
    
    def shutdown(self):
        """Shutdown Kafka connections."""
        logger.info("🔄 Shutting down Kafka manager...")
        
        # Stop consumer
        self.consumer.stop_consuming()
        if self.consumer_thread and self.consumer_thread.is_alive():
            self.consumer_thread.join(timeout=5)
        
        # Close connections
        self.producer.close()
        self.consumer.close()
        
        logger.info("✅ Kafka manager shutdown complete")


# Global Kafka manager instance
kafka_manager: Optional[KafkaManager] = None


def get_kafka_manager() -> Optional[KafkaManager]:
    """Get the global Kafka manager instance."""
    return kafka_manager


async def init_kafka_manager(bootstrap_servers: str = 'localhost:9092', websocket_manager=None):
    """Initialize the global Kafka manager."""
    global kafka_manager
    
    try:
        kafka_manager = KafkaManager(bootstrap_servers)
        await kafka_manager.initialize(websocket_manager)
        logger.info("🎉 Kafka integration initialized successfully")
        return kafka_manager
    except Exception as e:
        logger.error(f"❌ Failed to initialize Kafka integration: {e}")
        kafka_manager = None
        raise


def shutdown_kafka_manager():
    """Shutdown the global Kafka manager."""
    global kafka_manager
    
    if kafka_manager:
        kafka_manager.shutdown()
        kafka_manager = None
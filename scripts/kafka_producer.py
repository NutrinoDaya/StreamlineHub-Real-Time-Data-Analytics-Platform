#!/usr/bin/env python
"""
Real-time data producer for Kafka topics.
Generates realistic customer events, analytics data, and campaign events.
"""

import asyncio
import json
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List
import uuid

from confluent_kafka import Producer
from faker import Faker
import structlog

logger = structlog.get_logger()
fake = Faker()

class DataProducer:
    """
    Produces realistic data events for the streaming pipeline.
    """
    
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'streamlinehub-producer'
        })
        self.fake = Faker()
        
        # Customer segments for realistic data
        self.customer_segments = ['premium', 'standard', 'basic', 'enterprise']
        self.event_types = ['page_view', 'purchase', 'click', 'signup', 'login', 'logout']
        self.campaign_types = ['email', 'social', 'display', 'search', 'video']
        self.products = ['Product A', 'Product B', 'Product C', 'Product D', 'Product E']
        
    def delivery_callback(self, err, msg):
        """Callback for message delivery confirmation."""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    
    def generate_customer_event(self) -> Dict:
        """Generate a realistic customer event."""
        return {
            'event_id': str(uuid.uuid4()),
            'customer_id': f"cust_{random.randint(1000, 9999)}",
            'event_type': random.choice(self.event_types),
            'timestamp': datetime.utcnow().isoformat(),
            'session_id': str(uuid.uuid4()),
            'page_url': f"/products/{random.choice(self.products).lower().replace(' ', '-')}",
            'user_agent': self.fake.user_agent(),
            'ip_address': self.fake.ipv4(),
            'customer_segment': random.choice(self.customer_segments),
            'revenue': round(random.uniform(0, 500), 2) if random.choice(self.event_types) == 'purchase' else 0,
            'product_id': f"prod_{random.randint(100, 999)}",
            'category': random.choice(['electronics', 'clothing', 'books', 'home', 'sports']),
            'source': random.choice(['organic', 'paid', 'social', 'email', 'direct'])
        }
    
    def generate_analytics_data(self) -> Dict:
        """Generate analytics metrics data."""
        return {
            'metric_id': str(uuid.uuid4()),
            'timestamp': datetime.utcnow().isoformat(),
            'metrics': {
                'total_users': random.randint(50, 200),
                'active_sessions': random.randint(10, 50),
                'page_views': random.randint(100, 1000),
                'bounce_rate': round(random.uniform(0.2, 0.8), 3),
                'conversion_rate': round(random.uniform(0.01, 0.15), 3),
                'avg_session_duration': random.randint(30, 600),
                'revenue': round(random.uniform(100, 5000), 2),
                'orders': random.randint(1, 50),
                'cart_abandonment': round(random.uniform(0.3, 0.7), 3)
            },
            'dimensions': {
                'source': random.choice(['organic', 'paid', 'social', 'email']),
                'device': random.choice(['desktop', 'mobile', 'tablet']),
                'location': self.fake.country(),
                'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge'])
            }
        }
    
    def generate_campaign_event(self) -> Dict:
        """Generate campaign performance event."""
        campaign_id = f"camp_{random.randint(100, 999)}"
        return {
            'event_id': str(uuid.uuid4()),
            'campaign_id': campaign_id,
            'campaign_name': f"Campaign {campaign_id}",
            'campaign_type': random.choice(self.campaign_types),
            'timestamp': datetime.utcnow().isoformat(),
            'metrics': {
                'impressions': random.randint(1000, 10000),
                'clicks': random.randint(10, 500),
                'conversions': random.randint(0, 50),
                'spend': round(random.uniform(50, 1000), 2),
                'ctr': round(random.uniform(0.01, 0.1), 4),  # Click-through rate
                'cpc': round(random.uniform(0.5, 5.0), 2),   # Cost per click
                'cpa': round(random.uniform(10, 100), 2),    # Cost per acquisition
                'roas': round(random.uniform(1.5, 8.0), 2)   # Return on ad spend
            },
            'targeting': {
                'age_group': random.choice(['18-24', '25-34', '35-44', '45-54', '55+']),
                'gender': random.choice(['male', 'female', 'all']),
                'location': self.fake.country(),
                'interests': random.sample(['technology', 'fashion', 'sports', 'travel', 'food'], 2)
            }
        }
    
    async def produce_events(self, duration_seconds: int = 300):
        """
        Produce events continuously for the specified duration.
        
        Args:
            duration_seconds: How long to run the producer (default: 5 minutes)
        """
        logger.info(f"Starting data producer for {duration_seconds} seconds")
        start_time = time.time()
        event_count = 0
        
        try:
            while time.time() - start_time < duration_seconds:
                # Generate and send customer event
                customer_event = self.generate_customer_event()
                self.producer.produce(
                    topic='customer-events',
                    key=customer_event['customer_id'],
                    value=json.dumps(customer_event),
                    callback=self.delivery_callback
                )
                event_count += 1
                
                # Generate and send analytics data (less frequent)
                if event_count % 5 == 0:
                    analytics_data = self.generate_analytics_data()
                    self.producer.produce(
                        topic='analytics-data',
                        value=json.dumps(analytics_data),
                        callback=self.delivery_callback
                    )
                
                # Generate and send campaign event (less frequent)
                if event_count % 10 == 0:
                    campaign_event = self.generate_campaign_event()
                    self.producer.produce(
                        topic='campaign-events',
                        key=campaign_event['campaign_id'],
                        value=json.dumps(campaign_event),
                        callback=self.delivery_callback
                    )
                
                # Flush messages and wait
                self.producer.flush(timeout=1)
                
                # Log progress every 100 events
                if event_count % 100 == 0:
                    logger.info(f"Produced {event_count} events")
                
                # Random delay between events (0.1 to 2 seconds)
                await asyncio.sleep(random.uniform(0.1, 2.0))
        
        except KeyboardInterrupt:
            logger.info("Producer interrupted by user")
        except Exception as e:
            logger.error(f"Producer error: {e}")
        finally:
            # Flush any remaining messages
            self.producer.flush()
            logger.info(f"Producer finished. Total events produced: {event_count}")
    
    def produce_batch_events(self, batch_size: int = 1000):
        """
        Produce a batch of events quickly for testing.
        
        Args:
            batch_size: Number of events to produce
        """
        logger.info(f"Producing {batch_size} events in batch mode")
        
        for i in range(batch_size):
            # Customer event
            customer_event = self.generate_customer_event()
            self.producer.produce(
                topic='customer-events',
                key=customer_event['customer_id'],
                value=json.dumps(customer_event),
                callback=self.delivery_callback
            )
            
            # Analytics data every 5th event
            if i % 5 == 0:
                analytics_data = self.generate_analytics_data()
                self.producer.produce(
                    topic='analytics-data',
                    value=json.dumps(analytics_data),
                    callback=self.delivery_callback
                )
            
            # Campaign event every 10th event
            if i % 10 == 0:
                campaign_event = self.generate_campaign_event()
                self.producer.produce(
                    topic='campaign-events',
                    key=campaign_event['campaign_id'],
                    value=json.dumps(campaign_event),
                    callback=self.delivery_callback
                )
            
            if i % 100 == 0:
                logger.info(f"Batch progress: {i}/{batch_size}")
        
        # Flush all messages
        self.producer.flush()
        logger.info(f"Batch complete: {batch_size} events produced")


async def main():
    """Main function to run the data producer."""
    import sys
    
    # Configure logging
    structlog.configure(
        processors=[
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer()
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    producer = DataProducer(bootstrap_servers="localhost:9092")
    
    # Check command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "batch":
            batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
            producer.produce_batch_events(batch_size)
        else:
            duration = int(sys.argv[1])
            await producer.produce_events(duration)
    else:
        # Default: run for 5 minutes
        await producer.produce_events(300)


if __name__ == "__main__":
    asyncio.run(main())
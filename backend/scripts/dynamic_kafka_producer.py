#!/usr/bin/env python3
"""
Dynamic Kafka Producer with Redis Buffering and Threshold Processing
Generates realistic events continuously and triggers Spark processing at thresholds
"""

import asyncio
import json
import logging
import logging.handlers
import os
import random
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from uuid import uuid4

# Add project root to path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

import redis.asyncio as redis
from confluent_kafka import Producer

from src.core.config import get_settings

# Setup file-based logging for Kafka processing
log_dir = ROOT_DIR / "logs"
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler(
            log_dir / "kafka_processing.log",
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        ),
        logging.StreamHandler()  # Also log to console
    ]
)
logger = logging.getLogger(__name__)

class DynamicKafkaProducer:
    """
    Dynamic Kafka Producer that generates realistic events continuously
    and integrates with Redis buffering for threshold-based Spark processing
    """
    
    def __init__(self, 
                 kafka_broker: Optional[str] = None,
                 redis_url: Optional[str] = None,
                 buffer_threshold: int = 100,
                 processing_interval: int = 30,
                 event_rate: int = 10):
        
        self.settings = get_settings()
        
        # Configuration
        self.kafka_broker = kafka_broker or os.getenv('KAFKA_BROKER', 'kafka:9092')
        self.redis_url = redis_url or os.getenv('REDIS_URL', 'redis://:redis_secret@redis:6379')
        self.buffer_threshold = int(os.getenv('BUFFER_THRESHOLD', str(buffer_threshold)))
        self.processing_interval = int(os.getenv('PROCESSING_INTERVAL', str(processing_interval)))
        self.event_rate = int(os.getenv('EVENT_RATE', str(event_rate)))
        self.interval = 1.0 / self.event_rate
        
        # Clients
        self.kafka_producer = None
        self.redis_client = None
        
        # Event generation state
        self.user_pool = self._generate_user_pool(1000)
        self.session_cache = {}
        self.product_catalog = self._generate_product_catalog()
        
        # Statistics
        self.stats = {
            "events_generated": 0,
            "kafka_messages_sent": 0,
            "redis_buffered": 0,
            "spark_triggers": 0,
            "errors": 0,
            "start_time": None
        }
        
        # Topics mapping
        self.topics = {
            "customer_behavior": "customer-events",
            "transaction_completed": "transaction-events", 
            "system_metric": "analytics-events"
        }
        
        # Running state
        self.running = False
    
    def _generate_user_pool(self, size: int) -> List[Dict[str, Any]]:
        """Generate pool of realistic users"""
        locations = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
        devices = ["Desktop", "Mobile", "Tablet"]
        browsers = ["Chrome", "Firefox", "Safari", "Edge", "Opera"]
        
        users = []
        for i in range(size):
            user = {
                "user_id": f"user_{i:06d}",
                "email": f"user{i}@example.com",
                "location": random.choice(locations),
                "device_type": random.choice(devices),
                "browser": random.choice(browsers),
                "registration_date": (datetime.now() - timedelta(days=random.randint(1, 730))).isoformat(),
                "tier": random.choice(["bronze", "silver", "gold", "platinum"]),
                "avg_session_duration": random.randint(120, 1800)  # seconds
            }
            users.append(user)
        
        return users
    
    def _generate_product_catalog(self) -> List[Dict[str, Any]]:
        """Generate product catalog"""
        categories = ["Electronics", "Clothing", "Books", "Home", "Sports", "Beauty", "Automotive", "Toys"]
        products = []
        
        for i in range(500):
            product = {
                "product_id": f"prod_{i:04d}",
                "name": f"{random.choice(['Premium', 'Deluxe', 'Standard', 'Basic'])} {random.choice(['Widget', 'Gadget', 'Tool', 'Device'])} {i}",
                "category": random.choice(categories),
                "price": round(random.uniform(9.99, 999.99), 2),
                "rating": round(random.uniform(3.0, 5.0), 1),
                "stock": random.randint(0, 1000)
            }
            products.append(product)
        
        return products
    
    async def initialize(self) -> bool:
        """Initialize Kafka producer and Redis client"""
        try:
            logger.info("[INIT] Initializing Dynamic Kafka Producer...")
            
            # Initialize Kafka Producer
            logger.info("[KAFKA] Connecting to Kafka...")
            kafka_config = {
                'bootstrap.servers': self.kafka_broker,
                'client.id': 'dynamic-producer',
                'batch.size': 16384,
                'linger.ms': 10,
                'compression.type': 'snappy',
                'retries': 3,
                'retry.backoff.ms': 100
            }
            
            self.kafka_producer = Producer(kafka_config)
            logger.info("[SUCCESS] Kafka producer initialized")
            
            # Initialize Redis Client
            logger.info("[REDIS] Connecting to Redis...")
            self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
            await self.redis_client.ping()
            logger.info("[SUCCESS] Redis client connected")
            
            # Initialize stats
            self.stats["start_time"] = datetime.now()
            
            logger.info("[SUCCESS] Dynamic Kafka Producer initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"[ERROR] Failed to initialize: {e}")
            return False
    
    def _get_or_create_session(self, user: Dict[str, Any]) -> str:
        """Get or create user session"""
        user_id = user["user_id"]
        
        # 20% chance to start new session
        if user_id not in self.session_cache or random.random() < 0.2:
            self.session_cache[user_id] = {
                "session_id": str(uuid4()),
                "start_time": datetime.now(),
                "page_views": 0,
                "actions": 0
            }
        
        return self.session_cache[user_id]["session_id"]
    
    def generate_customer_behavior_event(self) -> Dict[str, Any]:
        """Generate realistic customer behavior event"""
        user = random.choice(self.user_pool)
        session_id = self._get_or_create_session(user)
        
        actions = ["page_view", "click", "scroll", "search", "filter", "add_to_cart", "remove_from_cart", "checkout_start"]
        pages = ["/", "/products", "/search", "/cart", "/checkout", "/profile", "/orders", "/support"]
        
        event = {
            "event_type": "customer_behavior",
            "event_id": str(uuid4()),
            "user_id": user["user_id"],
            "session_id": session_id,
            "action": random.choice(actions),
            "page_url": random.choice(pages),
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "user_agent": f"{user['browser']}/{random.randint(90, 120)}.0 ({user['device_type']})",
            "ip_address": f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
            "location": user["location"],
            "device_type": user["device_type"],
            "referrer": random.choice(["google.com", "facebook.com", "twitter.com", "direct", "email"]),
            "duration_ms": random.randint(100, 5000)
        }
        
        # Update session cache
        if user["user_id"] in self.session_cache:
            self.session_cache[user["user_id"]]["page_views"] += 1
            self.session_cache[user["user_id"]]["actions"] += 1
        
        return event
    
    def generate_transaction_event(self) -> Dict[str, Any]:
        """Generate realistic transaction event"""
        user = random.choice(self.user_pool)
        product = random.choice(self.product_catalog)
        
        # Higher chance for gold/platinum users to make purchases
        tier_multiplier = {"bronze": 0.1, "silver": 0.3, "gold": 0.6, "platinum": 0.9}
        if random.random() > tier_multiplier.get(user["tier"], 0.1):
            return None  # Skip transaction
        
        quantity = random.randint(1, 5)
        unit_price = product["price"]
        total_amount = round(quantity * unit_price, 2)
        
        # Apply discounts occasionally
        discount = 0.0
        if random.random() < 0.15:  # 15% chance of discount
            discount = round(total_amount * random.uniform(0.05, 0.25), 2)
        
        payment_methods = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]
        
        event = {
            "event_type": "transaction_completed",
            "event_id": str(uuid4()),
            "user_id": user["user_id"],
            "transaction_id": f"txn_{int(time.time())}_{random.randint(1000, 9999)}",
            "product_id": product["product_id"],
            "product_name": product["name"],
            "category": product["category"],
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total_amount - discount,
            "discount_amount": discount,
            "payment_method": random.choice(payment_methods),
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "location": user["location"],
            "user_tier": user["tier"],
            "success": random.random() > 0.02,  # 98% success rate
            "processing_time_ms": random.randint(200, 2000)
        }
        
        return event
    
    def generate_system_metric_event(self) -> Dict[str, Any]:
        """Generate realistic system metric event"""
        services = ["api-gateway", "user-service", "product-service", "order-service", "payment-service", "notification-service"]
        metrics = ["cpu_usage", "memory_usage", "response_time", "error_rate", "throughput"]
        
        service = random.choice(services)
        metric_type = random.choice(metrics)
        
        # Generate realistic values based on metric type
        if metric_type == "cpu_usage":
            value = round(random.uniform(10, 85), 2)
            unit = "percent"
        elif metric_type == "memory_usage":
            value = round(random.uniform(200, 1800), 2)
            unit = "MB"
        elif metric_type == "response_time":
            value = round(random.uniform(50, 500), 2)
            unit = "ms"
        elif metric_type == "error_rate":
            value = round(random.uniform(0, 5), 3)
            unit = "percent"
        else:  # throughput
            value = round(random.uniform(100, 1000), 2)
            unit = "req/sec"
        
        event = {
            "event_type": "system_metric",
            "event_id": str(uuid4()),
            "service_name": service,
            "metric_name": metric_type,
            "value": value,
            "unit": unit,
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
            "host": f"{service}-{random.randint(1, 5)}",
            "environment": "production",
            "region": random.choice(["us-east-1", "us-west-2", "eu-west-1"]),
            "alert_threshold": value * random.uniform(1.2, 2.0) if metric_type != "error_rate" else 10.0
        }
        
        return event
    
    def generate_dynamic_event(self) -> Optional[Dict[str, Any]]:
        """Generate a random event type with realistic distribution"""
        # Event distribution: 60% customer behavior, 25% transactions, 15% system metrics
        rand = random.random()
        
        if rand < 0.6:
            return self.generate_customer_behavior_event()
        elif rand < 0.85:
            event = self.generate_transaction_event()
            return event if event else self.generate_customer_behavior_event()
        else:
            return self.generate_system_metric_event()
    
    def _delivery_callback(self, err, msg):
        """Kafka delivery callback"""
        if err is not None:
            logger.error(f"[DELIVERY_ERROR] Kafka delivery failed: {err}")
            self.stats["errors"] += 1
        else:
            self.stats["kafka_messages_sent"] += 1
            if self.stats["kafka_messages_sent"] % 100 == 0:
                logger.info(f"[SENT] Sent {self.stats['kafka_messages_sent']} messages to Kafka")
    
    async def send_to_kafka(self, event: Dict[str, Any]) -> bool:
        """Send event to appropriate Kafka topic"""
        try:
            event_type = event["event_type"]
            topic = self.topics.get(event_type)
            
            if not topic:
                logger.warning(f"[WARN] Unknown event type: {event_type}")
                return False
            
            # Send to Kafka
            self.kafka_producer.produce(
                topic=topic,
                key=event.get("user_id", event.get("service_name", "system")),
                value=json.dumps(event),
                callback=self._delivery_callback
            )
            
            # Poll for delivery reports
            self.kafka_producer.poll(0)
            
            return True
            
        except Exception as e:
            logger.error(f"[ERROR] Failed to send to Kafka: {e}")
            self.stats["errors"] += 1
            return False
    
    async def buffer_to_redis(self, event: Dict[str, Any]) -> bool:
        """Buffer event in Redis for threshold-based processing"""
        try:
            event_type = event["event_type"]
            buffer_key = f"spark:buffer:{event_type}"
            
            # Add to Redis buffer
            await self.redis_client.lpush(buffer_key, json.dumps(event))
            await self.redis_client.expire(buffer_key, 3600)  # 1 hour TTL
            
            self.stats["redis_buffered"] += 1
            
            # Check buffer threshold
            buffer_size = await self.redis_client.llen(buffer_key)
            
            # Only trigger when we EXACTLY reach the threshold (not after)
            if buffer_size == self.buffer_threshold:
                logger.info(f"[THRESHOLD] THRESHOLD REACHED! {event_type}: {buffer_size}/{self.buffer_threshold}")
                await self.trigger_spark_processing(event_type, buffer_size)
                return True
            
            # Log progress every 25 events
            if buffer_size % 25 == 0:
                logger.info(f"[BUFFER] Buffer '{event_type}': {buffer_size}/{self.buffer_threshold}")
            
            return True
            
        except Exception as e:
            logger.error(f"[ERROR] Failed to buffer to Redis: {e}")
            self.stats["errors"] += 1
            return False
    
    async def trigger_spark_processing(self, event_type: str, buffer_size: int):
        """Trigger Spark processing when threshold is reached"""
        try:
            logger.info(f"[SPARK_TRIGGER] TRIGGERING SPARK PROCESSING for {event_type} ({buffer_size} events)")
            
            # Create trigger signal in Redis
            trigger_key = f"spark:trigger:{event_type}"
            trigger_data = {
                "event_type": event_type,
                "buffer_size": buffer_size,
                "trigger_time": datetime.now().isoformat(),
                "threshold": self.buffer_threshold
            }
            
            await self.redis_client.setex(trigger_key, 300, json.dumps(trigger_data))  # 5 min TTL
            
            # Update stats
            self.stats["spark_triggers"] += 1
            
            logger.info(f"[SPARK_SET] Spark processing trigger set for {event_type}")
            
        except Exception as e:
            logger.error(f"[ERROR] Failed to set Spark trigger: {e}")
    
    def print_stats(self):
        """Print current statistics"""
        if not self.stats["start_time"]:
            return
        
        runtime = datetime.now() - self.stats["start_time"]
        runtime_seconds = runtime.total_seconds()
        
        rate = self.stats["events_generated"] / runtime_seconds if runtime_seconds > 0 else 0
        
        print("\n" + "="*60)
        print("[STATS] DYNAMIC KAFKA PRODUCER STATISTICS")
        print("="*60)
        print(f"[TIME] Runtime: {runtime}")
        print(f"[GENERATED] Events Generated: {self.stats['events_generated']}")
        print(f"[SENT] Kafka Messages Sent: {self.stats['kafka_messages_sent']}")
        print(f"[BUFFER] Redis Buffered: {self.stats['redis_buffered']}")
        print(f"[SPARK] Spark Triggers: {self.stats['spark_triggers']}")
        print(f"[ERRORS] Errors: {self.stats['errors']}")
        print(f"[RATE] Generation Rate: {rate:.2f} events/sec")
        print(f"[TARGET] Target Rate: {self.event_rate} events/sec")
        print("="*60)
    
    async def run_continuous_generation(self, duration_minutes: int = 10):
        """Run continuous event generation"""
        try:
            logger.info(f"[START] Starting continuous event generation for {duration_minutes} minutes...")
            logger.info(f"[CONFIG] Event rate: {self.event_rate} events/sec")
            logger.info(f"[CONFIG] Buffer threshold: {self.buffer_threshold} events")
            
            self.running = True
            end_time = datetime.now() + timedelta(minutes=duration_minutes)
            
            while self.running and datetime.now() < end_time:
                try:
                    # Generate event
                    event = self.generate_dynamic_event()
                    if not event:
                        continue
                    
                    self.stats["events_generated"] += 1
                    
                    # Send to Kafka
                    await self.send_to_kafka(event)
                    
                    # Buffer in Redis
                    await self.buffer_to_redis(event)
                    
                    # Print stats every 200 events
                    if self.stats["events_generated"] % 200 == 0:
                        self.print_stats()
                    
                    # Rate limiting
                    await asyncio.sleep(self.interval)
                    
                except KeyboardInterrupt:
                    logger.info("[STOP] Stopping event generation...")
                    break
                except Exception as e:
                    logger.error(f"[ERROR] Error in generation loop: {e}")
                    self.stats["errors"] += 1
                    await asyncio.sleep(1)
            
            self.running = False
            logger.info("[COMPLETE] Event generation completed")
            
        except Exception as e:
            logger.error(f"[ERROR] Failed during continuous generation: {e}")
        finally:
            # Final stats
            self.print_stats()
            
            # Flush Kafka producer
            if self.kafka_producer:
                logger.info("[FLUSH] Flushing Kafka producer...")
                self.kafka_producer.flush(30)
                logger.info("[SUCCESS] Kafka producer flushed")
    
    async def stop(self):
        """Stop the producer"""
        logger.info("[STOP] Stopping Dynamic Kafka Producer...")
        self.running = False
        
        if self.kafka_producer:
            self.kafka_producer.flush(10)
        
        if self.redis_client:
            await self.redis_client.aclose()
        
        logger.info("[SUCCESS] Dynamic Kafka Producer stopped")
    
    async def run_exact_count(self, count: int):
        """Generate exactly N events and stop"""
        try:
            logger.info(f"[START] Generating exactly {count} events...")
            logger.info(f"[CONFIG] Event rate: {self.event_rate} events/sec")
            logger.info(f"[CONFIG] Buffer threshold: {self.buffer_threshold} events")
            
            self.running = True
            
            for i in range(count):
                if not self.running:
                    break
                    
                try:
                    # Generate event
                    event = self.generate_dynamic_event()
                    if not event:
                        continue
                    
                    self.stats["events_generated"] += 1
                    
                    # Send to Kafka
                    await self.send_to_kafka(event)
                    
                    # Buffer in Redis
                    await self.buffer_to_redis(event)
                    
                    # Print progress every 10 events
                    if (i + 1) % 10 == 0:
                        logger.info(f"[PROGRESS] Generated {i + 1}/{count} events")
                    
                    # Rate limiting
                    await asyncio.sleep(self.interval)
                    
                except Exception as e:
                    logger.error(f"[ERROR] Error generating event {i + 1}: {e}")
                    self.stats["errors"] += 1
            
            self.running = False
            logger.info(f"[COMPLETE] Generated {self.stats['events_generated']} events")
            
        except Exception as e:
            logger.error(f"[ERROR] Failed during event generation: {e}")
        finally:
            # Final stats
            self.print_stats()

async def main():
    """Main execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Dynamic Kafka Producer')
    parser.add_argument('--duration', type=float, default=0.5, help='Duration in minutes')
    parser.add_argument('--rate', type=int, default=5, help='Events per second')
    parser.add_argument('--count', type=int, help='Generate exactly N events and stop')
    args = parser.parse_args()

    # Get configuration from environment variables or use defaults
    kafka_broker = os.getenv("KAFKA_BROKER", os.getenv("STREAMLINEHUB_KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))
    redis_url = os.getenv("REDIS_URL", os.getenv("STREAMLINEHUB_REDIS_URL", "redis://:redis_secret@localhost:16379"))
    buffer_threshold = int(os.getenv("BUFFER_THRESHOLD", "50"))
    event_rate = 50

    logger.info(f"Starting producer with: Kafka={kafka_broker}, Redis={redis_url}, Threshold={buffer_threshold}, Rate={event_rate}, Duration={args.duration}m")

    producer = DynamicKafkaProducer(
        kafka_broker=kafka_broker,
        redis_url=redis_url,
        buffer_threshold=buffer_threshold,
        event_rate=event_rate
    )
    
    try:
        # Initialize
        if not await producer.initialize():
            logger.error("[ERROR] Failed to initialize producer")
            return
        
        # Run generation based on mode
        if args.count:
            await producer.run_exact_count(count=args.count)
        else:
            await producer.run_continuous_generation(duration_minutes=args.duration)
        
    except KeyboardInterrupt:
        logger.info("[INTERRUPT] Interrupted by user")
    except Exception as e:
        logger.error(f"[ERROR] Unexpected error: {e}")
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())
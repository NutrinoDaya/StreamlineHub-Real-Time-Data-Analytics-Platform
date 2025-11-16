#!/usr/bin/env python
"""
Send test events to Kafka to demonstrate live dashboard updates.
This script generates various event types and sends them to Kafka topics.
"""

import asyncio
import random
from datetime import datetime
from typing import Dict, Any
import time
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.confluent_kafka_integration import kafka_manager


def generate_customer_event() -> Dict[str, Any]:
    """Generate a customer behavior event"""
    actions = ['view_product', 'add_to_cart', 'purchase', 'search', 'register', 'login']
    categories = ['electronics', 'clothing', 'books', 'home', 'sports', 'food']
    devices = ['desktop', 'mobile', 'tablet']
    locations = ['US', 'UK', 'CA', 'DE', 'FR', 'AU', 'JP']
    
    return {
        'event_type': 'customer_behavior',
        'timestamp': datetime.now().isoformat(),
        'customer_id': random.randint(1000, 9999),
        'session_id': f"sess_{random.randint(100000, 999999)}",
        'action': random.choice(actions),
        'product_id': random.randint(1, 500) if random.random() > 0.3 else None,
        'category': random.choice(categories),
        'device_type': random.choice(devices),
        'location': random.choice(locations),
        'value': round(random.uniform(10, 500), 2) if random.random() > 0.5 else None
    }


def generate_transaction_event() -> Dict[str, Any]:
    """Generate a transaction event"""
    statuses = ['completed', 'pending', 'failed']
    payment_methods = ['credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay']
    currencies = ['USD', 'EUR', 'GBP', 'CAD', 'AUD']
    
    return {
        'event_type': 'transaction',
        'timestamp': datetime.now().isoformat(),
        'transaction_id': f"txn_{random.randint(1000000, 9999999)}",
        'customer_id': random.randint(1000, 9999),
        'amount': round(random.uniform(5, 1000), 2),
        'currency': random.choice(currencies),
        'payment_method': random.choice(payment_methods),
        'status': random.choice(statuses),
        'merchant_id': f"merchant_{random.randint(1, 50)}",
        'items_count': random.randint(1, 10)
    }


def generate_analytics_event() -> Dict[str, Any]:
    """Generate an analytics metric event"""
    metrics = ['page_view', 'session_start', 'conversion', 'bounce', 'engagement', 'revenue']
    sources = ['web_analytics', 'mobile_app', 'api', 'batch_processing']
    environments = ['production', 'staging', 'development']
    
    return {
        'event_type': 'analytics_metric',
        'timestamp': datetime.now().isoformat(),
        'customer_id': random.randint(1000, 9999),
        'metric_name': random.choice(metrics),
        'value': round(random.uniform(1, 100), 2),
        'source': random.choice(sources),
        'environment': random.choice(environments),
        'server_id': f"server_{random.randint(1, 20)}"
    }


async def send_event_batch(count: int = 10) -> int:
    """Send a batch of mixed events to Kafka"""
    events = []
    
    for _ in range(count):
        # Generate random event type
        event_type = random.choices(
            ['customer', 'transaction', 'analytics'],
            weights=[50, 30, 20],  # Customer events more frequent
            k=1
        )[0]
        
        if event_type == 'customer':
            event = generate_customer_event()
        elif event_type == 'transaction':
            event = generate_transaction_event()
        else:
            event = generate_analytics_event()
            
        events.append(event)
    
    # Send batch to Kafka
    if kafka_manager.producer:
        sent_count = kafka_manager.send_batch(events)
        kafka_manager.producer.flush(timeout=5)  # Ensure delivery
        return sent_count
    else:
        print("⚠️ Kafka producer not available")
        return 0


async def continuous_event_stream(duration_minutes: int = 5, events_per_second: float = 2.0):
    """Send events continuously for a specified duration"""
    print(f"🚀 Starting continuous event stream for {duration_minutes} minutes")
    print(f"📊 Target rate: {events_per_second} events/second")
    print("=" * 60)
    
    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)
    total_sent = 0
    batch_size = max(1, int(events_per_second * 2))  # Batch events for efficiency
    
    try:
        while time.time() < end_time:
            batch_start = time.time()
            
            # Send batch
            sent = await send_event_batch(batch_size)
            total_sent += sent
            
            # Calculate stats
            elapsed = time.time() - start_time
            current_rate = total_sent / elapsed if elapsed > 0 else 0
            
            print(f"⏱️  {elapsed:.1f}s | Sent: {sent} events | Total: {total_sent} | Rate: {current_rate:.1f}/s")
            
            # Sleep to maintain target rate
            batch_duration = time.time() - batch_start
            target_interval = batch_size / events_per_second
            sleep_time = max(0, target_interval - batch_duration)
            
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            
    except KeyboardInterrupt:
        print("\n🛑 Stream interrupted by user")
    except Exception as e:
        print(f"\n❌ Error in event stream: {e}")
    
    final_elapsed = time.time() - start_time
    final_rate = total_sent / final_elapsed if final_elapsed > 0 else 0
    
    print("\n" + "=" * 60)
    print("📈 Stream Summary")
    print("=" * 60)
    print(f"Duration: {final_elapsed:.1f} seconds")
    print(f"Total Events Sent: {total_sent}")
    print(f"Average Rate: {final_rate:.2f} events/second")
    print(f"Kafka Producer Stats:")
    
    if kafka_manager.producer:
        stats = kafka_manager.producer.get_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")


async def send_burst_events(burst_count: int = 50):
    """Send a burst of events quickly"""
    print(f"💥 Sending burst of {burst_count} events")
    
    start_time = time.time()
    sent = await send_event_batch(burst_count)
    elapsed = time.time() - start_time
    
    print(f"✅ Sent {sent}/{burst_count} events in {elapsed:.2f} seconds")
    print(f"📊 Rate: {sent/elapsed:.1f} events/second")


async def main():
    """Main function - initialize Kafka and start sending events"""
    print("StreamLineHub Analytics - Kafka Event Generator")
    print("=" * 60)
    
    # Initialize Kafka manager
    try:
        success = await kafka_manager.initialize()
        if not success:
            print("❌ Failed to initialize Kafka manager")
            print("💡 Make sure Kafka is running: docker-compose up kafka")
            return
            
        print("✅ Kafka manager initialized successfully")
        
    except Exception as e:
        print(f"❌ Kafka initialization error: {e}")
        print("💡 Make sure Kafka is running: docker-compose up kafka")
        return
    
    print("\nChoose an option:")
    print("1. Send single burst (50 events)")
    print("2. Continuous stream (5 minutes)")
    print("3. High-rate stream (10 events/sec for 2 minutes)")
    print("4. Ultra high-rate stream (2.5k events/sec for 1 minute)")
    print("5. Custom stream")
    
    try:
        choice = input("\nEnter choice (1-5): ").strip()
        
        if choice == "1":
            await send_burst_events(50)
            
        elif choice == "2":
            await continuous_event_stream(duration_minutes=5, events_per_second=2.0)
            
        elif choice == "3":
            await continuous_event_stream(duration_minutes=2, events_per_second=10.0)
            
        elif choice == "4":
            await continuous_event_stream(duration_minutes=1, events_per_second=2500.0)
            
        elif choice == "5":
            duration = float(input("Duration in minutes: "))
            rate = float(input("Events per second: "))
            await continuous_event_stream(duration_minutes=duration, events_per_second=rate)
            
        else:
            print("Invalid choice")
            
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        # Cleanup
        await kafka_manager.close()
        print("🔚 Kafka connections closed")


if __name__ == "__main__":
    asyncio.run(main())
#!/usr/bin/env python3
"""
StreamLineHub Analytics Kafka Data Publisher

Professional Kafka data publisher for generating real-time streaming data
for the StreamLineHub Analytics platform. This script demonstrates enterprise-grade
event streaming capabilities suitable for production environments.

Usage:
    python kafka_publisher.py --events 100 --interval 1 --topics all
    python kafka_publisher.py --setup-only  # Just setup topics
    python kafka_publisher.py --continuous   # Run continuously
"""

import argparse
import time
import signal
import sys
from typing import Optional
from datetime import datetime

try:
    from producers.topic_setup import setup_kafka_infrastructure
    from producers.event_producers import (
        CustomerEventProducer, 
        TransactionEventProducer, 
        AnalyticsEventProducer,
        generate_sample_events
    )
except ImportError:
    # If running directly from producers directory
    from topic_setup import setup_kafka_infrastructure
    from event_producers import (
        CustomerEventProducer, 
        TransactionEventProducer, 
        AnalyticsEventProducer,
        generate_sample_events
    )


class StreamLineHubKafkaPublisher:
    """
    Main Kafka publisher orchestrator for StreamLineHub Analytics.
    
    Manages multiple event producers and provides a unified interface
    for publishing various types of events to the Kafka streaming platform.
    """
    
    def __init__(self):
        """Initialize the publisher with all producer instances."""
        self.customer_producer = None
        self.transaction_producer = None  
        self.analytics_producer = None
        self.running = False
        self._setup_signal_handlers()
        
    def _setup_signal_handlers(self):
        """Setup graceful shutdown signal handlers."""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        print(f"\nReceived signal {signum}. Shutting down gracefully...")
        self.running = False
        self.cleanup()
        sys.exit(0)
        
    def initialize_producers(self):
        """Initialize all Kafka producers."""
        print("Initializing Kafka producers...")
        try:
            self.customer_producer = CustomerEventProducer()
            self.transaction_producer = TransactionEventProducer() 
            self.analytics_producer = AnalyticsEventProducer()
            print("✓ All producers initialized successfully")
            return True
        except Exception as e:
            print(f"✗ Failed to initialize producers: {e}")
            return False
            
    def publish_sample_batch(self, num_events: int = 50) -> bool:
        """
        Publish a batch of sample events.
        
        Args:
            num_events: Number of events to generate and publish
            
        Returns:
            bool: Success status
        """
        if not self.customer_producer:
            if not self.initialize_producers():
                return False
                
        print(f"Publishing batch of {num_events} events...")
        start_time = time.time()
        
        try:
            generate_sample_events(num_events)
            duration = time.time() - start_time
            rate = num_events / duration
            print(f"✓ Published {num_events} events in {duration:.2f}s ({rate:.1f} events/sec)")
            return True
        except Exception as e:
            print(f"✗ Failed to publish events: {e}")
            return False
            
    def run_continuous(self, interval: float = 2.0):
        """
        Run continuous event publishing.
        
        Args:
            interval: Time interval between event batches (seconds)
        """
        if not self.initialize_producers():
            return
            
        print(f"Starting continuous publishing (interval: {interval}s)")
        print("Press Ctrl+C to stop...")
        
        self.running = True
        batch_count = 0
        
        while self.running:
            try:
                batch_count += 1
                print(f"\n--- Batch {batch_count} at {datetime.now().strftime('%H:%M:%S')} ---")
                
                if self.publish_sample_batch(25):
                    print("✓ Batch published successfully")
                else:
                    print("✗ Batch publishing failed")
                    
                time.sleep(interval)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error in continuous publishing: {e}")
                time.sleep(interval)
                
        print("\nContinuous publishing stopped.")
        
    def cleanup(self):
        """Cleanup all producers and connections."""
        print("Cleaning up producers...")
        
        if self.customer_producer:
            self.customer_producer.flush_and_close()
            
        if self.transaction_producer:
            self.transaction_producer.flush_and_close()
            
        if self.analytics_producer:
            self.analytics_producer.flush_and_close()
            
        print("✓ Cleanup complete")


def main():
    """Main entry point with command-line interface."""
    parser = argparse.ArgumentParser(
        description="StreamLineHub Analytics Kafka Publisher",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python kafka_publisher.py --setup-only
  python kafka_publisher.py --events 100
  python kafka_publisher.py --continuous --interval 3
  python kafka_publisher.py --events 500 --setup
        """
    )
    
    parser.add_argument(
        '--setup', 
        action='store_true', 
        help='Setup Kafka topics before publishing'
    )
    
    parser.add_argument(
        '--setup-only',
        action='store_true',
        help='Only setup Kafka topics, do not publish events'
    )
    
    parser.add_argument(
        '--events', 
        type=int, 
        default=50,
        help='Number of events to publish (default: 50)'
    )
    
    parser.add_argument(
        '--continuous',
        action='store_true',
        help='Run continuous publishing'
    )
    
    parser.add_argument(
        '--interval',
        type=float,
        default=2.0,
        help='Interval between batches in continuous mode (default: 2.0 seconds)'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("StreamLineHub Analytics - Kafka Event Publisher")
    print("=" * 60)
    
    # Setup Kafka infrastructure if requested
    if args.setup or args.setup_only:
        try:
            setup_kafka_infrastructure()
        except Exception as e:
            print(f"Failed to setup Kafka infrastructure: {e}")
            return 1
            
        if args.setup_only:
            print("Kafka setup complete. Exiting.")
            return 0
    
    # Initialize publisher
    publisher = StreamLineHubKafkaPublisher()
    
    try:
        if args.continuous:
            # Run continuous publishing
            publisher.run_continuous(args.interval)
        else:
            # Publish single batch
            success = publisher.publish_sample_batch(args.events)
            return 0 if success else 1
            
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        return 0
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1
    finally:
        publisher.cleanup()


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
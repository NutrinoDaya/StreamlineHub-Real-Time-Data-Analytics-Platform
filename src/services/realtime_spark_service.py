"""
Real-time Spark Streaming Service for Analytics Dashboard
Optimized for maximum performance and real-time processing.
"""

import json
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from pathlib import Path
import threading
import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery

from src.core.spark_session import get_spark_session, get_kafka_config
from src.services.fast_metrics_service import FastMetricsService

logger = logging.getLogger(__name__)

class RealTimeSparkProcessor:
    """High-performance Spark streaming processor for Analytics dashboard."""
    
    def __init__(self):
        self.spark = get_spark_session("RealTimeAnalytics")
        self.kafka_config = get_kafka_config()
        self.metrics_service = FastMetricsService("/tmp/spark_realtime_metrics.parquet")
        
        # Streaming queries
        self.queries: Dict[str, StreamingQuery] = {}
        self.is_running = False
        
        # Performance optimizations
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        self.spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        self.spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint")
        
        # Kafka topics
        topics = [
            "customer-events",
            "transaction-events", 
            "analytics-events"
        ]
    
    def get_kafka_stream_schema(self) -> StructType:
        """Define optimized schema for Kafka stream."""
        return StructType([
            StructField("timestamp", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("event_data", MapType(StringType(), StringType()), True),
            StructField("value", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("source", StringType(), True)
        ])
    
    def create_kafka_stream(self) -> DataFrame:
        """Create optimized Kafka streaming DataFrame."""
        bootstrap_servers = self.kafka_config.get('bootstrap_servers', 'kafka:9092')
        topics_str = ",".join(self.topics)
        
        # Read from Kafka with optimal settings
        kafka_df = (self.spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topics_str)
            .option("startingOffsets", "latest")
            .option("maxOffsetsPerTrigger", "10000")  # Process up to 10k events per batch
            .option("kafka.consumer.session.timeout.ms", "30000")
            .option("kafka.consumer.request.timeout.ms", "40000")
            .load()
        )
        
        # Parse JSON and apply schema
        parsed_df = (kafka_df
            .select(
                col("timestamp").alias("kafka_timestamp"),
                col("topic"),
                col("partition"),
                col("offset"),
                from_json(col("value").cast("string"), self.get_kafka_stream_schema()).alias("data")
            )
            .select("kafka_timestamp", "topic", "partition", "offset", "data.*")
            .withColumn("processing_time", current_timestamp())
        )
        
        return parsed_df
    
    def start_real_time_aggregations(self) -> None:
        """Start real-time aggregation streams."""
        
        # Main stream
        stream_df = self.create_kafka_stream()
        
        # Real-time event counting (5-second windows)
        event_counts = (stream_df
            .withWatermark("processing_time", "30 seconds")
            .groupBy(
                window(col("processing_time"), "5 seconds"),
                col("event_type")
            )
            .agg(
                count("*").alias("event_count"),
                countDistinct("customer_id").alias("unique_customers"),
                sum("value").alias("total_value"),
                first("processing_time").alias("window_start")
            )
        )
        
        # Write to console for debugging (optional)
        debug_query = (event_counts.writeStream
            .outputMode("update")
            .format("console")
            .option("truncate", False)
            .trigger(processingTime="5 seconds")
            .queryName("debug_aggregations")
            .start()
        )
        
        # Write metrics to fast storage
        metrics_query = (event_counts.writeStream
            .outputMode("update")
            .trigger(processingTime="2 seconds")  # Very fast updates
            .foreachBatch(self._write_metrics_batch)
            .queryName("metrics_writer")
            .start()
        )
        
        # Store queries for management
        self.queries["debug"] = debug_query
        self.queries["metrics"] = metrics_query
        
        self.is_running = True
        logger.info("✅ Real-time Spark processing started")
    
    def _write_metrics_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """Write aggregated metrics to fast storage."""
        try:
            # Collect metrics from batch
            rows = batch_df.collect()
            
            if not rows:
                return
                
            # Aggregate metrics across all event types
            total_events = sum(row.event_count for row in rows)
            total_customers = sum(row.unique_customers for row in rows)
            total_value = sum(row.total_value or 0 for row in rows)
            
            # Calculate events per second (5-second window)
            events_per_second = total_events / 5.0 if total_events > 0 else 0.0
            
            # Prepare metrics
            metrics = {
                'timestamp': datetime.now().isoformat(),
                'batch_id': batch_id,
                'total_events': total_events,
                'events_per_second': events_per_second,
                'unique_customers': total_customers,
                'total_value': total_value,
                'window_count': len(rows),
                'data_source': 'spark_realtime',
                'processing_time': datetime.now().isoformat()
            }
            
            # Write to fast storage
            self.metrics_service.write_metrics(metrics)
            
            logger.debug(f"Batch {batch_id}: {total_events} events, {events_per_second:.1f} eps")
            
        except Exception as e:
            logger.error(f"Error writing metrics batch {batch_id}: {e}")
    
    def get_real_time_metrics(self) -> Optional[Dict[str, Any]]:
        """Get latest real-time metrics."""
        return self.metrics_service.read_metrics()
    
    def stop_processing(self) -> None:
        """Stop all streaming queries."""
        logger.info("🛑 Stopping real-time Spark processing...")
        
        for name, query in self.queries.items():
            try:
                query.stop()
                logger.info(f"Stopped query: {name}")
            except Exception as e:
                logger.error(f"Error stopping query {name}: {e}")
        
        self.queries.clear()
        self.is_running = False
        logger.info("✅ Real-time processing stopped")
    
    def get_status(self) -> Dict[str, Any]:
        """Get processing status."""
        query_status = {}
        for name, query in self.queries.items():
            try:
                query_status[name] = {
                    'is_active': query.isActive,
                    'last_progress': query.lastProgress if hasattr(query, 'lastProgress') else None
                }
            except Exception:
                query_status[name] = {'is_active': False, 'error': 'Status check failed'}
        
        return {
            'is_running': self.is_running,
            'queries': query_status,
            'spark_context_active': not self.spark.sparkContext._jsc.sc().isStopped(),
            'topics': self.topics
        }


# Global instance
realtime_processor = RealTimeSparkProcessor()

def start_realtime_processing():
    """Start the real-time processing service."""
    if not realtime_processor.is_running:
        realtime_processor.start_real_time_aggregations()
    return realtime_processor

def stop_realtime_processing():
    """Stop the real-time processing service."""
    realtime_processor.stop_processing()

def get_realtime_metrics() -> Optional[Dict[str, Any]]:
    """Get latest real-time metrics from Spark processing."""
    return realtime_processor.get_real_time_metrics()

if __name__ == "__main__":
    print("🚀 Testing Real-time Spark Processor")
    print("=" * 50)
    
    processor = start_realtime_processing()
    
    try:
        # Run for 30 seconds
        time.sleep(30)
        
        # Check metrics
        metrics = get_realtime_metrics()
        if metrics:
            print("\n📊 Latest Metrics:")
            for key, value in metrics.items():
                print(f"  {key}: {value}")
        else:
            print("No metrics available yet")
            
        # Check status
        status = processor.get_status()
        print(f"\n🔍 Status: {status}")
        
    finally:
        stop_realtime_processing()
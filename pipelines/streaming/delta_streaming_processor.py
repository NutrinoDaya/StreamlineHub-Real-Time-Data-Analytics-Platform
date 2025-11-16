#!/usr/bin/env python
"""
Delta Lake Streaming Processor

Real-time processor that reads from Kafka and writes to Delta Lake tables
following the Medallion Architecture (Bronze -> Silver -> Gold).

This replaces JSONL file storage with proper Delta Lake tables for efficiency.
"""

import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Add the project root to sys.path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Import centralized Spark session
from src.core.spark_session import init_spark_session

# Event schemas
CUSTOMER_BEHAVIOR_SCHEMA = StructType([
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("session_id", StringType(), True),
    StructField("action", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("category", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("location", StringType(), True),
    StructField("value", DoubleType(), True)
])

TRANSACTION_SCHEMA = StructType([
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("status", StringType(), True),
    StructField("product_ids", ArrayType(IntegerType()), True),
    StructField("discount_applied", DoubleType(), True)
])

ANALYTICS_SCHEMA = StructType([
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("metric_name", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("dimensions", StringType(), True)
])

class DeltaStreamingProcessor:
    def __init__(self):
        self.spark = init_spark_session("DeltaStreamingProcessor")
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Delta Lake paths
        self.bronze_path = "data/delta/bronze"
        self.silver_path = "data/delta/silver"
        self.gold_path = "data/delta/gold"
        
        # Create directories
        for path in [self.bronze_path, self.silver_path, self.gold_path]:
            os.makedirs(path, exist_ok=True)
        
        print(f"🚀 Delta Streaming Processor initialized")
        print(f"📁 Bronze: {self.bronze_path}")
        print(f"📁 Silver: {self.silver_path}")
        print(f"📁 Gold: {self.gold_path}")
    
    def create_kafka_stream(self):
        """Create streaming DataFrame from Kafka."""
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "customer-behavior,transaction-data,analytics-metrics") \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", 1000) \
            .load()
    
    def parse_kafka_messages(self, kafka_df):
        """Parse Kafka messages and add metadata."""
        return kafka_df.select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("value").cast("string").alias("raw_data")
        ).withColumn(
            "processing_timestamp", current_timestamp()
        ).withColumn(
            "date_partition", date_format(col("kafka_timestamp"), "yyyy-MM-dd")
        )
    
    def write_to_bronze(self, df):
        """Write raw data to Bronze layer (Delta Lake)."""
        return df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", f"{self.bronze_path}/_checkpoints/raw_events") \
            .partitionBy("date_partition", "topic") \
            .trigger(processingTime="30 seconds") \
            .start(f"{self.bronze_path}/raw_events")
    
    def create_silver_transformations(self):
        """Create Silver layer transformations from Bronze."""
        
        # Read from Bronze
        bronze_df = self.spark.readStream \
            .format("delta") \
            .load(f"{self.bronze_path}/raw_events")
        
        # Parse JSON based on topic
        customer_events = bronze_df.filter(col("topic") == "customer-behavior") \
            .select(
                "*",
                from_json(col("raw_data"), CUSTOMER_BEHAVIOR_SCHEMA).alias("parsed_data")
            ).select(
                col("kafka_timestamp"),
                col("processing_timestamp"),
                col("date_partition"),
                col("parsed_data.*")
            ).withColumn(
                "event_timestamp", 
                to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
            )
        
        transaction_events = bronze_df.filter(col("topic") == "transaction-data") \
            .select(
                "*",
                from_json(col("raw_data"), TRANSACTION_SCHEMA).alias("parsed_data")
            ).select(
                col("kafka_timestamp"),
                col("processing_timestamp"),
                col("date_partition"),
                col("parsed_data.*")
            ).withColumn(
                "event_timestamp",
                to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
            )
        
        analytics_events = bronze_df.filter(col("topic") == "analytics-metrics") \
            .select(
                "*",
                from_json(col("raw_data"), ANALYTICS_SCHEMA).alias("parsed_data")
            ).select(
                col("kafka_timestamp"),
                col("processing_timestamp"),
                col("date_partition"),
                col("parsed_data.*")
            ).withColumn(
                "event_timestamp",
                to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
            )
        
        return {
            "customer_events": customer_events,
            "transaction_events": transaction_events,
            "analytics_events": analytics_events
        }
    
    def write_silver_tables(self, silver_streams):
        """Write cleaned data to Silver layer."""
        queries = []
        
        for table_name, stream_df in silver_streams.items():
            query = stream_df.writeStream \
                .format("delta") \
                .outputMode("append") \
                .option("checkpointLocation", f"{self.silver_path}/_checkpoints/{table_name}") \
                .partitionBy("date_partition") \
                .trigger(processingTime="60 seconds") \
                .start(f"{self.silver_path}/{table_name}")
            queries.append(query)
        
        return queries
    
    def create_gold_aggregations(self):
        """Create Gold layer aggregations for analytics."""
        
        # Read from Silver
        customer_events = self.spark.readStream \
            .format("delta") \
            .load(f"{self.silver_path}/customer_events")
        
        transaction_events = self.spark.readStream \
            .format("delta") \
            .load(f"{self.silver_path}/transaction_events")
        
        # 5-minute window aggregations for real-time metrics
        customer_metrics = customer_events \
            .withWatermark("event_timestamp", "10 minutes") \
            .groupBy(
                window(col("event_timestamp"), "5 minutes"),
                col("action"),
                col("device_type")
            ).agg(
                count("customer_id").alias("event_count"),
                countDistinct("customer_id").alias("unique_customers"),
                avg("value").alias("avg_value"),
                sum("value").alias("total_value")
            ).select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("action"),
                col("device_type"),
                col("event_count"),
                col("unique_customers"),
                round(col("avg_value"), 2).alias("avg_value"),
                round(col("total_value"), 2).alias("total_value"),
                current_timestamp().alias("created_at")
            )
        
        transaction_metrics = transaction_events \
            .withWatermark("event_timestamp", "10 minutes") \
            .groupBy(
                window(col("event_timestamp"), "5 minutes"),
                col("status"),
                col("payment_method")
            ).agg(
                count("transaction_id").alias("transaction_count"),
                sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount"),
                countDistinct("customer_id").alias("unique_customers")
            ).select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("status"),
                col("payment_method"),
                col("transaction_count"),
                round(col("total_amount"), 2).alias("total_amount"),
                round(col("avg_amount"), 2).alias("avg_amount"),
                col("unique_customers"),
                current_timestamp().alias("created_at")
            )
        
        return {
            "customer_metrics": customer_metrics,
            "transaction_metrics": transaction_metrics
        }
    
    def write_gold_tables(self, gold_streams):
        """Write aggregated data to Gold layer."""
        queries = []
        
        for table_name, stream_df in gold_streams.items():
            query = stream_df.writeStream \
                .format("delta") \
                .outputMode("update") \
                .option("checkpointLocation", f"{self.gold_path}/_checkpoints/{table_name}") \
                .trigger(processingTime="60 seconds") \
                .start(f"{self.gold_path}/{table_name}")
            queries.append(query)
        
        return queries
    
    def create_dashboard_metrics(self):
        """Create real-time metrics for dashboard."""
        
        # Read from Bronze for real-time event counting
        bronze_df = self.spark.readStream \
            .format("delta") \
            .load(f"{self.bronze_path}/raw_events")
        
        # Real-time event rate calculation
        event_rates = bronze_df \
            .withWatermark("processing_timestamp", "30 seconds") \
            .groupBy(
                window(col("processing_timestamp"), "30 seconds"),
                col("topic")
            ).agg(
                count("*").alias("event_count")
            ).select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("topic"),
                col("event_count"),
                (col("event_count") / 30).alias("events_per_second"),
                current_timestamp().alias("created_at")
            )
        
        return event_rates
    
    def write_dashboard_metrics(self, metrics_df):
        """Write dashboard metrics to shared location."""
        return metrics_df.writeStream \
            .format("delta") \
            .outputMode("update") \
            .option("checkpointLocation", f"{self.gold_path}/_checkpoints/dashboard_metrics") \
            .trigger(processingTime="10 seconds") \
            .start(f"{self.gold_path}/dashboard_metrics")
    
    def start_streaming(self):
        """Start all streaming processes."""
        print("🚀 Starting Delta Lake streaming pipeline...")
        
        # 1. Bronze layer - raw data ingestion
        kafka_stream = self.create_kafka_stream()
        parsed_stream = self.parse_kafka_messages(kafka_stream)
        bronze_query = self.write_to_bronze(parsed_stream)
        
        print("✅ Bronze layer started (raw data ingestion)")
        
        # Wait a bit for Bronze to start writing
        import time
        time.sleep(10)
        
        # 2. Silver layer - cleaned data
        silver_streams = self.create_silver_transformations()
        silver_queries = self.write_silver_tables(silver_streams)
        
        print("✅ Silver layer started (data cleaning)")
        
        # Wait a bit for Silver to start
        time.sleep(10)
        
        # 3. Gold layer - aggregations
        gold_streams = self.create_gold_aggregations()
        gold_queries = self.write_gold_tables(gold_streams)
        
        print("✅ Gold layer started (aggregations)")
        
        # 4. Dashboard metrics
        dashboard_metrics = self.create_dashboard_metrics()
        dashboard_query = self.write_dashboard_metrics(dashboard_metrics)
        
        print("✅ Dashboard metrics started")
        
        # Collect all queries
        all_queries = [bronze_query] + silver_queries + gold_queries + [dashboard_query]
        
        try:
            print("\n🔄 Streaming pipeline is running...")
            print("📊 Processing events in real-time to Delta Lake tables")
            print("⏹️  Press Ctrl+C to stop\n")
            
            # Wait for all queries
            for query in all_queries:
                query.awaitTermination()
                
        except KeyboardInterrupt:
            print("\n⏹️ Stopping streaming pipeline...")
            for query in all_queries:
                query.stop()
            print("✅ Streaming pipeline stopped")

def main():
    processor = DeltaStreamingProcessor()
    processor.start_streaming()

if __name__ == "__main__":
    main()
#!/usr/bin/env python
"""
Real-time Data Processor

Production-grade Spark Structured Streaming processor for real-time data ingestion.
Implements Medallion Architecture: Bronze (raw) -> Silver (cleansed) -> Gold (aggregated).
Processes customer events, transactions, and analytics from Kafka to Delta Lake tables.

Author: StreamLineHub Analytics Team
Version: 2.0.0
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import os
from datetime import datetime

# Define schemas matching the Kafka event generator
customer_behavior_schema = StructType([
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

transaction_schema = StructType([
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

analytics_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("metric_name", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("dimensions", MapType(StringType(), StringType()), True)
])

# Delta Lake paths
DELTA_BASE_PATH = "/opt/StreamLineHub/data/delta"
BRONZE_PATH = f"{DELTA_BASE_PATH}/bronze"
SILVER_PATH = f"{DELTA_BASE_PATH}/silver"
GOLD_PATH = f"{DELTA_BASE_PATH}/gold"

class RealtimeDataProcessor:
    """Enhanced Spark streaming processor with Delta Lake medallion architecture."""
    
    def __init__(self):
        """Initialize Spark session with Delta Lake support."""
        self.spark = SparkSession.builder \
            .appName("StreamLineHub-Streaming-Enhanced") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        print("✅ Spark session initialized with Delta Lake support")

    def create_delta_tables_if_not_exists(self):
        """Create Delta Lake tables if they don't exist."""
        try:
            # Bronze tables (raw data)
            bronze_tables = [
                ("customer_behavior", customer_behavior_schema),
                ("transactions", transaction_schema),
                ("analytics_metrics", analytics_schema)
            ]
            
            for table_name, schema in bronze_tables:
                table_path = f"{BRONZE_PATH}/{table_name}"
                if not self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                    self.spark._jsc.hadoopConfiguration()
                ).exists(self.spark._jvm.org.apache.hadoop.fs.Path(table_path)):
                    # Create empty Delta table
                    empty_df = self.spark.createDataFrame([], schema)
                    empty_df.write.format("delta").mode("overwrite").save(table_path)
                    print(f"📊 Created Bronze table: {table_name} at {table_path}")
                else:
                    print(f"✅ Bronze table exists: {table_name}")
            
            print("🎯 All Delta Lake tables initialized")
        except Exception as e:
            print(f"⚠️ Error creating tables: {e}")

    def process_customer_behavior_to_bronze(self):
        """Stream customer behavior data to Bronze layer."""
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "customer-behavior") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON data
        parsed_df = df.select(
            from_json(col("value").cast("string"), customer_behavior_schema).alias("data"),
            col("offset"),
            col("partition"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "offset", "partition", "kafka_timestamp") \
         .withColumn("ingestion_timestamp", current_timestamp()) \
         .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        
        # Write to Bronze Delta table
        query = parsed_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("path", f"{BRONZE_PATH}/customer_behavior") \
            .option("checkpointLocation", "/tmp/spark-checkpoints/bronze-customer-behavior") \
            .trigger(processingTime="10 seconds") \
            .start()
        
        print("📋 Started customer behavior Bronze streaming")
        return query

    def process_transactions_to_bronze(self):
        """Stream transaction data to Bronze layer."""
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "transaction-data") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON data
        parsed_df = df.select(
            from_json(col("value").cast("string"), transaction_schema).alias("data"),
            col("offset"),
            col("partition"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "offset", "partition", "kafka_timestamp") \
         .withColumn("ingestion_timestamp", current_timestamp()) \
         .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        
        # Write to Bronze Delta table
        query = parsed_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("path", f"{BRONZE_PATH}/transactions") \
            .option("checkpointLocation", "/tmp/spark-checkpoints/bronze-transactions") \
            .trigger(processingTime="10 seconds") \
            .start()
        
        print("💳 Started transaction Bronze streaming")
        return query

    def process_analytics_to_bronze(self):
        """Stream analytics data to Bronze layer."""
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "analytics-metrics") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON data
        parsed_df = df.select(
            from_json(col("value").cast("string"), analytics_schema).alias("data"),
            col("offset"),
            col("partition"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "offset", "partition", "kafka_timestamp") \
         .withColumn("ingestion_timestamp", current_timestamp()) \
         .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        
        # Write to Bronze Delta table
        query = parsed_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("path", f"{BRONZE_PATH}/analytics_metrics") \
            .option("checkpointLocation", "/tmp/spark-checkpoints/bronze-analytics") \
            .trigger(processingTime="10 seconds") \
            .start()
        
        print("📈 Started analytics Bronze streaming")
        return query

    def create_silver_aggregations(self):
        """Create Silver layer aggregations from Bronze data."""
        # Read from Bronze tables and create real-time aggregations
        customer_behavior_df = self.spark \
            .readStream \
            .format("delta") \
            .load(f"{BRONZE_PATH}/customer_behavior")
        
        # Real-time event counts and metrics
        real_time_metrics = customer_behavior_df \
            .withWatermark("event_timestamp", "30 seconds") \
            .groupBy(
                window(col("event_timestamp"), "30 seconds"),
                col("action"),
                col("category")
            ).agg(
                count("*").alias("event_count"),
                countDistinct("customer_id").alias("unique_customers"),
                sum(coalesce(col("value"), lit(0))).alias("total_value"),
                avg(coalesce(col("value"), lit(0))).alias("avg_value")
            ) \
            .withColumn("metric_timestamp", current_timestamp()) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .drop("window")
        
        # Write Silver aggregations
        query = real_time_metrics.writeStream \
            .format("delta") \
            .outputMode("update") \
            .option("path", f"{SILVER_PATH}/real_time_metrics") \
            .option("checkpointLocation", "/tmp/spark-checkpoints/silver-metrics") \
            .trigger(processingTime="15 seconds") \
            .start()
        
        print("🔄 Started Silver layer real-time aggregations")
        return query

    def create_gold_dashboard_metrics(self):
        """Create Gold layer metrics for dashboard consumption."""
        # Read Silver metrics
        silver_metrics_df = self.spark \
            .readStream \
            .format("delta") \
            .load(f"{SILVER_PATH}/real_time_metrics")
        
        # Create dashboard-ready metrics
        dashboard_metrics = silver_metrics_df \
            .withWatermark("metric_timestamp", "1 minute") \
            .groupBy(window(col("metric_timestamp"), "1 minute")) \
            .agg(
                sum("event_count").alias("total_events_per_minute"),
                sum("unique_customers").alias("total_unique_customers"),
                sum("total_value").alias("total_revenue"),
                avg("avg_value").alias("avg_event_value"),
                count("*").alias("metric_records")
            ) \
            .withColumn("events_per_second", col("total_events_per_minute") / 60) \
            .withColumn("dashboard_timestamp", current_timestamp()) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .drop("window")
        
        # Write Gold metrics for dashboard
        query = dashboard_metrics.writeStream \
            .format("delta") \
            .outputMode("update") \
            .option("path", f"{GOLD_PATH}/dashboard_metrics") \
            .option("checkpointLocation", "/tmp/spark-checkpoints/gold-dashboard") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        print("📊 Started Gold layer dashboard metrics")
        return query

    def start_streaming(self):
        """Start all streaming queries."""
        print("🚀 Starting Enhanced Spark Streaming Pipeline...")
        print("📋 Implementing Medallion Architecture: Bronze -> Silver -> Gold")
        
        # Initialize Delta tables
        self.create_delta_tables_if_not_exists()
        
        # Start Bronze layer streaming (raw data ingestion)
        bronze_queries = []
        bronze_queries.append(self.process_customer_behavior_to_bronze())
        bronze_queries.append(self.process_transactions_to_bronze())
        bronze_queries.append(self.process_analytics_to_bronze())
        
        # Start Silver layer processing (real-time aggregations)
        silver_query = self.create_silver_aggregations()
        
        # Start Gold layer processing (dashboard metrics)
        gold_query = self.create_gold_dashboard_metrics()
        
        print("✅ All streaming pipelines started!")
        print("📊 Processing Kafka data -> Bronze -> Silver -> Gold")
        print("🎯 Data will be available for dashboard consumption")
        
        # Monitor streaming
        all_queries = bronze_queries + [silver_query, gold_query]
        
        try:
            print("⏳ Streaming active... Press Ctrl+C to stop")
            for query in all_queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            print("\n⏹️ Stopping all streaming jobs...")
            for query in all_queries:
                if query.isActive:
                    query.stop()
            print("✅ All streaming jobs stopped")

def main():
    """Main function to run the enhanced Spark streaming processor."""
    print("🎯 StreamLineHub Analytics - Enhanced Streaming Pipeline")
    print("=" * 60)
    
    processor = RealtimeDataProcessor()
    processor.start_streaming()

if __name__ == "__main__":
    main()
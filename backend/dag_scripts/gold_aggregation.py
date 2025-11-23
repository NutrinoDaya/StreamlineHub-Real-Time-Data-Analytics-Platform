#!/usr/bin/env python3
"""
Gold Layer Aggregation Spark Script
Reads Bronze/Silver Delta tables with time-based filtering and creates Gold layer aggregations
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pyspark.sql.functions import (
    col, lit, current_timestamp, count, countDistinct, avg, sum as spark_sum, 
    min as spark_min, max as spark_max, date_trunc, when, coalesce, to_timestamp
)

# Add src to path for imports
sys.path.insert(0, '/opt/airflow/src')
from src.core.spark_session import get_spark_session

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GoldLayerAggregator:
    """Gold layer aggregation processor with time-based filtering"""
    
    def __init__(self, start_threshold_ms: int, end_threshold_ms: int):
        self.start_threshold_ms = start_threshold_ms
        self.end_threshold_ms = end_threshold_ms
        self.spark = None
        
        # Define paths
        # Use writable paths within the Airflow container
        self.bronze_path = "/opt/airflow/data/bronze"
        self.silver_path = "/opt/airflow/data/silver" 
        self.gold_path = "/opt/airflow/data/gold"
        
        # Windows paths for local testing
        if sys.platform == "win32":
            self.bronze_path = r"D:\StreamlineHubC\data\bronze_delta"
            self.silver_path = r"D:\StreamlineHubC\data\silver_delta"
            self.gold_path = r"D:\StreamlineHubC\data\gold_delta"
        
        self.stats = {
            "records_processed": 0,
            "gold_records_created": 0,
            "tables_processed": 0
        }
    
    def initialize_spark(self) -> bool:
        """Initialize Spark session with Delta Lake support"""
        try:
            logger.info("[PROCESSING]  Initializing Spark session with Delta Lake...")
            
            self.spark = get_spark_session("GoldLayerAggregation")
            
            logger.info("[SUCCESS]  Spark session initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"[ERROR]  Failed to initialize Spark session: {e}")
            return False
    
    def read_silver_table_with_filter(self, table_name: str):
        """Read Silver Delta table with date-based partition filtering and time range filtering"""
        try:
            table_path = f"{self.silver_path}/{table_name}_silver"
            delta_log_path = f"{table_path}/_delta_log"
            
            # Check if Delta table exists using Spark's filesystem access
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            fs = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
            
            table_path_hadoop = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(table_path)
            delta_log_path_hadoop = self.spark.sparkContext._jvm.org.apache.hadoop.fs.Path(delta_log_path)
            
            if not fs.exists(table_path_hadoop) or not fs.exists(delta_log_path_hadoop):
                logger.warning(f"[WARNING]  Silver table not found or not initialized: {table_path}")
                return None
            
            logger.info(f"[LOADING]  Reading Silver table: {table_name}")
            
            # Convert millisecond thresholds to timestamps
            start_timestamp = datetime.fromtimestamp(self.start_threshold_ms / 1000.0, tz=timezone.utc)
            end_timestamp = datetime.fromtimestamp(self.end_threshold_ms / 1000.0, tz=timezone.utc)
            
            # Get date range for partition pruning (date-only)
            start_date = start_timestamp.date()
            end_date = end_timestamp.date()
            
            logger.info(f"[FILTERING]  Filtering data between {start_timestamp} and {end_timestamp}")
            logger.info(f"[SCHEDULING]  Using date partitions from {start_date} to {end_date}")
            
            # Read table with proper error handling and schema merging
            try:
                df = (self.spark.read
                      .format("delta")
                      .option("mergeSchema", "true")
                      .option("ignoreCorruptFiles", "true")
                      .option("ignoreChanges", "true")
                      .load(table_path))
            except Exception as read_error:
                logger.error(f"[ERROR]  Failed to read Delta table at {table_path}: {read_error}")
                return None
            
            # Apply partition pruning on processing_date (Silver layer partition), then precise time filtering on insertionTime
            filtered_df = df.filter(
                (col("processing_date") >= lit(start_date)) &
                (col("processing_date") <= lit(end_date)) &
                (col("insertionTime") >= lit(start_timestamp)) &
                (col("insertionTime") < lit(end_timestamp)) &
                (col("data_quality_status") == "validated")
            )
            
            # Try to count with error handling
            try:
                record_count = filtered_df.count()
                self.stats["records_processed"] += record_count
                logger.info(f"[SUCCESS]  Filtered {table_name}: {record_count} records in time window")
            except Exception as count_error:
                logger.error(f"[ERROR]  Failed to count records for {table_name}: {count_error}")
                return None
            
            return filtered_df
            
        except Exception as e:
            logger.error(f"[ERROR]  Failed to read Silver table {table_name}: {e}")
            return None
    
    def create_customer_behavior_aggregations(self, df):
        """Create customer behavior aggregations"""
        try:
            logger.info("[ANALYZING]  Creating customer behavior aggregations...")
            
            if df is None or df.rdd.isEmpty():
                logger.warning("[WARNING]  No customer behavior data to aggregate")
                return 0
            
            # Hourly aggregations
            hourly_agg = df.withColumn("hour", date_trunc("hour", col("insertionTime"))) \
                .groupBy("hour") \
                .agg(
                    count("*").alias("total_events"),
                    countDistinct("user_id").alias("unique_users"),
                    countDistinct("session_id").alias("unique_sessions"),
                    count(when(col("action") == "page_view", 1)).alias("page_views"),
                    count(when(col("action") == "click", 1)).alias("clicks"),
                    count(when(col("action") == "add_to_cart", 1)).alias("add_to_carts"),
                    count(when(col("action") == "purchase", 1)).alias("purchases")
                ) \
                .withColumn("aggregation_type", lit("hourly")) \
                .withColumn("event_category", lit("customer_behavior")) \
                .withColumn("created_at", current_timestamp())
            
            # Daily aggregations
            daily_agg = df.withColumn("day", date_trunc("day", col("insertionTime"))) \
                .groupBy("day") \
                .agg(
                    count("*").alias("total_events"),
                    countDistinct("user_id").alias("unique_users"),
                    countDistinct("session_id").alias("unique_sessions"),
                    count(when(col("action") == "page_view", 1)).alias("page_views"),
                    count(when(col("action") == "click", 1)).alias("clicks"),
                    count(when(col("action") == "add_to_cart", 1)).alias("add_to_carts"),
                    count(when(col("action") == "purchase", 1)).alias("purchases")
                ) \
                .withColumn("aggregation_type", lit("daily")) \
                .withColumn("event_category", lit("customer_behavior")) \
                .withColumn("created_at", current_timestamp())
            
            # Union and write to Gold layer
            combined_agg = hourly_agg.union(daily_agg)
            gold_table_path = f"{self.gold_path}/customer_behavior_gold"
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(gold_table_path), exist_ok=True)
            
            combined_agg.write.format("delta") \
                .mode("append") \
                .partitionBy("aggregation_type", "event_category") \
                .option("mergeSchema", "true") \
                .save(gold_table_path)
            
            record_count = combined_agg.count()
            self.stats["gold_records_created"] += record_count
            self.stats["tables_processed"] += 1
            
            logger.info(f"[SUCCESS]  Customer behavior Gold aggregations: {record_count} records")
            return record_count
            
        except Exception as e:
            logger.error(f"[ERROR]  Failed to create customer behavior aggregations: {e}")
            raise e
    
    def create_transaction_aggregations(self, df):
        """Create transaction aggregations"""
        try:
            logger.info("[PROCESSING]  Creating transaction aggregations...")
            
            if df is None or df.rdd.isEmpty():
                logger.warning("[WARNING]  No transaction data to aggregate")
                return 0
            
            # Hourly aggregations
            hourly_agg = df.withColumn("hour", date_trunc("hour", col("insertionTime"))) \
                .groupBy("hour") \
                .agg(
                    count("*").alias("total_transactions"),
                    countDistinct("user_id").alias("unique_customers"),
                    spark_sum("amount").alias("total_revenue"),
                    avg("amount").alias("avg_transaction_amount"),
                    spark_min("amount").alias("min_transaction_amount"),
                    spark_max("amount").alias("max_transaction_amount"),
                    count(when(col("payment_method") == "credit_card", 1)).alias("credit_card_payments"),
                    count(when(col("payment_method") == "paypal", 1)).alias("paypal_payments")
                ) \
                .withColumn("aggregation_type", lit("hourly")) \
                .withColumn("event_category", lit("transaction_completed")) \
                .withColumn("created_at", current_timestamp())
            
            # Daily aggregations
            daily_agg = df.withColumn("day", date_trunc("day", col("insertionTime"))) \
                .groupBy("day") \
                .agg(
                    count("*").alias("total_transactions"),
                    countDistinct("user_id").alias("unique_customers"),
                    spark_sum("amount").alias("total_revenue"),
                    avg("amount").alias("avg_transaction_amount"),
                    spark_min("amount").alias("min_transaction_amount"),
                    spark_max("amount").alias("max_transaction_amount"),
                    count(when(col("payment_method") == "credit_card", 1)).alias("credit_card_payments"),
                    count(when(col("payment_method") == "paypal", 1)).alias("paypal_payments")
                ) \
                .withColumn("aggregation_type", lit("daily")) \
                .withColumn("event_category", lit("transaction_completed")) \
                .withColumn("created_at", current_timestamp())
            
            # Union and write to Gold layer
            combined_agg = hourly_agg.union(daily_agg)
            gold_table_path = f"{self.gold_path}/transaction_gold"
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(gold_table_path), exist_ok=True)
            
            combined_agg.write.format("delta") \
                .mode("append") \
                .partitionBy("aggregation_type", "event_category") \
                .option("mergeSchema", "true") \
                .save(gold_table_path)
            
            record_count = combined_agg.count()
            self.stats["gold_records_created"] += record_count
            self.stats["tables_processed"] += 1
            
            logger.info(f"[SUCCESS]  Transaction Gold aggregations: {record_count} records")
            return record_count
            
        except Exception as e:
            logger.error(f"[ERROR]  Failed to create transaction aggregations: {e}")
            raise e
    
    def create_system_metrics_aggregations(self, df):
        """Create system metrics aggregations"""
        try:
            logger.info("[PROCESSING]  Creating system metrics aggregations...")
            
            if df is None or df.rdd.isEmpty():
                logger.warning("[WARNING]  No system metrics data to aggregate")
                return 0
            
            # Hourly aggregations by metric type
            hourly_agg = df.withColumn("hour", date_trunc("hour", col("insertionTime"))) \
                .groupBy("hour", "metric_name", "service_name") \
                .agg(
                    count("*").alias("total_measurements"),
                    avg("value").alias("avg_metric_value"),
                    spark_min("value").alias("min_metric_value"),
                    spark_max("value").alias("max_metric_value")
                ) \
                .withColumn("aggregation_type", lit("hourly")) \
                .withColumn("event_category", lit("system_metric")) \
                .withColumn("created_at", current_timestamp())
            
            # Daily aggregations
            daily_agg = df.withColumn("day", date_trunc("day", col("insertionTime"))) \
                .groupBy("day", "metric_name", "service_name") \
                .agg(
                    count("*").alias("total_measurements"),
                    avg("value").alias("avg_metric_value"),
                    spark_min("value").alias("min_metric_value"),
                    spark_max("value").alias("max_metric_value")
                ) \
                .withColumn("aggregation_type", lit("daily")) \
                .withColumn("event_category", lit("system_metric")) \
                .withColumn("created_at", current_timestamp())
            
            # Union and write to Gold layer
            combined_agg = hourly_agg.union(daily_agg)
            gold_table_path = f"{self.gold_path}/system_metrics_gold"
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(gold_table_path), exist_ok=True)
            
            combined_agg.write.format("delta") \
                .mode("append") \
                .partitionBy("aggregation_type", "event_category") \
                .option("mergeSchema", "true") \
                .save(gold_table_path)
            
            record_count = combined_agg.count()
            self.stats["gold_records_created"] += record_count
            self.stats["tables_processed"] += 1
            
            logger.info(f"[SUCCESS]  System metrics Gold aggregations: {record_count} records")
            return record_count
            
        except Exception as e:
            logger.error(f"[ERROR]  Failed to create system metrics aggregations: {e}")
            raise e
    
    def run_aggregation(self):
        """Run the complete Gold layer aggregation process"""
        try:
            logger.info("\n" + "="*70)
            logger.info("[STARTING]  STARTING GOLD LAYER AGGREGATION")
            logger.info("="*70)
            
            start_dt = datetime.fromtimestamp(self.start_threshold_ms / 1000.0, tz=timezone.utc)
            end_dt = datetime.fromtimestamp(self.end_threshold_ms / 1000.0, tz=timezone.utc)
            
            logger.info(f"[FILTERING]  Time window: {start_dt.isoformat()} to {end_dt.isoformat()}")
            
            # Initialize Spark
            if not self.initialize_spark():
                raise Exception("Failed to initialize Spark session")
            
            # Create Gold directory
            os.makedirs(self.gold_path, exist_ok=True)
            
            # Process each Silver table
            total_gold_records = 0
            
            # Customer behavior aggregations
            customer_df = self.read_silver_table_with_filter("customer_behavior")
            total_gold_records += self.create_customer_behavior_aggregations(customer_df)
            
            # Transaction aggregations
            transaction_df = self.read_silver_table_with_filter("transaction_completed")
            total_gold_records += self.create_transaction_aggregations(transaction_df)
            
            # System metrics aggregations
            metrics_df = self.read_silver_table_with_filter("system_metric")
            total_gold_records += self.create_system_metrics_aggregations(metrics_df)
            
            # Final statistics
            logger.info("\n" + "="*70)
            logger.info("[ANALYZING]  AGGREGATION COMPLETED")
            logger.info("="*70)
            logger.info(f"📥 Records Processed: {self.stats['records_processed']}")
            logger.info(f"✨ Gold Records Created: {self.stats['gold_records_created']}")
            logger.info(f"📊 Tables Processed: {self.stats['tables_processed']}")
            logger.info("="*70 + "\n")
            
            return {
                "status": "success",
                "records_processed": self.stats["records_processed"],
                "gold_records_created": self.stats["gold_records_created"],
                "tables_processed": self.stats["tables_processed"],
                "start_threshold": self.start_threshold_ms,
                "end_threshold": self.end_threshold_ms
            }
            
        except Exception as e:
            logger.error(f"[ERROR]  Gold layer aggregation failed: {e}", exc_info=True)
            raise
        finally:
            if self.spark:
                self.spark.stop()

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="Gold Layer Aggregation")
    parser.add_argument("--start-threshold", type=int, required=True,
                    help="Start threshold in milliseconds (UTC)")
    parser.add_argument("--end-threshold", type=int, required=True,
                    help="End threshold in milliseconds (UTC)")
    
    args = parser.parse_args()
    
    aggregator = GoldLayerAggregator(args.start_threshold, args.end_threshold)
    result = aggregator.run_aggregation()
    
    logger.info(f"[SUCCESS]  Aggregation completed successfully: {result}")

if __name__ == "__main__":
    main()
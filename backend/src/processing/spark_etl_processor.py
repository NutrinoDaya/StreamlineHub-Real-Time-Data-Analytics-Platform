#!/usr/bin/env python3
"""
Spark ETL Processor
Handles the complete ETL pipeline: Bronze → Silver → Gold using Spark and Delta Lake
"""

import os
import sys
import logging
import logging.handlers
import time
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta, timezone
import json

# Add project root to path
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(ROOT_DIR))

# Setup file-based logging for ETL processing
log_dir = ROOT_DIR / "logs"
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler(
            log_dir / "etl_processing.log",
            maxBytes=20*1024*1024,  # 20MB for Spark processing
            backupCount=10
        ),
        logging.StreamHandler()  # Also log to console
    ]
)
logger = logging.getLogger(__name__)

try:
    from pyspark.sql import SparkSession, DataFrame
    from src.core.spark_session import get_spark_session
    from pyspark.sql.functions import (
        col, lit, current_timestamp, to_timestamp, 
        when, isnan, isnull, regexp_replace, trim,
        count, sum as spark_sum, avg, max as spark_max, min as spark_min,
        date_format, hour, dayofweek, month, year
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, 
        LongType, TimestampType, BooleanType, IntegerType
    )
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    logger.warning("PySpark not available - using file-based fallback")

class SparkETLProcessor:
    """
    Complete Spark ETL processor for Bronze → Silver → Gold pipeline
    Uses Delta Lake for all layers with proper schema evolution and data quality
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.spark: Optional[SparkSession] = None
        self.is_initialized = False
        
        # Storage paths - prioritize config, then env vars, then defaults
        self.bronze_path = config.get("bronze_layer_path", os.getenv("BRONZE_PATH", "/opt/StreamlineHub/data/bronze"))
        self.silver_path = config.get("silver_layer_path", os.getenv("SILVER_PATH", "/opt/StreamlineHub/data/silver")) 
        self.gold_path = config.get("gold_layer_path", os.getenv("GOLD_PATH", "/opt/StreamlineHub/data/gold"))
        
        # Processing stats
        self.stats = {
            "bronze_records_processed": 0,
            "silver_records_created": 0,
            "gold_tables_created": 0,
            "last_processing_time": None,
            "processing_errors": 0
        }
    
    async def initialize(self) -> bool:
        """Initialize Spark session with Delta Lake support"""
        try:
            if not SPARK_AVAILABLE:
                logger.warning("Spark not available - ETL processor disabled")
                return False
            
            logger.info("[STARTING]  Initializing Spark ETL Processor...")
            
            # Use centralized Spark session
            self.spark = get_spark_session("StreamlineHub-ETL-Processor")
            
            # Set log level to reduce noise
            self.spark.sparkContext.setLogLevel("WARN")
            
            # Create layer directories
            for path in [self.bronze_path, self.silver_path, self.gold_path]:
                Path(path).mkdir(parents=True, exist_ok=True)
            
            self.is_initialized = True
            logger.info("[SUCCESS]  Spark ETL Processor initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"[ERROR]  Failed to initialize Spark ETL Processor: {e}")
            return False
    
    async def process_kafka_events_to_bronze(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process Kafka events into Bronze Delta tables partitioned by insertionTime date"""
        try:
            if not self.is_initialized or not events:
                return {"status": "failed", "error": "Not initialized or no events"}
            
            logger.info(f"[LOADING]  Processing {len(events)} Kafka events to Bronze layer")
            
            # Create DataFrame from events
            df = self.spark.createDataFrame(events)
            
            # Add bronze layer metadata with insertionTime-based partitioning
            current_time_ms = int(time.time() * 1000)
            current_date = datetime.now(timezone.utc)
            partition_date = current_date.strftime("%m-%d-%Y")  # MM-dd-yyyy format
            
            bronze_df = df.withColumn("bronze_ingestion_time", current_timestamp()) \
                        .withColumn("insertionTime", lit(current_time_ms)) \
                        .withColumn("bronze_batch_id", lit(f"batch_{int(datetime.now().timestamp())}")) \
                        .withColumn("data_source", lit("kafka")) \
                        .withColumn("partition_date", lit(partition_date))
            
            # Get event type from first event or use default
            event_type = events[0].get("event_type", "unknown_event") if events else "unknown_event"
            
            # Define partitioned Bronze table path: bronze/event_type/MM-dd-yyyy/
            base_path = f"{self.bronze_path}/{event_type}"
            partitioned_path = f"{base_path}/{partition_date}"
            
            logger.info(f"[DATA]  Writing Kafka events to partitioned path: {partitioned_path}")
            
            # Ensure the partition directory exists
            from pathlib import Path
            Path(partitioned_path).mkdir(parents=True, exist_ok=True)
            
            # Write to Bronze Delta table with date-based partitioning
            bronze_df.write \
                .format("delta") \
                .mode("append") \
                .partitionBy("partition_date") \
                .option("mergeSchema", "true") \
                .option("compression", "snappy") \
                .save(base_path)
            
            self.stats["bronze_records_processed"] += len(events)
            self.stats["last_processing_time"] = datetime.now().isoformat()
            
            logger.info(f"[SUCCESS]  Successfully wrote {len(events)} events to Bronze Delta table")
            
            return {
                "status": "success",
                "events_processed": len(events),
                "table_path": base_path,
                "processing_time": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"[ERROR]  Failed to process events to Bronze: {e}")
            self.stats["processing_errors"] += 1
            return {"status": "failed", "error": str(e)}
    
    async def process_bronze_to_silver(self, date_filter: Optional[str] = None) -> Dict[str, Any]:
        """Process Bronze data to Silver layer with data cleaning and validation"""
        try:
            if not self.is_initialized:
                return {"status": "failed", "error": "Not initialized"}
            
            if not date_filter:
                date_filter = datetime.now().strftime("%Y-%m-%d")
            
            logger.info(f"[PROCESSING]  Processing Bronze to Silver for date: {date_filter}")
            
            # Read Bronze data
            bronze_table_path = f"{self.bronze_path}/events_delta"
            
            if not Path(bronze_table_path).exists():
                logger.warning("No Bronze data found")
                return {"status": "success", "records_processed": 0}
            
            bronze_df = self.spark.read.format("delta").load(bronze_table_path)
            
            # Filter by date
            filtered_df = bronze_df.filter(col("partition_date") == date_filter)
            
            if filtered_df.count() == 0:
                logger.info(f"No Bronze data found for date {date_filter}")
                return {"status": "success", "records_processed": 0}
            
            # Process each event type separately
            processing_results = {}
            
            for event_type in ["customer_behavior", "transaction_completed", "system_metric"]:
                event_df = filtered_df.filter(col("event_type") == event_type)
                
                if event_df.count() > 0:
                    cleaned_df = await self._clean_event_data(event_df, event_type)
                    
                    if cleaned_df.count() > 0:
                        # Write to Silver Delta table
                        silver_table_path = f"{self.silver_path}/{event_type}_clean_delta"
                        
                        cleaned_df.write \
                            .format("delta") \
                            .mode("append") \
                            .partitionBy("partition_date") \
                            .option("mergeSchema", "true") \
                            .save(silver_table_path)
                        
                        processing_results[event_type] = {
                            "records_processed": cleaned_df.count(),
                            "table_path": silver_table_path
                        }
                        
                        self.stats["silver_records_created"] += cleaned_df.count()
            
            logger.info(f"[SUCCESS]  Bronze to Silver processing completed: {processing_results}")
            
            return {
                "status": "success",
                "processing_results": processing_results,
                "date_filter": date_filter
            }
            
        except Exception as e:
            logger.error(f"[ERROR]  Bronze to Silver processing failed: {e}")
            self.stats["processing_errors"] += 1
            return {"status": "failed", "error": str(e)}
    
    async def _clean_event_data(self, df: DataFrame, event_type: str) -> DataFrame:
        """Clean and validate event data for Silver layer"""
        try:
            # Add Silver layer metadata
            base_df = df.withColumn("silver_processed_time", current_timestamp()) \
            .withColumn("data_quality_score", lit(1.0)) \
            .withColumn("is_valid", lit(True))
            
            if event_type == "customer_behavior":
                return self._clean_customer_behavior(base_df)
            elif event_type == "transaction_completed":
                return self._clean_transaction_data(base_df)
            elif event_type == "system_metric":
                return self._clean_system_metrics(base_df)
            else:
                return base_df
                
        except Exception as e:
            logger.error(f"Failed to clean {event_type} data: {e}")
            return df
    
    def _clean_customer_behavior(self, df: DataFrame) -> DataFrame:
        """Clean customer behavior data"""
        return df.withColumn("user_id", regexp_replace(col("user_id"), "[^a-zA-Z0-9-]", "")) \
                .withColumn("session_id", regexp_replace(col("session_id"), "[^a-zA-Z0-9-]", "")) \
                .withColumn("action", trim(col("action"))) \
                .filter(col("user_id").isNotNull() & (col("user_id") != "")) \
                .filter(col("action").isNotNull() & (col("action") != ""))
    
    def _clean_transaction_data(self, df: DataFrame) -> DataFrame:
        """Clean transaction data"""
        return df.withColumn("transaction_id", regexp_replace(col("transaction_id"), "[^a-zA-Z0-9-]", "")) \
                .withColumn("amount", col("amount").cast(DoubleType())) \
                .withColumn("currency", trim(col("currency"))) \
                .filter(col("transaction_id").isNotNull() & (col("transaction_id") != "")) \
                .filter(col("amount").isNotNull() & (col("amount") > 0))
    
    def _clean_system_metrics(self, df: DataFrame) -> DataFrame:
        """Clean system metrics data"""
        return df.withColumn("metric_name", trim(col("metric_name"))) \
                .withColumn("metric_value", col("metric_value").cast(DoubleType())) \
                .filter(col("metric_name").isNotNull() & (col("metric_name") != "")) \
                .filter(col("metric_value").isNotNull())
    
    async def process_silver_to_gold(self, date_filter: Optional[str] = None) -> Dict[str, Any]:
        """Process Silver data to Gold layer with analytics and aggregations"""
        try:
            if not self.is_initialized:
                return {"status": "failed", "error": "Not initialized"}
            
            if not date_filter:
                date_filter = datetime.now().strftime("%Y-%m-%d")
            
            logger.info(f"🏆 Processing Silver to Gold for date: {date_filter}")
            
            # Create analytics tables
            analytics_results = {}
            
            # Customer analytics
            customer_analytics = await self._create_customer_analytics(date_filter)
            if customer_analytics:
                analytics_results["customer_analytics"] = customer_analytics
            
            # Transaction analytics
            transaction_analytics = await self._create_transaction_analytics(date_filter)
            if transaction_analytics:
                analytics_results["transaction_analytics"] = transaction_analytics
            
            # System performance analytics
            system_analytics = await self._create_system_analytics(date_filter)
            if system_analytics:
                analytics_results["system_analytics"] = system_analytics
            
            self.stats["gold_tables_created"] += len(analytics_results)
            
            logger.info(f"[SUCCESS]  Silver to Gold processing completed: {list(analytics_results.keys())}")
            
            return {
                "status": "success",
                "analytics_created": analytics_results,
                "date_filter": date_filter
            }
            
        except Exception as e:
            logger.error(f"[ERROR]  Silver to Gold processing failed: {e}")
            self.stats["processing_errors"] += 1
            return {"status": "failed", "error": str(e)}
    
    async def _create_customer_analytics(self, date_filter: str) -> Optional[Dict[str, Any]]:
        """Create customer analytics in Gold layer"""
        try:
            customer_table_path = f"{self.silver_path}/customer_behavior_clean_delta"
            
            if not Path(customer_table_path).exists():
                return None
            
            customer_df = self.spark.read.format("delta").load(customer_table_path)
            customer_df = customer_df.filter(col("partition_date") == date_filter)
            
            if customer_df.count() == 0:
                return None
            
            # Customer activity summary
            activity_summary = customer_df.groupBy("user_id") \
                .agg(
                    count("*").alias("total_events"),
                    spark_sum(when(col("action") == "page_view", 1).otherwise(0)).alias("page_views"),
                    spark_sum(when(col("action") == "click", 1).otherwise(0)).alias("clicks"),
                    spark_sum(when(col("action") == "purchase", 1).otherwise(0)).alias("purchases")
                ) \
                .withColumn("conversion_rate", col("purchases") / col("page_views") * 100) \
                .withColumn("analysis_date", lit(date_filter)) \
                .withColumn("created_at", current_timestamp())
            
            # Save to Gold layer
            gold_table_path = f"{self.gold_path}/customer_activity_summary_delta"
            
            activity_summary.write \
                .format("delta") \
                .mode("append") \
                .partitionBy("analysis_date") \
                .option("mergeSchema", "true") \
                .save(gold_table_path)
            
            return {
                "table_name": "customer_activity_summary",
                "records_created": activity_summary.count(),
                "table_path": gold_table_path
            }
            
        except Exception as e:
            logger.error(f"Failed to create customer analytics: {e}")
            return None
    
    async def _create_transaction_analytics(self, date_filter: str) -> Optional[Dict[str, Any]]:
        """Create transaction analytics in Gold layer"""
        try:
            transaction_table_path = f"{self.silver_path}/transaction_completed_clean_delta"
            
            if not Path(transaction_table_path).exists():
                return None
            
            transaction_df = self.spark.read.format("delta").load(transaction_table_path)
            transaction_df = transaction_df.filter(col("partition_date") == date_filter)
            
            if transaction_df.count() == 0:
                return None
            
            # Daily revenue summary
            revenue_summary = transaction_df.groupBy("currency") \
                .agg(
                    count("*").alias("transaction_count"),
                    spark_sum("amount").alias("total_revenue"),
                    avg("amount").alias("avg_transaction_value"),
                    spark_max("amount").alias("max_transaction"),
                    spark_min("amount").alias("min_transaction")
                ) \
                .withColumn("analysis_date", lit(date_filter)) \
                .withColumn("created_at", current_timestamp())
            
            # Save to Gold layer
            gold_table_path = f"{self.gold_path}/daily_revenue_summary_delta"
            
            revenue_summary.write \
                .format("delta") \
                .mode("append") \
                .partitionBy("analysis_date") \
                .option("mergeSchema", "true") \
                .save(gold_table_path)
            
            return {
                "table_name": "daily_revenue_summary",
                "records_created": revenue_summary.count(),
                "table_path": gold_table_path
            }
            
        except Exception as e:
            logger.error(f"Failed to create transaction analytics: {e}")
            return None
    
    async def _create_system_analytics(self, date_filter: str) -> Optional[Dict[str, Any]]:
        """Create system performance analytics in Gold layer"""
        try:
            system_table_path = f"{self.silver_path}/system_metric_clean_delta"
            
            if not Path(system_table_path).exists():
                return None
            
            system_df = self.spark.read.format("delta").load(system_table_path)
            system_df = system_df.filter(col("partition_date") == date_filter)
            
            if system_df.count() == 0:
                return None
            
            # System performance summary
            performance_summary = system_df.groupBy("metric_name") \
                .agg(
                    count("*").alias("measurement_count"),
                    avg("metric_value").alias("avg_value"),
                    spark_max("metric_value").alias("max_value"),
                    spark_min("metric_value").alias("min_value")
                ) \
                .withColumn("analysis_date", lit(date_filter)) \
                .withColumn("created_at", current_timestamp())
            
            # Save to Gold layer
            gold_table_path = f"{self.gold_path}/system_performance_summary_delta"
            
            performance_summary.write \
                .format("delta") \
                .mode("append") \
                .partitionBy("analysis_date") \
                .option("mergeSchema", "true") \
                .save(gold_table_path)
            
            return {
                "table_name": "system_performance_summary",
                "records_created": performance_summary.count(),
                "table_path": gold_table_path
            }
            
        except Exception as e:
            logger.error(f"Failed to create system analytics: {e}")
            return None
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get ETL processing statistics"""
        return {
            **self.stats.copy(),
            "spark_initialized": self.is_initialized,
            "storage_paths": {
                "bronze": self.bronze_path,
                "silver": self.silver_path,
                "gold": self.gold_path
            },
            "timestamp": datetime.now().isoformat()
        }
    
    async def shutdown(self):
        """Shutdown Spark ETL processor"""
        if self.spark:
            self.spark.stop()
        self.is_initialized = False
        logger.info("[CONNECTING]  Spark ETL Processor shutdown complete")

# Global processor instance
_spark_etl_processor: Optional[SparkETLProcessor] = None

def get_spark_etl_processor(config: Optional[Dict[str, Any]] = None) -> Optional[SparkETLProcessor]:
    """Get or create global Spark ETL processor instance"""
    global _spark_etl_processor
    
    if _spark_etl_processor is None and config:
        _spark_etl_processor = SparkETLProcessor(config)
    
    return _spark_etl_processor

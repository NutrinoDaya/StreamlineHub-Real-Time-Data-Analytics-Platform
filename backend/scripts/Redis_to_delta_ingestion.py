#!/usr/bin/env python3
"""
Redis to Delta Tables Ingestion Service
Continuously monitors Redis buffers and processes events to Bronze and Silver Delta tables.
Clean, modular architecture where processing functions receive data buffers directly.
"""

import asyncio
import json
import logging
import os
import sys
import shutil
from pathlib import Path
from typing import Dict, List, Any
from uuid import uuid4

# Add project root to path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

import redis.asyncio as redis
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_timestamp, when,
    to_date, trim, upper, lower, coalesce, 
    round as spark_round, abs as spark_abs
)
from delta.tables import DeltaTable

from src.core.spark_session import get_spark_session
from src.core.config import get_settings

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

REDIS_URL = os.getenv('REDIS_URL', 'redis://:redis_secret@redis:6379')
BUFFER_THRESHOLD = int(os.getenv('BUFFER_THRESHOLD', '50'))
PROCESSING_INTERVAL = int(os.getenv('PROCESSING_INTERVAL', '30'))
BRONZE_PATH = os.getenv('BRONZE_PATH', '/app/data/bronze')
SILVER_PATH = os.getenv('SILVER_PATH', '/app/data/silver')

EVENT_TYPES = ['customer_behavior', 'transaction_completed', 'system_metric']


# ============================================================================
# BRONZE LAYER PROCESSING
# ============================================================================

def process_bronze_layer(event_buffer: List[Dict[str, Any]], event_type: str, spark: SparkSession) -> DataFrame:
    """
    Process event buffer to Bronze layer.
    Applies basic transformations and writes to Bronze Delta table.
    
    Args:
        event_buffer: List of event dictionaries from Redis
        event_type: Type of event being processed
        spark: Spark session
        
    Returns:
        Bronze DataFrame for downstream processing
    """
    logger.info(f"Processing Bronze layer for {event_type} with {len(event_buffer)} events")
    
    try:
        # Create DataFrame from event buffer
        df = spark.createDataFrame(event_buffer)
        logger.debug(f"Created DataFrame with schema: {df.schema.simpleString()}")
        
        # Apply Bronze transformations
        bronze_df = df \
            .withColumn("insertionTime", to_timestamp(col("timestamp") / 1000)) \
            .withColumn("insertion_date", to_date(col("insertionTime"))) \
            .withColumn("batch_id", lit(str(uuid4().hex[:8]))) \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("bronze_processing_time", current_timestamp())
        
        # Write to Bronze Delta table
        bronze_table_path = f"{BRONZE_PATH}/{event_type}_bronze"
        write_to_delta_table(bronze_df, bronze_table_path, partition_by="insertion_date")
        
        record_count = bronze_df.count()
        logger.info(f"Bronze layer processed: {record_count} records written to {bronze_table_path}")
        
        return bronze_df
        
    except Exception as e:
        logger.error(f"Bronze layer processing failed for {event_type}: {e}", exc_info=True)
        raise


# ============================================================================
# SILVER LAYER PROCESSING
# ============================================================================

def process_silver_layer(bronze_df: DataFrame, event_type: str, spark: SparkSession) -> None:
    """
    Process Bronze DataFrame to Silver layer.
    Applies data cleaning, deduplication, and enrichment.
    
    Args:
        bronze_df: Bronze DataFrame to process
        event_type: Type of event being processed
        spark: Spark session
    """
    logger.info(f"Processing Silver layer for {event_type}")
    
    try:
        # Apply common Silver transformations
        silver_df = bronze_df \
            .withColumn("silver_processing_time", current_timestamp()) \
            .withColumn("processing_date", to_date(current_timestamp())) \
            .withColumn("data_quality_status", lit("validated"))
        
        # Apply event-specific transformations
        if event_type == "customer_behavior":
            silver_df = transform_customer_behavior_silver(silver_df)
        elif event_type == "transaction_completed":
            silver_df = transform_transaction_silver(silver_df)
        elif event_type == "system_metric":
            silver_df = transform_system_metric_silver(silver_df)
        
        # Write to Silver Delta table
        silver_table_path = f"{SILVER_PATH}/{event_type}_silver"
        write_to_delta_table(silver_df, silver_table_path, partition_by="processing_date")
        
        record_count = silver_df.count()
        logger.info(f"Silver layer processed: {record_count} records written to {silver_table_path}")
        
    except Exception as e:
        logger.error(f"Silver layer processing failed for {event_type}: {e}", exc_info=True)
        raise


# ============================================================================
# EVENT-SPECIFIC SILVER TRANSFORMATIONS
# ============================================================================

def transform_customer_behavior_silver(df: DataFrame) -> DataFrame:
    """Apply customer behavior specific Silver transformations"""
    return df \
        .withColumn("action", upper(trim(col("action")))) \
        .withColumn("page_url", trim(col("page_url"))) \
        .withColumn("user_agent", trim(col("user_agent"))) \
        .filter(col("user_id").isNotNull()) \
        .filter(col("action").isNotNull()) \
        .filter(col("timestamp") > 0) \
        .dropDuplicates(["user_id", "timestamp", "action"]) \
        .withColumn("is_mobile", 
                   when(col("user_agent").contains("Mobile"), True).otherwise(False))


def transform_transaction_silver(df: DataFrame) -> DataFrame:
    """Apply transaction specific Silver transformations"""
    return df \
        .withColumn("status", upper(trim(col("status")))) \
        .withColumn("payment_method", upper(trim(col("payment_method")))) \
        .withColumn("currency", upper(trim(col("currency")))) \
        .filter(col("transaction_id").isNotNull()) \
        .filter(col("amount") > 0) \
        .filter(col("timestamp") > 0) \
        .dropDuplicates(["transaction_id"]) \
        .withColumn("amount_rounded", spark_round(col("amount"), 2)) \
        .withColumn("is_high_value", when(col("amount") > 1000, True).otherwise(False)) \
        .withColumn("processing_fee", spark_round(col("amount") * 0.025, 2))


def transform_system_metric_silver(df: DataFrame) -> DataFrame:
    """Apply system metrics specific Silver transformations"""
    return df \
        .withColumn("metric_name", trim(col("metric_name"))) \
        .withColumn("host", lower(trim(col("host")))) \
        .withColumn("unit", trim(col("unit"))) \
        .filter(col("event_id").isNotNull()) \
        .filter(col("value").isNotNull()) \
        .filter(col("timestamp") > 0) \
        .dropDuplicates(["event_id", "timestamp", "host"]) \
        .withColumn("value_absolute", spark_abs(col("value"))) \
        .withColumn("is_critical", 
                   when((col("metric_name") == "CPU_USAGE") & (col("value") > 90), True)
                   .when((col("metric_name") == "MEMORY_USAGE") & (col("value") > 85), True)
                   .when((col("metric_name") == "DISK_USAGE") & (col("value") > 95), True)
                   .otherwise(False))


# ============================================================================
# ORCHESTRATION - MAIN PROCESSING FUNCTION
# ============================================================================

def process_event_buffer(event_buffer: List[Dict[str, Any]], event_type: str, spark: SparkSession) -> None:
    """
    Orchestrate Bronze and Silver layer processing for an event buffer.
    This is the main processing function that receives data from Redis and
    coordinates the full Bronze→Silver pipeline.
    
    Args:
        event_buffer: List of event dictionaries from Redis buffer
        event_type: Type of event being processed
        spark: Global Spark session
    """
    logger.info(f"Processing event buffer for {event_type} with {len(event_buffer)} events")
    
    try:
        # Step 1: Process to Bronze layer
        bronze_df = process_bronze_layer(event_buffer, event_type, spark)
        
        # Step 2: Process to Silver layer
        process_silver_layer(bronze_df, event_type, spark)
        
        logger.info(f"Successfully processed {event_type} buffer through Bronze→Silver pipeline")
        
    except Exception as e:
        logger.error(f"Error processing {event_type} buffer: {e}", exc_info=True)
        raise


# ============================================================================
# DELTA TABLE UTILITIES
# ============================================================================

def write_to_delta_table(df: DataFrame, table_path: str, partition_by: str = None) -> None:
    """
    Write DataFrame to Delta table with proper handling.
    
    Args:
        df: DataFrame to write
        table_path: Path to Delta table
        partition_by: Column to partition by (optional)
    """
    try:
        # Ensure parent directory exists
        os.makedirs(os.path.dirname(table_path), exist_ok=True)
        
        # Check if table exists
        table_exists = DeltaTable.isDeltaTable(df.sparkSession, table_path)
        write_mode = "append" if table_exists else "overwrite"
        
        logger.debug(f"Writing to Delta table: {table_path} (mode={write_mode}, exists={table_exists})")
        
        # Build write operation
        writer = df.write.format("delta").mode(write_mode)
        
        if partition_by:
            writer = writer.partitionBy(partition_by)
        
        if not table_exists:
            writer = writer.option("overwriteSchema", "true")
        else:
            writer = writer.option("mergeSchema", "true")
        
        # Execute write
        writer.save(table_path)
        
        logger.debug(f"Successfully wrote to Delta table: {table_path}")
        
    except Exception as e:
        logger.error(f"Failed to write to Delta table {table_path}: {e}")
        raise


def clean_corrupted_table(table_path: str) -> None:
    """
    Clean corrupted Delta table by removing all files.
    
    Args:
        table_path: Path to corrupted Delta table
    """
    try:
        if os.path.exists(table_path):
            logger.warning(f"Removing corrupted table: {table_path}")
            shutil.rmtree(table_path)
            logger.info(f"Cleaned corrupted table: {table_path}")
    except Exception as e:
        logger.error(f"Failed to clean corrupted table {table_path}: {e}")
        raise


# ============================================================================
# REDIS TO DELTA INGESTION SERVICE
# ============================================================================

class RedisToDeltaIngestion:
    """Redis to Bronze and Silver Delta Tables Ingestion Service"""
    
    def __init__(self):
        self.redis_client = None
        self.spark = None
        self.settings = get_settings()
        
        # Statistics
        self.stats = {
            "cycles_completed": 0,
            "events_processed": 0,
            "errors": 0
        }
    
    async def initialize_redis(self) -> bool:
        """Initialize Redis connection"""
        try:
            logger.info("Initializing Redis connection")
            self.redis_client = redis.from_url(
                REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            
            await self.redis_client.ping()
            logger.info("Redis connection established")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return False
    
    def initialize_spark(self) -> bool:
        """Initialize Spark session with Delta Lake"""
        try:
            logger.info("Initializing Spark session")
            
            # Ensure directories exist
            os.makedirs(BRONZE_PATH, exist_ok=True)
            os.makedirs(SILVER_PATH, exist_ok=True)
            
            # Get Spark session
            self.spark = get_spark_session(
                job_name="RedisToDeltaIngestion",
                master="spark://spark-master:7077",
                additional_config={
                    "spark.cores.max": "2",
                    "spark.executor.cores": "2",
                    "spark.executor.memory": "2g",
                    "spark.driver.memory": "1g",
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true"
                }
            )
            
            logger.info(f"Spark session initialized (version: {self.spark.version})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Spark: {e}")
            return False
    
    async def get_event_buffer(self, event_type: str) -> List[Dict[str, Any]]:
        """
        Retrieve and clear event buffer from Redis.
        
        Args:
            event_type: Type of event to retrieve
            
        Returns:
            List of event dictionaries
        """
        try:
            buffer_key = f"spark:buffer:{event_type}"
            
            # Get all events from buffer
            raw_events = await self.redis_client.lrange(buffer_key, 0, -1)
            
            if not raw_events:
                return []
            
            # Clear buffer after retrieval
            await self.redis_client.delete(buffer_key)
            
            # Parse JSON events
            events = []
            for raw_event in raw_events:
                try:
                    events.append(json.loads(raw_event))
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse event JSON: {e}")
            
            logger.info(f"Retrieved {len(events)} events from {event_type} buffer")
            return events
            
        except Exception as e:
            logger.error(f"Failed to retrieve {event_type} buffer: {e}")
            return []
    
    async def process_event_type(self, event_type: str) -> bool:
        """
        Process events for a specific event type.
        
        Args:
            event_type: Type of event to process
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get event buffer from Redis
            event_buffer = await self.get_event_buffer(event_type)
            
            if not event_buffer:
                return True  # No events to process is not an error
            
            # Process the buffer through Bronze→Silver pipeline
            process_event_buffer(event_buffer, event_type, self.spark)
            
            self.stats["events_processed"] += len(event_buffer)
            return True
            
        except Exception as e:
            logger.error(f"Failed to process {event_type}: {e}")
            self.stats["errors"] += 1
            return False
    
    async def run_processing_cycle(self) -> None:
        """Run a single processing cycle for all event types"""
        try:
            logger.info("Starting processing cycle")
            
            success_count = 0
            for event_type in EVENT_TYPES:
                if await self.process_event_type(event_type):
                    success_count += 1
            
            self.stats["cycles_completed"] += 1
            
            if success_count == len(EVENT_TYPES):
                logger.info("Processing cycle completed: success")
            else:
                logger.warning(f"Processing cycle completed: {success_count}/{len(EVENT_TYPES)} successful")
            
        except Exception as e:
            logger.error(f"Processing cycle failed: {e}")
            self.stats["errors"] += 1
    
    def print_statistics(self) -> None:
        """Print current statistics"""
        logger.info("=== Statistics ===")
        logger.info(f"Cycles: {self.stats['cycles_completed']}")
        logger.info(f"Events: {self.stats['events_processed']}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info("==================")
    
    async def run_continuous_ingestion(self) -> None:
        """Run continuous ingestion service"""
        logger.info("Starting Redis to Delta ingestion service")
        logger.info(f"Processing interval: {PROCESSING_INTERVAL}s")
        logger.info(f"Bronze path: {BRONZE_PATH}")
        logger.info(f"Silver path: {SILVER_PATH}")
        
        # Initialize connections
        if not await self.initialize_redis():
            logger.error("Failed to initialize Redis")
            return
        
        if not self.initialize_spark():
            logger.error("Failed to initialize Spark")
            return
        
        logger.info("Ingestion service initialized")
        
        try:
            while True:
                await self.run_processing_cycle()
                
                # Print stats every 10 cycles
                if self.stats["cycles_completed"] % 10 == 0:
                    self.print_statistics()
                
                logger.info(f"Waiting {PROCESSING_INTERVAL}s before next cycle")
                await asyncio.sleep(PROCESSING_INTERVAL)
                
        except KeyboardInterrupt:
            logger.info("Service stopped by user")
        except Exception as e:
            logger.error(f"Service failed: {e}")
        finally:
            # Cleanup
            if self.redis_client:
                await self.redis_client.close()
            if self.spark:
                self.spark.stop()
            
            self.print_statistics()


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

async def main():
    """Main execution function"""
    ingestion_service = RedisToDeltaIngestion()
    await ingestion_service.run_continuous_ingestion()


if __name__ == '__main__':
    asyncio.run(main())

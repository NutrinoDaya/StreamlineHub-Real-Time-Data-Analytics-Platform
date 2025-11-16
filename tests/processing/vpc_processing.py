#!/usr/bin/env python
"""
VPC Data Processor - Completely Refactored

This module provides robust processing for VPC data with:
- Event processing
- State management
- Error recovery
"""

import os
import sys
import json
import uuid
import time
import shutil
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

# Add the project root directory to sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(ROOT_DIR))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType

from utils import LoggerManager, readConfig

logger = LoggerManager.get_logger("ETL_Processing.log")

# Delta Lake imports with fallback
try:
    from delta.tables import DeltaTable
    DELTA_AVAILABLE = True
except ImportError:
    logger.warning("Delta Lake not available, using fallback implementation")
    DELTA_AVAILABLE = False


class VPCProcessor:
    """
    VPC data processor for events and clerk status
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
        # Load configuration
        self._load_configuration()
        self._load_schemas()
        
        # Processing statistics
        self.processing_stats = {
            "events_processed": 0,
            "clerks_processed": 0,
            "batches_processed": 0,
            "errors": 0
        }
    
    def _load_configuration(self):
        """Load configuration from XML files"""
        try:
            config_dir = ROOT_DIR / "config"
            
            # ETL Configuration
            etl_config = readConfig(config_dir / "xml/ETL.xml")
            self.etl_config = etl_config.get("Write", {})
            
            # DML Configuration
            dml_config = readConfig(config_dir / "xml/DML.xml").get("FILE", {})
            self.bronze_layer = dml_config["bronze_layer"]
            self.silver_layer = dml_config["silver_layer"]
            self.warehouse = dml_config["warehouse"]
            
            # Processing parameters
            self.max_attempts = int(self.etl_config.get("max_attempts", 5))
            self.compression_algorithm = self.etl_config.get("compression_algorithm", "gzip")
            self.max_items = int(self.etl_config.get("max_items", 400))
            
        except Exception as e:
            logger.error(f"Configuration loading failed: {e}")
            # Set defaults
            self.bronze_layer = "bronze"
            self.silver_layer = "silver"
            self.warehouse = "data"
            self.max_attempts = 5
            self.compression_algorithm = "gzip"
            self.max_items = 400
    
    def _load_schemas(self):
        """Load VPC data schemas"""
        # VPC Events schema
        self.vpc_events_schema = StructType([
            StructField("EventId", StringType(), False),
            StructField("EventType", StringType(), True),
            StructField("EventTime", TimestampType(), True),
            StructField("VpcId", StringType(), True),
            StructField("InstanceId", StringType(), True),
            StructField("ResourceId", StringType(), True),
            StructField("State", StringType(), True),
            StructField("PreviousState", StringType(), True),
            StructField("Region", StringType(), True),
            StructField("AvailabilityZone", StringType(), True),
            StructField("Source", StringType(), True),
            StructField("Details", StringType(), True),
            StructField("Severity", StringType(), True)
        ])
        
        # VPC Clerks schema
        self.vpc_clerks_schema = StructType([
            StructField("ClerkId", StringType(), False),
            StructField("ClerkName", StringType(), True),
            StructField("Status", StringType(), True),
            StructField("LastUpdate", TimestampType(), True),
            StructField("VpcId", StringType(), True),
            StructField("Region", StringType(), True),
            StructField("IsActive", BooleanType(), True),
            StructField("ProcessingLoad", IntegerType(), True),
            StructField("LastHeartbeat", TimestampType(), True)
        ])
    
    def safe_write_table(self, df: DataFrame, path: str, mode: str = "append", max_retries: int = 5) -> bool:
        """Safely write DataFrame to table with comprehensive error handling"""
        for attempt in range(max_retries):
            try:
                # Clear cache before each attempt (except first)
                if attempt > 0:
                    self.spark.catalog.clearCache()
                    time.sleep(min(2 ** attempt, 10))
                
                # Ensure directory exists
                os.makedirs(path, exist_ok=True)
                
                # Check for corrupted table
                if os.path.exists(path) and os.listdir(path):
                    try:
                        # Try to read a small sample to validate table health
                        sample_df = self.spark.read.format("delta" if DELTA_AVAILABLE else "parquet").load(path).limit(1)
                        sample_df.count()
                    except Exception as corruption_error:
                        logger.warning(f"Table corruption detected: {corruption_error}")
                        # Backup and remove corrupted table
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        backup_path = f"{path}_backup_{timestamp}"
                        try:
                            shutil.copytree(path, backup_path)
                            logger.info(f"Created backup: {backup_path}")
                        except Exception:
                            pass
                        shutil.rmtree(path)
                        mode = "overwrite"
                
                # Perform write
                writer = df.write.format("delta" if DELTA_AVAILABLE else "parquet").mode(mode)
                
                if DELTA_AVAILABLE:
                    writer = writer.option("mergeSchema", "true") \
                                   .option("autoOptimize.optimizeWrite", "true") \
                                   .option("autoOptimize.autoCompact", "true")
                
                writer.save(path)
                
                # Validate write
                if os.path.exists(path) and os.listdir(path):
                    logger.info(f"Successfully wrote {df.count()} records to {path}")
                    return True
                else:
                    logger.error(f"Write validation failed for {path}")
                    continue
            
            except Exception as e:
                error_str = str(e).lower()
                logger.error(f"Write attempt {attempt + 1} failed for {path}: {e}")
                
                # Handle specific error patterns
                if any(pattern in error_str for pattern in [
                    "sparkfilenotfoundexception", "delta_log", "file does not exist",
                    "deltaanalysisexception", "protocol mismatch", "checkpoint",
                    "transaction log", "concurrent"
                ]):
                    logger.warning(f"Detected corruption/concurrency error, attempting recovery")
                    self.spark.catalog.clearCache()
                    if os.path.exists(path):
                        try:
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            backup_path = f"{path}_backup_{timestamp}"
                            shutil.copytree(path, backup_path)
                            shutil.rmtree(path)
                        except Exception:
                            pass
                    continue
                
                if attempt == max_retries - 1:
                    logger.error(f"All {max_retries} write attempts failed for {path}")
                    return False
        
        return False
    
    def process_vpc_events(self, events_data: List[Dict]) -> bool:
        """Process VPC events data"""
        logger.info(f"Processing VPC events: {len(events_data)} records")
        
        try:
            if not events_data:
                logger.warning("No VPC events to process")
                return True
            
            # Create DataFrame directly from raw data
            df = self.spark.createDataFrame(events_data)
            
            # Add processing metadata
            df = df.withColumn("etl_batch_id", lit(str(uuid.uuid4()))) \
                .withColumn("etl_processing_timestamp", current_timestamp())
            
            # Get table path
            table_name = "VPC_Event"
            base_path = os.path.join(ROOT_DIR, self.warehouse, self.bronze_layer)
            table_path = os.path.join(base_path, "VPC", table_name)
            
            # Write to table
            success = self.safe_write_table(df, table_path, "append", self.max_attempts)
            
            if success:
                logger.info(f"Successfully processed VPC events: {len(events_data)} records")
                self.processing_stats["events_processed"] += len(events_data)
            else:
                logger.error("Failed to process VPC events")
                self.processing_stats["errors"] += 1
            
            return success
            
        except Exception as e:
            logger.error(f"VPC events processing failed: {e}")
            self.processing_stats["errors"] += 1
            return False
    
    def process_vpc_clerks(self, clerks_data: List[Dict]) -> bool:
        """Process VPC clerks data"""
        logger.info(f"Processing VPC clerks: {len(clerks_data)} records")
        
        try:
            if not clerks_data:
                logger.warning("No VPC clerks to process")
                return True
            
            # Create DataFrame directly from raw data
            df = self.spark.createDataFrame(clerks_data)
            
            # Add processing metadata
            df = df.withColumn("etl_batch_id", lit(str(uuid.uuid4()))) \
                .withColumn("etl_processing_timestamp", current_timestamp())
            
            # Get table path
            table_name = "VPC_Clerks"
            base_path = os.path.join(ROOT_DIR, self.warehouse, self.bronze_layer)
            table_path = os.path.join(base_path, "VPC", table_name)
            
            # Write to table
            success = self.safe_write_table(df, table_path, "append", self.max_attempts)
            
            if success:
                logger.info(f"Successfully processed VPC clerks: {len(clerks_data)} records")
                self.processing_stats["clerks_processed"] += len(clerks_data)
            else:
                logger.error("Failed to process VPC clerks")
                self.processing_stats["errors"] += 1
            
            return success
            
        except Exception as e:
            logger.error(f"VPC clerks processing failed: {e}")
            self.processing_stats["errors"] += 1
            return False
    
    def get_processing_statistics(self) -> Dict[str, Any]:
        """Get processing statistics"""
        return self.processing_stats.copy()


def process_vpc_data(spark: SparkSession, data: List[Dict]) -> bool:
    """Main entry point for VPC events data processing"""
    try:
        processor = VPCProcessor(spark)
        success = processor.process_vpc_events(data)
        
        if success:
            logger.info("VPC events processing completed successfully.")
            return True
        else:
            logger.warning("VPC events processing completed with failures.")
            return False
            
    except Exception as e:
        logger.error(f"An error occurred during VPC events processing: {e}", exc_info=True)
        return False


def process_clerk_data(spark: SparkSession, data: List[Dict]) -> bool:
    """Main entry point for VPC clerk data processing"""
    try:
        processor = VPCProcessor(spark)
        success = processor.process_vpc_clerks(data)
        
        if success:
            logger.info("VPC clerk processing completed successfully.")
            return True
        else:
            logger.warning("VPC clerk processing completed with failures.")
            return False
            
    except Exception as e:
        logger.error(f"An error occurred during VPC clerk processing: {e}", exc_info=True)
        return False

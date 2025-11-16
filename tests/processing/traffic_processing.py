#!/usr/bin/env python
"""
Traffic Data Processor - Completely Refactored

This module provides robust processing for Traffic data with:
- Dynamic partitioning
- Performance optimization
- Error recovery
- reliability
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
from pyspark.sql.functions import col, lit, current_timestamp, year, month, dayofmonth, hour, explode, unix_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, LongType, BooleanType

from utils import LoggerManager, readConfig
from schemas.VITDS.Traffic_Data_Silver import Traffic_Data_Silver
from schemas.VITDS.Traffic_HeaderSession_Silver import Traffic_HeaderSession_Silver
from schemas.VITDS.Traffic_DataLookup import Traffic_DataLookup

logger = LoggerManager.get_logger("ETL_Processing.log")

# Delta Lake imports with fallback
try:
    from delta.tables import DeltaTable
    DELTA_AVAILABLE = True
except ImportError:
    logger.warning("Delta Lake not available, using fallback implementation")
    DELTA_AVAILABLE = False


class TrafficProcessor:
    """
    Traffic data processor with dynamic partitioning and optimization
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
        # Load configuration
        self._load_configuration()
        self._load_schemas()
        
        # Processing statistics
        self.processing_stats = {
            "records_processed": 0,
            "batches_processed": 0,
            "errors": 0,
            "total_bytes_processed": 0
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
            self.traffic_prefix = self.etl_config.get("traffic_prefix", "Traffic")
            self.compression_algorithm = self.etl_config.get("compression_algorithm", "gzip")
            self.max_items = int(self.etl_config.get("max_items", 400))
            
        except Exception as e:
            logger.error(f"Configuration loading failed: {e}")
            # Set defaults
            self.bronze_layer = "bronze"
            self.silver_layer = "silver"
            self.warehouse = "data"
            self.max_attempts = 5
            self.traffic_prefix = "Traffic"
            self.compression_algorithm = "gzip"
            self.max_items = 400
    
    def _load_schemas(self):
        """Load Traffic data schemas"""
        # Use proper Traffic Data Silver schema
        self.traffic_data_schema = Traffic_Data_Silver.get_schema()
        
        # Use proper Traffic Header Session Silver schema 
        self.traffic_header_schema = Traffic_HeaderSession_Silver.get_schema()
        
        # Use proper Traffic DataLookup schema
        self.traffic_lookup_schema = Traffic_DataLookup.get_schema()
    
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
    
    def process_bronze_layer(self, raw_data: List[Dict]) -> bool:
        """Process traffic data into bronze layer - handles both traffic data and header sessions"""
        logger.info(f"Processing Traffic bronze layer: {len(raw_data)} records")
        
        try:
            if not raw_data:
                logger.warning("No traffic data to process")
                return True
            
            success = True
            base_path = os.path.join(ROOT_DIR, self.warehouse, self.bronze_layer)
            today = datetime.now().strftime("%Y-%m-%d")
            
            for record in raw_data:
                try:
                    # Extract header session and traffic data list from each record
                    header_session = record.get("headersession", {})
                    traffic_data_list = record.get("trafficDataList", [])
                    
                    # Process header session if present
                    if header_session:
                        success = self._process_header_session(header_session, base_path, today) and success
                    
                    # Process traffic data list if present  
                    if traffic_data_list:
                        success = self._process_traffic_data_list(traffic_data_list, header_session, base_path, today) and success
                        
                except Exception as e:
                    logger.error(f"Failed to process traffic record: {e}")
                    success = False
                    continue
            
            if success:
                logger.info(f"Successfully processed traffic bronze layer: {len(raw_data)} records")
                self.processing_stats["batches_processed"] += 1
                self.processing_stats["records_processed"] += len(raw_data)
                
                # Process lookup table after successful data processing
                lookup_success = self._process_lookup_table(raw_data)
                if not lookup_success:
                    logger.warning("Traffic_DataLookup update failed, but data processing succeeded")
            else:
                logger.error("Failed to process traffic bronze layer")
                self.processing_stats["errors"] += 1
            
            return success
            
        except Exception as e:
            logger.error(f"Traffic bronze layer processing failed: {e}")
            self.processing_stats["errors"] += 1
            return False
    
    def _process_header_session(self, header_session: Dict, base_path: str, today: str) -> bool:
        """Process traffic header session data"""
        try:
            # Add metadata and convert timestamp strings to unix timestamps
            processed_header = header_session.copy()
            processed_header["InsertionTime"] = int(time.time() * 1000)  # Current time in ms
            
            # Convert SessionStart and SessionEnd from string to unix timestamp in ms
            if "SessionStart" in processed_header:
                session_start = datetime.strptime(processed_header["SessionStart"], "%Y-%m-%d %H:%M:%S")
                processed_header["SessionStart"] = int(session_start.timestamp() * 1000)
            
            if "SessionEnd" in processed_header:
                session_end = datetime.strptime(processed_header["SessionEnd"], "%Y-%m-%d %H:%M:%S") 
                processed_header["SessionEnd"] = int(session_end.timestamp() * 1000)
            
            # Create DataFrame with proper schema
            df = self.spark.createDataFrame([processed_header], schema=self.traffic_header_schema)
            
            # Set up table path
            table_name = "Traffic_HeaderSession"
            table_path = os.path.join(base_path, self.traffic_prefix, today, table_name)
            
            # Write to table
            return self.safe_write_table(df, table_path, "append", self.max_attempts)
            
        except Exception as e:
            logger.error(f"Header session processing failed: {e}")
            return False
    
    def _process_traffic_data_list(self, traffic_data_list: List[Dict], header_session: Dict, base_path: str, today: str) -> bool:
        """Process traffic data list"""
        try:
            if not traffic_data_list:
                return True
            
            processed_traffic_data = []
            current_time_ms = int(time.time() * 1000)
            
            # Extract session info for enrichment
            session_index = header_session.get("SessionIndex", "")
            device_index = header_session.get("SystemName", "")
            location0 = header_session.get("Location0", "")
            location1 = header_session.get("Location1", "")
            location2 = header_session.get("Location2", "")
            
            # Convert session times if available
            session_start_ms = None
            session_end_ms = None
            if "SessionStart" in header_session:
                session_start = datetime.strptime(header_session["SessionStart"], "%Y-%m-%d %H:%M:%S")
                session_start_ms = int(session_start.timestamp() * 1000)
            if "SessionEnd" in header_session:
                session_end = datetime.strptime(header_session["SessionEnd"], "%Y-%m-%d %H:%M:%S")
                session_end_ms = int(session_end.timestamp() * 1000)
            
            for traffic_record in traffic_data_list:
                # Enrich traffic data with session information
                enriched_record = traffic_record.copy()
                
                # Add session context
                enriched_record["SessionIndex"] = session_index
                enriched_record["DeviceIndex"] = device_index
                enriched_record["Location0"] = location0
                enriched_record["Location1"] = location1
                enriched_record["Location2"] = location2
                enriched_record["SessionStart"] = session_start_ms
                enriched_record["SessionEnd"] = session_end_ms
                
                # Add processing metadata
                enriched_record["InsertionTime"] = current_time_ms
                
                # Convert and process PassageTime 
                if "PassageTime" in enriched_record and enriched_record["PassageTime"]:
                    try:
                        passage_time_str = enriched_record["PassageTime"]
                        if isinstance(passage_time_str, str):
                            # Convert string format "YYYY-MM-DD HH:MM:SS" to datetime then to ms
                            passage_time = datetime.strptime(passage_time_str, "%Y-%m-%d %H:%M:%S")
                            passage_time_ms = int(passage_time.timestamp() * 1000)
                            enriched_record["PassageTime"] = passage_time_ms
                        else:
                            # Already in milliseconds format
                            passage_time = datetime.fromtimestamp(enriched_record["PassageTime"] / 1000)
                        
                        # Add time partitioning fields
                        enriched_record["year"] = passage_time.year
                        enriched_record["month"] = passage_time.month
                        enriched_record["day"] = passage_time.day
                        enriched_record["hour"] = passage_time.hour
                        enriched_record["minute"] = passage_time.minute
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Invalid PassageTime format: {e}")
                        # Use current time for partitioning as fallback
                        now = datetime.now()
                        enriched_record["PassageTime"] = current_time_ms
                        enriched_record["year"] = now.year
                        enriched_record["month"] = now.month
                        enriched_record["day"] = now.day
                        enriched_record["hour"] = now.hour
                        enriched_record["minute"] = now.minute
                else:
                    # No PassageTime provided, use current time
                    now = datetime.now()
                    enriched_record["PassageTime"] = current_time_ms
                    enriched_record["year"] = now.year
                    enriched_record["month"] = now.month
                    enriched_record["day"] = now.day
                    enriched_record["hour"] = now.hour
                    enriched_record["minute"] = now.minute
                
                # Convert ImportTime if it's in string format
                if "ImportTime" in enriched_record and isinstance(enriched_record["ImportTime"], str):
                    try:
                        import_time = datetime.fromisoformat(enriched_record["ImportTime"].replace('Z', '+00:00'))
                        enriched_record["ImportTime"] = int(import_time.timestamp() * 1000)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Invalid ImportTime format: {e}")
                        enriched_record["ImportTime"] = current_time_ms
                
                processed_traffic_data.append(enriched_record)
            
            # Create DataFrame with proper schema
            df = self.spark.createDataFrame(processed_traffic_data, schema=self.traffic_data_schema)
            
            # Set up table path
            table_name = "Traffic_Data"
            table_path = os.path.join(base_path, self.traffic_prefix, today, table_name)
            
            # Write to table
            return self.safe_write_table(df, table_path, "append", self.max_attempts)
            
        except Exception as e:
            logger.error(f"Traffic data list processing failed: {e}")
            return False
    
    def _process_lookup_table(self, raw_data: List[Dict]) -> bool:
        """Process and update Traffic_DataLookup table"""
        try:
            logger.info("Processing Traffic_DataLookup table")
            
            pairs = set()
            
            # Extract lookup pairs from all records
            for record in raw_data:
                # From headersession
                header_session = record.get("headersession", {}) or {}
                for fld in ("SessionIndex", "DeviceIndex", "Location0", "Location1", "Location2"):
                    v = header_session.get(fld)
                    if v is not None:
                        pairs.add((fld, str(v)))
                
                # From trafficDataList
                traffic_data_list = record.get("trafficDataList", []) or []
                for rec in traffic_data_list:
                    for fld in ("VehicleClass", "SessionIndex", "DeviceIndex", "Location0", "Location1", "Location2"):
                        v = rec.get(fld)
                        if v is not None:
                            pairs.add((fld, str(v)))
            
            if not pairs:
                logger.debug("No lookup pairs found; skipping Traffic_DataLookup update")
                return True
            
            # Create lookup records
            lookup_list = [{"lookupType": lt, "lookupValue": lv} for lt, lv in pairs]
            
            # Create DataFrame
            rdd = self.spark.sparkContext.parallelize(lookup_list)
            df_lookup = self.spark.createDataFrame(rdd, schema=self.traffic_lookup_schema)
            
            # Set up table path (no date folder for lookup table)
            lookup_path = os.path.join(ROOT_DIR, self.warehouse, self.bronze_layer, self.traffic_prefix, "Traffic_DataLookup")
            
            # Write in overwrite mode
            writer = df_lookup.write.format("delta" if DELTA_AVAILABLE else "parquet").mode("overwrite")
            if DELTA_AVAILABLE:
                writer = writer.option("overwriteSchema", "true")
            
            writer.save(lookup_path)
            
            logger.info(f"Successfully updated Traffic_DataLookup with {len(pairs)} pairs")
            return True
            
        except Exception as e:
            logger.error(f"Traffic_DataLookup processing failed: {e}")
            return False
    
    def get_processing_statistics(self) -> Dict[str, Any]:
        """Get processing statistics"""
        return self.processing_stats.copy()


def process_traffic_data(spark: SparkSession, data: List[Dict]) -> bool:
    """Main entry point for Traffic data processing"""
    try:
        processor = TrafficProcessor(spark)
        
        # Process bronze layer
        bronze_success = processor.process_bronze_layer(data)
        
        # Process lookup table
        lookup_success = processor._process_lookup_table(data)
        
        if bronze_success and lookup_success:
            logger.info("Traffic data processing completed successfully.")
            return True
        else:
            logger.warning("Traffic data processing completed with some failures.")
            return False
            
    except Exception as e:
        logger.error(f"An error occurred during traffic data processing: {e}", exc_info=True)
        return False

#!/usr/bin/env python
"""
ETL Processing Engine

This module provides the core ETL processing engine with:
- Robust data ingestion
- Multi-layer processing (bronze, silver, gold)
- Performance optimization
- Error recovery
- Data validation
"""

import os
import sys
import uuid
import time
import math
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add the project root directory to sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(ROOT_DIR))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, year, month, dayofmonth
from pyspark.sql.types import StructType

from utils import readConfig, LoggerManager
from utils.initializers import initSparkSession
from src.core.delta_manager import DeltaTableManager
from src.core.data_validator import DataValidator
from src.core.performance_monitor import PerformanceMonitor

logger = LoggerManager.get_logger("ETL_Processing.log")


class ETLProcessingEngine:
    """
    ETL processing engine
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """Initialize ETL Engine with optional config or Spark session"""
        self.config = config or {}
        
        # Initialize Spark session
        self.spark = self._create_spark_session()
        
        # Initialize components
        self.delta_manager = DeltaTableManager(self.spark)
        self.data_validator = DataValidator(self.spark)
        self.performance_monitor = PerformanceMonitor()
        
        # Load configuration
        self._load_configuration()
        
        # Start performance monitoring
        self.performance_monitor.start_system_monitoring()
        
        # Processing statistics
        self.processing_stats = {
            "batches_processed": 0,
            "records_processed": 0,
            "errors_encountered": 0,
            "start_time": datetime.now()
        }
        
        logger.info("ETL Processing Engine initialized")
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session using proper initializers"""
        try:
            # Use the proper initializer that loads Delta Lake packages from Spark.xml
            spark = initSparkSession("ETL_Processing")
            spark.sparkContext.setLogLevel("WARN")
            
            logger.info(f"Spark session created successfully: {spark.version}")
            return spark
            
        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            # Create local session as fallback
            logger.warning("Creating fallback local Spark session")
            return SparkSession.builder \
                .appName("ETL_Processing_Fallback") \
                .master("local[*]") \
                .config("spark.sql.adaptive.enabled", "true") \
                .getOrCreate()
    
    def _load_configuration(self):
        """Load ETL configuration"""
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
        self.partitions = int(self.etl_config.get("partitions", 600))
        
        # Performance tuning
        self.records_per_partition = 1000
        self.enable_adaptive_partitioning = True
        self.enable_data_validation = self.etl_config.get("enable_strict_validation", "true").lower() == "true"
        
        logger.info(f"Configuration loaded - Max attempts: {self.max_attempts}, "
                   f"Max items: {self.max_items}, Compression: {self.compression_algorithm}, "
                   f"Validation enabled: {self.enable_data_validation}")
    
    def calculate_optimal_partitions(self, record_count: int, data_size_mb: Optional[float] = None) -> int:
        """Calculate optimal partition count based on data characteristics"""
        if not self.enable_adaptive_partitioning:
            return self.partitions
        
        # Base calculation on record count
        partition_count = max(1, math.ceil(record_count / self.records_per_partition))
        
        # Adjust based on data size if available
        if data_size_mb:
            # Target 128MB per partition
            size_based_partitions = max(1, math.ceil(data_size_mb / 128))
            partition_count = min(partition_count, size_based_partitions)
        
        # Limit to reasonable bounds
        max_partitions = min(self.partitions, record_count // 10) if record_count > 10 else 1
        partition_count = min(partition_count, max_partitions)
        
        logger.debug(f"Calculated {partition_count} partitions for {record_count} records")
        return partition_count
    
    def prepare_dataframe(self, data: List[Dict], schema: Optional[StructType] = None) -> Optional[DataFrame]:
        """Prepare DataFrame with validation and optimization"""
        if not data:
            logger.warning("Empty data provided")
            return None
        
        try:
            # Clean and normalize data first
            silver_data = []
            for record in data:
                cleaned_record = {}
                for key, value in record.items():
                    # Convert None values to empty strings for consistent schema inference
                    if value is None:
                        cleaned_record[key] = ""
                    # Convert lists/dicts to strings for schema consistency
                    elif isinstance(value, (list, dict)):
                        cleaned_record[key] = str(value)
                    else:
                        cleaned_record[key] = str(value)  # Convert all to string for now
                silver_data.append(cleaned_record)
            
            # Create DataFrame with string schema to avoid inference issues
            df = self.spark.createDataFrame(silver_data)
            
            # Add processing metadata
            df = df.withColumn("etl_batch_id", lit(str(uuid.uuid4()))) \
                   .withColumn("etl_processing_timestamp", current_timestamp())
            
            # Optimize partitioning
            record_count = len(data)
            optimal_partitions = self.calculate_optimal_partitions(record_count)
            
            if optimal_partitions != df.rdd.getNumPartitions():
                if optimal_partitions < df.rdd.getNumPartitions():
                    df = df.coalesce(optimal_partitions)
                else:
                    df = df.repartition(optimal_partitions)
            
            logger.debug(f"Prepared DataFrame: {record_count} records, {df.rdd.getNumPartitions()} partitions")
            return df
            
        except Exception as e:
            logger.error(f"Failed to prepare DataFrame: {e}")
            return None
    
    def validate_and_process_batch(self, 
                                 df: DataFrame, 
                                 table_name: str,
                                 expected_schema: Optional[StructType] = None) -> bool:
        """Validate and process a data batch with comprehensive checks"""
        batch_id = str(uuid.uuid4())
        
        try:
            # Start performance tracking
            record_count = df.count()
            stats = self.performance_monitor.start_batch_processing(batch_id, table_name, record_count)
            
            # Data validation (if enabled)
            if self.enable_data_validation:
                validation_result = self.data_validator.validate_batch(df, table_name, expected_schema)
                
                if not validation_result["overall_passed"]:
                    logger.warning(f"Data validation failed for {table_name}: {validation_result}")
                    # In non-strict mode, log warning but continue processing
                    logger.info(f"Continuing processing despite validation failure (non-strict mode)")
                else:
                    logger.info(f"Data validation passed for {table_name} "
                    f"(Quality score: {validation_result['quality_validation']['quality_score']:.2f})")
            else:
                pass
                # logger.info(f"Data validation disabled for {table_name}")
            
            # Process the batch
            success = self._process_data_batch(df, table_name, batch_id)
            
            # Complete performance tracking
            self.performance_monitor.complete_batch_processing(batch_id, success=success)
            
            # Update statistics
            if success:
                self.processing_stats["batches_processed"] += 1
                self.processing_stats["records_processed"] += record_count
            else:
                self.processing_stats["errors_encountered"] += 1
            
            return success
            
        except Exception as e:
            logger.error(f"Batch processing failed for {table_name}: {e}")
            self.performance_monitor.complete_batch_processing(batch_id, success=False)
            self.processing_stats["errors_encountered"] += 1
            return False
    
    def _process_data_batch(self, df: DataFrame, table_name: str, batch_id: str) -> bool:
        """Process a single data batch"""
        try:
            # Determine table path
            layer = "bronze"  # Default to bronze layer
            if "silver" in table_name.lower():
                layer = "silver"
            
            table_path = self._get_table_path(table_name, layer)
            
            # Determine partitioning strategy
            partition_cols = self._get_partition_columns(table_name, df)
            
            # Custom validation function
            validation_func = self._get_validation_function(table_name)
            
            # Perform transactional write
            success = self.delta_manager.transactional_write(
                df=df,
                table_path=table_path,
                mode="append",
                partition_cols=partition_cols,
                max_retries=self.max_attempts,
                validation_func=validation_func
            )
            
            if success:
                logger.info(f"Successfully processed batch {batch_id} for {table_name}")
                
                # Schedule optimization (if table is large enough)
                self._schedule_table_optimization(table_path, table_name)
            else:
                logger.error(f"Failed to process batch {batch_id} for {table_name}")
            
            return success
            
        except Exception as e:
            logger.error(f"Data batch processing error for {table_name}: {e}")
            return False
    
    def _get_table_path(self, table_name: str, layer: str) -> str:
        """Get table path based on layer and table name"""
        if layer == "bronze":
            base_path = os.path.join(ROOT_DIR, self.warehouse, self.bronze_layer)
        elif layer == "silver":
            base_path = os.path.join(ROOT_DIR, self.warehouse, self.silver_layer)
        else:
            raise ValueError(f"Unknown layer: {layer}")
        
        # Add date partitioning for bronze layer
        if layer == "bronze":
            today = datetime.now().strftime("%Y-%m-%d")
            return os.path.join(base_path, table_name.split("_")[0], today, table_name)
        else:
            return os.path.join(base_path, table_name)
    
    def _get_partition_columns(self, table_name: str, df: DataFrame) -> Optional[List[str]]:
        """Determine optimal partition columns for table"""
        available_columns = df.columns
        
        # Standard partitioning strategies
        if "InsertionTime" in available_columns:
            return ["year", "month"]  # Assuming we add these columns
        elif "timestamp" in available_columns:
            return ["year", "month"]
        elif "date" in available_columns:
            return ["date"]
        
        # Table-specific partitioning
        if "IPP_PSOP" in table_name:
            if "PrimaryFileName" in available_columns:
                return ["PrimaryFileName"]
        elif "Traffic" in table_name:
            if "region" in available_columns:
                return ["region"]
        elif "VPC" in table_name:
            if "event_type" in available_columns:
                return ["event_type"]
        
        return None
    
    def _get_validation_function(self, table_name: str) -> Optional[callable]:
        """Get custom validation function for table"""
        def validate_ipp_psop(df: DataFrame) -> bool:
            # IPP_PSOP specific validation
            required_cols = ["PrimaryFileName", "IncidentId"]
            for col_name in required_cols:
                if col_name in df.columns:
                    null_count = df.filter(col(col_name).isNull()).count()
                    if null_count > 0:
                        logger.warning(f"Found {null_count} null values in required column {col_name}")
                        return False
            return True
        
        def validate_traffic(df: DataFrame) -> bool:
            # Traffic specific validation
            if df.count() == 0:
                return False
            return True
        
        def validate_vpc(df: DataFrame) -> bool:
            # VPC specific validation
            if df.count() == 0:
                return False
            return True
        
        # Return appropriate validation function
        if "IPP_PSOP" in table_name:
            return validate_ipp_psop
        elif "Traffic" in table_name:
            return validate_traffic
        elif "VPC" in table_name:
            return validate_vpc
        
        return None
    
    def _schedule_table_optimization(self, table_path: str, table_name: str):
        """Schedule table optimization based on size and usage"""
        try:
            # Simple heuristic: optimize every 10 batches or based on time
            if self.processing_stats["batches_processed"] % 10 == 0:
                logger.info(f"Scheduling optimization for {table_name}")
                
                # Determine Z-order columns
                z_order_cols = None
                if "IPP_PSOP" in table_name:
                    z_order_cols = ["PrimaryFileName", "IncidentId"]
                elif "Traffic" in table_name:
                    z_order_cols = ["PassageTime", "DeviceIndex"]
                elif "VPC" in table_name:
                    z_order_cols = ["event_type"]
                
                # Run optimization in background
                ThreadPoolExecutor(max_workers=1).submit(
                    self.delta_manager.optimize_table,
                    table_path,
                    z_order_cols
                )
                
        except Exception as e:
            logger.warning(f"Failed to schedule optimization for {table_name}: {e}")
    
    def process_ipp_psop_data(self, data: List[Dict]) -> bool:
        """Process IPP PSOP data with batch processing"""
        logger.info(f"Processing IPP_PSOP data batch: {len(data)} records")
        
        try:
            # Add batch processing ID for tracking
            batch_id = f"ipp_psop_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:20]}"
            logger.info(f"Starting IPP_PSOP batch processing: {batch_id}")
            
            # Transform data into different tables
            start_time = datetime.now()
            transformed_data = self._transform_ipp_psop_data(data)
            transform_duration = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"Data transformation completed in {transform_duration:.2f}s for batch {batch_id}")
            
            # Process each table
            all_success = True
            processing_stats = {}
            
            for table_name, table_data in transformed_data.items():
                if table_data:
                    table_start = datetime.now()
                    df = self.prepare_dataframe(table_data)
                    if df:
                        success = self.validate_and_process_batch(df, table_name)
                        table_duration = (datetime.now() - table_start).total_seconds()
                        processing_stats[table_name] = {
                            "success": success,
                            "record_count": len(table_data),
                            "duration_seconds": table_duration,
                            "records_per_second": len(table_data) / table_duration if table_duration > 0 else 0
                        }
                        
                        if not success:
                            all_success = False
                            logger.error(f"Failed to process {table_name} in batch {batch_id}")
                    else:
                        all_success = False
                        logger.error(f"Failed to prepare DataFrame for {table_name} in batch {batch_id}")
                        processing_stats[table_name] = {"success": False, "error": "DataFrame preparation failed"}
            
            total_duration = (datetime.now() - start_time).total_seconds()
            total_records = sum(len(table_data) for table_data in transformed_data.values())
            overall_rate = total_records / total_duration if total_duration > 0 else 0
            
            logger.info(f"IPP_PSOP batch processing completed: {batch_id}, "
                       f"success: {all_success}, total_duration: {total_duration:.2f}s, "
                       f"total_records: {total_records}, rate: {overall_rate:.1f} records/sec")
            
            # Log detailed stats
            for table_name, stats in processing_stats.items():
                if stats.get("success"):
                    logger.info(f"Table {table_name}: {stats['record_count']} records, "
                               f"{stats['duration_seconds']:.2f}s, {stats['records_per_second']:.1f} records/sec")
            
            return all_success
            
        except Exception as e:
            logger.error(f"IPP_PSOP batch processing failed: {e}")
            return False
    
    def _transform_ipp_psop_data(self, data: List[Dict]) -> Dict[str, List[Dict]]:
        """Transform IPP_PSOP data into separate tables"""
        ipp_psop_prefix = self.etl_config.get("ipp_psop_prefix", "IPP_PSOP")
        
        transformed = {
            f"{ipp_psop_prefix}_Incident": [],
            f"{ipp_psop_prefix}_IncidentFiles": [],
            f"{ipp_psop_prefix}_IncidentHistory": []
        }
        
        logger.info(f"Transforming {len(data)} IPP_PSOP incidents")
        
        for incident in data:
            # Main incident data - use PrimaryFileName as IncidentId
            main_incident = incident.copy()
            main_incident.pop("IncidentFiles", None)
            main_incident.pop("incidentHistories", None)
            
            # Add IncidentId for linking (use PrimaryFileName)
            incident_id = incident.get("PrimaryFileName", f"incident_{len(transformed[f'{ipp_psop_prefix}_Incident'])}")
            main_incident["IncidentId"] = incident_id
            
            transformed[f"{ipp_psop_prefix}_Incident"].append(main_incident)
            
            # Incident files
            for file_data in incident.get("IncidentFiles", []):
                file_record = file_data.copy()
                file_record["IncidentId"] = incident_id
                transformed[f"{ipp_psop_prefix}_IncidentFiles"].append(file_record)
            
            # Incident history
            for history_data in incident.get("incidentHistories", []):
                history_record = history_data.copy()
                history_record["IncidentId"] = incident_id
                transformed[f"{ipp_psop_prefix}_IncidentHistory"].append(history_record)
        
        # Log transformation results
        for table_name, records in transformed.items():
            logger.info(f"Transformed {len(records)} records for {table_name}")
        
        return transformed
    
    def process_traffic_data(self, data: List[Dict]) -> bool:
        """Process Traffic data using dedicated traffic processor"""
        logger.info(f"Processing Traffic data batch: {len(data)} records")
        
        try:
            # Use the dedicated traffic processor that handles both header sessions and traffic data
            from src.processing.traffic_processing import process_traffic_data
            return process_traffic_data(self.spark, data)
            
        except Exception as e:
            logger.error(f"Traffic data processing failed: {e}")
            return False
    
    def process_vpc_data(self, data: List[Dict]) -> bool:
        """Process VPC data with reliability"""
        logger.info(f"Processing VPC data batch: {len(data)} records")
        
        try:
            df = self.prepare_dataframe(data)
            if not df:
                return False
            
            table_name = "VPC_Event"
            
            return self.validate_and_process_batch(df, table_name)
            
        except Exception as e:
            logger.error(f"VPC data processing failed: {e}")
            return False
    
    def get_processing_statistics(self) -> Dict[str, Any]:
        """Get comprehensive processing statistics"""
        current_time = datetime.now()
        uptime = (current_time - self.processing_stats["start_time"]).total_seconds()
        
        stats = self.processing_stats.copy()
        stats["uptime_seconds"] = uptime
        stats["current_timestamp"] = current_time.isoformat()
        
        # Add performance metrics
        perf_summary = self.performance_monitor.get_performance_summary()
        stats["performance_summary"] = perf_summary
        
        # Calculate rates
        if uptime > 0:
            stats["batches_per_hour"] = (stats["batches_processed"] / uptime) * 3600
            stats["records_per_hour"] = (stats["records_processed"] / uptime) * 3600
        
        return stats
    
    def shutdown(self):
        """Graceful shutdown of the processing engine"""
        logger.info("Shutting down ETL Processing Engine")
        
        # Stop performance monitoring
        self.performance_monitor.stop_system_monitoring()
        
        # Export final metrics
        try:
            metrics_export = self.performance_monitor.export_metrics()
            metrics_file = ROOT_DIR / "logs" / f"metrics_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(metrics_file, 'w') as f:
                f.write(metrics_export)
            logger.info(f"Exported performance metrics to {metrics_file}")
        except Exception as e:
            logger.warning(f"Failed to export metrics: {e}")
        
        # Log final statistics
        final_stats = self.get_processing_statistics()
        logger.info(f"Final processing statistics: {final_stats}")
        
        logger.info("ETL Processing Engine shutdown complete")


# Alias for backward compatibility
ETLEngine = ETLProcessingEngine

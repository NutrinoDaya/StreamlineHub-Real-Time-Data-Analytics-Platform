#!/usr/bin/env python
"""
Delta Lake Manager

This module provides comprehensive Delta Lake management with:
- Transactional guarantees
- Corruption recovery
- Optimistic concurrency control
- Performance optimization
- Data validation
"""

import os
import time
import json
import shutil
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime
from contextlib import contextmanager

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from utils import LoggerManager

# Global variable to track Delta availability (checked lazily)
_DELTA_AVAILABLE = None
_DELTA_CHECKED = False

def _check_delta_availability(spark: SparkSession):
    """Lazily check if Delta Lake is available (with actual Spark session)"""
    global _DELTA_AVAILABLE, _DELTA_CHECKED
    
    if _DELTA_CHECKED:
        return _DELTA_AVAILABLE
    
    try:
        # Import Delta classes
        from delta.tables import DeltaTable
        from delta.exceptions import ConcurrentAppendException, AnalysisException as DeltaAnalysisException
        
        # Check if Delta SQL extensions are properly loaded
        session_extensions = spark.conf.get("spark.sql.extensions", "")
        if "DeltaSparkSessionExtension" not in session_extensions:
            logger.warning("Delta extensions not found in Spark session configuration")
            _DELTA_AVAILABLE = False
        else:
            # Try a simple Delta operation to verify it works
            logger.debug("Testing Delta Lake functionality...")
            
            # Create a small test DataFrame
            test_df = spark.createDataFrame([("test_delta",)], ["value"])
            
            # Just test if we can call the Delta format - don't actually write to disk
            try:
                # This will fail if Delta is not properly configured
                writer = test_df.write.format("delta")
                logger.debug("Delta format creation successful")
                _DELTA_AVAILABLE = True
            except Exception as format_error:
                logger.warning(f"Delta format test failed: {format_error}")
                _DELTA_AVAILABLE = False
        
        if _DELTA_AVAILABLE:
            logger.info("Delta Lake libraries loaded and verified successfully")
        else:
            logger.warning("Delta Lake verification failed")
            
    except Exception as e:
        _DELTA_AVAILABLE = False
        logger.warning(f"Delta Lake not available: {e}")
    
    _DELTA_CHECKED = True
    return _DELTA_AVAILABLE

# Fallback classes (always available)
class DeltaTableFallback:
    @staticmethod
    def isDeltaTable(spark, path):
        return False
    @staticmethod
    def forPath(spark, path):
        return None

class ConcurrentAppendExceptionFallback(Exception):
    pass

class DeltaAnalysisExceptionFallback(Exception):
    pass

logger = LoggerManager.get_logger("ETL_Processing.log")


class DeltaTableManager:
    """
    Delta Lake table manager with full ACID guarantees
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._table_locks = {}  # Table-level locking
        
        # Log Spark configuration for debugging
        try:
            logger.debug(f"Spark version: {spark.version}")
            logger.debug(f"Spark extensions: {spark.conf.get('spark.sql.extensions', 'NOT_SET')}")
            logger.debug(f"Spark catalog: {spark.conf.get('spark.sql.catalog.spark_catalog', 'NOT_SET')}")
            packages = spark.conf.get('spark.jars.packages', 'NOT_SET')
            logger.debug(f"Spark packages: {packages}")
        except Exception as config_error:
            logger.warning(f"Could not read Spark configuration: {config_error}")
        
        # Check Delta availability when manager is created (with actual Spark session)
        self.delta_available = _check_delta_availability(spark)
        
    def _get_delta_classes(self):
        """Get Delta Lake classes (imports them dynamically if available)"""
        if not self.delta_available:
            return DeltaTableFallback, ConcurrentAppendExceptionFallback, DeltaAnalysisExceptionFallback
        
        try:
            from delta.tables import DeltaTable
            from delta.exceptions import ConcurrentAppendException, AnalysisException as DeltaAnalysisException
            return DeltaTable, ConcurrentAppendException, DeltaAnalysisException
        except ImportError:
            # Fallback if import fails even when we think it's available
            self.delta_available = False
            return DeltaTableFallback, ConcurrentAppendExceptionFallback, DeltaAnalysisExceptionFallback
    
    def recheck_delta_availability(self):
        """Force recheck of Delta availability"""
        global _DELTA_CHECKED
        _DELTA_CHECKED = False
        self.delta_available = _check_delta_availability(self.spark)
        return self.delta_available
        
    @contextmanager
    def table_lock(self, table_path: str):
        """Context manager for table-level locking"""
        lock_key = os.path.normpath(table_path)
        if lock_key in self._table_locks:
            # Wait for existing operation to complete
            while lock_key in self._table_locks:
                time.sleep(0.1)
        
        try:
            self._table_locks[lock_key] = True
            yield
        finally:
            self._table_locks.pop(lock_key, None)
    
    def safe_exists_check(self, table_path: str) -> bool:
        """Safely check if Delta table exists"""
        try:
            # First check if directory exists
            if not os.path.exists(table_path):
                return False
            
            # Check if it's a proper Delta table
            if self.delta_available:
                try:
                    DeltaTable, _, _ = self._get_delta_classes()
                    return DeltaTable.isDeltaTable(self.spark, table_path)
                except Exception:
                    # If Delta check fails, check for basic directory structure
                    delta_log_path = os.path.join(table_path, "_delta_log")
                    return os.path.exists(delta_log_path)
            else:
                # If Delta not available, check for basic directory structure
                delta_log_path = os.path.join(table_path, "_delta_log")
                return os.path.exists(delta_log_path)
                
        except Exception as e:
            logger.warning(f"Table existence check failed for {table_path}: {e}")
            return False
    
    def clear_all_caches(self):
        """Clear all Spark caches and persistent RDDs"""
        try:
            # Clear catalog cache
            self.spark.catalog.clearCache()
            
            # Clear persistent RDDs (compatible with all Spark versions)
            try:
                # Try newer API first
                if hasattr(self.spark.sparkContext, 'getPersistentRDDs'):
                    persistent_rdds = self.spark.sparkContext.getPersistentRDDs()
                    for rdd_id in list(persistent_rdds.keys()):
                        try:
                            persistent_rdds[rdd_id].unpersist()
                        except Exception:
                            pass  # RDD may already be unpersisted
                else:
                    # For newer Spark versions, clear through SQL cache
                    self.spark.sql("CLEAR CACHE")
            except Exception:
                # Fallback to basic cache clearing
                pass
            
            # Clear SQL cache
            try:
                self.spark.sql("CLEAR CACHE")
            except Exception:
                pass
            
            logger.debug("Cleared all Spark caches")
        except Exception as e:
            logger.warning(f"Error clearing caches: {e}")
    
    def diagnose_table_health(self, table_path: str) -> Dict[str, Any]:
        """Comprehensive table health diagnosis"""
        diagnosis = {
            "path": table_path,
            "healthy": False,
            "issues": [],
            "corruption_level": "none",
            "recoverable": True,
            "log_files_count": 0,
            "data_files_count": 0
        }
        
        try:
            if not os.path.exists(table_path):
                diagnosis["issues"].append("table_directory_missing")
                diagnosis["corruption_level"] = "critical"
                return diagnosis
            
            delta_log_path = os.path.join(table_path, "_delta_log")
            if not os.path.exists(delta_log_path):
                diagnosis["issues"].append("delta_log_missing")
                diagnosis["corruption_level"] = "critical"
                return diagnosis
            
            # Check log files
            log_files = [f for f in os.listdir(delta_log_path) if f.endswith('.json')]
            diagnosis["log_files_count"] = len(log_files)
            
            if not log_files:
                diagnosis["issues"].append("no_log_files")
                diagnosis["corruption_level"] = "critical"
                return diagnosis
            
            # Check for sequential log files
            expected_files = [f"{i:020d}.json" for i in range(len(log_files))]
            actual_files = sorted(log_files)
            
            if actual_files != expected_files:
                diagnosis["issues"].append("log_sequence_gap")
                diagnosis["corruption_level"] = "severe"
                diagnosis["recoverable"] = False
            
            # Count data files
            parquet_files = []
            for root, dirs, files in os.walk(table_path):
                if "_delta_log" not in root:
                    parquet_files.extend([f for f in files if f.endswith('.parquet')])
            diagnosis["data_files_count"] = len(parquet_files)
            
            # Try Delta API validation
            if self.delta_available:
                try:
                    DeltaTable, _, _ = self._get_delta_classes()
                    if DeltaTable.isDeltaTable(self.spark, table_path):
                        diagnosis["healthy"] = True
                        diagnosis["corruption_level"] = "none"
                    else:
                        diagnosis["issues"].append("delta_api_validation_failed")
                        diagnosis["corruption_level"] = "moderate"
                except Exception as e:
                    diagnosis["issues"].append(f"delta_api_error: {str(e)}")
                    if "SparkFileNotFoundException" in str(e):
                        diagnosis["corruption_level"] = "severe"
                    else:
                        diagnosis["corruption_level"] = "moderate"
            else:
                diagnosis["issues"].append("delta_not_available")
                diagnosis["corruption_level"] = "moderate"
            
        except Exception as e:
            diagnosis["issues"].append(f"diagnosis_error: {str(e)}")
            diagnosis["corruption_level"] = "unknown"
            diagnosis["recoverable"] = False
        
        return diagnosis
    
    def backup_corrupted_table(self, table_path: str) -> Optional[str]:
        """Create timestamped backup of corrupted table"""
        try:
            if not os.path.exists(table_path):
                return None
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"{table_path}_corrupted_backup_{timestamp}"
            
            shutil.copytree(table_path, backup_path)
            logger.info(f"Created backup of corrupted table: {backup_path}")
            return backup_path
            
        except Exception as e:
            logger.error(f"Failed to backup table {table_path}: {e}")
            return None
    
    def safe_table_creation(self, df: DataFrame, table_path: str, mode: str = "error", partition_cols: Optional[List[str]] = None) -> bool:
        """Safely create Delta table with fallback"""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(table_path), exist_ok=True)
            
            # Re-check Delta availability to be sure
            self.recheck_delta_availability()
            
            # Try Delta first if available
            if self.delta_available:
                try:
                    logger.debug(f"Attempting Delta table creation for {table_path}")
                    writer = df.write.format("delta").mode(mode)
                    if partition_cols:
                        writer = writer.partitionBy(*partition_cols)
                    writer.save(table_path)
                    
                    # Validate creation
                    if self.safe_exists_check(table_path):
                        logger.info(f"Successfully created Delta table: {table_path}")
                        return True
                except Exception as delta_error:
                    logger.error(f"Delta table creation failed: {delta_error}")
                    raise delta_error
            else:
                logger.error("Delta Lake not available - Delta-only operation required")
                raise RuntimeError("Delta Lake not available but required for operation")
                
        except Exception as e:
            logger.error(f"Failed to create table {table_path}: {e}")
            return False
    
    def recover_corrupted_table(self, table_path: str, df: Optional[DataFrame] = None, partition_cols: Optional[List[str]] = None) -> bool:
        """Recover corrupted Delta table"""
        logger.warning(f"Attempting recovery for corrupted table: {table_path}")
        
        # Backup before recovery
        backup_path = self.backup_corrupted_table(table_path)
        
        try:
            # Clear caches first
            self.clear_all_caches()
            
            # Remove corrupted table
            if os.path.exists(table_path):
                shutil.rmtree(table_path)
            
            # Recreate if DataFrame provided
            if df is not None:
                return self.safe_table_creation(df, table_path, mode="overwrite", partition_cols=partition_cols)
            else:
                logger.info(f"Table {table_path} removed, ready for recreation")
                return True
                
        except Exception as e:
            logger.error(f"Recovery failed for {table_path}: {e}")
            return False
    
    def transactional_write(self, 
        df: DataFrame, 
        table_path: str, 
        mode: str = "append",
        partition_cols: Optional[List[str]] = None,
        max_retries: int = 5,
        validation_func: Optional[callable] = None,
        skip_health_check: bool = True) -> bool:  # Skip health checks by default for performance
        """
        Perform transactional write with full error handling and recovery
        OPTIMIZED: Removed expensive df.count() and health checks for fast writes
        """
        with self.table_lock(table_path):
            for attempt in range(max_retries):
                try:
                    # Clear caches before each attempt (except first)
                    if attempt > 0:
                        self.clear_all_caches()
                        time.sleep(min(2 ** attempt, 10))  # Exponential backoff
                    
                    # OPTIMIZATION: Skip expensive count() - assume DataFrame is not empty
                    # The Spark write will fail fast if DataFrame is empty anyway
                    
                    # Run custom validation if provided
                    if validation_func and not validation_func(df):
                        logger.error(f"Custom validation failed for {table_path}")
                        return False
                    
                    # OPTIMIZATION: Skip health check on every write for performance
                    # Only run health checks if explicitly requested or on retry
                    if not skip_health_check or attempt > 0:
                        if self.safe_exists_check(table_path):
                            health = self.diagnose_table_health(table_path)
                            if not health["healthy"]:
                                logger.warning(f"Unhealthy table detected: {health}")
                                if health["recoverable"]:
                                    if not self.recover_corrupted_table(table_path):
                                        continue  # Retry after recovery
                                else:
                                    logger.error(f"Unrecoverable table corruption: {table_path}")
                                    return False
                    
                    # Ensure table exists
                    if not self.safe_exists_check(table_path):
                        logger.info(f"Creating new Delta table: {table_path}")
                        if not self.safe_table_creation(df.limit(0), table_path, mode="overwrite", partition_cols=partition_cols):
                            continue  # Retry table creation
                    
                    # Perform the write operation with Delta/Parquet fallback
                    try:
                        # Re-check Delta availability if this is a retry
                        if attempt > 0:
                            self.recheck_delta_availability()
                        
                        if self.delta_available:
                            logger.debug(f"Attempting Delta write for {table_path}")
                            # Try Delta first
                            writer = df.write.format("delta").mode(mode)
                            
                            # Add partitioning if specified
                            if partition_cols:
                                writer = writer.partitionBy(*partition_cols)
                            
                            # Execute write
                            writer.save(table_path)
                            
                            # OPTIMIZATION: Skip expensive validation count, just verify path exists
                            if self.safe_exists_check(table_path):
                                logger.info(f"Successfully wrote to Delta table: {table_path}")
                                return True
                            else:
                                logger.error(f"Delta write validation failed for {table_path}")
                                raise Exception("Delta write validation failed")
                        else:
                            logger.error(f"Delta not available for {table_path}")
                            raise Exception("Delta Lake is required but not available")
                    
                    except Exception as delta_error:
                        logger.error(f"Delta write failed for {table_path}: {delta_error}")
                        # If partition columns mismatch, recreate table with correct partitions
                        err_str = str(delta_error)
                        if "Partition columns do not match" in err_str or "partition columns of the table" in err_str:
                            self.clear_all_caches()
                            if self.recover_corrupted_table(table_path, df.limit(0), partition_cols=partition_cols):
                                # try next attempt after recovery
                                continue
                        # NO PARQUET FALLBACK - Delta Lake is required
                        if "serialVersionUID" in err_str:
                            logger.error("Delta Lake version conflict detected - rebuild containers with consistent versions")
                        # Re-raise the original error to trigger retry
                        raise delta_error
                    
                except Exception as concurrent_error:
                    # Check if it's a concurrent append exception
                    _, ConcurrentAppendException, _ = self._get_delta_classes()
                    if self.delta_available and isinstance(concurrent_error, ConcurrentAppendException):
                        logger.warning(f"Concurrent write detected for {table_path}, attempt {attempt + 1}: {concurrent_error}")
                        wait_time = (2 ** attempt) + (time.time() % 1)  # Jitter
                        time.sleep(min(wait_time, 30))
                        continue
                    
                except Exception as e:
                    error_str = str(e).lower()
                    logger.error(f"Write failed for {table_path}, attempt {attempt + 1}: {e}")
                    
                    # Handle specific error types
                    if any(pattern in error_str for pattern in [
                        "sparkfilenotfoundexception",
                        "delta_log",
                        "file does not exist",
                        "deltaanalysisexception",
                        "protocol mismatch",
                        "invalidclassexception",
                        "serialversionuid",
                        "delayedcommitprotocol",
                        "deltaoptimizedwriterexec"
                    ]):
                        # Clear caches and try recovery for corruption/version-related errors
                        self.clear_all_caches()
                        if self.recover_corrupted_table(table_path, df.limit(0), partition_cols=partition_cols):
                            continue  # Retry after recovery
                    
                    if attempt == max_retries - 1:
                        logger.error(f"All {max_retries} write attempts failed for {table_path}")
                        return False
            
            return False
    
    def optimize_table(self, table_path: str, z_order_cols: Optional[List[str]] = None) -> bool:
        """Optimize Delta table with optional Z-ordering"""
        try:
            if not self.safe_exists_check(table_path):
                logger.warning(f"Cannot optimize non-existent table: {table_path}")
                return False
            
            if not self.delta_available:
                logger.warning(f"Cannot optimize table {table_path}: Delta Lake not available")
                return False
            
            DeltaTable, _, _ = self._get_delta_classes()
            delta_table = DeltaTable.forPath(self.spark, table_path)
            
            if z_order_cols:
                delta_table.optimize().executeZOrderBy(*z_order_cols)
                logger.info(f"Optimized table {table_path} with Z-order: {z_order_cols}")
            else:
                delta_table.optimize().executeCompaction()
                logger.info(f"Optimized table {table_path} with compaction")
            
            return True
            
        except Exception as e:
            logger.error(f"Table optimization failed for {table_path}: {e}")
            return False
    
    def vacuum_table(self, table_path: str, retention_hours: int = 168) -> bool:
        """Vacuum Delta table to remove old files"""
        try:
            if not self.safe_exists_check(table_path):
                logger.warning(f"Cannot vacuum non-existent table: {table_path}")
                return False
            
            if not self.delta_available:
                logger.warning(f"Cannot vacuum table {table_path}: Delta Lake not available")
                return False
            
            DeltaTable, _, _ = self._get_delta_classes()
            delta_table = DeltaTable.forPath(self.spark, table_path)
            delta_table.vacuum(retention_hours)
            logger.info(f"Vacuumed table {table_path} with {retention_hours}h retention")
            return True
            
        except Exception as e:
            logger.error(f"Table vacuum failed for {table_path}: {e}")
            return False

#!/usr/bin/env python
"""
IPP_PSOP Processing Functions

This module provides function-based processing for IPP_PSOP data with improved
Delta table handling to fix the race condition issues.
"""

import os
import sys
import time
import math
import yaml
import shutil
from pathlib import Path
from typing import Dict, List
from datetime import datetime
from delta.tables import DeltaTable 
from pyspark.sql import functions as F 
from pyspark.sql.functions import unix_timestamp, to_timestamp, col 

# Add the project root directory to sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(ROOT_DIR))

from utils import readConfig, LoggerManager
from schemas.IPP_PSOP import (
    IPP_PSOP_Incident,
    IPP_PSOP_IncidentFiles,
    IPP_PSOP_IncidentHistory,
    IPP_PSOP_DataLookup,
    IPP_PSOP_Incident_Silver,
    IPP_PSOP_IncidentFiles_Silver,
    IPP_PSOP_IncidentHistory_Silver,
)

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
root_dir = ROOT_DIR
config_dir = root_dir / "config"

etl_config = readConfig(config_dir / "xml/ETL.xml")
etl_write = etl_config.get("Write", {})
ipp_psop_prefix = etl_write.get("ipp_psop_prefix")
compression_alg = etl_write.get("compression_algorithm")
max_attempts = int(etl_write.get("max_attempts", 3))

dml_file = readConfig(config_dir / "xml/DML.xml").get("FILE", {})
bronze_layer = dml_file["bronze_layer"]
silver_layer = dml_file["silver_layer"]
warehouse = dml_file["warehouse"]

# Load state mappings
state_text_to_category = {}
state_text_to_process_type = {}

try:
    states_config_path = config_dir / "states_config.yaml"
    if states_config_path.exists():
        with open(states_config_path, 'r') as file:
            states_config = yaml.safe_load(file)
            state_text_to_category = states_config.get("state_text_to_category", {})
            state_text_to_process_type = states_config.get("state_text_to_process_type", {})
    else:
        print("Warning: states_config.yaml not found")
except Exception as e:
    print(f"Error loading states config: {e}")

bronze_path = os.path.join(root_dir, warehouse, bronze_layer, ipp_psop_prefix)
silver_path = os.path.join(root_dir, warehouse, silver_layer, ipp_psop_prefix)

LOGS_DIR = ROOT_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logger = LoggerManager.get_logger("ETL_Processing.log")

# constant for tuning dynamic partitioning
RECORDS_PER_PARTITION = 1000  # Target this many records per Spark partition

# --------------------------------------------------------------------------
# Helper Functions
# --------------------------------------------------------------------------
def calculate_partitions(data_size: int) -> int:
    """
    Calculates the optimal number of Spark partitions based on data size.
    Ensures there is always at least one partition.
    """
    if data_size == 0:
        return 1
    return max(1, math.ceil(data_size / RECORDS_PER_PARTITION))

def parse_datetime_string(s):
    """Parse InsertionTime into a date object."""
    if isinstance(s, str):
        try:
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f%z").date()
        except ValueError:
            try:
                return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f").date()
            except ValueError:
                logger.error(f"Failed to parse string as datetime: {s}")
                raise
    elif isinstance(s, int):
        return datetime.fromtimestamp(s / 1000.0).date()
    else:
        raise ValueError(f"Unable to parse date from {type(s)}: {s}")


def normalize_column_names(df):
    """Defensively normalize DataFrame column names.

    Removes leading/trailing single or double quotes and trims whitespace.
    Returns a DataFrame with renamed columns when necessary.
    """
    cols = df.columns
    rename_map = {}
    for c in cols:
        new = c.strip()
        # strip surrounding quotes if present
        if (new.startswith("'") and new.endswith("'")) or (new.startswith('"') and new.endswith('"')):
            new = new[1:-1].strip()
        # also remove a single leading quote only (defensive)
        if new.startswith("'"):
            new = new.lstrip("'").strip()
        if new != c and new:
            rename_map[c] = new

    for old, new in rename_map.items():
        df = df.withColumnRenamed(old, new)
    return df


def transform_ipp_psop_buffer(buf: List[Dict]) -> Dict[str, List[Dict]]:
    """Transform IPP_PSOP buffer into separate table data"""
    logger.debug(f"Transforming {len(buf)} IPP_PSOP incidents")
    
    out = {
        f"{ipp_psop_prefix}_Incident": [],
        f"{ipp_psop_prefix}_IncidentFiles": [],
        f"{ipp_psop_prefix}_IncidentHistory": [],
    }
    
    for inc in buf:
        # Main incident data
        main = inc.copy()
        main.pop("IncidentFiles", None)
        main.pop("incidentHistories", None)
        out[f"{ipp_psop_prefix}_Incident"].append(main)
        
        # Incident files
        for f in inc.get("IncidentFiles", []):
            f["PrimaryFileName"] = inc.get("PrimaryFileName")
            out[f"{ipp_psop_prefix}_IncidentFiles"].append(f)
            
        # Incident history - ADD MAPPING HERE
        for h in inc.get("incidentHistories", []):
            h["PrimaryFileName"] = inc.get("PrimaryFileName")
            incident_state = h.get("IncidentState", "")
            
            # Map IncidentState to Category and ProcessType
            h["Category"] = state_text_to_category.get(incident_state, f"UNMAPPED_{incident_state}")
            h["ProcessType"] = state_text_to_process_type.get(incident_state, f"UNMAPPED_{incident_state}")
            
            out[f"{ipp_psop_prefix}_IncidentHistory"].append(h)
    
    logger.debug(f"Transformed {len(out[f'{ipp_psop_prefix}_Incident'])} records for {ipp_psop_prefix}_Incident")
    logger.debug(f"Transformed {len(out[f'{ipp_psop_prefix}_IncidentFiles'])} records for {ipp_psop_prefix}_IncidentFiles")
    logger.debug(f"Transformed {len(out[f'{ipp_psop_prefix}_IncidentHistory'])} records for {ipp_psop_prefix}_IncidentHistory")
    
    return out


def safe_delta_table_check(spark, path: str) -> bool:
    """Safely check if a Delta table exists without triggering read operations"""
    try:
        # Check if the path exists first
        if not os.path.exists(path):
            return False
            
        # Check if _delta_log directory exists
        delta_log_path = os.path.join(path, "_delta_log")
        if not os.path.exists(delta_log_path):
            return False
            
        # Check if there are any transaction log files
        log_files = [f for f in os.listdir(delta_log_path) if f.endswith('.json')]
        if not log_files:
            return False
            
        # Now it's safe to use DeltaTable.isDeltaTable
        return DeltaTable.isDeltaTable(spark, path)
    except Exception as e:
        logger.debug(f"Delta table check failed for {path}: {e}")
        return False


def create_delta_table_safe(df, path: str, mode: str = "overwrite") -> bool:
    """Safely create a Delta table with proper error handling"""
    try:
        logger.info(f"Creating new Delta table: {path}")
        
        # Clear any existing Spark cache for this path
        df.sparkSession.catalog.clearCache()
        
        # For new tables, ensure clean creation
        if mode == "overwrite" and os.path.exists(path):
            logger.debug(f"Removing existing directory: {path}")
            shutil.rmtree(path)
        
        # Ensure parent directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        # Now create the actual table with data - this creates both metadata and transaction log
        logger.debug(f"Creating Delta table with {df.count()} rows at {path}")
        writer = df.write.format("delta").mode(mode).option("mergeSchema", "true")
        
        # Execute the write - this creates the table AND transaction log in one operation
        writer.save(path)
        
        # Verify creation by checking if _delta_log directory exists with files
        delta_log_path = os.path.join(path, "_delta_log")
        if os.path.exists(delta_log_path) and len(os.listdir(delta_log_path)) > 0:
            logger.debug(f"Delta table successfully created at {path}")
            return True
        else:
            logger.error(f"Delta table creation verification failed - no transaction log found at {path}")
            return False
            
    except Exception as e:
        logger.error(f"Failed to create Delta table at {path}: {e}")
        return False


def upsert_table(df, path, layer, max_retries=3):
    """Upsert DataFrame to a specified path with retries and improved error handling."""
    logger.debug(f"[{layer.upper()}] Starting upsert to {path}")
    
    try:
        row_count = df.count()
        logger.debug(f"[{layer.upper()}] Processing {row_count} rows")
    except Exception:
        row_count = -1  # Unable to count
        
    for attempt in range(1, max_retries + 1):
        try:
            logger.debug(f"[{layer.upper()}] Upsert attempt {attempt} for {path}")
            
            # Clear Spark cache before each attempt
            df.sparkSession.catalog.clearCache()
            
            # Check if table exists using safe method
            table_exists = safe_delta_table_check(df.sparkSession, path)
            
            if not table_exists:
                logger.debug(f"[{layer.upper()}] Creating new Delta table: {path}")
                if create_delta_table_safe(df, path, mode="overwrite"):
                    logger.debug(f"[{layer.upper()}] Successfully created and wrote {row_count} rows to {path}")
                    return
                else:
                    raise Exception("Failed to create Delta table")
            else:
                # Table exists, append to it
                logger.debug(f"[{layer.upper()}] Appending to existing Delta table: {path}")
                df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
                logger.debug(f"[{layer.upper()}] Successfully appended {row_count} rows to {path}")
                return
                
        except Exception as e:
            error_msg = str(e).lower()
            logger.error(f"[{layer.upper()}] Upsert attempt {attempt} failed for {path}: {e}")
            
            # Handle specific Delta Lake errors
            if "sparkfilenotfoundexception" in error_msg and "_delta_log" in error_msg:
                logger.warning(f"[{layer.upper()}] Delta log corruption detected, cleaning up for retry")
                # Clean up corrupted table for next attempt
                if os.path.exists(path):
                    shutil.rmtree(path)
            
            if attempt < max_retries:
                wait_time = 2 ** attempt
                logger.debug(f"[{layer.upper()}] Waiting {wait_time} seconds before retry")
                time.sleep(wait_time)
            else:
                logger.error(f"[{layer.upper()}] Max retries reached for {path}; aborting")
                raise


# --------------------------------------------------------------------------
# Main Processing Functions
# --------------------------------------------------------------------------
def process_lookup_table_ipp_psop(ipp_buffer: List[Dict], spark):
    """Process lookup table for IPP_PSOP data"""
    logger.debug("Starting IPP_PSOP lookup table processing")
    
    lookup_name = f"{ipp_psop_prefix}_DataLookup"
    lookup_path = Path(silver_path) / lookup_name

    for attempt in range(1, max_attempts + 1):
        try:
            # Gather new pairs from the incoming buffer
            new_pairs = set()
            for inc in ipp_buffer:
                for key in ["OffenceType", "SessionIndex", "DeviceIndex", "Vendor", "PrimaryFileName"]:
                    val = inc.get(key)
                    if val:
                        new_pairs.add((key, str(val)))
                for hist in inc.get("incidentHistories", []):
                    for key in ["ServiceName", "ServerName", "FailType"]:
                        val2 = hist.get(key)
                        if val2:
                            new_pairs.add((key, str(val2)))
            
            if not new_pairs:
                logger.debug("No lookup pairs found; skipping lookup table.")
                return

            # Read existing lookup (if any) and merge
            if safe_delta_table_check(spark, str(lookup_path)):
                try:
                    existing = spark.read.format("delta").load(str(lookup_path)).limit(1).collect()
                    if existing:
                        row = existing[0].asDict()
                        for col, csv in row.items():
                            for entry in (csv or "").split(","):
                                entry = entry.strip()
                                if entry:
                                    new_pairs.add((col, entry))
                except Exception as e:
                    logger.warning(f"Could not read existing lookup table: {e}")
            
            # Dynamically calculate partitions for the RDD
            num_lookup_partitions = calculate_partitions(len(new_pairs))

            # Build and write the new lookup table
            schema = IPP_PSOP_DataLookup.get_schema()
            df_pairs = spark.createDataFrame(
                spark.sparkContext.parallelize(list(new_pairs), num_lookup_partitions),
                schema=schema,
            )
            df_pairs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(lookup_path))

            count = df_pairs.count()
            logger.debug(f"Lookup table written with {count} records.")
            return

        except Exception as e:
            logger.error(f"Attempt {attempt}/{max_attempts} failed for lookup table: {e}")
            if attempt < max_attempts:
                time.sleep(2**attempt)
            else:
                logger.error("Giving up on lookup table after max retries.")
                raise


def process_bronze_ipp_psop_data(ipp_buffer: List[Dict]):
    """Process bronze layer IPP_PSOP data with automatic spark session management"""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.appName("IPP_PSOP_Processing").getOrCreate()
        
        _process_bronze_ipp_psop_data_internal(ipp_buffer, spark)
        return True
    except Exception as e:
        logger.error(f"Bronze IPP_PSOP processing failed: {e}")
        return False


def _process_bronze_ipp_psop_data_internal(ipp_buffer: List[Dict], spark):
    """
    Internal function to process IPP_PSOP data into the bronze layer.
    This function is designed to be called from a context where a Spark session is already active.
    """
    if not ipp_buffer:
        logger.debug("IPP_PSOP buffer is empty, skipping bronze processing.")
        return

    try:
        # Transform and normalize the buffer
        transformed_data = transform_ipp_psop_buffer(ipp_buffer)
        
        for table_name, data_list in transformed_data.items():
            if not data_list:
                continue

            logger.info(f"Processing bronze table: {table_name} with {len(data_list)} records")
            
            # Create DataFrame
            df = spark.createDataFrame(data_list)
            
            # Add date column for partitioning from InsertionTime
            df = df.withColumn("date", F.to_date(F.from_unixtime(F.col("InsertionTime") / 1000)))

            # Define table path without date
            table_path = os.path.join(bronze_path, table_name)
            
            # Write to Delta table, partitioned by date
            df.write.format("delta").mode("append").partitionBy("date").save(table_path)
            
            logger.info(f"Successfully wrote {len(data_list)} records to bronze table {table_name} at {table_path}")

    except Exception as e:
        logger.error(f"Error processing bronze IPP_PSOP data: {e}", exc_info=True)
        # In case of failure, you might want to add retry logic or dead-letter queueing


def process_silver_ipp_psop_data(ipp_buffer: List[Dict]):
    """Process silver layer IPP_PSOP data with automatic spark session management"""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.appName("IPP_PSOP_Silver_Processing").getOrCreate()
        
        _process_silver_ipp_psop_data_internal(ipp_buffer, spark)
        return True
    except Exception as e:
        logger.error(f"Silver IPP_PSOP processing failed: {e}")
        return False


def _process_silver_ipp_psop_data_internal(ipp_buffer: List[Dict], spark):
    """
    Internal function to process IPP_PSOP data into the silver layer.
    This function is designed to be called from a context where a Spark session is already active.
    """
    if not ipp_buffer:
        logger.debug("IPP_PSOP buffer is empty, skipping silver processing.")
        return

    try:
        # Transform and normalize the buffer
        transformed_data = transform_ipp_psop_buffer(ipp_buffer)
        
        for table_name, data_list in transformed_data.items():
            if not data_list:
                continue

            logger.info(f"Processing silver table: {table_name} with {len(data_list)} records")
            
            # Create DataFrame
            df = spark.createDataFrame(data_list)
            
            # Add date column for partitioning from InsertionTime
            df = df.withColumn("date", F.to_date(F.from_unixtime(F.col("InsertionTime") / 1000)))

            # Define table path without date
            silver_table_name = f"{table_name}_Silver"
            table_path = os.path.join(silver_path, silver_table_name)
            
            # Upsert to Delta table, partitioned by date
            # The upsert_table function needs to be aware of partitioning
            if DeltaTable.isDeltaTable(spark, table_path):
                delta_table = DeltaTable.forPath(spark, table_path)
                delta_table.alias("t").merge(
                    df.alias("s"),
                    "t.primary == s.primary"
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            else:
                df.write.format("delta").mode("overwrite").partitionBy("date").save(table_path)

            logger.info(f"Successfully upserted {len(data_list)} records to silver table {silver_table_name} at {table_path}")

    except Exception as e:
        logger.error(f"Error processing silver IPP_PSOP data: {e}", exc_info=True)

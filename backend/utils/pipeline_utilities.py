from functools import reduce
from pathlib import Path
import traceback
from typing import Any, List, Optional, Tuple
from pyspark.sql import DataFrame, SparkSession 
from datetime import datetime, timedelta, timezone
from pyspark.sql.functions import col, lit, current_timestamp 
from pyspark.sql.types import StructType, LongType 
import os
import sys
import yaml

# --- OpenTelemetry Imports ---
from opentelemetry import trace
from opentelemetry.trace.status import Status, StatusCode

# Setup paths and logger
ROOT_DIR = Path(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(str(ROOT_DIR))

from utils import LoggerManager, readConfig

logger = LoggerManager.get_logger("ETL_Processing.log")
tracer = trace.get_tracer(__name__)

# Load ETL config
etl_cfg = readConfig(str(ROOT_DIR / "config" / "ETL.xml"))["Write"]
ipp_psop_prefix = etl_cfg.get("ipp_psop_prefix")
num_partitions_cfg_str = etl_cfg.get("partitions")

# ------------------------------------------------------------------------------
# YAML loader
# ------------------------------------------------------------------------------
def load_yaml_config(yaml_path: Path) -> dict:
    with open(yaml_path, "r") as f:
        return yaml.safe_load(f)


def read_delta_table(
    spark: SparkSession,
    root_dir: str,
    warehouse: str,
    layer: str,
    database: str,
    table_name: str,
    is_partitioned: bool = True, 
    start_threshold: Optional[int] = None,
    end_threshold: Optional[int] = None,
    schema: Optional[StructType] = None,
    timestamp_column: str = "InsertionTime"
) -> DataFrame:
    """
    Reads a Delta table, handling both non-partitioned and date-partitioned layouts.

    Args:
        spark: The active SparkSession.
        root_dir, warehouse, layer, database, table_name: Path components for the table.
        is_partitioned: If True (default), scans date-partitioned folders (e.g., /YYYY-MM-DD/table).
                        If False, reads directly from the base path (e.g., /database/table).
        start_threshold, end_threshold: Optional time range in milliseconds for filtering.
        schema: Optional schema to apply if the table read fails or is empty.
        timestamp_column: The column to use for time-based filtering.
    """
    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("read_delta_table_utility") as span:
        # Set common trace attributes
        span.set_attribute("table.name", table_name)
        span.set_attribute("data.layer", layer)
        span.set_attribute("table.is_partitioned", is_partitioned)

        try:
            # --- Path component validation ---
            if not all([root_dir, warehouse, layer, database, table_name]):
                raise ValueError("Missing required path component for Delta table read.")
            
            base_path = Path(root_dir) / warehouse / layer / database
            
            # --- LOGIC FOR NON-PARTITIONED TABLES ---
            if not is_partitioned:
                table_path = base_path / table_name
                logger.info(f"Reading NON-PARTITIONED Delta table: {table_path}")
                span.set_attribute("table.path", str(table_path))

                if not table_path.exists() or not (table_path / "_delta_log").is_dir():
                    logger.warning(f"Non-partitioned Delta table not found at path: {table_path}")
                    return spark.createDataFrame([], schema or StructType([]))

                df = spark.read.format("delta").load(str(table_path))
                
                # Apply timestamp filter if provided
                if start_threshold is not None and end_threshold is not None:
                    df = df.filter(
                        (col(timestamp_column) >= lit(start_threshold)) &
                        (col(timestamp_column) <= lit(end_threshold))
                    )
                
                logger.info(f"Successfully loaded {df.count()} rows from {table_path}")
                return df

            # --- LOGIC FOR DATE-PARTITIONED TABLES ---
            else:
                logger.info(f"Reading DATE-PARTITIONED Delta table: {table_name}")
                if start_threshold is None or end_threshold is None:
                    raise ValueError("start_threshold and end_threshold are required for date-partitioned tables.")
                if start_threshold > end_threshold:
                    raise ValueError("start_threshold must be <= end_threshold")

                start_dt_utc = datetime.fromtimestamp(start_threshold / 1000.0, tz=timezone.utc)
                end_dt_utc = datetime.fromtimestamp(end_threshold / 1000.0, tz=timezone.utc)
                logger.debug(f"Scanning UTC date range: {start_dt_utc.date()} to {end_dt_utc.date()}")

                paths_to_read = []
                current_date = start_dt_utc.date()
                while current_date <= end_dt_utc.date():
                    folder_format = current_date.strftime("%Y-%m-%d")
                    path = base_path / folder_format / table_name
                    if path.exists() and (path / "_delta_log").is_dir():
                        paths_to_read.append(str(path))
                    current_date += timedelta(days=1)

                span.set_attribute("partitions.found", len(paths_to_read))
                if not paths_to_read:
                    logger.warning(f"No existing date partitions found for table '{table_name}' in the specified date range.")
                    return spark.createDataFrame([], schema or StructType([]))
                
                # Load all found partitions and union them
                df_list = [spark.read.format("delta").load(p) for p in paths_to_read]
                if not df_list:
                    return spark.createDataFrame([], schema or StructType([]))
                
                combined_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), df_list)

                # Apply final precise timestamp filtering
                filtered_df = combined_df.filter(
                    (col(timestamp_column) >= lit(start_threshold)) &
                    (col(timestamp_column) <= lit(end_threshold))
                )
                
                logger.info(f"Successfully loaded {filtered_df.count()} rows from {len(paths_to_read)} partitions for {table_name}.")
                return filtered_df

        except Exception as e:
            logger.error(f"A critical error occurred in read_delta_table for table '{table_name}': {e}", exc_info=True)
            current_span = trace.get_current_span()
            if current_span.is_recording():
                current_span.record_exception(e)
                current_span.set_status(Status(StatusCode.ERROR, "Failed to read delta table"))
            return spark.createDataFrame([], schema or StructType([]))
        

def run_and_save(
    spark: SparkSession,
    fn,
    fn_args: Tuple[Any, ...],
    root_dir: Path,
    warehouse: str,
    gold_layer: str,
    name: str,  # Table name
    start_ms: int,  # UTC ms
    end_ms: int,    # UTC ms
    num_partitions_cfg: Optional[str] = None,  # num_partitions from config (string)
):
    try:
        default_partitions = 20
        try:
            target_partitions = int(num_partitions_cfg) if num_partitions_cfg is not None else default_partitions
        except ValueError:
            target_partitions = default_partitions

        result = fn(*fn_args)  # Execute the function that generates data

        if result is None:
            logger.debug(f"'{name}' returned None, skipping save.")
            return

        df_to_save = None
        record_count = 0

        if isinstance(result, DataFrame):
            df_to_save = result
            if df_to_save.rdd.isEmpty():  # Check before count for potentially large, empty DFs
                logger.debug(f"'{name}' produced 0 rows (DataFrame is empty), skipping save.")
                return
            record_count = df_to_save.count()
            # df_to_save.show(5)  # Optional: for debugging
        elif isinstance(result, list):
            if not result:  # Empty list
                logger.debug(f"'{name}' produced 0 rows (empty list), skipping save.")
                return
            record_count = len(result)
            try:
                df_to_save = spark.createDataFrame(result)  # Spark will try to infer schema
            except Exception as e:
                logger.error(f"Could not create DataFrame from list for '{name}': {e}. Ensure list contains uniform dicts or Row objects.")
                return  # Skip if DataFrame creation fails
        elif isinstance(result, dict):  # Handle single dict result
            record_count = 1
            try:
                df_to_save = spark.createDataFrame([result])
            except Exception as e:
                logger.error(f"Could not create DataFrame from dict for '{name}': {e}.")
                return
        else:
            logger.error(f"Unrecognized return type for '{name}': {type(result)}, skipping save.")
            return
        
        if record_count == 0:  # Double check after conversion or if it was an empty DataFrame initially
            logger.debug(f"'{name}' resulted in 0 records after processing, skipping save.")
            return

        # Adjust number of partitions based on record count
        effective_partitions = target_partitions
        if record_count <= 5000 and target_partitions > 1:  # Example threshold
            effective_partitions = 1

        save_df_to_delta_by_date(
            df_to_save,
            root_dir=root_dir,
            warehouse=warehouse,
            gold_layer=gold_layer,
            table=name,
            start_ms=start_ms,
            end_ms=end_ms,
            target_partitions=effective_partitions,
        )

    except Exception as e:
        logger.error(f"Error in run_and_save for '{name}': {e}")
        logger.debug(traceback.format_exc())
        
def save_df_to_delta_by_date(
    df: DataFrame,
    root_dir: Path,
    warehouse: str,
    gold_layer: str,
    table: str,
    start_ms: int,  # UTC millisecond timestamp
    end_ms: int,    # UTC millisecond timestamp
    target_partitions: Optional[str] = "20",
):
    """
    Saves a DataFrame to a Delta table inside a date folder in the gold layer.
    The structure is gold_layer/date/table, e.g., processed_data/2025-6-26/avg_processing_time_kpi.
    """
    dt_utc_start = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc)
    # Use portable date formatting consistent with folder naming
    date_str_utc = f"{dt_utc_start.year}-{dt_utc_start.month}-{dt_utc_start.day}"


    df_enriched = (
        df.withColumn("AggregationTime", current_timestamp().cast("timestamp"))
        .withColumn("IntervalStartTime", lit(start_ms).cast(LongType()))
        .withColumn("IntervalEndTime", lit(end_ms).cast(LongType()))
    )

    out_path = root_dir / warehouse / gold_layer / date_str_utc / table
    out_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        effective_partitions = int(target_partitions)
    except (ValueError, TypeError):
        effective_partitions = 20
    
    record_count = df_enriched.count()
    if record_count <= 5000:
        effective_partitions = 1

    logger.info(f"Saving {record_count} records to {out_path}")
    (
        df_enriched.repartition(effective_partitions)
        .write.format("delta")
        .mode("append")
        .option("partitionOverwriteMode", "dynamic")
        .save(str(out_path))
    )
    logger.info(f"Successfully saved data to {out_path}")


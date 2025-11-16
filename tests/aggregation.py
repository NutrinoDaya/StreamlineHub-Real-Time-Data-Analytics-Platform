#!/usr/bin/env python
"""
Aggregation Script to Perform SQL Queries on Delta Tables

This script performs post-processing KPIs, pre-processing aggregations,
VPC/Clerk KPI aggregations, and traffic data aggregations by reading from
Delta Lake tables. Each result is written to the Gold layer via
`save_aggregation_result`.
"""

# ------------------------------------------------------------------------------
# Standard Library Imports
# ------------------------------------------------------------------------------
import os
import sys
from pathlib import Path
import argparse

# ------------------------------------------------------------------------------
# Local imports & path setup
# ------------------------------------------------------------------------------
ROOT_DIR = Path(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(str(ROOT_DIR))

from utils import (get_config_dir,
    initSparkSession,
    LoggerManager,
    readConfig,
    load_yaml_config)
from utils.aggregation_processor import (
    process_traffic_aggregations,
    process_vpc_aggregations,
    load_config_and_data,
)

# ------------------------------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------------------------------
logger = LoggerManager.get_logger("ETL_Processing.log")
etl_cfg = readConfig(str(get_config_dir() / "xml/ETL.xml"))["Write"]
# read paths from config
cfg = readConfig(str(get_config_dir() / "xml/DML.xml"))["FILE"]
warehouse = cfg["warehouse"]
bronze_layer = cfg["bronze_layer"]
silver_layer = cfg["silver_layer"]
gold_layer = cfg["gold_layer"]

# load YAML maps
state_map = load_yaml_config(get_config_dir() / "states_config.yaml")[
    "state_text_to_process_type"
]

# ------------------------------------------------------------------------------
# Main Orchestration Function
# ------------------------------------------------------------------------------
def main(start_threshold_millis: int, end_threshold_millis: int):
    """Main function to orchestrate the aggregation pipeline."""
    spark = initSparkSession("Aggregation Script")
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Aggregation script started for time range: {start_threshold_millis} to {end_threshold_millis}")

    # Load all configurations and data sources
    data = load_config_and_data(spark, start_threshold_millis, end_threshold_millis)
    
    # Extract variables needed for various processes
    warehouse = data.get("warehouse")
    gold_layer = data.get("gold_layer")
    bronze_layer = data.get("bronze_layer")
    
    # Safely get DataFrames using .get() to avoid KeyErrors
    raw_traffic_data = data.get("raw_traffic_data")
    raw_traffic_hdr = data.get("raw_traffic_hdr")
    df_inc = data.get("df_inc")
    df_hist = data.get("df_hist")
    
    logger.info("Starting Traffic Data Aggregations")
    # --- Process traffic aggregations ---
    process_traffic_aggregations(
        spark,
        raw_traffic_data,
        raw_traffic_hdr,
    )

    # --- Process aggregations for the old incident model ---
    # if df_inc is None or df_inc.rdd.isEmpty():
    #     logger.debug("No data for old incident model; skipping its pre-processing and post-processing aggregations.")
    # else:
    #     pass
        # logger.info("Starting pre-processing aggregations for old model...")
        # process_preprocessing_aggregations(
        #     spark, 
        #     df_inc, 
        #     df_hist, 
        #     data.get("pre_import_failures"), 
        #     ROOT_DIR, 
        #     warehouse, 
        #     gold_layer, 
        #     start_threshold_millis, 
        #     end_threshold_millis
        # )
        
        # logger.info("Starting post-processing aggregations for old model...")
        # process_post_processing_aggregations(
        #     spark, 
        #     df_inc, 
        #     data.get("df_yesterday"), 
        #     df_hist, 
        #     data.get("df_hist_yesterday"), 
        #     data.get("primary_today"), 
        #     data.get("primaries_count"), 
        #     data.get("VENDORS"), 
        #     data.get("OFFENCES"), 
        #     data.get("PROCESS_TYPES"), 
        #     data.get("IDEALS"), 
        #     ROOT_DIR, 
        #     warehouse, 
        #     gold_layer, 
        #     start_threshold_millis, 
        #     end_threshold_millis
        # )
    
    # --- Process VPC aggregations ---
    # logger.info("Starting VPC aggregations...")
    process_vpc_aggregations(
        spark,
        ROOT_DIR,
        warehouse,
        bronze_layer,
        gold_layer,
        start_threshold_millis,
        end_threshold_millis
    )
    

    spark.stop()
    logger.info("All aggregations complete. Spark session stopped.")

# ------------------------------------------------------------------------------
# Top-level argument parsing & entry
# ------------------------------------------------------------------------------
parser = argparse.ArgumentParser(
    description="Aggregation Script for KPI Metrics on Delta Tables"
)
parser.add_argument(
    "--start-threshold", type=int, required=True, help="Start threshold in ms"
)
parser.add_argument(
    "--end-threshold", type=int, required=True, help="End threshold in ms"
)
args = parser.parse_args()
main(args.start_threshold, args.end_threshold)
#!/usr/bin/env python
"""
ETL Data Processing Module

This module manages the processing of large PSOP and IPP_PSOP data buffers. It configures logging,
reads ETL configuration settings, and defines functions to process bulk data for PSOP
and IPP_PSOP requests.

All informational messages have been demoted to DEBUG level; errors remain at ERROR.
"""

import os
from pathlib import Path
import sys

ROOT_DIR = Path(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(str(ROOT_DIR))

# Import processing functions for bulk requests
from src import (
    process_bronze_ipp_psop_data,
    process_lookup_table_ipp_psop,
    process_silver_ipp_psop_data,
    process_bronze_traffic_data,
    process_lookup_table_traffic,
    process_vpc_events,
    process_clerk_statuses,
    process_bronze_psop_full_model,
    process_silver_psop_full_model
)
from .initializers import initSparkSession, readConfig
from .logger_config import LoggerManager

# ------------------------------------------------------------------------------
# Logger and Global Configuration Setup
# ------------------------------------------------------------------------------

logger = LoggerManager.get_logger("ETL_Processing.log")

# Define the root directory for the project
root_dir = Path(__file__).parent.parent

# Build the configuration path and read the ETL configuration file
config_path = root_dir / "config"
etl_config = readConfig(config_path / "ETL.xml")

# Retrieve the write configuration from the ETL configuration file; use default values if missing
etl_write_config = etl_config.get("Write", {})

# Initialize Spark globally
spark = initSparkSession("ETL Process")
logger.debug("Global Spark session initialized for ETL Process")

# ------------------------------------------------------------------------------
# Function Definitions
# ------------------------------------------------------------------------------

def process_ipp_psop_data(ipp_psop_buffer) -> None:
    """
    Process bulk IPP_PSOP data buffers.

    Args:
        ipp_psop_buffer: The list of IPP_PSOP incident data to process.
    """
    logger.debug(f"Received IPP_PSOP buffer with {len(ipp_psop_buffer)} records")
    try:
        process_bronze_ipp_psop_data(ipp_psop_buffer, spark)
        process_lookup_table_ipp_psop(ipp_psop_buffer, spark)
        process_silver_ipp_psop_data(ipp_psop_buffer, spark)
        ipp_psop_buffer.clear()
        logger.debug("Processed and cleaned IPP_PSOP buffers")
    except Exception as e:
        logger.error(f"Error processing IPP_PSOP buffers: {e}")




def process_traffic_data(traffic_buffer) -> None:
    """Process bulk Traffic data buffers."""
    try:
        process_bronze_traffic_data(traffic_buffer, spark)
        process_lookup_table_traffic(traffic_buffer, spark)
        traffic_buffer.clear()
    except Exception as e:
        logger.error(f"Error processing Traffic data: {e}")


def process_vpc_data(vpc_events: list[dict]) -> None:
    """
    Immediately process a batch of VPC events (no in‑memory buffering).
    """
    try:
        process_vpc_events(spark, vpc_events)
        vpc_events.clear()
        logger.debug(f"Processed {len(vpc_events)} VPC event(s)")
    except Exception as e:
        logger.error(f"Error processing VPC events: {e}")


def process_clerk_data(clerk_statuses: list[dict]) -> None:
    """
    Immediately process a batch of Clerk status updates (no buffering).
    """
    try:
        process_clerk_statuses(spark, clerk_statuses)
        clerk_statuses.clear()
    except Exception as e:
        logger.error(f"Error processing clerk status updates: {e}")

def process_psop_full_model_data(psop_full_model_buffer) -> None:
    """
    Process bulk PSOP Full Incident Model data buffers.
    This function will orchestrate the Bronze, Silver, etc. processing.
    """
    logger.debug(f"Received PSOP Full Model buffer with {len(psop_full_model_buffer)} records")
    try:
        # Pass the global spark session to the processing function
        process_bronze_psop_full_model(psop_full_model_buffer, spark)
        process_silver_psop_full_model(psop_full_model_buffer, spark)
        psop_full_model_buffer.clear()
        logger.debug("Processed and cleaned PSOP Full Model buffers")
    except Exception as e:
        logger.error(f"Error processing PSOP Full Model buffers: {e}", exc_info=True)

        
def process_rrm_data(rrm_data: list[dict]) -> None: 
    """
    
    """
    pass

def process_ITS_data(its_data: list[dict]) -> None: 
    """
    
    """
    pass

def process_claims_data(claims_data: list[dict]) -> None: 
    """
    
    """
    pass


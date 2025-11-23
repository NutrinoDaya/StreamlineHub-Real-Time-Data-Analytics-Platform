#!/usr/bin/env python3
"""
Utility functions for time threshold computation and configuration reading.
"""
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import xml.etree.ElementTree as ET
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def parse_interval(interval_str: str) -> tuple[int, str]:
    """Parses an interval string like '2 hours' into a number and unit."""
    parts = interval_str.strip().split()
    if len(parts) != 2:
        raise ValueError(f"Invalid interval format: {interval_str}")
    return int(parts[0]), parts[1].lower()

def compute_time_threshold(
    interval_str: str, base_time: datetime
) -> tuple[int, int]:
    """
    Calculates a start and end time window and returns them as UTC milliseconds.

    Args:
        interval_str: A string defining the duration to go back, e.g., '2 hours', '30 minutes'.
        base_time: The reference 'now' time, which MUST be a timezone-aware UTC datetime object.
    """

    if base_time.tzinfo is None or base_time.utcoffset() != timedelta(0):
        raise ValueError("The provided base_time must be a timezone-aware object with a UTC offset of zero.")

    num_value, unit_str = parse_interval(interval_str)

    # 1) Compute the start of the window by subtracting the interval from the base UTC time.
    if unit_str in ("minute", "minutes"):
        start_utc = base_time - timedelta(minutes=num_value)
    elif unit_str in ("hour", "hours"):
        start_utc = base_time - timedelta(hours=num_value)
    elif unit_str in ("day", "days"):
        start_utc = base_time - timedelta(days=num_value)
    elif unit_str in ("week", "weeks"):
        start_utc = base_time - timedelta(weeks=num_value)
    elif unit_str in ("month", "months"):
        start_utc = base_time - relativedelta(months=num_value)
    else:
        # Default to no change if the unit is unrecognized
        start_utc = base_time

    # 2) Truncate the end time to the beginning of the current minute for a clean window.
    end_utc = base_time.replace(second=0, microsecond=0)

    return (
        int(start_utc.timestamp() * 1000),
        int(end_utc.timestamp() * 1000),
    )

def readConfig(config_file_path: Path) -> dict:
    """
    Read XML configuration file and return as dictionary.
    
    Args:
        config_file_path: Path to XML configuration file
        
    Returns:
        Dictionary containing configuration values
    """
    try:
        tree = ET.parse(config_file_path)
        root = tree.getroot()
        
        config = {}
        
        def parse_element(element, parent_dict):
            """Recursively parse XML elements into dictionary"""
            if len(element) == 0:
                # Leaf node - store text value
                parent_dict[element.tag] = element.text
            else:
                # Parent node - create nested dictionary
                if element.tag not in parent_dict:
                    parent_dict[element.tag] = {}
                for child in element:
                    parse_element(child, parent_dict[element.tag])
        
        # Parse all root children
        for child in root:
            parse_element(child, config)
        
        logger.info(f"Successfully loaded configuration from {config_file_path}")
        return config
        
    except Exception as e:
        logger.error(f"Failed to read configuration from {config_file_path}: {e}")
        raise
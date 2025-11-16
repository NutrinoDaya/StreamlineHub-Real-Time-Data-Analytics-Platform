#!/usr/bin/env python

"""
Elasticsearch Ingestion Script for Gold + Silver Layers (UTC-based)

This script ingests data from the Gold layer (standard Hive-partitioned tables)
and from lookup tables in Bronze and Silver layers.
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, List
from functools import reduce
import xml.etree.ElementTree as ET

from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

ROOT_DIR = Path(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(str(ROOT_DIR))
from utils import LoggerManager, readConfig, get_config_dir, initSparkSession



# Global Configurations and Logger
ROOT_DIR = Path(__file__).parent.parent
CFG_DIR = get_config_dir()
logger = LoggerManager.get_logger("ETL_Processing.log")

# --- Local Configuration Reader ---
def readConfig(cfg_file_path: str) -> dict:
    """
    Read configuration from an XML file and convert it into a dictionary.
    """
    tree = ET.parse(cfg_file_path)
    root = tree.getroot()
    def xml_to_dict(elem):
        if len(elem) == 0:
            return elem.text.strip() if elem.text else None
        return {child.tag: xml_to_dict(child) for child in elem}
    return {root.tag: xml_to_dict(root)}

try:
    dml_cfg = readConfig(str(CFG_DIR / "xml/DML.xml"))["Configuration"]["FILE"]
    warehouse = dml_cfg["warehouse"]
    gold_layer = dml_cfg["gold_layer"]
    silver_layer = dml_cfg["silver_layer"]
    bronze_layer = dml_cfg["bronze_layer"]
    etl_write_cfg = readConfig(str(CFG_DIR / "xml/ETL.xml"))["Configuration"]["Write"]
    ipp_psop_prefix = etl_write_cfg.get("ipp_psop_prefix", "IPP_PSOP")
    traffic_prefix = etl_write_cfg.get("traffic_prefix", "Traffic")
except Exception as e:
    logger.critical(f"Failed to load DML.xml or xml/ETL.xml configuration: {e}", exc_info=True)
    sys.exit(1)

# --- Utility Functions ---
def get_gold_table_names_from_xml(xml_path: str) -> List[str]:
    """
    Extracts a list of all table names from the Gold_Tables.xml config,
    correctly handling multiple <table> tags by parsing the XML directly.
    """
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        # Find all 'name' tags within the specified path
        table_elements = root.findall("./Gold_Tables/table/name")
        return [elem.text for elem in table_elements if elem.text]
    except (ET.ParseError, KeyError, TypeError) as e:
        logger.error(f"Could not parse Gold_Tables.xml. Check its structure. Error: {e}", exc_info=True)
        return []

def is_valid_delta_table(path: Path) -> bool:
    """Checks if a path contains a valid Delta table (has _delta_log directory)."""
    delta_log_path = path / "_delta_log"
    return path.is_dir() and delta_log_path.is_dir()

def read_date_partitioned_delta(spark: SparkSession, gold_dir: Path, database: str, table_name: str, start_threshold: int, end_threshold: int) -> DataFrame:
    """
    Reads date-partitioned Delta tables by checking for both padded (YYYY-MM-DD)
    and non-padded (YYYY-M-D) date folder formats.
    """
    try:
        start_dt_utc = datetime.fromtimestamp(start_threshold / 1000.0, tz=timezone.utc).date()
        end_dt_utc = datetime.fromtimestamp(end_threshold / 1000.0, tz=timezone.utc).date()
        logger.info(f"Date range for table '{table_name}': {start_dt_utc} to {end_dt_utc}")

        paths_to_read = []
        current_date = start_dt_utc
        while current_date <= end_dt_utc:
            padded_format = current_date.strftime("%Y-%m-%d")
            non_padded_format = f"{current_date.year}-{current_date.month}-{current_date.day}"
            possible_date_formats = {padded_format, non_padded_format}

            for date_str in possible_date_formats:
                if database:
                    table_path = gold_dir / database / date_str / table_name
                else:
                    table_path = gold_dir / date_str / table_name
                
                if is_valid_delta_table(table_path):
                    paths_to_read.append(str(table_path))
                    break 
            
            current_date += timedelta(days=1)
        
        if not paths_to_read:
            logger.debug(f"No valid Delta table partitions found for table '{table_name}'.")
            return spark.createDataFrame([], StructType([]))

        dfs = [spark.read.format("delta").load(path) for path in paths_to_read]
        non_empty_dfs = [df for df in dfs if not df.rdd.isEmpty()]

        if not non_empty_dfs:
            logger.debug(f"No valid data loaded from any partitions for table '{table_name}'.")
            return spark.createDataFrame([], StructType([]))

        combined_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), non_empty_dfs)
        count = combined_df.count()
        logger.info(f"Combined {len(non_empty_dfs)} partitions for table '{table_name}', total rows: {count}")
        return combined_df
    except Exception as e:
        logger.error(f"Error in read_date_partitioned_delta for '{table_name}': {e}", exc_info=True)
        return spark.createDataFrame([], StructType([]))

def read_full_delta_table(spark: SparkSession, path: str) -> Optional[DataFrame]:
    path_obj = Path(path)
    try:
        if not is_valid_delta_table(path_obj):
            logger.debug(f"Path is not a valid Delta table: {path}")
            return None
        
        df = spark.read.format("delta").load(path)
        
        if df.rdd.isEmpty():
            logger.debug(f"Delta table at {path} is empty.")
            return None

        count = df.count()
        return df
    except Exception as e:
        logger.error(f"Unexpected error reading Delta table from {path}: {e}", exc_info=True)
        return None

def write_to_es(df: DataFrame, opts: dict, index: str) -> int:
    df.cache()
    try:
        cnt = df.count()
        
        if cnt == 0:
            logger.info(f"No data to write to ES index [{index}]")
            return 0
        
        es_opts_with_resource = {**opts, "es.resource": f"{index}/_doc"}
        df.write.format("org.elasticsearch.spark.sql").options(**es_opts_with_resource).mode("append").save()
        logger.info(f"Successfully wrote {cnt} rows to ES index [{index}]")
        return cnt
    except Exception as e:
        logger.error(f"Error writing to ES index [{index}]: {e}", exc_info=True)
        return 0
    finally:
        df.unpersist()

def overwrite_to_es(df: DataFrame, es_client: Elasticsearch, es_opts: dict, index: str) -> int:
    logger.info(f"Preparing to overwrite Elasticsearch index [{index}].")
    try:
        if es_client.indices.exists(index=index):
            es_client.indices.delete(index=index, ignore_unavailable=True)
            logger.info(f"Successfully deleted index [{index}].")
        return write_to_es(df, es_opts, index)
    except Exception as e:
        logger.error(f"Failed during overwrite operation for index [{index}]: {e}", exc_info=True)
        return 0

# --- Main Execution Logic ---
def main(start_threshold_ms: int, end_threshold_ms: int):
    spark = None
    try:
        spark = initSparkSession("ETL_Processing")
        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set("spark.sql.session.timeZone", "UTC")

        es_cfg = readConfig(str(CFG_DIR / "Elasticsearch_Dag.xml"))["Configuration"]["Elasticsearch"]
        es_auth = (es_cfg.get("es_user"), es_cfg.get("es_password")) if es_cfg.get("es_user") else None
        
        es_client = Elasticsearch(
            hosts=[{"host": es_cfg["es_host"], "port": int(es_cfg["es_port"]), "scheme": es_cfg.get("es_scheme", "http")}],
            http_auth=es_auth,
            request_timeout=30, max_retries=3, retry_on_timeout=True
        )
        if not es_client.ping():
            raise ConnectionError(f"Failed to connect to Elasticsearch at {es_cfg['es_host']}:{es_cfg['es_port']}")

        es_opts = {
            "es.nodes": es_cfg["es_host"], "es.port": str(es_cfg["es_port"]),
            "es.nodes.wan.only": "true", "es.batch.size.entries": es_cfg.get("batchsize", "1000")
        }
        if es_auth:
            es_opts.update({"es.net.http.auth.user": es_auth[0], "es.net.http.auth.pass": es_auth[1]})

        # Ingest Lookup Tables
        logger.info("--- Starting Lookup Table Ingestion ---")
        traffic_lookup_path = ROOT_DIR / warehouse / bronze_layer / "Traffic" / "Traffic_DataLookup"
        df_traffic_lookup = read_full_delta_table(spark, str(traffic_lookup_path))
        if df_traffic_lookup:
            overwrite_to_es(df_traffic_lookup, es_client, es_opts, "traffic_datalookup")
        
        ipp_lookup_path = ROOT_DIR / warehouse / silver_layer / ipp_psop_prefix / f"{ipp_psop_prefix}_DataLookup"
        df_ipp_lookup = read_full_delta_table(spark, str(ipp_lookup_path))
        if df_ipp_lookup:
            overwrite_to_es(df_ipp_lookup, es_client, es_opts, "ipp_psop_datalookup")
        logger.info("--- Finished Lookup Table Ingestion ---")

        # Gold tables ingestion
        logger.info("--- Starting Gold Layer Ingestion ---")
        gold_dir = ROOT_DIR / warehouse / gold_layer

        if not gold_dir.is_dir():
            logger.debug(f"Gold layer directory does not exist: {gold_dir}. Skipping Gold layer ingestion.")
        else:
            logger.info("Reading list of Gold tables from configuration...")
            # MODIFIED: Pass the file path directly to get_gold_table_names_from_xml
            TABLE_NAMES = get_gold_table_names_from_xml(str(CFG_DIR / "Gold_Tables.xml"))
            
            logger.info(f"TABLE NAMES: {TABLE_NAMES}")
            if not TABLE_NAMES:
                logger.debug("No gold tables found in Gold_Tables.xml. Skipping Gold layer ingestion.")
            else:
                for table_name in TABLE_NAMES:
                    logger.info(f"Processing table: {table_name}")
                    
                    
                    database_prefix = ""  # No prefix for non-Traffic tables

                    df_gold_filtered = read_date_partitioned_delta(
                        spark,
                        gold_dir,
                        database_prefix,
                        table_name,
                        start_threshold_ms,
                        end_threshold_ms
                    )    

                    if df_gold_filtered and not df_gold_filtered.rdd.isEmpty():
                        write_to_es(df_gold_filtered, es_opts, table_name.lower())
                    else:
                        logger.info(f"No data for Gold table [{table_name}]. Skipping.")

    except Exception as e_main:
        logger.critical(f"A fatal error occurred in the ES ingestion main process: {e_main}", exc_info=True)
    finally:
        if spark:
            logger.info("Stopping Spark session.")
            spark.stop()

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Ingest Gold layer data into Elasticsearch.")
    p.add_argument("--start-threshold", type=int, required=True, help="Start time in ms since epoch (UTC)")
    p.add_argument("--end-threshold", type=int, required=True, help="End time in ms since epoch (UTC)")
    args = p.parse_args()
    main(args.start_threshold, args.end_threshold)
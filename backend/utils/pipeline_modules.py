"""
Aggregation Script to Perform SQL Queries on Delta Tables

This script performs post-processing KPIs, pre-processing aggregations,
VPC/Clerk KPI aggregations, traffic data aggregations, and PSOP_FULL_MODEL aggregations
by reading from Delta Lake tables. Each result is written to the Gold layer via
`save_aggregation_result`.
"""

# ------------------------------------------------------------------------------
# Standard Library Imports
# ------------------------------------------------------------------------------
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

# ------------------------------------------------------------------------------
# Third-Party Imports (Spark-related)
# ------------------------------------------------------------------------------
from pyspark.sql import DataFrame, SparkSession 
from pyspark.sql import functions as F   

# ------------------------------------------------------------------------------
# Local imports & path setup
# ------------------------------------------------------------------------------
ROOT_DIR = Path(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(str(ROOT_DIR))

from utils import (
    LoggerManager,
    readConfig,
    load_incident_for_primary,
    load_incidenthistory_for_primary,
    load_yaml_config,
    get_primary_filenames,
    run_and_save
)
from pipeline_utilities import read_delta_table
# ------------------------------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------------------------------
logger = LoggerManager.get_logger("ETL_Processing.log")
etl_cfg = readConfig(str(ROOT_DIR / "config" / "ETL.xml"))["Write"]
cfg = readConfig(str(Path(__file__).parent.parent / "config" / "DML.xml"))["FILE"]
warehouse = cfg["warehouse"]
bronze_layer = cfg["bronze_layer"]
silver_layer = cfg["silver_layer"]
gold_layer = cfg["gold_layer"]
state_map = load_yaml_config(ROOT_DIR / "config" / "states_config.yaml")[
    "state_text_to_process_type"
]


def process_psop_full_model_aggregations(
    spark: SparkSession,
    df_psop_full_model: DataFrame,
    ROOT_DIR: Path,
    warehouse: str,
    gold_layer: str,
    start_threshold_millis: int,
    end_threshold_millis: int
):
    """
    Performs optimized aggregations on the PSOP_FULL_MODEL table and saves results.
    """
    logger.info("Starting Optimized PSOP Full Model Aggregations")
    if df_psop_full_model is None or df_psop_full_model.rdd.isEmpty():
        logger.warning("PSOP_FULL_MODEL DataFrame is empty; skipping all related aggregations.")
        return

    # Create a pre-filtered 'violations' DataFrame once for efficiency
    violations_df = df_psop_full_model.filter(F.col("OffenceGroup").isNotNull()).cache()
    
    if violations_df.rdd.isEmpty():
        logger.warning("No valid violation records found (OffenceGroup is always null).")
    
    # Map of aggregation functions to their output table and the DataFrame they need
    kpi_map = {
        get_violation_details: ("psop_violation_details", violations_df),
        get_non_compliant_incidents: ("psop_non_compliant_incidents", df_psop_full_model),
        get_complied_incidents: ("psop_complied_incidents", violations_df),
    }

    # Loop and execute each aggregation
    for agg_function, (table_name, source_df) in kpi_map.items():
        if source_df is None or source_df.rdd.isEmpty():
            logger.warning(f"Skipping aggregation '{table_name}' because its source DataFrame is empty.")
            continue
        
        # It passes the arguments positionally, matching the function's definition.
        run_and_save(
            spark,      
            agg_function,      
            (source_df,),  
            ROOT_DIR,
            warehouse,
            gold_layer,  
            table_name,  
            start_threshold_millis,
            end_threshold_millis,
        )

    # Cleanup the cached DataFrame
    if not violations_df.rdd.isEmpty():
        violations_df.unpersist()
        
    logger.info("Successfully completed all PSOP Full Model aggregations.")


def load_config_and_data(
    spark: SparkSession,
    start_threshold_millis: int,
    end_threshold_millis: int
) -> Dict[str, Any]:
    """
    Loads all necessary configurations and data from Bronze and Silver layers in an
    optimized and robust manner.
    """
    logger.info("Starting to load all configurations and data sources.")
    
    # --- 1. Load All Configurations Once ---
    dml_cfg = readConfig(str(ROOT_DIR / "config" / "DML.xml"))["FILE"]
    etl_cfg = readConfig(str(ROOT_DIR / "config" / "ETL.xml"))["Write"]
    filters_cfg = load_yaml_config(ROOT_DIR / "config" / "filters.yaml")
    state_map_cfg = load_yaml_config(ROOT_DIR / "config" / "states_config.yaml")

    warehouse = dml_cfg["warehouse"]
    bronze_layer = dml_cfg["bronze_layer"]
    silver_layer = dml_cfg["silver_layer"]
    gold_layer = dml_cfg["gold_layer"]
    traffic_prefix = etl_cfg.get("traffic_prefix", "traffic")
    ipp_psop_prefix = etl_cfg.get("ipp_psop_prefix", "ipp_psop")
    psop_prefix = etl_cfg.get("psop_prefix", "PSOP")

    data_context: Dict[str, Any] = {
        "ROOT_DIR": ROOT_DIR, "warehouse": warehouse, "bronze_layer": bronze_layer, "silver_layer": silver_layer,
        "gold_layer": gold_layer, "traffic_prefix": traffic_prefix, "ipp_psop_prefix": ipp_psop_prefix,
        "psop_prefix": psop_prefix,
        "state_map": state_map_cfg.get("state_text_to_process_type", {}),
        "IDEALS": filters_cfg.get("Ideals", {}),
        "PROCESS_TYPES": filters_cfg.get("ProcessType", {}),
        "pre_import_failures": set(dml_cfg.get("pre_import_failure_states", [])),
        "raw_traffic_data": None, "raw_traffic_hdr": None, "df_inc": None, "df_inc_y": None,
        "df_hist": None, "df_hist_yesterday": None, "df_yesterday": None,
        "primary_today": [], "primary_yesterday": [], "primaries_count": 0,
        "VENDORS": [], "OFFENCES": [], "df_psop_full_model": None
    }

    # --- 2. Load Old Model Silver Incident Data & Dependencies ---
    def _extract_primary_keys(result_obj: Any) -> List[str]:
        """Safely extracts PrimaryFileNames from various input types."""
        if not result_obj:
            return []
        try:
            if isinstance(result_obj, DataFrame):
                return [row["PrimaryFileName"] for row in result_obj.collect()]
            elif isinstance(result_obj, list):
                if not result_obj: return []
                if isinstance(result_obj[0], str):
                    return result_obj
                else:
                    return [row["PrimaryFileName"] for row in result_obj]
            else:
                logger.warning(f"Cannot extract primary keys from type {type(result_obj)}.")
                return []
        except Exception as e:
            logger.error(f"Exception during primary key extraction: {e}", exc_info=True)
            return []

    one_day_millis = 24 * 60 * 60 * 1000
    yesterday_start = start_threshold_millis - one_day_millis
    yesterday_end = start_threshold_millis - 1

    primary_today_result, df_inc = get_primary_filenames(spark, warehouse, silver_layer, start_threshold_millis, end_threshold_millis)

    if df_inc and not df_inc.rdd.isEmpty():
        logger.info("Data found for the old incident model. Loading all dependent data.")
        data_context["df_inc"] = df_inc.cache()
        
        primary_yesterday_result, df_inc_y = get_primary_filenames(spark, warehouse, silver_layer, yesterday_start, yesterday_end)
        if df_inc_y and not df_inc_y.rdd.isEmpty():
            data_context["df_inc_y"] = df_inc_y.cache()
            data_context["df_yesterday"] = df_inc_y

        primary_today = _extract_primary_keys(primary_today_result)
        primary_yesterday = _extract_primary_keys(primary_yesterday_result)
        
        data_context["primary_today"] = primary_today
        data_context["primary_yesterday"] = primary_yesterday
        data_context["primaries_count"] = len(primary_today)

        data_context["df_hist"] = load_incidenthistory_for_primary(spark, ROOT_DIR, warehouse, silver_layer, primary_today, start_threshold_millis, end_threshold_millis)
        if data_context["df_hist"]: data_context["df_hist"].cache()

        data_context["df_hist_yesterday"] = load_incidenthistory_for_primary(spark, ROOT_DIR, warehouse, silver_layer, primary_yesterday, start_threshold_millis, end_threshold_millis)
        if data_context["df_hist_yesterday"]: data_context["df_hist_yesterday"].cache()
    else:
        logger.debug("No data found for the old incident model in the given time range.")

    # --- 3. Load Lookup Data ---
    try:
        lookup_path = str(ROOT_DIR / warehouse / silver_layer / ipp_psop_prefix / f"{ipp_psop_prefix}_DataLookup")
        df_lookup = spark.read.format("delta").load(lookup_path)
        vendors_rows = df_lookup.filter(F.col("lookupType") == "Vendor").select("lookupValue").distinct().collect()
        data_context["VENDORS"] = [r["lookupValue"] for r in vendors_rows]
        offence_rows = df_lookup.filter(F.col("lookupType") == "OffenceType").select("lookupValue").distinct().collect()
        data_context["OFFENCES"] = [r["lookupValue"] for r in offence_rows]
    except Exception as e:
        logger.debug(f"Could not load DataLookup table. VENDORS and OFFENCES will be empty. {e}")

    # --- 4. Load PSOP Full Model Data ---
    logger.info("Attempting to load PSOP Full Model data...")
    try:
        df_psop = read_delta_table(
            spark=spark,
            root_dir=str(ROOT_DIR),
            warehouse=warehouse,
            layer=silver_layer,
            database=psop_prefix, # This corresponds to the "PSOP" folder
            table_name="PSOP_FULL_MODEL",
            is_partitioned=True, # It is partitioned by date
            start_threshold=start_threshold_millis,
            end_threshold=end_threshold_millis
        )
        
        # Check if the DataFrame is not empty before caching and assigning
        if df_psop and not df_psop.rdd.isEmpty():
            # Cache the DataFrame for better performance in subsequent operations
            data_context["df_psop_full_model"] = df_psop.cache()
            count = data_context['df_psop_full_model'].count()
            logger.info(f"Successfully loaded {count} rows for PSOP Full Model.")
        else:
            logger.info("No data found for PSOP Full Model in the given time range.")

    except Exception as e:
        logger.error(f"A critical error occurred while loading PSOP Full Model data: {e}", exc_info=True)

    data_context["start_threshold_millis"] = start_threshold_millis
    data_context["end_threshold_millis"] = end_threshold_millis
    logger.info("Finished loading all configurations and data sources.")
    return data_context


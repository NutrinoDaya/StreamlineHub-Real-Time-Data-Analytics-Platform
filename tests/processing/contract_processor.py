#!/usr/bin/env python
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import concurrent.futures

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, from_unixtime, to_timestamp, when, year, month, dayofmonth
)
from pyspark.sql.types import StructType, StructField, StringType, LongType

from utils import readConfig, LoggerManager
from src.core.delta_manager import DeltaTableManager

# Beam processing flag - set to False if apache_beam not available
USE_BEAM = os.environ.get("USE_BEAM", "false").lower() == "true"
if USE_BEAM:
    try:
        from src.processing.beam_contract_processor import run_beam_contract_processing
    except ImportError:
        USE_BEAM = False
        LoggerManager.get_logger("ETL_Processing.log").warning("Apache Beam not available, using Spark-only processing")

# Streaming processing flag
USE_STREAMING = os.environ.get("USE_STREAMING", "false").lower() == "true"
if USE_STREAMING:
    try:
        from src.processing.streaming_contract_processor import process_micro_batches
    except ImportError:
        USE_STREAMING = False
        LoggerManager.get_logger("ETL_Processing.log").warning("Streaming processor not available")

# Project root (../../ from this file): Big_Data_ETL
ROOT_DIR = Path(__file__).resolve().parents[2]
logger = LoggerManager.get_logger("ETL_Processing.log")


def _infer_string_schema_from_fields(fields: List[Dict[str, Any]]) -> StructType:
    # For simplicity, create all fields as StringType unless name is 'insertionTime' which becomes LongType
    struct_fields: List[StructField] = []
    for f in fields:
        name = f.get("name") or f.get("field")
        if not name:
            continue
        if name.lower() == "insertiontime":
            struct_fields.append(StructField(name, LongType(), True))
        else:
            struct_fields.append(StructField(name, StringType(), True))
    # ensure insertionTime exists
    if not any(sf.name.lower() == "insertiontime" for sf in struct_fields):
        struct_fields.append(StructField("InsertionTime", LongType(), True))
    return StructType(struct_fields)


class ContractProcessor:
    def __init__(self, spark: SparkSession, delta_manager: Optional[DeltaTableManager] = None):
        self.spark = spark
        self.delta = delta_manager or DeltaTableManager(spark)

        # Load DML paths for warehouse layout
        dml = readConfig(ROOT_DIR / "config" / "xml/DML.xml").get("FILE", {})
        self.bronze_layer = dml.get("bronze_layer", "bronze")
        self.silver_layer = dml.get("silver_layer", "silver")
        self.gold_layer = dml.get("gold_layer", "gold")
        self.warehouse = dml.get("warehouse", "data")

    def _paths(self, table: str, layer: str) -> str:
        base = os.path.join(ROOT_DIR, self.warehouse, layer)
        return os.path.join(base, table)

    def _apply_ms_conversions(self, df: DataFrame, fields: List[Dict[str, Any]]) -> DataFrame:
        # If a field has ms:true and is string-like, convert to epoch milliseconds in a new column or overwrite
        result = df
        for f in fields:
            name = f.get("name") or f.get("field")
            if not name:
                continue
            # Only apply MS conversion if the field exists in the DataFrame AND ms:true
            if f.get("ms") is True and name in result.columns:
                # Try parsing as timestamp, then cast to ms since epoch
                # Assumes input is string ISO or yyyy-MM-dd HH:mm:ss like
                result = result.withColumn(
                    name,
                    (when(col(name).isNotNull(), (to_timestamp(col(name))
                        .cast("timestamp").cast("long") * lit(1000)))
                    .otherwise(col(name).cast("bigint")))
                )
        return result

    def _ensure_partition_columns(self, df: DataFrame) -> DataFrame:
        # Require InsertionTime (ms epoch). Add year/month/day derived from it for partitioning.
        if "InsertionTime" not in df.columns:
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            df = df.withColumn("InsertionTime", lit(now_ms))
        # derive date parts
        ts_sec = to_timestamp((col("InsertionTime")/lit(1000)).cast("double"))
        return df.withColumn("year", year(ts_sec)).withColumn("month", month(ts_sec)).withColumn("day", dayofmonth(ts_sec))

    def _apply_quality_rules(self, df: DataFrame, quality_rules: Dict[str, Any]) -> DataFrame:
        # Minimal support: drop rows missing required fields
        required = quality_rules.get("required", []) if isinstance(quality_rules, dict) else []
        if not required:
            return df
        
        # Combine all filters into a single condition for better performance
        filter_condition = None
        for field in required:
            if field in df.columns:
                condition = col(field).isNotNull()
                filter_condition = condition if filter_condition is None else (filter_condition & condition)
        
        return df.filter(filter_condition) if filter_condition is not None else df

    def _write_delta(self, df: DataFrame, table: str, layer: str = "bronze") -> bool:
        path = self._paths(table, layer)
        # Partition by year, month, day derived from insertionTime
        return self.delta.transactional_write(
            df=df,
            table_path=path,
            mode="append",
            partition_cols=["year", "month", "day"],
            max_retries=5,
        )

    def _run_aggregations(self, table: str, aggregations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        if not aggregations:
            return results

        bronze_path = self._paths(table, "bronze")
        if not self.delta.safe_exists_check(bronze_path):
            logger.warning(f"Cannot run aggregations; bronze table missing: {bronze_path}")
            return results

        src_df = self.spark.read.format("delta").load(bronze_path)
        for agg in aggregations:
            name = agg.get("name") or "agg"
            sql_text = agg.get("sql") or agg.get("query")
            target = agg.get("target_table") or f"{table}_{name}"
            if not sql_text:
                continue

            src_df.createOrReplaceTempView("src")
            try:
                out = self.spark.sql(sql_text)
                out = self._ensure_partition_columns(out)
                self._write_delta(out, target, layer="gold")
                # Skip expensive count() - just report success
                results.append({"name": name, "target": target, "status": "success"})
            except Exception as e:
                logger.error(f"Aggregation '{name}' failed: {e}")
                results.append({"name": name, "error": str(e)})

        return results

    def process(self, domain: str, product: str, items: List[Dict[str, Any]], contract: Dict[str, Any]) -> Dict[str, Any]:
        """Process data using Streaming (priority), Beam, or Spark"""
        if not items:
            return {"success": True, "rows": 0}

        # Priority 1: Streaming micro-batch processing for ultra-high throughput
        if USE_STREAMING and len(items) >= 5000:
            try:
                result = process_micro_batches(
                    spark=self.spark,
                    records=items,
                    contract=contract,
                    domain=domain,
                    product=product,
                    batch_size=10000  # 10k micro-batches for optimal Delta writes
                )
                logger.info(f"Streaming processed {result['records']} records at {result['rate']:.2f} items/sec")
                return {"success": result['success'], "rows": result['records']}
            except Exception as e:
                logger.warning(f"Streaming processing failed, falling back: {e}")
                # Fall through to Beam or Spark

        # Priority 2: Beam processing for high-speed bulk loads
        if USE_BEAM and len(items) >= 1000:  # Use Beam for batches >= 1000 records
            try:
                base_path = os.path.join(ROOT_DIR, self.warehouse, self.bronze_layer)
                result = run_beam_contract_processing(
                    contract=contract,
                    domain=domain,
                    product=product,
                    records=items,
                    base_path=base_path,
                    batch_size=min(5000, len(items))  # Adaptive batch size
                )
                return result
            except Exception as e:
                logger.warning(f"Beam processing failed, falling back to Spark: {e}")
                # Fall through to Spark processing
        
        # Spark processing (default or fallback)
        return self._process_with_spark(domain, product, items, contract)
    
    def _process_with_spark(self, domain: str, product: str, items: List[Dict[str, Any]], contract: Dict[str, Any]) -> Dict[str, Any]:
        """Optimized Spark-based processing"""
        table = f"{domain}_{product}".replace("-", "_")
        schema_fields = contract.get("schema", {}).get("fields", [])
        quality_rules = contract.get("quality_rules", {})
        aggregations = contract.get("aggregations", [])
        
        input_count = len(items)

        # Normalize and create DataFrame
        normalized: List[Dict[str, Any]] = []
        for rec in items:
            if isinstance(rec, dict):
                normalized.append({k: ("" if v is None else v) for k, v in rec.items()})
            else:
                normalized.append({"value": str(rec)})

        df = self.spark.createDataFrame(normalized)
        
        # Optimize partition count based on data size (reduce overhead for small batches)
        # Use 1 partition per 500 records for better throughput
        num_partitions = max(1, min(50, input_count // 500))
        df = df.coalesce(num_partitions)

        # Apply ms conversion, ensure partition columns, and quality rules
        df = self._apply_ms_conversions(df, schema_fields)
        df = self._ensure_partition_columns(df)
        df = self._apply_quality_rules(df, quality_rules)
        
        # Cache the DataFrame to avoid recomputation during write
        df.cache()

        # Write to bronze with reduced retries for faster failure detection
        write_ok = self.delta.transactional_write(
            df=df,
            table_path=self._paths(table, "bronze"),
            mode="append",
            partition_cols=["year", "month", "day"],
            max_retries=2,  # Reduce from 5 to 2 for faster throughput
            skip_health_check=True
        )
        
        # Unpersist after write to free memory
        df.unpersist()

        # Run aggregations into gold (skip if disabled for performance)
        # Set SKIP_AGGREGATIONS=true for high-speed bulk ingestion
        agg_results = []
        skip_agg = os.environ.get("SKIP_AGGREGATIONS", "false").lower() == "true"
        if aggregations and write_ok and not skip_agg:
            agg_results = self._run_aggregations(table, aggregations)

        return {
            "success": write_ok,
            "rows": input_count,  # Use input count instead of expensive df.count()
            "aggregations": agg_results,
            "table": table,
        }
    
    def process_batch_parallel(self, batches: List[Tuple[str, str, List[Dict[str, Any]], Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Process multiple batches in parallel for maximum throughput"""
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(self.process, domain, product, items, contract)
                for domain, product, items, contract in batches
            ]
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Parallel batch processing failed: {e}")
                    results.append({"success": False, "error": str(e)})
        
        return results

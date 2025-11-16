"""
Apache Beam Pipeline for High-Performance Contract-Based Processing
Integrates with Spark for Delta Lake operations
"""
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from typing import Dict, Any, List, Optional, Tuple
import logging
from datetime import datetime, timezone
import json
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, to_timestamp, when, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, LongType

logger = logging.getLogger(__name__)


class ContractBasedTransform(beam.DoFn):
    """
    Beam DoFn that applies contract-based transformations to records
    Optimized for high-throughput processing
    """
    
    def __init__(self, contract: Dict[str, Any]):
        self.contract = contract
        self.schema_fields = contract.get("schema", {}).get("fields", [])
        self.quality_rules = contract.get("quality_rules", {})
        self.ms_fields = [f.get("name") for f in self.schema_fields if f.get("ms") is True]
        self.required_fields = self.quality_rules.get("required", [])
        
    def setup(self):
        """Initialize per-worker resources"""
        logger.info(f"Setting up worker for contract processing")
        
    def process(self, element: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Process a single record through contract transformations
        Returns empty list if record fails quality checks
        """
        try:
            # Apply MS conversions
            for field in self.ms_fields:
                if field in element and element[field]:
                    try:
                        # Convert datetime string to milliseconds epoch
                        dt = datetime.fromisoformat(str(element[field]).replace('Z', '+00:00'))
                        element[field] = int(dt.timestamp() * 1000)
                    except Exception as e:
                        logger.debug(f"MS conversion failed for {field}: {e}")
                        element[field] = None
            
            # Add InsertionTime if missing
            if "InsertionTime" not in element or not element["InsertionTime"]:
                element["InsertionTime"] = int(datetime.now(timezone.utc).timestamp() * 1000)
            
            # Derive partition columns from InsertionTime
            insertion_time = element.get("InsertionTime", 0)
            if insertion_time:
                dt = datetime.fromtimestamp(insertion_time / 1000, tz=timezone.utc)
                element["year"] = dt.year
                element["month"] = dt.month
                element["day"] = dt.day
            
            # Quality checks
            for field in self.required_fields:
                if field not in element or element[field] is None or element[field] == "":
                    logger.debug(f"Record failed quality check: missing {field}")
                    return []  # Filter out invalid record
            
            yield element
            
        except Exception as e:
            logger.error(f"Error processing record: {e}")
            return []


class BatchRecordsFn(beam.DoFn):
    """
    Accumulates records into batches for efficient Spark writes
    """
    
    def __init__(self, batch_size: int = 5000):
        self.batch_size = batch_size
        self.buffer = []
        
    def process(self, element: Dict[str, Any]) -> List[List[Dict[str, Any]]]:
        """Accumulate records and emit when batch is full"""
        self.buffer.append(element)
        
        if len(self.buffer) >= self.batch_size:
            batch = self.buffer
            self.buffer = []
            yield batch
            
    def finish_bundle(self):
        """Emit remaining records at end of bundle"""
        if self.buffer:
            yield beam.pvalue.WindowedValue(
                value=self.buffer,
                timestamp=0,
                windows=[beam.window.GlobalWindow()]
            )
            self.buffer = []


class WriteToDeltaLake(beam.DoFn):
    """
    Writes batches to Delta Lake using Spark
    Optimized for high-speed bulk writes
    """
    
    def __init__(self, table_name: str, base_path: str, partition_cols: List[str]):
        self.table_name = table_name
        self.base_path = base_path
        self.partition_cols = partition_cols
        self.spark = None
        
    def setup(self):
        """Initialize Spark session per worker"""
        from pyspark.sql import SparkSession
        
        self.spark = SparkSession.builder \
            .appName(f"BeamDeltaWriter-{self.table_name}") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.default.parallelism", "2") \
            .getOrCreate()
        
        logger.info(f"Spark session initialized for {self.table_name}")
        
    def process(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Write a batch of records to Delta Lake"""
        if not batch:
            return []
            
        try:
            start_time = datetime.now()
            
            # Create DataFrame from batch
            df = self.spark.createDataFrame(batch)
            
            # Optimize partitions based on batch size
            num_partitions = max(1, min(10, len(batch) // 500))
            df = df.coalesce(num_partitions)
            
            # Write to Delta Lake
            table_path = os.path.join(self.base_path, self.table_name)
            
            writer = df.write.format("delta").mode("append")
            if self.partition_cols:
                writer = writer.partitionBy(*self.partition_cols)
                
            writer.save(table_path)
            
            duration = (datetime.now() - start_time).total_seconds()
            rate = len(batch) / duration if duration > 0 else 0
            
            logger.info(f"Wrote {len(batch)} records to {self.table_name} in {duration:.2f}s ({rate:.0f} rec/s)")
            
            yield {
                "table": self.table_name,
                "records": len(batch),
                "duration": duration,
                "rate": rate
            }
            
        except Exception as e:
            logger.error(f"Error writing batch to Delta Lake: {e}")
            yield {"error": str(e), "records": len(batch)}
            
    def teardown(self):
        """Cleanup Spark session"""
        if self.spark:
            self.spark.stop()


def create_beam_pipeline(
    contract: Dict[str, Any],
    domain: str,
    product: str,
    input_records: List[Dict[str, Any]],
    base_path: str = "/opt/Vi_Big_Data_Main/Big_Data_ETL/data/bronze",
    batch_size: int = 5000
) -> Tuple[beam.Pipeline, Dict[str, Any]]:
    """
    Create Apache Beam pipeline for contract-based processing
    
    Args:
        contract: Contract definition with schema, quality rules, aggregations
        domain: Data domain (e.g., 'ipp_psop')
        product: Data product (e.g., 'IncidentFile')
        input_records: List of records to process
        base_path: Base path for Delta Lake tables
        batch_size: Number of records per batch for Spark writes
        
    Returns:
        Tuple of (pipeline, options_dict)
    """
    
    table_name = f"{domain}_{product}".replace("-", "_")
    partition_cols = ["year", "month", "day"]
    
    # Pipeline options
    options = PipelineOptions([
        '--runner=DirectRunner',  # Use DirectRunner for local execution
        '--direct_num_workers=4',  # Parallel workers
        '--direct_running_mode=multi_processing',  # Enable multiprocessing
    ])
    options.view_as(StandardOptions).streaming = False
    
    # Create pipeline
    pipeline = beam.Pipeline(options=options)
    
    # Build processing pipeline
    (
        pipeline
        | "CreateRecords" >> beam.Create(input_records)
        | "ApplyContractTransforms" >> beam.ParDo(ContractBasedTransform(contract))
        | "BatchRecords" >> beam.ParDo(BatchRecordsFn(batch_size))
        | "WriteToDelta" >> beam.ParDo(WriteToDeltaLake(table_name, base_path, partition_cols))
        | "LogResults" >> beam.Map(lambda x: logger.info(f"Batch result: {x}"))
    )
    
    return pipeline, {
        "domain": domain,
        "product": product,
        "table": table_name,
        "batch_size": batch_size
    }


def run_beam_contract_processing(
    contract: Dict[str, Any],
    domain: str,
    product: str,
    records: List[Dict[str, Any]],
    base_path: str = "/opt/Vi_Big_Data_Main/Big_Data_ETL/data/bronze",
    batch_size: int = 5000
) -> Dict[str, Any]:
    """
    Execute Beam pipeline for high-speed contract processing
    
    Returns:
        Dictionary with processing statistics
    """
    start_time = datetime.now()
    
    logger.info(f"Starting Beam pipeline for {domain}.{product} with {len(records)} records")
    
    # Create and run pipeline
    pipeline, config = create_beam_pipeline(
        contract=contract,
        domain=domain,
        product=product,
        input_records=records,
        base_path=base_path,
        batch_size=batch_size
    )
    
    # Execute pipeline
    result = pipeline.run()
    result.wait_until_finish()
    
    duration = (datetime.now() - start_time).total_seconds()
    rate = len(records) / duration if duration > 0 else 0
    
    logger.info(f"Beam pipeline completed: {len(records)} records in {duration:.2f}s ({rate:.0f} rec/s)")
    
    return {
        "success": True,
        "records": len(records),
        "duration": duration,
        "rate": rate,
        "table": config["table"],
        "domain": domain,
        "product": product
    }


# Parallel processing with multiple Beam pipelines
def run_parallel_beam_pipelines(
    contracts_and_data: List[Tuple[Dict[str, Any], str, str, List[Dict[str, Any]]]],
    base_path: str = "/opt/Vi_Big_Data_Main/Big_Data_ETL/data/bronze",
    batch_size: int = 5000
) -> List[Dict[str, Any]]:
    """
    Run multiple Beam pipelines in parallel for maximum throughput
    
    Args:
        contracts_and_data: List of tuples (contract, domain, product, records)
        base_path: Base path for Delta Lake tables
        batch_size: Batch size for writes
        
    Returns:
        List of results from each pipeline
    """
    import concurrent.futures
    
    results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(
                run_beam_contract_processing,
                contract, domain, product, records, base_path, batch_size
            )
            for contract, domain, product, records in contracts_and_data
        ]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Pipeline failed: {e}")
                results.append({"success": False, "error": str(e)})
    
    return results

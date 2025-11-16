"""
High-Performance Spark Structured Streaming Processor
Optimized for contract-based processing with Delta Lake
"""
from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.functions import (
    col, lit, to_timestamp, when, year, month, dayofmonth,
    from_json, to_json, struct, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from typing import Dict, Any, List, Optional
import logging
from pathlib import Path
import os

logger = logging.getLogger(__name__)

# Project root
ROOT_DIR = Path(__file__).resolve().parents[2]


class StreamingContractProcessor:
    """
    High-throughput streaming processor using Spark Structured Streaming
    Processes records as micro-batches for optimal performance
    """
    
    def __init__(self, spark: SparkSession, base_path: str = None):
        self.spark = spark
        self.base_path = base_path or os.path.join(ROOT_DIR, "data", "bronze")
        
        # Configure Spark for high-throughput streaming
        self.spark.conf.set("spark.sql.streaming.checkpointFileManagerClass", 
                           "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        self.spark.conf.set("spark.sql.streaming.stateStore.providerClass",
                           "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        
    def create_streaming_source(self, 
                                source_type: str = "memory",
                                topic_name: str = "contract_data",
                                kafka_servers: str = None) -> Any:
        """
        Create streaming source (memory, socket, or Kafka)
        
        Args:
            source_type: 'memory', 'socket', or 'kafka'
            topic_name: Topic/table name for source
            kafka_servers: Kafka bootstrap servers if using Kafka
        """
        if source_type == "memory":
            # In-memory table for testing/fast ingestion
            return self.spark.readStream.format("memory").table(topic_name)
            
        elif source_type == "kafka" and kafka_servers:
            # Kafka source for production streaming
            return self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_servers) \
                .option("subscribe", topic_name) \
                .option("startingOffsets", "latest") \
                .option("maxOffsetsPerTrigger", 10000) \
                .load()
                
        elif source_type == "socket":
            # Socket source for development
            return self.spark.readStream \
                .format("socket") \
                .option("host", "localhost") \
                .option("port", 9999) \
                .load()
                
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    
    def apply_contract_transforms(self, 
                                  df: Any,
                                  contract: Dict[str, Any],
                                  domain: str,
                                  product: str) -> Any:
        """
        Apply contract-based transformations to streaming DataFrame
        """
        schema_fields = contract.get("schema", {}).get("fields", [])
        quality_rules = contract.get("quality_rules", {})
        
        # Parse JSON if reading from Kafka
        if "value" in df.columns and df.schema["value"].dataType == StringType():
            # Infer schema from contract
            json_schema = self._contract_to_spark_schema(schema_fields)
            df = df.select(from_json(col("value").cast("string"), json_schema).alias("data"))
            df = df.select("data.*")
        
        # Apply MS conversions
        for field in schema_fields:
            field_name = field.get("name")
            if field.get("ms") is True and field_name in df.columns:
                df = df.withColumn(
                    field_name,
                    (when(col(field_name).isNotNull(),
                         to_timestamp(col(field_name)).cast("long") * lit(1000))
                     .otherwise(None))
                )
        
        # Add InsertionTime if missing
        if "InsertionTime" not in df.columns:
            df = df.withColumn("InsertionTime", 
                             (current_timestamp().cast("long") * lit(1000)))
        
        # Add partition columns
        ts_sec = (col("InsertionTime") / lit(1000)).cast("double")
        df = df.withColumn("year", year(to_timestamp(ts_sec)))
        df = df.withColumn("month", month(to_timestamp(ts_sec)))
        df = df.withColumn("day", dayofmonth(to_timestamp(ts_sec)))
        
        # Apply quality filters
        required_fields = quality_rules.get("required", [])
        for field in required_fields:
            if field in df.columns:
                df = df.filter(col(field).isNotNull())
        
        return df
    
    def _contract_to_spark_schema(self, fields: List[Dict[str, Any]]) -> StructType:
        """Convert contract schema to Spark StructType"""
        spark_fields = []
        
        for field in fields:
            name = field.get("name")
            field_type = field.get("type", "string")
            
            if field_type == "long" or field.get("ms") is True:
                spark_type = LongType()
            elif field_type == "integer":
                spark_type = IntegerType()
            else:
                spark_type = StringType()
                
            spark_fields.append(StructField(name, spark_type, True))
        
        # Add partition columns
        spark_fields.extend([
            StructField("InsertionTime", LongType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("day", IntegerType(), True)
        ])
        
        return StructType(spark_fields)
    
    def write_to_delta_streaming(self,
                                 df: Any,
                                 table_name: str,
                                 checkpoint_location: str = None,
                                 trigger_interval: str = "10 seconds",
                                 partition_cols: List[str] = None) -> Any:
        """
        Write streaming DataFrame to Delta Lake with optimizations
        
        Args:
            df: Streaming DataFrame
            table_name: Target table name
            checkpoint_location: Checkpoint directory
            trigger_interval: How often to trigger micro-batches
            partition_cols: Partition columns
            
        Returns:
            StreamingQuery object
        """
        table_path = os.path.join(self.base_path, table_name)
        
        if checkpoint_location is None:
            checkpoint_location = os.path.join(ROOT_DIR, "data", "checkpoints", table_name)
        
        partition_cols = partition_cols or ["year", "month", "day"]
        
        # Configure streaming write
        query = df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_location) \
            .option("mergeSchema", "true") \
            .partitionBy(*partition_cols) \
            .trigger(processingTime=trigger_interval) \
            .start(table_path)
        
        logger.info(f"Started streaming query for {table_name} with trigger interval {trigger_interval}")
        
        return query
    
    def process_streaming_contract(self,
                                   contract: Dict[str, Any],
                                   domain: str,
                                   product: str,
                                   source_type: str = "memory",
                                   topic_name: str = None,
                                   trigger_interval: str = "5 seconds") -> Any:
        """
        End-to-end streaming contract processing
        
        Args:
            contract: Contract definition
            domain: Data domain
            product: Data product
            source_type: Streaming source type
            topic_name: Source topic/table name
            trigger_interval: Micro-batch trigger interval
            
        Returns:
            StreamingQuery object
        """
        table_name = f"{domain}_{product}".replace("-", "_")
        topic = topic_name or table_name
        
        # Create streaming source
        source_df = self.create_streaming_source(source_type, topic)
        
        # Apply contract transformations
        processed_df = self.apply_contract_transforms(source_df, contract, domain, product)
        
        # Write to Delta Lake
        query = self.write_to_delta_streaming(
            processed_df,
            table_name,
            trigger_interval=trigger_interval
        )
        
        return query


def create_optimized_spark_session(app_name: str = "ContractStreamingProcessor") -> SparkSession:
    """
    Create Spark session optimized for high-throughput streaming
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "8") \
        .config("spark.sql.streaming.metricsEnabled", "true") \
        .config("spark.sql.streaming.stateStore.compression.codec", "lz4") \
        .config("spark.sql.streaming.fileSource.log.deletion", "true") \
        .config("spark.sql.streaming.fileSource.log.cleanupDelay", "10") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()


# Batch micro-batch processing for ultra-high throughput
def process_micro_batches(spark: SparkSession,
                         records: List[Dict[str, Any]],
                         contract: Dict[str, Any],
                         domain: str,
                         product: str,
                         batch_size: int = 10000) -> Dict[str, Any]:
    """
    Process large record sets as micro-batches using foreachBatch
    Provides streaming-like performance for batch data
    
    Args:
        spark: Spark session
        records: List of records to process
        contract: Contract definition
        domain: Data domain
        product: Data product
        batch_size: Records per micro-batch
        
    Returns:
        Processing statistics
    """
    from datetime import datetime
    
    start_time = datetime.now()
    table_name = f"{domain}_{product}".replace("-", "_")
    table_path = os.path.join(ROOT_DIR, "data", "bronze", table_name)
    
    # Create temporary in-memory table
    temp_table = f"temp_{table_name}_{int(start_time.timestamp())}"
    
    df = spark.createDataFrame(records)
    df.createOrReplaceTempView(temp_table)
    
    # Process as stream with micro-batches
    processor = StreamingContractProcessor(spark)
    source_df = spark.readStream.table(temp_table)
    
    # Apply transformations
    processed_df = processor.apply_contract_transforms(source_df, contract, domain, product)
    
    # Write with foreachBatch for maximum control
    def write_micro_batch(batch_df, batch_id):
        if batch_df.count() > 0:
            batch_df.write.format("delta").mode("append") \
                .partitionBy("year", "month", "day") \
                .save(table_path)
            logger.info(f"Wrote micro-batch {batch_id} with {batch_df.count()} records")
    
    query = processed_df.writeStream \
        .foreachBatch(write_micro_batch) \
        .trigger(availableNow=True) \
        .start()
    
    query.awaitTermination()
    
    duration = (datetime.now() - start_time).total_seconds()
    rate = len(records) / duration if duration > 0 else 0
    
    return {
        "success": True,
        "records": len(records),
        "duration": duration,
        "rate": rate,
        "table": table_name
    }

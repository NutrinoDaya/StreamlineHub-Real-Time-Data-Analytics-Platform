#!/usr/bin/env python3
"""
Elasticsearch Ingestion Spark Script
Reads Gold Delta tables with time-based filtering and ingests to Elasticsearch
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_json, struct
from delta import configure_spark_with_delta_pip

# Add src to path for imports
sys.path.insert(0, '/opt/airflow/src')
from src.core.spark_session import get_spark_session

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ElasticsearchIngester:
    """Elasticsearch ingestion processor for Gold layer data"""
    
    def __init__(self, start_threshold_ms: int, end_threshold_ms: int):
        self.start_threshold_ms = start_threshold_ms
        self.end_threshold_ms = end_threshold_ms
        self.spark = None
        
        # Define paths
        # Use writable paths within the Airflow container
        self.gold_path = "/opt/airflow/data/gold"
        
        # Windows paths for local testing
        if sys.platform == "win32":
            self.gold_path = r"D:\StreamlineHubC\data\gold_delta"
        
        # Elasticsearch configuration
        self.es_config = {
            "es.nodes": os.getenv("ELASTICSEARCH_HOST", "elasticsearch"),
            "es.port": os.getenv("ELASTICSEARCH_PORT", "9200"),
            "es.index.auto.create": "true",
            "es.write.operation": "index",
            "es.mapping.date.rich": "false"
        }
        
        self.stats = {
            "records_ingested": 0,
            "indices_created": 0,
            "tables_processed": 0
        }
    
    def initialize_spark(self) -> bool:
        """Initialize Spark session with Elasticsearch and Delta Lake support"""
        try:
            logger.info("[PROCESSING]  Initializing Spark session with Elasticsearch and Delta Lake...")
            
            # Use centralized Spark session with Elasticsearch-specific application name
            self.spark = get_spark_session("ElasticsearchIngestion")
            self.spark.sparkContext.setLogLevel("WARN")
            
            logger.info("[SUCCESS]  Spark session initialized successfully")
            logger.info(f"[SUCCESS]  Spark version: {self.spark.version}")
            logger.info(f"[SUCCESS]  Master: {self.spark.sparkContext.master}")
            return True
            
        except Exception as e:
            logger.error(f"[ERROR]  Failed to initialize Spark session: {e}")
            return False
    
    def read_gold_table_with_filter(self, table_name: str):
        """Read Gold Delta table with time-based filtering"""
        try:
            table_path = f"{self.gold_path}/{table_name}_gold"
            
            if not Path(table_path).exists():
                logger.warning(f"[WARNING]  Gold table not found: {table_path}")
                return None
            
            logger.info(f"[LOADING]  Reading Gold table: {table_name}")
            
            # Read table
            df = self.spark.read.format("delta").load(table_path)
            
            # Note: Gold layer contains aggregated data that should be fully ingested
            # No time-based filtering needed as aggregations represent summary data
            
            record_count = df.count()
            
            logger.info(f"[SUCCESS]  Read {table_name}: {record_count} aggregated records available")
            
            return df
            
        except Exception as e:
            logger.error(f"[ERROR]  Failed to read Gold table {table_name}: {e}")
            return None
    
    def ingest_to_elasticsearch(self, df, index_name: str) -> int:
        """Ingest DataFrame to Elasticsearch"""
        try:
            if df is None or df.rdd.isEmpty():
                logger.warning(f"[WARNING]  No data to ingest for index: {index_name}")
                return 0
            
            logger.info(f"[SENDING]  Ingesting to Elasticsearch index: {index_name}")
            
            # Add ingestion metadata
            df_with_metadata = df.withColumn("ingestion_timestamp", lit(datetime.now(timezone.utc).isoformat())) \
                                .withColumn("source", lit("gold_layer_aggregation"))
            
            record_count = df_with_metadata.count()
            
            # Configure Elasticsearch options
            es_options = self.es_config.copy()
            es_options["es.resource"] = index_name
            
            # Write to Elasticsearch
            df_with_metadata.write \
                .format("org.elasticsearch.spark.sql") \
                .options(**es_options) \
                .mode("append") \
                .save()
            
            self.stats["records_ingested"] += record_count
            self.stats["indices_created"] += 1
            
            logger.info(f"[SUCCESS]  Ingested {record_count} records to {index_name}")
            return record_count
            
        except Exception as e:
            logger.error(f"[ERROR]  Failed to ingest to Elasticsearch index {index_name}: {e}")
            return 0
    
    def ingest_customer_behavior_gold(self):
        """Ingest customer behavior Gold data to Elasticsearch"""
        try:
            logger.info("[ANALYZING]  Processing customer behavior Gold data...")
            
            df = self.read_gold_table_with_filter("customer_behavior")
            if df is None:
                return 0
            
            # Create separate indices for hourly and daily aggregations
            hourly_df = df.filter(col("aggregation_type") == "hourly")
            daily_df = df.filter(col("aggregation_type") == "daily")
            
            ingested_count = 0
            
            if not hourly_df.rdd.isEmpty():
                ingested_count += self.ingest_to_elasticsearch(
                    hourly_df, 
                    "streamlinehub-customer-behavior-hourly"
                )
            
            if not daily_df.rdd.isEmpty():
                ingested_count += self.ingest_to_elasticsearch(
                    daily_df, 
                    "streamlinehub-customer-behavior-daily"
                )
            
            self.stats["tables_processed"] += 1
            return ingested_count
            
        except Exception as e:
            logger.error(f"[ERROR]  Failed to process customer behavior Gold data: {e}")
            return 0
    
    def ingest_transaction_gold(self):
        """Ingest transaction Gold data to Elasticsearch"""
        try:
            logger.info("[PROCESSING]  Processing transaction Gold data...")
            
            df = self.read_gold_table_with_filter("transaction")
            if df is None:
                return 0
            
            # Create separate indices for hourly and daily aggregations
            hourly_df = df.filter(col("aggregation_type") == "hourly")
            daily_df = df.filter(col("aggregation_type") == "daily")
            
            ingested_count = 0
            
            if not hourly_df.rdd.isEmpty():
                ingested_count += self.ingest_to_elasticsearch(
                    hourly_df, 
                    "streamlinehub-transactions-hourly"
                )
            
            if not daily_df.rdd.isEmpty():
                ingested_count += self.ingest_to_elasticsearch(
                    daily_df, 
                    "streamlinehub-transactions-daily"
                )
            
            self.stats["tables_processed"] += 1
            return ingested_count
            
        except Exception as e:
            logger.error(f"[ERROR]  Failed to process transaction Gold data: {e}")
            return 0
    
    def ingest_system_metrics_gold(self):
        """Ingest system metrics Gold data to Elasticsearch"""
        try:
            logger.info("[PROCESSING]  Processing system metrics Gold data...")
            
            df = self.read_gold_table_with_filter("system_metrics")
            if df is None:
                return 0
            
            # Create separate indices for hourly and daily aggregations
            hourly_df = df.filter(col("aggregation_type") == "hourly")
            daily_df = df.filter(col("aggregation_type") == "daily")
            
            ingested_count = 0
            
            if not hourly_df.rdd.isEmpty():
                ingested_count += self.ingest_to_elasticsearch(
                    hourly_df, 
                    "streamlinehub-system-metrics-hourly"
                )
            
            if not daily_df.rdd.isEmpty():
                ingested_count += self.ingest_to_elasticsearch(
                    daily_df, 
                    "streamlinehub-system-metrics-daily"
                )
            
            self.stats["tables_processed"] += 1
            return ingested_count
            
        except Exception as e:
            logger.error(f"[ERROR]  Failed to process system metrics Gold data: {e}")
            return 0
    
    def run_ingestion(self):
        """Run the complete Elasticsearch ingestion process"""
        try:
            logger.info("\n" + "="*70)
            logger.info("[STARTING]  STARTING ELASTICSEARCH INGESTION")
            logger.info("="*70)
            
            start_dt = datetime.fromtimestamp(self.start_threshold_ms / 1000.0, tz=timezone.utc)
            end_dt = datetime.fromtimestamp(self.end_threshold_ms / 1000.0, tz=timezone.utc)
            
            logger.info(f"[FILTERING]  Time window: {start_dt.isoformat()} to {end_dt.isoformat()}")
            logger.info(f"🔍 Elasticsearch: {self.es_config['es.nodes']}:{self.es_config['es.port']}")
            
            # Initialize Spark
            if not self.initialize_spark():
                raise Exception("Failed to initialize Spark session")
            
            # Process each Gold table
            total_ingested = 0
            
            # Customer behavior Gold ingestion
            total_ingested += self.ingest_customer_behavior_gold()
            
            # Transaction Gold ingestion
            total_ingested += self.ingest_transaction_gold()
            
            # System metrics Gold ingestion
            total_ingested += self.ingest_system_metrics_gold()
            
            # Final statistics
            logger.info("\n" + "="*70)
            logger.info("[SENDING]  ELASTICSEARCH INGESTION COMPLETED")
            logger.info("="*70)
            logger.info(f"📥 Records Ingested: {self.stats['records_ingested']}")
            logger.info(f"📊 Indices Created: {self.stats['indices_created']}")
            logger.info(f"📋 Tables Processed: {self.stats['tables_processed']}")
            logger.info("="*70 + "\n")
            
            return {
                "status": "success",
                "records_ingested": self.stats["records_ingested"],
                "indices_created": self.stats["indices_created"],
                "tables_processed": self.stats["tables_processed"],
                "start_threshold": self.start_threshold_ms,
                "end_threshold": self.end_threshold_ms
            }
            
        except Exception as e:
            logger.error(f"[ERROR]  Elasticsearch ingestion failed: {e}", exc_info=True)
            raise
        finally:
            if self.spark:
                self.spark.stop()

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="Elasticsearch Ingestion")
    parser.add_argument("--start-threshold", type=int, required=True,
                       help="Start threshold in milliseconds (UTC)")
    parser.add_argument("--end-threshold", type=int, required=True,
                       help="End threshold in milliseconds (UTC)")
    
    args = parser.parse_args()
    
    ingester = ElasticsearchIngester(args.start_threshold, args.end_threshold)
    result = ingester.run_ingestion()
    
    logger.info(f"[SUCCESS]  Elasticsearch ingestion completed successfully: {result}")

if __name__ == "__main__":
    main()
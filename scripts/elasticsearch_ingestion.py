#!/usr/bin/env python3
"""
Elasticsearch Ingestion Script for StreamLineHub Analytics

This script reads aggregated data from Delta Lake tables and ingests them
into Elasticsearch for the analytics dashboard.
"""

import sys
import os
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any
import json

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from elasticsearch import Elasticsearch
    # Import centralized Spark session
    from src.core.spark_session import init_spark_session
except ImportError as e:
    print(f"Required libraries not found: {e}")
    sys.exit(1)

def get_spark_session():
    """Get Spark session with Delta Lake and Elasticsearch configuration."""
    try:
        spark = init_spark_session("StreamLineHub-Elasticsearch-Ingestion")
        
        # Configure Elasticsearch settings
        spark.conf.set("es.index.auto.create", "true")
        spark.conf.set("es.nodes", "elasticsearch")
        spark.conf.set("es.port", "9200")
        spark.conf.set("es.nodes.wan.only", "true")
        
        return spark
    except Exception as e:
        print(f"Failed to create Spark session: {e}")
        sys.exit(1)

def check_elasticsearch_connection() -> Optional[Elasticsearch]:
    """Check connection to Elasticsearch."""
    try:
        es = Elasticsearch(
            [{'host': 'elasticsearch', 'port': 9200, 'scheme': 'http'}],
            request_timeout=30,
            max_retries=3,
            retry_on_timeout=True
        )
        
        if es.ping():
            print("✅ Connected to Elasticsearch")
            return es
        else:
            print("❌ Could not connect to Elasticsearch")
            return None
    except Exception as e:
        print(f"❌ Elasticsearch connection error: {e}")
        return None

def read_delta_table(spark: SparkSession, table_path: str) -> Optional[DataFrame]:
    """Read a Delta table if it exists."""
    path = Path(table_path)
    
    if not path.exists():
        print(f"⚠️  Delta table does not exist: {table_path}")
        return None
    
    try:
        df = spark.read.format("delta").load(table_path)
        count = df.count()
        
        if count == 0:
            print(f"⚠️  Delta table is empty: {table_path}")
            return None
        
        print(f"📖 Read {count} rows from {table_path}")
        return df
    except Exception as e:
        print(f"❌ Error reading Delta table {table_path}: {e}")
        return None

def write_to_elasticsearch(df: DataFrame, index_name: str) -> bool:
    """Write DataFrame to Elasticsearch index."""
    try:
        # Add document ID and timestamp
        df_with_id = df.withColumn("doc_id", monotonically_increasing_id()) \
                      .withColumn("ingestion_timestamp", current_timestamp())
        
        count = df_with_id.count()
        
        if count == 0:
            print(f"⚠️  No data to write to index: {index_name}")
            return True
        
        # Write to Elasticsearch
        df_with_id.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.resource", f"{index_name}/_doc") \
            .option("es.mapping.id", "doc_id") \
            .mode("append") \
            .save()
        
        print(f"✅ Successfully wrote {count} documents to index: {index_name}")
        return True
        
    except Exception as e:
        print(f"❌ Error writing to Elasticsearch index {index_name}: {e}")
        return False

def ingest_customer_aggregations(spark: SparkSession, base_path: Path) -> bool:
    """Ingest customer aggregation data."""
    success = True
    
    # Customer action aggregations
    action_df = read_delta_table(spark, str(base_path / "customer_action_aggregations"))
    if action_df:
        success &= write_to_elasticsearch(action_df, "customer_actions")
    
    # Customer hourly aggregations
    hourly_df = read_delta_table(spark, str(base_path / "customer_hourly_aggregations"))
    if hourly_df:
        success &= write_to_elasticsearch(hourly_df, "customer_hourly")
    
    return success

def ingest_transaction_aggregations(spark: SparkSession, base_path: Path) -> bool:
    """Ingest transaction aggregation data."""
    success = True
    
    # Transaction status aggregations
    status_df = read_delta_table(spark, str(base_path / "transaction_status_aggregations"))
    if status_df:
        success &= write_to_elasticsearch(status_df, "transaction_status")
    
    # Revenue hourly aggregations
    revenue_df = read_delta_table(spark, str(base_path / "revenue_hourly_aggregations"))
    if revenue_df:
        success &= write_to_elasticsearch(revenue_df, "revenue_hourly")
    
    return success

def ingest_system_metrics(spark: SparkSession, base_path: Path) -> bool:
    """Ingest system metrics aggregation data."""
    success = True
    
    # System metrics summary
    summary_df = read_delta_table(spark, str(base_path / "system_metrics_summary"))
    if summary_df:
        success &= write_to_elasticsearch(summary_df, "system_metrics")
    
    # System metrics timeseries
    timeseries_df = read_delta_table(spark, str(base_path / "system_metrics_timeseries"))
    if timeseries_df:
        success &= write_to_elasticsearch(timeseries_df, "metrics_timeseries")
    
    return success

def ingest_pipeline_summary(spark: SparkSession, base_path: Path) -> bool:
    """Ingest pipeline run summary."""
    summary_df = read_delta_table(spark, str(base_path / "pipeline_run_summary"))
    if summary_df:
        return write_to_elasticsearch(summary_df, "pipeline_runs")
    return True

def create_elasticsearch_mappings(es: Elasticsearch) -> None:
    """Create Elasticsearch index mappings for better performance."""
    
    mappings = {
        "customer_actions": {
            "mappings": {
                "properties": {
                    "action": {"type": "keyword"},
                    "event_count": {"type": "long"},
                    "total_value": {"type": "double"},
                    "avg_value": {"type": "double"},
                    "unique_customers": {"type": "long"},
                    "created_at": {"type": "date"}
                }
            }
        },
        "customer_hourly": {
            "mappings": {
                "properties": {
                    "hour": {"type": "date"},
                    "event_count": {"type": "long"},
                    "total_value": {"type": "double"},
                    "unique_customers": {"type": "long"},
                    "unique_actions": {"type": "long"},
                    "created_at": {"type": "date"}
                }
            }
        },
        "transaction_status": {
            "mappings": {
                "properties": {
                    "status": {"type": "keyword"},
                    "transaction_count": {"type": "long"},
                    "total_amount": {"type": "double"},
                    "avg_amount": {"type": "double"},
                    "min_amount": {"type": "double"},
                    "max_amount": {"type": "double"},
                    "created_at": {"type": "date"}
                }
            }
        },
        "revenue_hourly": {
            "mappings": {
                "properties": {
                    "hour": {"type": "date"},
                    "transaction_count": {"type": "long"},
                    "total_revenue": {"type": "double"},
                    "avg_transaction_value": {"type": "double"},
                    "unique_customers": {"type": "long"},
                    "created_at": {"type": "date"}
                }
            }
        },
        "system_metrics": {
            "mappings": {
                "properties": {
                    "metric_name": {"type": "keyword"},
                    "measurement_count": {"type": "long"},
                    "avg_value": {"type": "double"},
                    "min_value": {"type": "double"},
                    "max_value": {"type": "double"},
                    "stddev_value": {"type": "double"},
                    "created_at": {"type": "date"}
                }
            }
        },
        "metrics_timeseries": {
            "mappings": {
                "properties": {
                    "time_bucket": {"type": "date"},
                    "metric_name": {"type": "keyword"},
                    "avg_value": {"type": "double"},
                    "measurement_count": {"type": "long"},
                    "created_at": {"type": "date"}
                }
            }
        },
        "pipeline_runs": {
            "mappings": {
                "properties": {
                    "pipeline_run_id": {"type": "keyword"},
                    "start_time": {"type": "date"},
                    "end_time": {"type": "date"},
                    "processing_duration_seconds": {"type": "double"},
                    "total_events_processed": {"type": "long"},
                    "status": {"type": "keyword"},
                    "created_at": {"type": "date"}
                }
            }
        }
    }
    
    for index_name, mapping in mappings.items():
        try:
            if not es.indices.exists(index=index_name):
                es.indices.create(index=index_name, body=mapping)
                print(f"✅ Created index: {index_name}")
            else:
                print(f"ℹ️  Index already exists: {index_name}")
        except Exception as e:
            print(f"⚠️  Could not create index {index_name}: {e}")

def main():
    """Main function to run the Elasticsearch ingestion."""
    parser = argparse.ArgumentParser(description='StreamLineHub Elasticsearch Ingestion')
    parser.add_argument('--start-threshold', type=int, required=True,
                       help='Start time threshold in milliseconds since epoch')
    parser.add_argument('--end-threshold', type=int, required=True,
                       help='End time threshold in milliseconds since epoch')
    
    args = parser.parse_args()
    
    print(f"Starting Elasticsearch ingestion...")
    print(f"Time range: {args.start_threshold} to {args.end_threshold}")
    
    # Check Elasticsearch connection
    es = check_elasticsearch_connection()
    if not es:
        print("❌ Cannot proceed without Elasticsearch connection")
        sys.exit(1)
    
    # Create index mappings
    print("Creating Elasticsearch index mappings...")
    create_elasticsearch_mappings(es)
    
    # Create Spark session
    spark = get_spark_session()
    
    try:
        base_path = Path("/opt/airflow/data/gold_layer")
        
        if not base_path.exists():
            print(f"❌ Gold layer directory does not exist: {base_path}")
            sys.exit(1)
        
        success = True
        
        print("Ingesting customer aggregations...")
        success &= ingest_customer_aggregations(spark, base_path)
        
        print("Ingesting transaction aggregations...")
        success &= ingest_transaction_aggregations(spark, base_path)
        
        print("Ingesting system metrics...")
        success &= ingest_system_metrics(spark, base_path)
        
        print("Ingesting pipeline summary...")
        success &= ingest_pipeline_summary(spark, base_path)
        
        if success:
            print("✅ Elasticsearch ingestion completed successfully!")
        else:
            print("⚠️  Elasticsearch ingestion completed with some errors")
            sys.exit(1)
        
    except Exception as e:
        print(f"❌ Error in Elasticsearch ingestion: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
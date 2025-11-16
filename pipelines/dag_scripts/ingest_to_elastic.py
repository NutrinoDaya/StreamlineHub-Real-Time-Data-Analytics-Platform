"""
Elasticsearch Ingestion Job

This script reads aggregated data from Delta Lake tables and
ingests them into Elasticsearch for fast queries and analytics.

Elasticsearch indices created:
- streamlinehub_customer_metrics
- streamlinehub_campaign_metrics
- streamlinehub_event_analytics
- streamlinehub_segment_metrics
"""

import argparse
import sys
import os
from pathlib import Path
from datetime import datetime

# Add project root to sys.path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from pyspark.sql.functions import col, date_format, unix_timestamp, concat, lit

# Import centralized Spark session
from src.core.spark_session import init_spark_session

def get_spark_session():
    """Get Spark session with Delta Lake and Elasticsearch support."""
    spark = init_spark_session("StreamLineHub-ES-Ingestion")
    
    # Configure Elasticsearch settings
    spark.conf.set("spark.es.batch.size.bytes", "10mb")
    spark.conf.set("spark.es.batch.size.entries", "10000")
    spark.conf.set("spark.es.batch.write.retry.count", "3")
    spark.conf.set("spark.es.batch.write.retry.wait", "10s")
    
    return spark


def read_from_delta(spark, table_name, window_start, window_end, delta_path="data/delta/gold"):
    """
    Read data from Delta Lake table for the specified time window.
    """
    table_path = f"{delta_path}/{table_name}"
    
    try:
        df = spark.read.format("delta").load(table_path)
        
        # Filter by window
        filtered_df = df.filter(
            (col("window_start") >= window_start) & 
            (col("window_end") <= window_end)
        )
        
        count = filtered_df.count()
        print(f"✓ Read {count} records from Delta table '{table_name}'")
        
        return filtered_df
    except Exception as e:
        print(f"✗ Error reading from {table_name}: {e}")
        return None


def prepare_for_elasticsearch(df, table_name):
    """
    Prepare DataFrame for Elasticsearch ingestion.
    Add document ID and convert timestamps to ISO format.
    """
    if df is None or df.count() == 0:
        return None
    
    # Convert timestamps to string for Elasticsearch
    for col_name in df.columns:
        if "timestamp" in col_name.lower() or col_name in ["window_start", "window_end", "created_at"]:
            df = df.withColumn(col_name, date_format(col(col_name), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    
    # Add document ID based on window_start and other dimensions
    if table_name == "customer_metrics":
        df = df.withColumn("doc_id", 
            unix_timestamp(col("window_start")).cast("string"))
    elif table_name == "campaign_metrics":
        df = df.withColumn("doc_id",
            concat(unix_timestamp(col("window_start")).cast("string"), lit("_"), col("campaign_id")))
    elif table_name == "event_analytics":
        df = df.withColumn("doc_id",
            concat(unix_timestamp(col("window_start")).cast("string"), lit("_"), col("event_type")))
    elif table_name == "segment_metrics":
        df = df.withColumn("doc_id",
            concat(unix_timestamp(col("window_start")).cast("string"), lit("_"), col("segment")))
    
    return df


def write_to_elasticsearch(df, index_name, es_nodes="elasticsearch:9200"):
    """
    Write DataFrame to Elasticsearch index.
    """
    if df is None or df.count() == 0:
        print(f"No data to write to Elasticsearch index '{index_name}'")
        return
    
    try:
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", es_nodes) \
            .option("es.port", "9200") \
            .option("es.resource", index_name) \
            .option("es.mapping.id", "doc_id") \
            .option("es.write.operation", "upsert") \
            .option("es.nodes.wan.only", "true") \
            .mode("append") \
            .save()
        
        count = df.count()
        print(f"✓ Wrote {count} documents to Elasticsearch index '{index_name}'")
    except Exception as e:
        print(f"✗ Error writing to Elasticsearch index '{index_name}': {e}")
        raise


def create_index_mapping(spark, index_name, es_nodes="elasticsearch:9200"):
    """
    Create Elasticsearch index with appropriate mappings if it doesn't exist.
    This is optional as Elasticsearch can auto-create indices.
    """
    # Index mappings based on table schema
    mappings = {
        "dataflow_customer_metrics": {
            "properties": {
                "window_start": {"type": "date"},
                "window_end": {"type": "date"},
                "total_customers": {"type": "integer"},
                "new_customers": {"type": "integer"},
                "active_customers": {"type": "integer"},
                "total_events": {"type": "integer"},
                "avg_customer_value": {"type": "double"},
                "created_at": {"type": "date"}
            }
        },
        "dataflow_campaign_metrics": {
            "properties": {
                "window_start": {"type": "date"},
                "window_end": {"type": "date"},
                "campaign_id": {"type": "keyword"},
                "total_impressions": {"type": "integer"},
                "total_clicks": {"type": "integer"},
                "total_conversions": {"type": "integer"},
                "total_spend": {"type": "double"},
                "ctr": {"type": "double"},
                "conversion_rate": {"type": "double"},
                "created_at": {"type": "date"}
            }
        },
        "dataflow_event_analytics": {
            "properties": {
                "window_start": {"type": "date"},
                "window_end": {"type": "date"},
                "event_type": {"type": "keyword"},
                "event_count": {"type": "integer"},
                "unique_customers": {"type": "integer"},
                "total_revenue": {"type": "double"},
                "avg_revenue": {"type": "double"},
                "created_at": {"type": "date"}
            }
        },
        "dataflow_segment_metrics": {
            "properties": {
                "window_start": {"type": "date"},
                "window_end": {"type": "date"},
                "segment": {"type": "keyword"},
                "customer_count": {"type": "integer"},
                "total_orders": {"type": "integer"},
                "total_revenue": {"type": "double"},
                "avg_order_value": {"type": "double"},
                "created_at": {"type": "date"}
            }
        }
    }
    
    # Note: Index creation with mappings would require REST API call
    # Elasticsearch will auto-create indices with dynamic mapping
    print(f"Index mappings defined for {index_name}")


def main():
    parser = argparse.ArgumentParser(description='Ingest Delta Lake data to Elasticsearch')
    parser.add_argument('--window-start', required=True, help='Window start time (ISO format)')
    parser.add_argument('--window-end', required=True, help='Window end time (ISO format)')
    parser.add_argument('--delta-path', default='/opt/spark-data/delta', help='Delta Lake base path')
    parser.add_argument('--es-nodes', default='elasticsearch:9200', help='Elasticsearch nodes')
    
    args = parser.parse_args()
    
    window_start = datetime.fromisoformat(args.window_start)
    window_end = datetime.fromisoformat(args.window_end)
    
    print(f"Starting Elasticsearch ingestion for window: {window_start} to {window_end}")
    
    # Create Spark session with centralized configuration
    spark = get_spark_session()
    
    try:
        # Define table to index mappings
        table_index_mapping = {
            "customer_metrics": "streamlinehub_customer_metrics",
            "campaign_metrics": "streamlinehub_campaign_metrics",
            "event_analytics": "streamlinehub_event_analytics",
            "segment_metrics": "streamlinehub_segment_metrics"
        }
        
        # Process each table
        for table_name, index_name in table_index_mapping.items():
            print(f"\nProcessing {table_name} -> {index_name}")
            
            # Read from Delta Lake
            df = read_from_delta(spark, table_name, window_start, window_end, args.delta_path)
            
            if df is not None and df.count() > 0:
                # Prepare for Elasticsearch
                prepared_df = prepare_for_elasticsearch(df, table_name)
                
                # Write to Elasticsearch
                write_to_elasticsearch(prepared_df, index_name, args.es_nodes)
            else:
                print(f"No data to ingest for {table_name}")
        
        print("\n✓ Elasticsearch ingestion completed successfully")
        
    except Exception as e:
        print(f"\n✗ Elasticsearch ingestion failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

"""
Spark Aggregation Job for Kafka Data

This script reads data from Delta Lake (or directly from Kafka if needed),
performs various aggregations, and writes results back to Delta Lake tables.

Aggregations performed:
- Customer metrics by 5-minute windows
- Campaign performance metrics
- Event analytics
- Customer segments analysis
"""

import argparse
import sys
import os
from pathlib import Path
from datetime import datetime

# Add project root to sys.path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    window, to_timestamp, from_json, current_timestamp, lit, date_format,
    round as spark_round, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, 
    TimestampType, BooleanType
)

# Import centralized Spark session
from src.core.spark_session import init_spark_session


def ensure_delta_tables(spark, delta_path="data/delta/gold"):
    """
    Ensure Delta Lake tables exist. Create them if they don't.
    """
    tables = {
        "customer_metrics": StructType([
            StructField("window_start", TimestampType(), False),
            StructField("window_end", TimestampType(), False),
            StructField("total_customers", IntegerType(), True),
            StructField("new_customers", IntegerType(), True),
            StructField("active_customers", IntegerType(), True),
            StructField("total_events", IntegerType(), True),
            StructField("avg_customer_value", DoubleType(), True),
            StructField("created_at", TimestampType(), False),
        ]),
        "campaign_metrics": StructType([
            StructField("window_start", TimestampType(), False),
            StructField("window_end", TimestampType(), False),
            StructField("campaign_id", StringType(), True),
            StructField("total_impressions", IntegerType(), True),
            StructField("total_clicks", IntegerType(), True),
            StructField("total_conversions", IntegerType(), True),
            StructField("total_spend", DoubleType(), True),
            StructField("ctr", DoubleType(), True),
            StructField("conversion_rate", DoubleType(), True),
            StructField("created_at", TimestampType(), False),
        ]),
        "event_analytics": StructType([
            StructField("window_start", TimestampType(), False),
            StructField("window_end", TimestampType(), False),
            StructField("event_type", StringType(), True),
            StructField("event_count", IntegerType(), True),
            StructField("unique_customers", IntegerType(), True),
            StructField("total_revenue", DoubleType(), True),
            StructField("avg_revenue", DoubleType(), True),
            StructField("created_at", TimestampType(), False),
        ]),
        "segment_metrics": StructType([
            StructField("window_start", TimestampType(), False),
            StructField("window_end", TimestampType(), False),
            StructField("segment", StringType(), True),
            StructField("customer_count", IntegerType(), True),
            StructField("total_orders", IntegerType(), True),
            StructField("total_revenue", DoubleType(), True),
            StructField("avg_order_value", DoubleType(), True),
            StructField("created_at", TimestampType(), False),
        ])
    }
    
    for table_name, schema in tables.items():
        table_path = f"{delta_path}/{table_name}"
        try:
            spark.read.format("delta").load(table_path)
            print(f"✓ Delta table '{table_name}' exists at {table_path}")
        except Exception as e:
            print(f"Creating Delta table '{table_name}' at {table_path}")
            empty_df = spark.createDataFrame([], schema)
            empty_df.write.format("delta").mode("overwrite").save(table_path)
            print(f"✓ Created Delta table '{table_name}'")


def read_delta_data(spark, window_start, window_end, delta_path="data/delta"):
    """
    Read data from Delta Lake silver tables for the specified time window.
    
    Returns dict with DataFrames for each event type.
    """
    
    dataframes = {}
    
    # Read from Delta Lake silver tables
    table_mappings = {
        "customer-events": "customer_events",
        "transaction-data": "transaction_events", 
        "analytics-data": "analytics_events"
    }
    
    for kafka_topic, table_name in table_mappings.items():
        try:
            table_path = f"{delta_path}/silver/{table_name}"
            
            # Check if table exists
            if os.path.exists(table_path):
                df = spark.read.format("delta").load(table_path)
                
                # Filter by time window if event_timestamp column exists
                if "event_timestamp" in df.columns:
                    filtered_df = df.filter(
                        (col("event_timestamp") >= window_start) & 
                        (col("event_timestamp") <= window_end)
                    )
                else:
                    # Fallback to processing_timestamp
                    filtered_df = df.filter(
                        (col("processing_timestamp") >= window_start) & 
                        (col("processing_timestamp") <= window_end)
                    )
                
                dataframes[kafka_topic] = filtered_df
                count = filtered_df.count()
                print(f"✓ Read {count} records from Delta table {table_name}")
            else:
                print(f"⚠️ Delta table {table_name} not found at {table_path}")
                # Create empty DataFrame with basic schema
                empty_schema = StructType([
                    StructField("event_type", StringType(), True),
                    StructField("timestamp", StringType(), True)
                ])
                dataframes[kafka_topic] = spark.createDataFrame([], empty_schema)
                
        except Exception as e:
            print(f"✗ Error reading from {table_name}: {e}")
            # Create empty DataFrame
            empty_schema = StructType([
                StructField("event_type", StringType(), True),
                StructField("timestamp", StringType(), True)
            ])
            dataframes[kafka_topic] = spark.createDataFrame([], empty_schema)
    
    return dataframes


def aggregate_customer_metrics(customer_events_df, window_start, window_end):
    """
    Aggregate customer metrics from customer events.
    """
    if customer_events_df.count() == 0:
        print("No customer events to aggregate")
        return None
    
    metrics = customer_events_df.agg(
        count(col("customer_id")).alias("total_events"),
        count(col("customer_id").isNotNull()).alias("active_customers"),
        spark_sum(coalesce(col("event_value"), lit(0))).alias("total_value"),
    ).withColumn(
        "avg_customer_value",
        spark_round(col("total_value") / col("active_customers"), 2)
    ).withColumn(
        "window_start", lit(window_start).cast(TimestampType())
    ).withColumn(
        "window_end", lit(window_end).cast(TimestampType())
    ).withColumn(
        "created_at", current_timestamp()
    ).withColumn(
        "total_customers", col("active_customers")
    ).withColumn(
        "new_customers", lit(0)  # Would need historical data to compute
    ).select(
        "window_start", "window_end", "total_customers", "new_customers",
        "active_customers", "total_events", "avg_customer_value", "created_at"
    )
    
    return metrics


def aggregate_campaign_metrics(campaign_events_df, window_start, window_end):
    """
    Aggregate campaign performance metrics.
    """
    if campaign_events_df.count() == 0:
        print("No campaign events to aggregate")
        return None
    
    metrics = campaign_events_df.groupBy("campaign_id").agg(
        spark_sum("impressions").alias("total_impressions"),
        spark_sum("clicks").alias("total_clicks"),
        spark_sum("conversions").alias("total_conversions"),
        spark_sum("spend").alias("total_spend"),
    ).withColumn(
        "ctr",
        spark_round((col("total_clicks") / col("total_impressions")) * 100, 2)
    ).withColumn(
        "conversion_rate",
        spark_round((col("total_conversions") / col("total_clicks")) * 100, 2)
    ).withColumn(
        "window_start", lit(window_start).cast(TimestampType())
    ).withColumn(
        "window_end", lit(window_end).cast(TimestampType())
    ).withColumn(
        "created_at", current_timestamp()
    )
    
    return metrics


def aggregate_event_analytics(customer_events_df, window_start, window_end):
    """
    Aggregate event analytics by event type.
    """
    if customer_events_df.count() == 0:
        print("No events to aggregate")
        return None
    
    metrics = customer_events_df.groupBy("event_type").agg(
        count("*").alias("event_count"),
        count(col("customer_id")).alias("unique_customers"),
        spark_sum(coalesce(col("event_value"), lit(0))).alias("total_revenue"),
    ).withColumn(
        "avg_revenue",
        spark_round(col("total_revenue") / col("event_count"), 2)
    ).withColumn(
        "window_start", lit(window_start).cast(TimestampType())
    ).withColumn(
        "window_end", lit(window_end).cast(TimestampType())
    ).withColumn(
        "created_at", current_timestamp()
    )
    
    return metrics


def aggregate_segment_metrics(customer_events_df, window_start, window_end):
    """
    Aggregate metrics by customer segment.
    """
    if customer_events_df.count() == 0:
        print("No customer events to aggregate")
        return None
    
    metrics = customer_events_df.groupBy("segment").agg(
        count(col("customer_id")).alias("customer_count"),
        count(col("event_type")).alias("total_orders"),
        spark_sum(coalesce(col("event_value"), lit(0))).alias("total_revenue"),
    ).withColumn(
        "avg_order_value",
        spark_round(col("total_revenue") / col("total_orders"), 2)
    ).withColumn(
        "window_start", lit(window_start).cast(TimestampType())
    ).withColumn(
        "window_end", lit(window_end).cast(TimestampType())
    ).withColumn(
        "created_at", current_timestamp()
    )
    
    return metrics


def write_to_delta(df, table_name, delta_path="data/delta/gold"):
    """
    Write aggregated data to Delta Lake table.
    """
    if df is None or df.count() == 0:
        print(f"No data to write to {table_name}")
        return
    
    table_path = f"{delta_path}/{table_name}"
    
    try:
        df.write \
            .format("delta") \
            .mode("append") \
            .save(table_path)
        
        count = df.count()
        print(f"✓ Wrote {count} records to Delta table '{table_name}'")
    except Exception as e:
        print(f"✗ Error writing to {table_name}: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(description='Aggregate Kafka data to Delta Lake')
    parser.add_argument('--window-start', required=True, help='Window start time (ISO format)')
    parser.add_argument('--window-end', required=True, help='Window end time (ISO format)')
    parser.add_argument('--delta-path', default='data/delta', help='Delta Lake base path')
    
    args = parser.parse_args()
    
    window_start = datetime.fromisoformat(args.window_start)
    window_end = datetime.fromisoformat(args.window_end)
    
    print(f"Starting aggregation for window: {window_start} to {window_end}")
    
    # Create Spark session using centralized configuration
    spark = init_spark_session("StreamLineHub-Aggregation")
    
    try:
        # Ensure Delta tables exist
        ensure_delta_tables(spark, args.delta_path)
        
        # Read Delta Lake data
        print("Reading data from Delta Lake silver tables...")
        dataframes = read_delta_data(spark, window_start, window_end, args.delta_path)
        
        # Perform aggregations
        print("Performing aggregations...")
        
        # Customer metrics
        customer_metrics = aggregate_customer_metrics(
            dataframes["customer-events"], 
            window_start, 
            window_end
        )
        write_to_delta(customer_metrics, "customer_metrics", f"{args.delta_path}/gold")
        
        # Campaign metrics
        campaign_metrics = aggregate_campaign_metrics(
            dataframes["campaign-events"],
            window_start,
            window_end
        )
        write_to_delta(campaign_metrics, "campaign_metrics", f"{args.delta_path}/gold")
        
        # Event analytics
        event_analytics = aggregate_event_analytics(
            dataframes["customer-events"],
            window_start,
            window_end
        )
        write_to_delta(event_analytics, "event_analytics", f"{args.delta_path}/gold")
        
        # Segment metrics
        segment_metrics = aggregate_segment_metrics(
            dataframes["customer-events"],
            window_start,
            window_end
        )
        write_to_delta(segment_metrics, "segment_metrics", f"{args.delta_path}/gold")
        
        print("✓ Aggregation completed successfully")
        
    except Exception as e:
        print(f"✗ Aggregation failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

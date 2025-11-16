#!/usr/bin/env python3
"""
Aggregation Runner for Airflow

This script processes Kafka events and creates aggregations that are stored
in Delta Lake format in the data/gold_layer directory.
"""

import sys
import os
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
import uuid

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Store built-in round function before importing Spark functions
builtin_round = round

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    # Import centralized Spark session
    from src.core.spark_session import init_spark_session
except ImportError as e:
    print(f"Required libraries not found: {e}")
    sys.exit(1)

def read_kafka_data_from_logs(spark: SparkSession, start_ms: int, end_ms: int) -> Dict[str, Any]:
    """
    Simulate reading Kafka data by parsing recent events from our log files or memory.
    In a real implementation, this would read from Kafka topics or stored event data.
    """
    
    # Sample event data structure based on our event generators
    sample_events = []
    
    # Generate some sample aggregation data for demonstration
    start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)
    
    print(f"Processing data for time range: {start_dt} to {end_dt}")
    
    # Customer events aggregation
    customer_events = []
    for i in range(50):  # Sample 50 customer events
        event_time = start_dt + timedelta(seconds=i * 10)
        customer_events.append({
            'event_id': str(uuid.uuid4()),
            'event_type': 'customer_behavior',
            'customer_id': 1000 + (i % 100),
            'action': ['view_product', 'add_to_cart', 'purchase'][i % 3],
            'timestamp': event_time.isoformat(),
            'value': builtin_round((i * 10.5) % 500, 2),
            'processed_at': datetime.now(timezone.utc).isoformat()
        })
    
    # Transaction events aggregation
    transaction_events = []
    for i in range(30):  # Sample 30 transaction events
        event_time = start_dt + timedelta(seconds=i * 15)
        transaction_events.append({
            'event_id': str(uuid.uuid4()),
            'event_type': 'transaction_completed',
            'transaction_id': f'txn_{10000 + i}',
            'customer_id': 2000 + (i % 50),
            'amount': builtin_round((i * 25.75) % 1000, 2),
            'status': 'completed',
            'timestamp': event_time.isoformat(),
            'processed_at': datetime.now(timezone.utc).isoformat()
        })
    
    # System metrics
    system_metrics = []
    for i in range(20):  # Sample 20 metric events
        event_time = start_dt + timedelta(seconds=i * 20)
        system_metrics.append({
            'event_id': str(uuid.uuid4()),
            'event_type': 'system_metric',
            'metric_name': ['cpu_usage_percent', 'memory_usage_mb', 'active_users'][i % 3],
            'value': builtin_round((i * 15.25) % 100, 2),
            'timestamp': event_time.isoformat(),
            'processed_at': datetime.now(timezone.utc).isoformat()
        })
    
    return {
        'customer_events': customer_events,
        'transaction_events': transaction_events,
        'system_metrics': system_metrics,
        'total_events': len(customer_events) + len(transaction_events) + len(system_metrics)
    }

def create_customer_aggregations(spark: SparkSession, events: List[Dict]) -> None:
    """Create customer behavior aggregations."""
    if not events:
        print("No customer events to process")
        return
    
    # Convert to DataFrame
    df = spark.createDataFrame(events)
    
    # Create aggregations by action type
    action_summary = df.groupBy('action') \
        .agg(
            count('*').alias('event_count'),
            sum('value').alias('total_value'),
            avg('value').alias('avg_value'),
            countDistinct('customer_id').alias('unique_customers')
        ) \
        .withColumn('aggregation_type', lit('customer_actions')) \
        .withColumn('created_at', current_timestamp())
    
    # Create hourly aggregations
    hourly_summary = df.withColumn('hour', date_format(col('timestamp'), 'yyyy-MM-dd HH:00:00')) \
        .groupBy('hour') \
        .agg(
            count('*').alias('event_count'),
            sum('value').alias('total_value'),
            countDistinct('customer_id').alias('unique_customers'),
            countDistinct('action').alias('unique_actions')
        ) \
        .withColumn('aggregation_type', lit('customer_hourly')) \
        .withColumn('created_at', current_timestamp())
    
    # Save to Delta Lake
    output_path = Path("/opt/airflow/data/gold_layer")
    output_path.mkdir(parents=True, exist_ok=True)
    
    action_path = str(output_path / "customer_action_aggregations")
    hourly_path = str(output_path / "customer_hourly_aggregations")
    
    try:
        # Use JSON format in Airflow environment for compatibility, Delta Lake otherwise
        if os.getenv('AIRFLOW_CTX_DAG_ID'):
            print(f"Airflow mode: Saving customer aggregations as JSON...")
            print(f"Saving action_summary to {action_path}_json")
            action_summary.write.format("json").mode("overwrite").save(action_path + "_json")
            print(f"✅ Saved customer action aggregations to {action_path}_json (JSON)")
            
            print(f"Saving hourly_summary to {hourly_path}_json")
            hourly_summary.write.format("json").mode("overwrite").save(hourly_path + "_json")
            print(f"✅ Saved customer hourly aggregations to {hourly_path}_json (JSON)")
        else:
            # Try Delta Lake first, fall back to parquet
            try:
                print(f"Attempting to save action_summary with {action_summary.count()} rows to Delta Lake...")
                action_summary.write.format("delta").mode("append").save(action_path)
                print(f"✅ Saved customer action aggregations to {action_path} (Delta)")
            except Exception as delta_e:
                print(f"Delta Lake failed for action_summary: {delta_e}")
                print(f"Attempting parquet fallback...")
                action_summary.write.format("parquet").mode("overwrite").save(action_path + "_parquet")
                print(f"✅ Saved customer action aggregations to {action_path}_parquet (Parquet fallback)")
            
            try:
                print(f"Attempting to save hourly_summary with {hourly_summary.count()} rows to Delta Lake...")
                hourly_summary.write.format("delta").mode("append").save(hourly_path)
                print(f"✅ Saved customer hourly aggregations to {hourly_path} (Delta)")
            except Exception as delta_e:
                print(f"Delta Lake failed for hourly_summary: {delta_e}")
                print(f"Attempting parquet fallback...")
                hourly_summary.write.format("parquet").mode("overwrite").save(hourly_path + "_parquet")
                print(f"✅ Saved customer hourly aggregations to {hourly_path}_parquet (Parquet fallback)")
    except Exception as e:
        print(f"❌ Error saving customer aggregations: {e}")
        import traceback
        traceback.print_exc()

def create_transaction_aggregations(spark: SparkSession, events: List[Dict]) -> None:
    """Create transaction aggregations."""
    if not events:
        print("No transaction events to process")
        return
    
    # Convert to DataFrame
    df = spark.createDataFrame(events)
    
    # Transaction summary by status
    status_summary = df.groupBy('status') \
        .agg(
            count('*').alias('transaction_count'),
            sum('amount').alias('total_amount'),
            avg('amount').alias('avg_amount'),
            min('amount').alias('min_amount'),
            max('amount').alias('max_amount')
        ) \
        .withColumn('aggregation_type', lit('transaction_status')) \
        .withColumn('created_at', current_timestamp())
    
    # Revenue by hour
    revenue_hourly = df.withColumn('hour', date_format(col('timestamp'), 'yyyy-MM-dd HH:00:00')) \
        .groupBy('hour') \
        .agg(
            count('*').alias('transaction_count'),
            sum('amount').alias('total_revenue'),
            avg('amount').alias('avg_transaction_value'),
            countDistinct('customer_id').alias('unique_customers')
        ) \
        .withColumn('aggregation_type', lit('revenue_hourly')) \
        .withColumn('created_at', current_timestamp())
    
    # Save to Delta Lake
    output_path = Path("/opt/airflow/data/gold_layer")
    
    status_path = str(output_path / "transaction_status_aggregations")
    revenue_path = str(output_path / "revenue_hourly_aggregations")
    
    try:
        # Use JSON format in Airflow environment for compatibility, Delta Lake otherwise
        if os.getenv('AIRFLOW_CTX_DAG_ID'):
            print(f"Airflow mode: Saving transaction aggregations as JSON...")
            print(f"Saving status_summary to {status_path}_json")
            status_summary.write.format("json").mode("overwrite").save(status_path + "_json")
            print(f"✅ Saved transaction status aggregations to {status_path}_json (JSON)")
            
            print(f"Saving revenue_hourly to {revenue_path}_json")
            revenue_hourly.write.format("json").mode("overwrite").save(revenue_path + "_json")
            print(f"✅ Saved revenue hourly aggregations to {revenue_path}_json (JSON)")
        else:
            # Try Delta Lake first, fall back to parquet
            try:
                print(f"Attempting to save status_summary with {status_summary.count()} rows to Delta Lake...")
                status_summary.write.format("delta").mode("append").save(status_path)
                print(f"✅ Saved transaction status aggregations to {status_path} (Delta)")
            except Exception as delta_e:
                print(f"Delta Lake failed for status_summary: {delta_e}")
                print(f"Attempting parquet fallback...")
                status_summary.write.format("parquet").mode("overwrite").save(status_path + "_parquet")
                print(f"✅ Saved transaction status aggregations to {status_path}_parquet (Parquet fallback)")
            
            try:
                print(f"Attempting to save revenue_hourly with {revenue_hourly.count()} rows to Delta Lake...")
                revenue_hourly.write.format("delta").mode("append").save(revenue_path)
                print(f"✅ Saved revenue hourly aggregations to {revenue_path} (Delta)")
            except Exception as delta_e:
                print(f"Delta Lake failed for revenue_hourly: {delta_e}")
                print(f"Attempting parquet fallback...")
                revenue_hourly.write.format("parquet").mode("overwrite").save(revenue_path + "_parquet")
                print(f"✅ Saved revenue hourly aggregations to {revenue_path}_parquet (Parquet fallback)")
    except Exception as e:
        print(f"❌ Error saving transaction aggregations: {e}")
        import traceback
        traceback.print_exc()

def create_system_metrics_aggregations(spark: SparkSession, events: List[Dict]) -> None:
    """Create system metrics aggregations."""
    if not events:
        print("No system metric events to process")
        return
    
    # Convert to DataFrame
    df = spark.createDataFrame(events)
    
    # Metrics summary by type
    metrics_summary = df.groupBy('metric_name') \
        .agg(
            count('*').alias('measurement_count'),
            avg('value').alias('avg_value'),
            min('value').alias('min_value'),
            max('value').alias('max_value'),
            stddev('value').alias('stddev_value')
        ) \
        .withColumn('aggregation_type', lit('system_metrics')) \
        .withColumn('created_at', current_timestamp())
    
    # Metrics by time intervals (5-minute buckets)
    from pyspark.sql.functions import floor as spark_floor
    time_series = df.withColumn(
        'time_bucket', 
        date_format(
            from_unixtime(
                spark_floor(unix_timestamp(col('timestamp')) / 300) * 300
            ), 
            'yyyy-MM-dd HH:mm:ss'
        )
    ).groupBy('time_bucket', 'metric_name') \
        .agg(
            avg('value').alias('avg_value'),
            count('*').alias('measurement_count')
        ) \
        .withColumn('aggregation_type', lit('metrics_timeseries')) \
        .withColumn('created_at', current_timestamp())
    
    # Save to Delta Lake
    output_path = Path("/opt/airflow/data/gold_layer")
    
    summary_path = str(output_path / "system_metrics_summary")
    timeseries_path = str(output_path / "system_metrics_timeseries")
    
    try:
        # Use JSON format in Airflow environment for compatibility, Delta Lake otherwise
        if os.getenv('AIRFLOW_CTX_DAG_ID'):
            print(f"Airflow mode: Saving system metrics aggregations as JSON...")
            print(f"Saving metrics_summary to {summary_path}_json")
            metrics_summary.write.format("json").mode("overwrite").save(summary_path + "_json")
            print(f"✅ Saved system metrics aggregations to {summary_path}_json (JSON)")
            
            print(f"Saving time_series to {timeseries_path}_json")
            time_series.write.format("json").mode("overwrite").save(timeseries_path + "_json")
            print(f"✅ Saved metrics timeseries to {timeseries_path}_json (JSON)")
        else:
            # Try Delta Lake first, fall back to parquet
            try:
                print(f"Attempting to save metrics_summary with {metrics_summary.count()} rows to Delta Lake...")
                metrics_summary.write.format("delta").mode("append").save(summary_path)
                print(f"✅ Saved system metrics aggregations to {summary_path} (Delta)")
            except Exception as delta_e:
                print(f"Delta Lake failed for metrics_summary: {delta_e}")
                print(f"Attempting parquet fallback...")
                metrics_summary.write.format("parquet").mode("overwrite").save(summary_path + "_parquet")
                print(f"✅ Saved system metrics aggregations to {summary_path}_parquet (Parquet fallback)")
            
            try:
                print(f"Attempting to save time_series with {time_series.count()} rows to Delta Lake...")
                time_series.write.format("delta").mode("append").save(timeseries_path)
                print(f"✅ Saved metrics timeseries to {timeseries_path} (Delta)")
            except Exception as delta_e:
                print(f"Delta Lake failed for time_series: {delta_e}")
                print(f"Attempting parquet fallback...")
                time_series.write.format("parquet").mode("overwrite").save(timeseries_path + "_parquet")
                print(f"✅ Saved metrics timeseries to {timeseries_path}_parquet (Parquet fallback)")
    except Exception as e:
        print(f"❌ Error saving system metrics aggregations: {e}")
        import traceback
        traceback.print_exc()

def create_overall_summary(spark: SparkSession, data: Dict[str, Any], start_ms: int, end_ms: int) -> None:
    """Create overall pipeline summary."""
    
    start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)
    
    summary_data = [{
        'pipeline_run_id': str(uuid.uuid4()),
        'start_time': start_dt.isoformat(),
        'end_time': end_dt.isoformat(),
        'processing_duration_seconds': (datetime.now(timezone.utc) - start_dt).total_seconds(),
        'total_events_processed': data['total_events'],
        'customer_events_count': len(data['customer_events']),
        'transaction_events_count': len(data['transaction_events']),
        'system_metrics_count': len(data['system_metrics']),
        'status': 'completed',
        'created_at': datetime.now(timezone.utc).isoformat()
    }]
    
    df = spark.createDataFrame(summary_data)
    
    output_path = Path("/opt/airflow/data/gold_layer")
    summary_path = str(output_path / "pipeline_run_summary")
    
    try:
        # Use JSON format in Airflow environment for compatibility, Delta Lake otherwise
        if os.getenv('AIRFLOW_CTX_DAG_ID'):
            print(f"Airflow mode: Saving pipeline summary as JSON...")
            print(f"Saving pipeline summary to {summary_path}_json")
            df.write.format("json").mode("overwrite").save(summary_path + "_json")
            print(f"✅ Saved pipeline summary to {summary_path}_json (JSON)")
        else:
            # Try Delta Lake first, fall back to parquet
            try:
                print(f"Attempting to save pipeline summary with {df.count()} rows to Delta Lake...")
                df.write.format("delta").mode("append").save(summary_path)
                print(f"✅ Saved pipeline summary to {summary_path} (Delta)")
            except Exception as delta_e:
                print(f"Delta Lake failed for pipeline summary: {delta_e}")
                print(f"Attempting parquet fallback...")
                df.write.format("parquet").mode("overwrite").save(summary_path + "_parquet")
                print(f"✅ Saved pipeline summary to {summary_path}_parquet (Parquet fallback)")
    except Exception as e:
        print(f"❌ Error saving pipeline summary: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main function to run the aggregation pipeline."""
    parser = argparse.ArgumentParser(description='StreamLineHub Aggregation Runner')
    parser.add_argument('--start-threshold', type=int, required=True,
                       help='Start time threshold in milliseconds since epoch')
    parser.add_argument('--end-threshold', type=int, required=True,
                       help='End time threshold in milliseconds since epoch')
    
    args = parser.parse_args()
    
    print(f"Starting aggregation pipeline...")
    print(f"Time range: {args.start_threshold} to {args.end_threshold}")
    
    # Create Spark session
    # Check if running in Airflow environment and use local mode
    if os.getenv('AIRFLOW_CTX_DAG_ID'):
        print("Running in Airflow - using local Spark mode without Delta Lake")
        spark = SparkSession.builder \
            .appName("StreamLineHub-Aggregation-Runner") \
            .master("local[*]") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
    else:
        spark = init_spark_session("StreamLineHub-Aggregation-Runner")
    
    try:
        # Read data (simulate Kafka data)
        print("Reading event data...")
        data = read_kafka_data_from_logs(spark, args.start_threshold, args.end_threshold)
        
        print(f"Processing {data['total_events']} total events")
        
        # Create aggregations
        print("Creating customer aggregations...")
        create_customer_aggregations(spark, data['customer_events'])
        
        print("Creating transaction aggregations...")
        create_transaction_aggregations(spark, data['transaction_events'])
        
        print("Creating system metrics aggregations...")
        create_system_metrics_aggregations(spark, data['system_metrics'])
        
        print("Creating overall summary...")
        create_overall_summary(spark, data, args.start_threshold, args.end_threshold)
        
        print("✅ Aggregation pipeline completed successfully!")
        
    except Exception as e:
        print(f"❌ Error in aggregation pipeline: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
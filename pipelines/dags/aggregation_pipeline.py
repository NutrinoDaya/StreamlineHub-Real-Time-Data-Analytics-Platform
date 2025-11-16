"""
Aggregation Pipeline DAG

This DAG orchestrates the data aggregation pipeline:
1. Runs Spark job every 5 minutes to aggregate Kafka data
2. Saves aggregated data to Delta Lake
3. Ingests aggregated data into Elasticsearch for fast queries

Schedule: Every 5 minutes (*/5 * * * *)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'streamlinehub-analytics',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': days_ago(1),
}

# DAG definition
dag = DAG(
    'aggregation_pipeline',
    default_args=default_args,
    description='5-minute data aggregation pipeline: Kafka → Spark → Delta Lake → Elasticsearch',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
    max_active_runs=1,
    tags=['aggregation', 'spark', 'delta-lake', 'elasticsearch'],
)


def check_prerequisites():
    """Verify that all required services are available."""
    import socket
    
    services = {
        'kafka': ('kafka1', 9092),
        'spark-master': ('spark-master', 7077),
        'elasticsearch': ('elasticsearch', 9200),
    }
    
    for service_name, (host, port) in services.items():
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                logging.info(f"✓ {service_name} is available at {host}:{port}")
            else:
                raise Exception(f"✗ {service_name} is not available at {host}:{port}")
        except Exception as e:
            logging.error(f"Failed to connect to {service_name}: {e}")
            raise


def compute_time_window(**context):
    """
    Calculate the time window for aggregation.
    Uses execution_date to process data from the last 5 minutes.
    """
    execution_date = context['execution_date']
    window_start = execution_date - timedelta(minutes=5)
    window_end = execution_date
    
    context['task_instance'].xcom_push(key='window_start', value=window_start.isoformat())
    context['task_instance'].xcom_push(key='window_end', value=window_end.isoformat())
    
    logging.info(f"Aggregation window: {window_start} to {window_end}")
    return {
        'window_start': window_start.isoformat(),
        'window_end': window_end.isoformat()
    }


def verify_delta_tables():
    """Verify that Delta Lake tables exist or create them."""
    logging.info("Verifying Delta Lake table structure...")
    # This will be implemented in the Spark job
    return True


def validate_aggregation_results(**context):
    """
    Validate that the aggregation produced valid results.
    Check row counts and data quality.
    """
    ti = context['task_instance']
    window_start = ti.xcom_pull(key='window_start', task_ids='compute_window')
    
    logging.info(f"Validating aggregation results for window starting at {window_start}")
    # Add validation logic here
    return True


# Task 1: Check prerequisites
check_prerequisites_task = PythonOperator(
    task_id='check_prerequisites',
    python_callable=check_prerequisites,
    dag=dag,
)

# Task 2: Compute time window
compute_window_task = PythonOperator(
    task_id='compute_window',
    python_callable=compute_time_window,
    provide_context=True,
    dag=dag,
)

# Task 3: Verify Delta Lake tables
verify_tables_task = PythonOperator(
    task_id='verify_delta_tables',
    python_callable=verify_delta_tables,
    dag=dag,
)

# Task 4: Run Spark aggregation job
# Using spark-submit to run the aggregation job
spark_aggregation_task = BashOperator(
    task_id='run_spark_aggregation',
    bash_command="""
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        /opt/airflow/dag_scripts/aggregation_job.py \
        --window-start {{ task_instance.xcom_pull(task_ids='compute_window', key='window_start') }} \
        --window-end {{ task_instance.xcom_pull(task_ids='compute_window', key='window_end') }}
    """,
    dag=dag,
)

# Task 5: Validate aggregation results
validate_task = PythonOperator(
    task_id='validate_aggregation',
    python_callable=validate_aggregation_results,
    provide_context=True,
    dag=dag,
)

# Task 6: Ingest to Elasticsearch
elasticsearch_ingestion_task = BashOperator(
    task_id='ingest_to_elasticsearch',
    bash_command="""
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --packages io.delta:delta-core_2.12:2.4.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0 \
        /opt/airflow/dag_scripts/ingest_to_elastic.py \
        --window-start {{ task_instance.xcom_pull(task_ids='compute_window', key='window_start') }} \
        --window-end {{ task_instance.xcom_pull(task_ids='compute_window', key='window_end') }}
    """,
    dag=dag,
)


# Define task dependencies
check_prerequisites_task >> compute_window_task >> verify_tables_task >> spark_aggregation_task >> validate_task >> elasticsearch_ingestion_task

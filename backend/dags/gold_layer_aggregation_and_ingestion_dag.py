"""
Gold Layer Aggregation and Elasticsearch Ingestion DAG
Reads Bronze/Silver Delta tables, creates Gold layer aggregations, and ingests to Elasticsearch
Uses time-based filtering with insertionTime for incremental processing
"""

from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago
import logging
import os
import sys
from pathlib import Path

# Add backend path to sys.path for imports
ROOT_DIR = Path(__file__).parent.parent
sys.path.append(str(ROOT_DIR))
from utils import readConfig, compute_time_threshold

logger = logging.getLogger(__name__)

# Configuration setup
root_dir = Path(__file__).parent.parent
config_dir = root_dir / "config"
config_file = config_dir / "Elasticsearch_Dag.xml"

if not config_file.is_file():
    raise FileNotFoundError(f"Configuration file not found at: {config_file}")

elasticsearch_config = readConfig(config_file)
dag_config = elasticsearch_config["DAG"]
schedule_interval = dag_config["schedule_interval"]
catchup = dag_config.get("catchup", "false").lower() == "true"
retries = int(dag_config.get("retries", 1))

# Spark packages for Delta Lake and Elasticsearch compatible with Spark 3.5
spark_packages = "io.delta:delta-spark_2.12:3.2.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.15.1"

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": retries,
    "start_date": days_ago(1),
    "retry_delay": timedelta(minutes=5),
}

def compute_thresholds_runtime(**kwargs):
    """
    Airflow Python task to compute the start/end filter thresholds
    based on the CURRENT UTC TIME.
    """
    ti = kwargs["ti"]
    logger.info("Starting compute_thresholds_runtime task.")

    try:
        # processes the most recent data based on its execution time.
        base_utc_dt = datetime.now(timezone.utc)

        interval_str = dag_config["interval"]
        logger.info(f"Calculating thresholds with base_time (current UTC)='{base_utc_dt.isoformat()}' and interval='{interval_str}'")
        
        start_ms, end_ms = compute_time_threshold(interval_str, base_time=base_utc_dt)

        start_dt_display = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc)
        end_dt_display = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)

        logger.info(
            f"Pushing thresholds to XCom: start_threshold={start_ms} ({start_dt_display.isoformat()}), "
            f"end_threshold={end_ms} ({end_dt_display.isoformat()})"
        )

        ti.xcom_push(key="start_threshold", value=start_ms)
        ti.xcom_push(key="end_threshold", value=end_ms)
        
        logger.info("Task finished successfully.")

    except Exception as e:
        logger.error(f"An unexpected error occurred in compute_thresholds_runtime: {e}", exc_info=True)
        raise AirflowException("Task failed due to an unexpected error. Check logs for details.")

# DAG definition
with DAG(
    dag_id="gold_layer_aggregation_and_ingestion_dag",
    default_args=default_args,
    description="Gold layer aggregation from Bronze/Silver Delta tables with time-based filtering and Elasticsearch ingestion",
    schedule_interval=schedule_interval,
    catchup=catchup,
    tags=['delta-lake', 'aggregation', 'gold-layer', 'elasticsearch'],
) as dag:

    start_process = EmptyOperator(task_id="start_process")

    compute_thresholds_task = PythonOperator(
        task_id="compute_time_thresholds",
        python_callable=compute_thresholds_runtime,
    )

    run_gold_aggregation_job = SparkSubmitOperator(
        task_id="run_gold_aggregation_job",
        application=str(root_dir / elasticsearch_config["Paths"]["aggregation_script"]),
        conn_id="spark-conn",
        name="gold_aggregation_job",
        verbose=True,
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.rpc.message.maxSize": "512",
            "spark.driver.maxResultSize": "2g",
        },
        packages=spark_packages,
        application_args=[
            "--start-threshold",
            "{{ ti.xcom_pull(task_ids='compute_time_thresholds', key='start_threshold') }}",
            "--end-threshold",
            "{{ ti.xcom_pull(task_ids='compute_time_thresholds', key='end_threshold') }}",
        ],
    )

    run_elastic_ingestion_job = SparkSubmitOperator(
        task_id="run_elastic_ingestion_job",
        application=str(root_dir / elasticsearch_config["Paths"]["elastic_script"]),
        conn_id="spark-conn",
        name="elastic_ingestion_job",
        verbose=True,
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        },
        packages=spark_packages,
        env_vars={
            "ELASTICSEARCH_HOST": "elasticsearch",
            "ELASTICSEARCH_PORT": "9200",
        },
        application_args=[
            "--start-threshold",
            "{{ ti.xcom_pull(task_ids='compute_time_thresholds', key='start_threshold') }}",
            "--end-threshold",
            "{{ ti.xcom_pull(task_ids='compute_time_thresholds', key='end_threshold') }}",
        ],
    )

    end_process = EmptyOperator(task_id="end_process")

    # Task dependencies
    (
        start_process
        >> compute_thresholds_task
        >> run_gold_aggregation_job
        >> run_elastic_ingestion_job
        >> end_process
    )
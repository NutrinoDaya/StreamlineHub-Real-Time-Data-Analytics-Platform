"""
Spark Session Initializer Module

This module provides a centralized Spark session initialization following
configuration-driven approach. It ensures a single Spark session is created
and reused across all pipelines.

Usage:
    from src.core.spark_session import get_spark_session, get_config
    
    spark = get_spark_session("MyJobName")
    config = get_config()
"""

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Optional
from pyspark.sql import SparkSession
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global Spark session instance (singleton pattern)
_spark_session: Optional[SparkSession] = None
_config_cache: Optional[Dict] = None


def _read_xml_config(config_path: Path) -> Dict:
    """
    Read configuration from XML file and convert to dictionary.
    
    Args:
        config_path: Path to XML configuration file
        
    Returns:
        Dictionary containing configuration
    """
    tree = ET.parse(config_path)
    root = tree.getroot()
    
    def xml_to_dict(elem):
        """Recursively convert XML element to dictionary"""
        if len(elem) == 0:
            return elem.text.strip() if elem.text else None
        
        result = {}
        for child in elem:
            child_data = xml_to_dict(child)
            if child.tag in result:
                # Handle multiple elements with same tag (convert to list)
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data
        return result
    
    return xml_to_dict(root)


def get_config() -> Dict:
    """
    Get configuration dictionary from XML file.
    Uses caching to avoid repeated file reads.
    
    Returns:
        Configuration dictionary
    """
    global _config_cache
    
    if _config_cache is None:
        config_path = Path(__file__).parent.parent.parent / "config" / "spark_config.xml"
        
        if not config_path.exists():
            logger.error(f"Configuration file not found: {config_path}")
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        logger.info(f"Loading configuration from: {config_path}")
        _config_cache = _read_xml_config(config_path)
    
    return _config_cache


def init_spark_session(job_name: Optional[str] = None) -> SparkSession:
    """
    Initialize Spark session with configuration from XML.
    Implements singleton pattern - only one session is created.
    
    Args:
        job_name: Optional custom job name (overrides config)
        
    Returns:
        Configured SparkSession instance
    """
    global _spark_session
    
    # Return existing session if already initialized
    if _spark_session is not None:
        if job_name:
            logger.info(f"Reusing existing Spark session for job: {job_name}")
        return _spark_session
    
    # Load configuration
    config = get_config()
    spark_config = config.get("SparkSession", {})
    
    # Extract configuration values
    app_name = job_name or spark_config.get("app_name", "StreamLineHub")
    master_url = spark_config.get("master_url", "local[*]")
    
    logger.info(f"Initializing Spark session: {app_name}")
    logger.info(f"Master URL: {master_url}")
    
    # Build Spark session
    builder = SparkSession.builder.appName(app_name)
    
    # Set master URL
    if master_url:
        builder = builder.master(master_url)
    
    # Maven packages (Delta Lake, Kafka connector)
    packages = spark_config.get("spark_packages")
    if packages:
        builder = builder.config("spark.jars.packages", packages)
        logger.info(f"Loading packages: {packages}")
    
    # Core Spark configurations
    config_mappings = {
        "spark_cores_max": "spark.cores.max",
        "spark_executor_memory": "spark.executor.memory",
        "spark_executor_cores": "spark.executor.cores",
        "spark_executor_memory_overhead": "spark.executor.memoryOverhead",
        "spark_driver_memory": "spark.driver.memory",
        "spark_kryoserializer_buffer": "spark.kryoserializer.buffer",
        "spark_kryoserializer_buffer_max": "spark.kryoserializer.buffer.max",
        "spark_task_maxDirectResultSize": "spark.task.maxDirectResultSize",
        "spark_driver_maxResultSize": "spark.driver.maxResultSize",
        "spark_rpc_message_maxSize": "spark.rpc.message.maxSize",
        "spark_task_maxSerializationSize": "spark.scheduler.maxTaskSerializationSize",
        "spark_dynamicAllocation_enabled": "spark.dynamicAllocation.enabled",
    }
    
    for xml_key, spark_key in config_mappings.items():
        value = spark_config.get(xml_key)
        if value is not None:
            builder = builder.config(spark_key, str(value))
            logger.debug(f"Set {spark_key} = {value}")
    
    # Delta Lake extensions
    sql_extensions = spark_config.get("spark_sql_extensions")
    if sql_extensions:
        builder = builder.config("spark.sql.extensions", sql_extensions)
        logger.info(f"Enabled SQL extensions: {sql_extensions}")
    
    catalog_impl = spark_config.get("spark_catalog")
    if catalog_impl:
        builder = builder.config("spark.sql.catalog.spark_catalog", catalog_impl)
        logger.info(f"Set catalog implementation: {catalog_impl}")
    
    # Security flags for Java 11+ compatibility
    builder = builder.config(
        "spark.driver.extraJavaOptions",
        "--add-opens java.base/sun.security.action=ALL-UNNAMED"
    ).config(
        "spark.executor.extraJavaOptions",
        "--add-opens java.base/sun.security.action=ALL-UNNAMED"
    )
    
    # Create Spark session
    _spark_session = builder.getOrCreate()
    
    # Runtime configurations
    debug_max_fields = spark_config.get("spark_sql_debug_maxToStringFields")
    if debug_max_fields:
        _spark_session.conf.set("spark.sql.debug.maxToStringFields", int(debug_max_fields))
    
    auto_merge = spark_config.get("spark_autoMerge")
    if auto_merge is not None:
        _spark_session.conf.set(
            "spark.databricks.delta.schema.autoMerge.enabled",
            str(auto_merge).lower()
        )
    
    # Set log level
    _spark_session.sparkContext.setLogLevel("WARN")
    
    logger.info("Spark session initialized successfully")
    logger.info(f"Spark version: {_spark_session.version}")
    logger.info(f"Master: {_spark_session.sparkContext.master}")
    
    return _spark_session


def get_spark_session(job_name: Optional[str] = None) -> SparkSession:
    """
    Get or create Spark session.
    This is the main entry point for all pipelines.
    
    Args:
        job_name: Optional custom job name
        
    Returns:
        SparkSession instance
        
    Example:
        >>> from src.core.spark_session import get_spark_session
        >>> spark = get_spark_session("MyETLJob")
    """
    return init_spark_session(job_name)


def stop_spark_session():
    """
    Stop the active Spark session.
    Useful for cleanup in scripts or tests.
    """
    global _spark_session
    
    if _spark_session is not None:
        logger.info("Stopping Spark session")
        _spark_session.stop()
        _spark_session = None
        logger.info("Spark session stopped")
    else:
        logger.warning("No active Spark session to stop")


def get_path(layer: str, table: Optional[str] = None) -> str:
    """
    Get path for Delta Lake layer/table.
    
    Args:
        layer: Layer name (bronze, silver, gold)
        table: Optional table name
        
    Returns:
        Full path to Delta Lake location
        
    Example:
        >>> get_path("bronze", "events")
        '/opt/bitnami/spark/data/delta/bronze/events'
    """
    config = get_config()
    paths = config.get("Paths", {})
    
    delta_base = paths.get("delta_base", "/data/delta")
    layer_name = paths.get(f"{layer}_layer", layer)
    
    if table:
        return f"{delta_base}/{layer_name}/{table}"
    return f"{delta_base}/{layer_name}"


def get_kafka_config() -> Dict:
    """
    Get Kafka configuration from config file.
    
    Returns:
        Dictionary with Kafka settings
        
    Example:
        >>> kafka_conf = get_kafka_config()
        >>> print(kafka_conf['bootstrap_servers'])
        'kafka:9092'
    """
    config = get_config()
    return config.get("Kafka", {})


# Convenience function for getting common Kafka settings
def get_kafka_bootstrap_servers() -> str:
    """Get Kafka bootstrap servers"""
    kafka_config = get_kafka_config()
    return kafka_config.get("bootstrap_servers", "kafka:9092")


def get_kafka_topic(topic_name: str) -> str:
    """
    Get Kafka topic name from config.
    
    Args:
        topic_name: Topic key (e.g., 'customer_events', 'transactions')
        
    Returns:
        Actual topic name
    """
    kafka_config = get_kafka_config()
    topics = kafka_config.get("topics", {})
    return topics.get(topic_name, topic_name)


if __name__ == "__main__":
    # Test initialization
    print("Testing Spark Session Initializer")
    print("=" * 80)
    
    # Initialize session
    spark = get_spark_session("TestJob")
    
    # Print configuration
    config = get_config()
    print("\nConfiguration loaded:")
    print(f"  App Name: {config['SparkSession']['app_name']}")
    print(f"  Master: {config['SparkSession']['master_url']}")
    print(f"  Executor Memory: {config['SparkSession']['spark_executor_memory']}")
    
    # Print paths
    print("\nDelta Lake Paths:")
    print(f"  Bronze: {get_path('bronze')}")
    print(f"  Silver: {get_path('silver')}")
    print(f"  Gold: {get_path('gold')}")
    print(f"  Example table: {get_path('bronze', 'events')}")
    
    # Print Kafka config
    print("\nKafka Configuration:")
    print(f"  Bootstrap Servers: {get_kafka_bootstrap_servers()}")
    print(f"  Customer Events Topic: {get_kafka_topic('customer_events')}")
    
    # Test session reuse
    spark2 = get_spark_session("AnotherJob")
    print(f"\nSession reuse test: {spark is spark2}")
    
    # Cleanup
    stop_spark_session()
    print("\nTest completed successfully")

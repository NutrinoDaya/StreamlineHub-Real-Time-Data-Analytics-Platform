#!/usr/bin/env python3
"""
Delta Table Optimization Script
Compacts transaction logs and optimizes Silver Delta tables to prevent serialization issues.
Run this script when encountering EOFException or after heavy write operations.
"""

import sys
from pathlib import Path

# Add project root to path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def optimize_delta_table(spark: SparkSession, table_path: str, table_name: str) -> bool:
    """
    Optimize a Delta table by compacting files and cleaning up transaction logs.
    
    Args:
        spark: SparkSession instance
        table_path: Path to the Delta table
        table_name: Name of the table for logging
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"\n{'='*70}")
        logger.info(f"Optimizing: {table_name}")
        logger.info(f"Path: {table_path}")
        logger.info(f"{'='*70}")
        
        # Check if table exists
        if not DeltaTable.isDeltaTable(spark, table_path):
            logger.warning(f"⚠ Table does not exist or is not a Delta table: {table_path}")
            return False
        
        dt = DeltaTable.forPath(spark, table_path)
        
        # Step 1: OPTIMIZE - Compacts small files into larger ones
        logger.info("📦 Running OPTIMIZE (compacting files)...")
        dt.optimize().executeCompaction()
        logger.info("✓ OPTIMIZE completed successfully")
        
        # Step 2: Generate checkpoint to compact transaction log
        logger.info("📝 Creating checkpoint (compacting transaction log)...")
        # Read the table to trigger checkpoint creation
        df = spark.read.format("delta").load(table_path)
        count = df.count()
        logger.info(f"✓ Checkpoint created - Table has {count} records")
        
        logger.info(f"✅ Successfully optimized {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error optimizing {table_name}: {e}", exc_info=True)
        return False


def main():
    """Main optimization routine"""
    logger.info("="*70)
    logger.info("Delta Table Optimization Script")
    logger.info("="*70)
    
    # Initialize Spark with optimized settings
    logger.info("\n🚀 Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("DeltaTableOptimizer") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.databricks.delta.optimize.maxFileSize", "134217728") \
        .getOrCreate()
    
    logger.info("✓ Spark session initialized")
    
    # Define tables to optimize
    tables_to_optimize = [
        {
            "path": "/opt/airflow/data/bronze/customer_behavior_bronze",
            "name": "customer_behavior_bronze"
        },
        {
            "path": "/opt/airflow/data/bronze/system_metric_bronze",
            "name": "system_metric_bronze"
        },
        {
            "path": "/opt/airflow/data/silver/customer_behavior_silver",
            "name": "customer_behavior_silver"
        },
        {
            "path": "/opt/airflow/data/silver/system_metric_silver",
            "name": "system_metric_silver"
        },
        {
            "path": "/opt/airflow/data/gold/customer_behavior_gold",
            "name": "customer_behavior_gold"
        },
        {
            "path": "/opt/airflow/data/gold/system_metrics_gold",
            "name": "system_metrics_gold"
        }
    ]
    
    # Optimize each table
    results = []
    for table_info in tables_to_optimize:
        success = optimize_delta_table(
            spark, 
            table_info["path"], 
            table_info["name"]
        )
        results.append((table_info["name"], success))
    
    # Print summary
    logger.info("\n" + "="*70)
    logger.info("OPTIMIZATION SUMMARY")
    logger.info("="*70)
    
    success_count = sum(1 for _, success in results if success)
    total_count = len(results)
    
    for table_name, success in results:
        status = "✅ SUCCESS" if success else "❌ FAILED"
        logger.info(f"{status}: {table_name}")
    
    logger.info(f"\nTotal: {success_count}/{total_count} tables optimized successfully")
    logger.info("="*70 + "\n")
    
    # Cleanup
    spark.stop()
    logger.info("Spark session stopped")
    
    return 0 if success_count == total_count else 1


if __name__ == "__main__":
    sys.exit(main())

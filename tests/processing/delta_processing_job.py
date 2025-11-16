"""
Spark job for Delta Lake processing
"""
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_delta_spark_session():
    """Create Spark session with Delta Lake support"""
    builder = SparkSession.builder \
        .appName("DeltaLakeProcessing") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # Configure Delta Lake
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

def process_ipp_psop_data(spark):
    """Process IPP PSOP data from Delta tables"""
    logger.info("Starting IPP PSOP Delta Lake processing...")
    
    # Path to Delta tables
    base_path = "/opt/Vi_Big_Data_Main/Big_Data_ETL/data/bronze/IPP_PSOP/2025-09-22"
    
    try:
        # Read IPP PSOP Incident data
        incident_path = f"{base_path}/IPP_PSOP_Incident"
        incident_df = spark.read.format("delta").load(incident_path)
        
        logger.info("IPP PSOP Incident data loaded:")
        incident_df.show(5)
        logger.info(f"Incident records count: {incident_df.count()}")
        
        # Read IPP PSOP IncidentFiles data
        files_path = f"{base_path}/IPP_PSOP_IncidentFiles"
        files_df = spark.read.format("delta").load(files_path)
        
        logger.info("IPP PSOP IncidentFiles data loaded:")
        files_df.show(5)
        logger.info(f"Files records count: {files_df.count()}")
        
        # Read IPP PSOP IncidentHistory data
        history_path = f"{base_path}/IPP_PSOP_IncidentHistory"
        history_df = spark.read.format("delta").load(history_path)
        
        logger.info("IPP PSOP IncidentHistory data loaded:")
        history_df.show(5)
        logger.info(f"History records count: {history_df.count()}")
        
        # Perform some aggregations
        if incident_df.count() > 0:
            # Get schema info
            logger.info("Incident table schema:")
            incident_df.printSchema()
            
            # Basic statistics
            logger.info("Basic statistics for Incident data:")
            incident_df.describe().show()
        
        return "Delta Lake processing completed successfully"
        
    except Exception as e:
        logger.error(f"Error reading Delta tables: {str(e)}")
        logger.info("This might be because Delta tables don't exist yet or path is incorrect")
        
        # Create sample Delta table instead
        logger.info("Creating sample Delta table...")
        sample_data = [
            (1, "Sample Incident 1", "2025-09-22", "Open"),
            (2, "Sample Incident 2", "2025-09-22", "Closed"),
            (3, "Sample Incident 3", "2025-09-22", "In Progress"),
        ]
        columns = ["incident_id", "description", "date_created", "status"]
        sample_df = spark.createDataFrame(sample_data, columns)
        
        # Write as Delta table
        sample_path = "/tmp/sample_delta_table"
        sample_df.write.format("delta").mode("overwrite").save(sample_path)
        logger.info(f"Sample Delta table created at {sample_path}")
        
        return "Sample Delta Lake processing completed"

def main():
    """Main execution function"""
    spark = None
    try:
        spark = create_delta_spark_session()
        result = process_ipp_psop_data(spark)
        logger.info(result)
        return result
    except Exception as e:
        logger.error(f"Error in Delta Lake processing: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
"""
Sample Spark job for data processing
"""
from pyspark.sql import SparkSession
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with required configurations"""
    return SparkSession.builder \
        .appName("SampleDataProcessing") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def process_sample_data(spark):
    """Process sample data"""
    logger.info("Starting sample data processing...")
    
    # Create sample data
    sample_data = [
        ("001", "John Doe", "Engineering", 75000),
        ("002", "Jane Smith", "Marketing", 68000),
        ("003", "Bob Johnson", "Engineering", 82000),
        ("004", "Alice Brown", "Sales", 59000),
        ("005", "Charlie Davis", "Engineering", 91000),
    ]
    
    columns = ["employee_id", "name", "department", "salary"]
    df = spark.createDataFrame(sample_data, columns)
    
    logger.info("Sample data created:")
    df.show()
    
    # Perform some transformations
    engineering_df = df.filter(df.department == "Engineering")
    avg_salary = engineering_df.agg({"salary": "avg"}).collect()[0][0]
    
    logger.info(f"Average Engineering salary: ${avg_salary:.2f}")
    
    # Write to temporary location (in production, this would be a proper path)
    output_path = "/tmp/sample_output"
    df.write.mode("overwrite").json(output_path)
    logger.info(f"Data written to {output_path}")
    
    return "Sample data processing completed successfully"

def main():
    """Main execution function"""
    spark = None
    try:
        spark = create_spark_session()
        result = process_sample_data(spark)
        logger.info(result)
        return result
    except Exception as e:
        logger.error(f"Error in sample data processing: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
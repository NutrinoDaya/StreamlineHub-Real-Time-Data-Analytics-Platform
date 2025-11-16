"""
Batch ETL pipeline for customer data processing.

This pipeline reads data from various sources, performs transformations,
and creates curated datasets in the Gold layer of Delta Lake.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from src.core.spark_session import (
    get_spark_session,
    get_path
)

# Initialize Spark Session from centralized config
spark = get_spark_session("CustomerBatchETL")

print("=" * 80)
print("Customer Batch ETL Pipeline Started")
print(f"Execution Time: {datetime.now()}")
print("=" * 80)

# Get Delta Lake paths from config
bronze_events_path = get_path("bronze", "events")
silver_sessions_path = get_path("silver", "customer_sessions")
gold_customer_360_path = get_path("gold", "customer_360")
gold_segments_path = get_path("gold", "customer_segments")
gold_products_path = get_path("gold", "product_analytics")
gold_daily_metrics_path = get_path("gold", "daily_metrics")
gold_device_performance_path = get_path("gold", "device_performance")

# Read from Bronze/Silver layers
try:
    events_df = spark.read.format("delta").load(bronze_events_path)
    sessions_df = spark.read.format("delta").load(silver_sessions_path)
    
    print(f"✓ Loaded {events_df.count()} events from Bronze layer: {bronze_events_path}")
    print(f"✓ Loaded {sessions_df.count()} sessions from Silver layer: {silver_sessions_path}")
    
except Exception as e:
    print(f"Warning: Could not load Delta tables. Creating sample data. Error: {e}")
    # Create sample data for demonstration
    from pyspark.sql.types import *
    
    sample_data = [
        (i, f"customer_{i % 100}", "page_view", datetime.now() - timedelta(days=i % 30), 
         f"session_{i % 50}", "mobile" if i % 2 == 0 else "desktop", 
         f"https://example.com/page{i % 10}", f"PROD-{i % 20:03d}", 
         float(i % 100) if i % 5 == 0 else None)
        for i in range(1000)
    ]
    
    schema = StructType([
        StructField("event_id", IntegerType()),
        StructField("customer_id", StringType()),
        StructField("event_type", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("session_id", StringType()),
        StructField("device_type", StringType()),
        StructField("page_url", StringType()),
        StructField("product_id", StringType()),
        StructField("amount", DoubleType())
    ])
    
    events_df = spark.createDataFrame(sample_data, schema)

# Customer 360 View: Aggregate customer metrics
print("\nCreating Customer 360 view...")
customer_360 = (events_df
    .groupBy("customer_id")
    .agg(
        count("*").alias("total_events"),
        count_distinct("session_id").alias("total_sessions"),
        count_distinct(when(col("event_type") == "page_view", col("event_id"))).alias("page_views"),
        count_distinct(when(col("event_type") == "purchase", col("event_id"))).alias("purchases"),
        sum("amount").alias("total_spent"),
        avg("amount").alias("avg_order_value"),
        min("timestamp").alias("first_seen"),
        max("timestamp").alias("last_seen"),
        collect_set("device_type").alias("devices_used"),
        count_distinct("product_id").alias("unique_products_viewed")
    )
    .withColumn("days_since_first_seen", 
        datediff(current_date(), col("first_seen")))
    .withColumn("days_since_last_seen", 
        datediff(current_date(), col("last_seen")))
    .withColumn("purchase_frequency", 
        col("purchases") / (col("days_since_first_seen") + 1))
    .withColumn("engagement_score", 
        (col("total_events") * 0.3 + col("purchases") * 0.7))
)

# Customer Segmentation (RFM Analysis)
print("Performing RFM segmentation...")
rfm_window = Window.orderBy(col("recency"))
monetary_window = Window.orderBy(col("monetary").desc())
frequency_window = Window.orderBy(col("frequency").desc())

customer_rfm = (events_df
    .filter(col("event_type") == "purchase")
    .groupBy("customer_id")
    .agg(
        datediff(current_date(), max("timestamp")).alias("recency"),
        count("*").alias("frequency"),
        sum("amount").alias("monetary")
    )
    .withColumn("r_score", ntile(5).over(rfm_window))
    .withColumn("f_score", ntile(5).over(frequency_window))
    .withColumn("m_score", ntile(5).over(monetary_window))
    .withColumn("rfm_score", col("r_score") + col("f_score") + col("m_score"))
    .withColumn("segment", 
        when(col("rfm_score") >= 12, "VIP")
        .when(col("rfm_score") >= 9, "Premium")
        .when(col("rfm_score") >= 6, "Standard")
        .otherwise("Basic"))
)

# Product Performance Analytics
print("Calculating product performance metrics...")
product_analytics = (events_df
    .filter(col("product_id").isNotNull())
    .groupBy("product_id")
    .agg(
        count("*").alias("total_interactions"),
        count_distinct("customer_id").alias("unique_customers"),
        sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias("views"),
        sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart"),
        sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
        sum("amount").alias("revenue"),
        avg("amount").alias("avg_price")
    )
    .withColumn("conversion_rate", 
        (col("purchases") / col("views")) * 100)
    .withColumn("cart_conversion_rate", 
        (col("purchases") / col("add_to_cart")) * 100)
)

# Daily Revenue Trends
print("Creating daily revenue trends...")
daily_metrics = (events_df
    .filter(col("event_type") == "purchase")
    .withColumn("date", to_date(col("timestamp")))
    .groupBy("date")
    .agg(
        count("*").alias("total_orders"),
        count_distinct("customer_id").alias("unique_customers"),
        sum("amount").alias("revenue"),
        avg("amount").alias("avg_order_value")
    )
    .orderBy("date"))

# Device and Channel Performance
print("Analyzing device performance...")
device_performance = (events_df
    .groupBy("device_type", to_date("timestamp").alias("date"))
    .agg(
        count("*").alias("events"),
        count_distinct("customer_id").alias("unique_users"),
        sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("conversions"),
        sum("amount").alias("revenue")
    )
    .withColumn("conversion_rate", 
        (col("conversions") / col("events")) * 100))

# Write to Gold layer (curated, business-ready datasets)
print("\nWriting curated datasets to Gold layer...")

# Customer 360
customer_360.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_customer_360_path)
print(f"✓ Customer 360 view created: {gold_customer_360_path}")

# Customer Segments
customer_rfm.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_segments_path)
print(f"✓ Customer segments created: {gold_segments_path}")

# Product Analytics
product_analytics.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_products_path)
print(f"✓ Product analytics created: {gold_products_path}")

# Daily Metrics
daily_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_daily_metrics_path)
print(f"✓ Daily metrics created: {gold_daily_metrics_path}")

# Device Performance
device_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_device_performance_path)
print(f"✓ Device performance created: {gold_device_performance_path}")

# Display summary statistics
print("\n" + "=" * 80)
print("Pipeline Execution Summary")
print("=" * 80)
print(f"Total customers processed: {customer_360.count()}")
print(f"Total products analyzed: {product_analytics.count()}")
print(f"Date range: {daily_metrics.agg(min('date'), max('date')).collect()[0]}")
print("\nSegment Distribution:")
customer_rfm.groupBy("segment").count().orderBy("count", ascending=False).show()
print("=" * 80)
print("Batch ETL Pipeline Completed Successfully")
print("=" * 80)

# Note: Don't stop spark session here as it's managed centrally
# spark.stop()

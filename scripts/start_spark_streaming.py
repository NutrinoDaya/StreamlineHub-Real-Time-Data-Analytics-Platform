#!/usr/bin/env python
"""
Simple script to start Spark streaming job processing Kafka data.
"""

import subprocess
import sys
import time

def run_spark_streaming():
    """Submit and run the Spark streaming job."""
    
    spark_submit_cmd = [
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "--conf", "spark.sql.streaming.checkpointLocation=/tmp/spark-checkpoints",
        "--conf", "spark.sql.adaptive.enabled=true",
        "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
        "--driver-memory", "1g",
        "--executor-memory", "1g", 
        "--executor-cores", "1",
        "--total-executor-cores", "2",
        "/app/pipelines/streaming/realtime_data_processor.py"
    ]
    
    print("🚀 Submitting Spark streaming job...")
    print(f"Command: {' '.join(spark_submit_cmd)}")
    
    try:
        # Start the Spark job
        process = subprocess.Popen(
            spark_submit_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        print("✅ Spark job submitted successfully!")
        print("📊 Processing real-time Kafka data...")
        print("🔍 Check Spark UI at http://localhost:7080 for job status")
        
        # Stream output
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                print(output.strip())
        
        # Get return code
        return_code = process.poll()
        if return_code != 0:
            error_output = process.stderr.read()
            print(f"❌ Spark job failed with return code {return_code}")
            print(f"Error: {error_output}")
        else:
            print("✅ Spark job completed successfully")
            
    except Exception as e:
        print(f"❌ Failed to start Spark job: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = run_spark_streaming()
    sys.exit(0 if success else 1)
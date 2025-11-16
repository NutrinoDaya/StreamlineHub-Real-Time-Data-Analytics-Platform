#!/bin/bash
"""
Script to submit Spark streaming job to the cluster.
"""

# Submit the Spark job with Kafka dependencies
/opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    --conf spark.sql.streaming.checkpointLocation=/tmp/spark-checkpoints \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 1 \
    --total-executor-cores 2 \
    /app/pipelines/streaming/spark_kafka_processor.py
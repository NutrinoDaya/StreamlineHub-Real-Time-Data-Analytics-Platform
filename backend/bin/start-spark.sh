#!/bin/bash

# Verify JAVA_HOME
if [ -z "$JAVA_HOME" ]; then
    echo "JAVA_HOME is not set. Exiting."
    exit 1
else
    echo "JAVA_HOME is set to $JAVA_HOME"
fi

# Load environment variables
. "${SPARK_HOME}/bin/load-spark-env.sh"

echo "Starting Spark in ${SPARK_MODE} mode..."

# Determine the mode in which to start Spark
if [ "$SPARK_MODE" == "master" ]; then
    # Create event logs directory
    mkdir -p /opt/spark/event_logs
    chmod -R 777 /opt/spark/event_logs
    
    # Start Spark master
    ${SPARK_HOME}/sbin/start-master.sh -h ${SPARK_MASTER_HOST} -p ${SPARK_MASTER_PORT} --webui-port ${SPARK_MASTER_WEBUI_PORT}
    
    # Wait for log file and tail it, or just keep container alive
    sleep 5
    if [ -f "${SPARK_HOME}/logs/spark-root-org.apache.spark.deploy.master.Master-1-${HOSTNAME}.out" ]; then
        tail -f "${SPARK_HOME}/logs/spark-root-org.apache.spark.deploy.master.Master-1-${HOSTNAME}.out"
    else
        # Keep container running
        tail -f /dev/null
    fi

elif [ "$SPARK_MODE" == "worker" ]; then
    # Start Spark worker
    ${SPARK_HOME}/sbin/start-worker.sh ${SPARK_MASTER_URL} --webui-port ${SPARK_WORKER_WEBUI_PORT}
    
    # Wait for log file and tail it, or just keep container alive
    sleep 5
    if [ -f "${SPARK_HOME}/logs/spark-root-org.apache.spark.deploy.worker.Worker-1-${HOSTNAME}.out" ]; then
        tail -f "${SPARK_HOME}/logs/spark-root-org.apache.spark.deploy.worker.Worker-1-${HOSTNAME}.out"
    else
        # Keep container running
        tail -f /dev/null
    fi
else
    echo "Invalid SPARK_MODE: $SPARK_MODE"
    exit 1
fi

#!/usr/bin/env python3
"""
Setup Airflow Spark Connection
Creates the spark-conn connection for SparkSubmitOperator
"""

import json
from airflow import settings
from airflow.models import Connection

def create_spark_connection():
    """Create or update the spark-conn connection"""
    
    session = settings.Session()
    extra_data = json.dumps({
        "master": "spark://spark-master:7077"
    })
    
    # Check if connection already exists
    existing_conn = session.query(Connection).filter(Connection.conn_id == 'spark-conn').first()
    
    if existing_conn:
        print("Updating existing spark-conn connection...")
        existing_conn.host = 'spark-master'
        existing_conn.port = 7077
        existing_conn.conn_type = 'spark'
        existing_conn.extra = extra_data
    else:
        print("Creating new spark-conn connection...")
        new_conn = Connection(
            conn_id='spark-conn',
            conn_type='spark',
            host='spark-master',
            port=7077,
            extra=extra_data
        )
        session.add(new_conn)
    
    try:
        session.commit()
        print("✅ Spark connection created/updated successfully!")
        print("Connection details:")
        print(f"  - Conn ID: spark-conn")
        print(f"  - Type: spark") 
        print(f"  - Host: spark-master")
        print(f"  - Port: 7077")
    except Exception as e:
        session.rollback()
        print(f"❌ Error creating connection: {e}")
        raise
    finally:
        session.close()

if __name__ == "__main__":
    create_spark_connection()
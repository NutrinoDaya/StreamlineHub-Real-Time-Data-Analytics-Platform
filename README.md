# StreamlineHub - Real-Time Data Analytics Platform

> **Production-Ready Enterprise Data Pipeline** with Apache Spark, Delta Lake, and Real-Time Analytics

A modern, scalable data platform for real-time event processing and analytics. Built with Apache Spark, Delta Lake, Kafka, Airflow, and Elasticsearch for enterprise-grade data operations.

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.11-blue.svg)
![Spark](https://img.shields.io/badge/spark-3.5.4-orange.svg)
![Delta Lake](https://img.shields.io/badge/delta%20lake-3.2.0-brightgreen.svg)

## Features

- **Real-Time Event Processing**: Kafka → Redis → Spark → Delta Lake pipeline
- **Medallion Architecture**: Bronze/Silver/Gold data layers with ACID transactions
- **Workflow Orchestration**: Apache Airflow for automated ETL scheduling
- **Interactive Analytics**: React dashboard with real-time metrics visualization
- **Advanced Search**: Elasticsearch integration for gold layer analytics
- **Containerized Deployment**: Full Docker Compose orchestration
- **System Monitoring**: Comprehensive health checks and performance metrics

## Screenshots

### Real-Time Analytics Dashboard
![Main Dashboard](assets/Main_Page.jpg)
*Main dashboard with real-time metrics and system overview*

![Customer Behavior Analytics](assets/Customer_Behavior_Page.png)
*Customer behavior analytics with event tracking and engagement metrics*

![Data Discovery](assets/Data_Discovery_Page.png)
*Data discovery interface for exploring datasets*

![Pipeline Health](assets/Pipeline_Health.png)
*Pipeline health monitoring with component status*

### Infrastructure Components

![Docker Containers](assets/Docker_Container.png)
*Docker containerized deployment showing all running services*

![Apache Airflow](assets/Apache_Airflow.jpg)
*Apache Airflow DAG orchestration and workflow management*

![Apache Spark](assets/Apache_Spark.png)
*Apache Spark cluster with master and worker nodes*

![Elasticsearch Indexes](assets/Elasticearch_Indexes.png)
*Elasticsearch indexes with gold layer aggregations*

## Architecture

### Data Flow

```
Event Sources → Kafka → Redis Buffer → Spark ETL → Delta Lake → Elasticsearch
                                                    ↓
                                            Bronze/Silver/Gold
                                                    ↓
                                            FastAPI Backend → React Frontend
```

### Component Overview

**Data Layer**
- **Apache Kafka**: High-throughput event streaming with multiple topics
- **Redis**: Event buffering with configurable thresholds (50 events default)
- **Delta Lake**: ACID-compliant data lake with Bronze/Silver/Gold layers
- **Elasticsearch**: Gold layer aggregations and analytics search

**Processing Layer**
- **Apache Spark 3.5.4**: Distributed data processing with PySpark
- **Delta Lake 3.2.0**: ACID transactions, time travel, schema evolution
- **Apache Airflow 2.7**: Workflow orchestration and scheduling

**Application Layer**
- **FastAPI Backend**: REST API with analytics endpoints
- **React Frontend**: Modern UI with real-time WebSocket updates
- **Nginx**: Production-grade web server

## Quick Start

### Prerequisites

- Docker 24.0+ and Docker Compose 2.21+
- 16GB RAM minimum (32GB recommended)
- 50GB disk space for data storage

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/StreamlineHub.git
cd StreamlineHub
```

2. **Download and install Apache Spark**
```bash
# Download Spark 3.5.4 with Hadoop 3
wget https://archive.apache.org/dist/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz

# Extract to backend/dependencies/spark directory
mkdir -p backend/dependencies/spark
tar -xzf spark-3.5.4-bin-hadoop3.tgz -C backend/dependencies/spark --strip-components=1

# Clean up the tarball
rm spark-3.5.4-bin-hadoop3.tgz
```

3. **Start all services**
```bash
docker-compose up -d
```

3. **Wait for initialization** (2-3 minutes)
```bash
# Check service status
docker-compose ps

# View logs
docker-compose logs -f
```

4. **Access the platform**
- **Frontend Dashboard**: http://localhost:3000
- **Backend API**: http://localhost:4000
- **API Documentation**: http://localhost:4000/docs
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Spark Master**: http://localhost:7080
- **Kafka UI**: http://localhost:9095
- **Kibana**: http://localhost:5691

## Usage

### Generate Sample Data

Start the Kafka producer to generate events:

```bash
# Run producer in detached mode for continuous events
docker exec -d streamlinehub-backend python3 /app/scripts/dynamic_kafka_producer.py

# Or generate specific number of events
docker exec streamlinehub-backend python3 /app/scripts/dynamic_kafka_producer.py --count 100
```

### Monitor Data Pipeline

1. **Check Redis buffers**:
```bash
docker exec streamlinehub-redis redis-cli -a redis_secret LLEN customer_behavior_events
```

2. **View ingestion logs**:
```bash
docker logs streamlinehub-redis-ingestion --tail 50
```

3. **Monitor Airflow DAGs**:
   - Navigate to http://localhost:8080
   - Check `gold_layer_aggregation_and_ingestion_dag` status

4. **Query Delta tables**:
```bash
# View bronze layer data
docker exec streamlinehub-airflow ls -lh /opt/airflow/data/bronze/

# View gold aggregations
docker exec streamlinehub-airflow ls -lh /opt/airflow/data/gold/
```

### Optimize Delta Tables

Periodically optimize Delta tables for better performance:

```bash
docker exec streamlinehub-airflow python /opt/airflow/scripts/optimize_delta_tables.py
```

## Project Structure

```
StreamlineHub/
├── backend/
│   ├── bin/                    # Binary utilities
│   ├── config/                 # Configuration files
│   │   ├── Elasticsearch_Dag.xml
│   │   └── spark_config.xml
│   ├── dags/                   # Airflow DAG definitions
│   │   ├── gold_layer_aggregation_and_ingestion_dag.py
│   │   └── utils.py
│   ├── dag_scripts/            # DAG processing scripts
│   │   ├── elasticsearch_ingestion.py
│   │   └── gold_aggregation.py
│   ├── scripts/                # Utility scripts
│   │   ├── dynamic_kafka_producer.py
│   │   ├── init_airflow_users.py
│   │   ├── optimize_delta_tables.py
│   │   ├── Redis_to_delta_ingestion.py
│   │   └── setup_spark_connection.py
│   ├── src/                    # Backend source code
│   │   ├── main.py            # FastAPI application
│   │   ├── api/               # API routes
│   │   │   └── routers/       # Analytics, health, auth endpoints
│   │   ├── core/              # Core infrastructure
│   │   │   ├── confluent_kafka_integration.py
│   │   │   ├── pipeline_manager.py
│   │   │   └── spark_session.py
│   │   ├── analytics/         # Analytics modules
│   │   └── processing/        # Data processors
│   └── utils/                  # Shared utilities
├── config/                     # Global configuration
├── data/                       # Delta Lake storage
│   ├── bronze/                # Raw events layer
│   ├── silver/                # Processed data layer
│   └── gold/                  # Aggregated analytics layer
├── docs/                       # Documentation
│   └── assets/                # Images and diagrams
├── frontend/                   # React web application
│   ├── public/                # Static assets
│   └── src/                   # React components
│       ├── pages/             # Dashboard pages
│       ├── services/          # API clients
│       └── components/        # UI components
├── logs/                       # Application logs
├── docker-compose.yml          # Service orchestration
├── Dockerfile.Airflow          # Airflow container
├── Dockerfile.Backend          # Backend container
├── Dockerfile.Spark            # Spark container
└── README.md                   # This file
```

## Configuration

### Environment Variables

Key environment variables (configured in `docker-compose.yml`):

```yaml
# Backend Configuration
STREAMLINEHUB_ENVIRONMENT: development
STREAMLINEHUB_REDIS_URL: redis://:redis_secret@redis:6379/0
STREAMLINEHUB_KAFKA_BOOTSTRAP_SERVERS: kafka:9092
STREAMLINEHUB_ELASTICSEARCH_URL: http://elasticsearch:9200

# Data Paths
BRONZE_PATH: /app/data/bronze
SILVER_PATH: /app/data/silver
GOLD_PATH: /app/data/gold

# Airflow Configuration
AIRFLOW__CORE__EXECUTOR: LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql://airflow_user:airflow_pass@postgres/airflow
```

### Airflow DAG Configuration

The gold layer aggregation DAG runs hourly by default. Modify the schedule in `backend/dags/gold_layer_aggregation_and_ingestion_dag.py`:

```python
schedule_interval="@hourly"  # Change to desired schedule
```

## Data Pipeline

### Medallion Architecture

**Bronze Layer** (Raw Data)
- Stores raw events from Kafka/Redis
- Partitioned by `insertion_date` and `event_type`
- No transformations, preserves original structure
- ACID transactions via Delta Lake

**Silver Layer** (Cleaned Data)
- Validated and cleaned data
- Partitioned by `processing_date` and `event_type`
- Business rules applied
- Schema enforcement

**Gold Layer** (Aggregated Analytics)
- Hourly and daily aggregations
- Optimized for analytics queries
- Indexed in Elasticsearch
- Partitioned by `aggregation_type` and `event_category`

### Event Types

1. **Customer Behavior Events**
   - Page views, clicks, purchases
   - User sessions and interactions
   - Device and location data

2. **System Metrics Events**
   - CPU, memory usage
   - Service-level metrics
   - Performance monitoring

## API Reference

### Analytics Endpoints

```bash
# Get real-time metrics
GET /api/v1/analytics/realtime

# Get customer behavior analytics
GET /api/v1/analytics/customer-behavior

# Get system metrics summary
GET /api/v1/analytics/transaction-summary

# Get pipeline health status
GET /api/v1/analytics/pipeline-health
```

### Example: Query Customer Behavior

```bash
curl http://localhost:4000/api/v1/analytics/customer-behavior | jq
```

Response:
```json
{
  "success": true,
  "data": {
    "summary": {
      "total_events": 10048,
      "unique_customers": 9308,
      "avg_engagement": 0.0,
      "avg_events_per_customer": 1.08
    },
    "hourly_data": [...]
  }
}
```

Full API documentation: http://localhost:4000/docs

## Maintenance

### Stop Services

```bash
docker-compose down
```

### Clean Up Data (Caution!)

```bash
# Remove all data (Bronze/Silver/Gold layers)
docker-compose down -v
sudo rm -rf data/bronze data/silver data/gold
```

### View Service Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f backend
docker-compose logs -f airflow-scheduler
docker-compose logs -f redis-ingestion
```

### Restart Specific Service

```bash
docker-compose restart backend
docker-compose restart airflow-scheduler
```

## Development

### Local Development Setup

1. **Backend Development**
```bash
cd backend
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn src.main:app --reload --host 0.0.0.0 --port 8000
```

2. **Frontend Development**
```bash
cd frontend
npm install
npm run dev
```

## Technology Stack

### Core Technologies
- **Apache Spark 3.5.4** - Distributed data processing
- **Delta Lake 3.2.0** - ACID data lake storage
- **Apache Kafka** - Event streaming
- **Apache Airflow 2.7** - Workflow orchestration
- **Redis 7** - Event buffering and caching
- **Elasticsearch 8** - Search and analytics
- **PostgreSQL** - Airflow metadata database

### Backend
- **Python 3.11** - Programming language
- **FastAPI 0.104** - Web framework
- **PySpark 3.5.4** - Spark Python API
- **Confluent Kafka** - Kafka client
- **Structlog** - Structured logging

### Frontend
- **React 18.2** - UI framework
- **TypeScript 5.2** - Type safety
- **TailwindCSS 3.3** - Styling
- **Vite** - Build tool
- **Recharts** - Data visualization

### Infrastructure
- **Docker & Docker Compose** - Containerization
- **Nginx** - Web server
- **Zookeeper** - Kafka coordination

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Apache Software Foundation for Spark, Kafka, and Airflow
- Delta Lake team at Databricks
- FastAPI and React communities

## Support

For questions and support:
- **Issues**: [GitHub Issues](https://github.com/yourusername/StreamlineHub/issues)
- **Documentation**: See `/docs` directory
- **Email**: support@streamlinehub.example.com

---

**StreamlineHub** - Built for scalable data analytics  
Version 2.0.0 | Last Updated: November 2025

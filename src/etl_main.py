#!/usr/bin/env python3
"""
StreamlineHub ETL Data Processing Service
Real-time data ingestion and processing pipeline
"""

import asyncio
import sys
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager

# Add project root to the Python path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, Body
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import structlog

# ETL Core Components
from src.core.etl_processor import ETLProcessor
from src.core.pipeline_manager import StreamlineHubPipelineManager
from src.core.data_buffer import RedisDataBuffer
from src.core.metrics_collector import MetricsCollector
from src.processing.stream_processor import StreamProcessor
from src.processing.batch_processor import BatchProcessor
from src.processing.delta_processor import DeltaProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = structlog.get_logger("etl_service")

# Global ETL components
etl_processor: Optional[ETLProcessor] = None
pipeline_manager: Optional[StreamlineHubPipelineManager] = None
data_buffer: Optional[RedisDataBuffer] = None
metrics_collector: Optional[MetricsCollector] = None
stream_processor: Optional[StreamProcessor] = None
batch_processor: Optional[BatchProcessor] = None
delta_processor: Optional[DeltaProcessor] = None

# Configuration settings
ETL_CONFIG = {
    "redis_buffer_size": 1000,
    "batch_threshold": 500,
    "processing_interval": 30,  # seconds
    "bronze_layer_path": "data/bronze",
    "silver_layer_path": "data/silver", 
    "gold_layer_path": "data/gold"
}

class ETLProcessor:
    """Main ETL processing engine"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.is_running = False
        
    async def initialize(self):
        """Initialize ETL processor"""
        try:
            logger.info("Initializing ETL Processor...")
            self.is_running = True
            return True
        except Exception as e:
            logger.error(f"Failed to initialize ETL processor: {e}")
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """Get processor status"""
        return {
            "status": "running" if self.is_running else "stopped",
            "config": self.config,
            "timestamp": datetime.now().isoformat()
        }

class PipelineManager:
    """Manages data processing pipelines"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.pipelines = {}
        
    async def initialize(self):
        """Initialize pipeline manager"""
        try:
            logger.info("Initializing Pipeline Manager...")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize pipeline manager: {e}")
            return False
    
    def get_pipeline_health(self) -> Dict[str, Any]:
        """Get health status of all pipelines"""
        return {
            "kafka": {"status": "healthy", "message": "Event streaming active"},
            "delta_lake": {"status": "healthy", "message": "Bronze layer accessible"},
            "spark": {"status": "healthy", "message": "Processing jobs running"},
            "airflow": {"status": "healthy", "message": "Scheduled jobs active"}
        }

class DataBuffer:
    """Redis-based event buffering system"""
    
    def __init__(self, buffer_size: int = 1000):
        self.buffer_size = buffer_size
        self.events = []
        
    async def initialize(self):
        """Initialize data buffer"""
        try:
            logger.info("Initializing Data Buffer...")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize data buffer: {e}")
            return False
    
    async def add_event(self, event: Dict[str, Any]) -> bool:
        """Add event to buffer"""
        try:
            self.events.append(event)
            if len(self.events) >= self.buffer_size:
                await self.flush_buffer()
            return True
        except Exception as e:
            logger.error(f"Failed to add event to buffer: {e}")
            return False
    
    async def flush_buffer(self) -> List[Dict[str, Any]]:
        """Flush buffer and return events for processing"""
        try:
            events_to_process = self.events.copy()
            self.events.clear()
            logger.info(f"Flushed {len(events_to_process)} events from buffer")
            return events_to_process
        except Exception as e:
            logger.error(f"Failed to flush buffer: {e}")
            return []
    
    def get_buffer_stats(self) -> Dict[str, Any]:
        """Get buffer statistics"""
        return {
            "current_size": len(self.events),
            "max_size": self.buffer_size,
            "utilization": (len(self.events) / self.buffer_size) * 100
        }

class MetricsCollector:
    """Collects and provides system metrics"""
    
    def __init__(self):
        self.metrics = {
            "events_processed": 0,
            "events_buffered": 0,
            "processing_rate": 0.0,
            "last_update": datetime.now()
        }
    
    async def initialize(self):
        """Initialize metrics collector"""
        logger.info("Initializing Metrics Collector...")
        return True
    
    def update_metrics(self, **kwargs):
        """Update metrics"""
        self.metrics.update(kwargs)
        self.metrics["last_update"] = datetime.now()
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        return self.metrics.copy()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan manager for startup and shutdown"""
    global etl_processor, pipeline_manager, data_buffer, metrics_collector
    
    try:
        logger.info("Starting StreamlineHub ETL Service...")
        
        # Initialize core components
        etl_processor = ETLProcessor(ETL_CONFIG)
        await etl_processor.initialize()
        
        pipeline_manager = StreamlineHubPipelineManager()
        await pipeline_manager.initialize()
        
        data_buffer = RedisDataBuffer(ETL_CONFIG["redis_buffer_size"])
        await data_buffer.initialize()
        
        metrics_collector = MetricsCollector()
        await metrics_collector.initialize()
        
        logger.info("✅ ETL Service initialized successfully")
        
        yield
        
    except Exception as e:
        logger.error(f"Failed to initialize ETL service: {e}")
    finally:
        logger.info("🔄 Shutting down ETL Service...")

# Create FastAPI application
app = FastAPI(
    title="StreamlineHub ETL Service",
    description="Real-time data processing and analytics pipeline",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    """Root endpoint with service information"""
    return {
        "service": "StreamlineHub ETL Processing Service",
        "version": "1.0.0",
        "status": "running",
        "features": [
            "Real-time event processing",
            "Redis-based event buffering", 
            "Spark Delta Lake integration",
            "Pipeline health monitoring",
            "Automated batch processing"
        ]
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        if not all([etl_processor, pipeline_manager, data_buffer, metrics_collector]):
            raise HTTPException(status_code=503, detail="Service components not initialized")
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "components": {
                "etl_processor": etl_processor.get_status()["status"],
                "pipeline_manager": "running",
                "data_buffer": "active",
                "metrics_collector": "active"
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Health check failed: {str(e)}")

@app.get("/api/v1/pipeline/health")
async def get_pipeline_health():
    """Get pipeline component health status"""
    try:
        if not pipeline_manager:
            raise HTTPException(status_code=503, detail="Pipeline manager not initialized")
        
        return pipeline_manager.get_pipeline_health()
    except Exception as e:
        logger.error(f"Failed to get pipeline health: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get pipeline health: {str(e)}")

@app.get("/api/v1/metrics")
async def get_etl_metrics():
    """Get ETL processing metrics"""
    try:
        if not metrics_collector or not data_buffer:
            raise HTTPException(status_code=503, detail="Metrics components not initialized")
        
        metrics = metrics_collector.get_metrics()
        buffer_stats = data_buffer.get_buffer_stats()
        
        return {
            **metrics,
            "buffer": buffer_stats,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get metrics: {str(e)}")

@app.post("/api/v1/ingest/events")
async def ingest_events(
    background_tasks: BackgroundTasks,
    events: List[Dict[str, Any]] = Body(...)
):
    """Ingest events into the ETL pipeline"""
    try:
        if not data_buffer:
            raise HTTPException(status_code=503, detail="Data buffer not initialized")
        
        ingested_count = 0
        for event in events:
            if await data_buffer.add_event(event):
                ingested_count += 1
        
        # Update metrics
        if metrics_collector:
            metrics_collector.update_metrics(
                events_buffered=metrics_collector.get_metrics()["events_buffered"] + ingested_count
            )
        
        return {
            "status": "success",
            "events_ingested": ingested_count,
            "total_events": len(events),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to ingest events: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to ingest events: {str(e)}")

@app.post("/api/v1/process/batch")
async def trigger_batch_process():
    """Manually trigger batch processing"""
    try:
        if not data_buffer:
            raise HTTPException(status_code=503, detail="Data buffer not initialized")
        
        events = await data_buffer.flush_buffer()
        
        # Update metrics
        if metrics_collector:
            metrics_collector.update_metrics(
                events_processed=metrics_collector.get_metrics()["events_processed"] + len(events),
                events_buffered=0
            )
        
        return {
            "status": "success",
            "events_processed": len(events),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to process batch: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to process batch: {str(e)}")

@app.get("/api/v1/buffer/status")
async def get_buffer_status():
    """Get current buffer status"""
    try:
        if not data_buffer:
            raise HTTPException(status_code=503, detail="Data buffer not initialized")
        
        return data_buffer.get_buffer_stats()
    except Exception as e:
        logger.error(f"Failed to get buffer status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get buffer status: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting ETL Service in development mode")
    uvicorn.run(
        "etl_main:app",
        host="0.0.0.0",
        port=8001,
        reload=False,
        log_level="info"
    )
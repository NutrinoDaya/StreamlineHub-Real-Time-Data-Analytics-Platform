#!/usr/bin/env python
"""
ETL Main Service
Production-ready ETL pipeline for high-volume data processing
"""

import os
import sys
import json
import time
import yaml
import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager

# Add project root to the Python path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, Body, Path
from fastapi.responses import JSONResponse
import redis
import uvicorn

# Import modules
try:
    from src.core.etl_engine import ETLEngine
    from src.core.config_manager import ConfigManager
    from src.core.performance_monitor import PerformanceMonitor
    from src.core.batch_manager import BatchManager, BatchConfig
    from src.contracts.contract_loader import ContractLoader
    from src.processing.contract_processor import ContractProcessor
    from utils.logger_config import LoggerManager
    logger = LoggerManager.get_logger("ETL_Processing.log")
except ImportError as e:
    # Fallback logging if modules fail
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.warning(f"Module import failed: {e}")
    
    class ETLEngine:
        def __init__(self, config): pass
        def process_traffic_data(self, data): return False
        def process_vpc_data(self, data): return False
        def shutdown(self): pass
    
    class ConfigManager:
        def load_configuration(self): return {}
    
    class PerformanceMonitor:
        def start_system_monitoring(self): pass
        def stop_system_monitoring(self): pass
        def get_current_metrics(self): return {}
        def get_detailed_metrics(self): return {}
        @property
        def system_monitoring_active(self): return False

    class BatchManager:
        def __init__(self, config, callback): pass
        def add_item(self, data_type, item): return None
        def get_statistics(self): return {"status": "fallback"}
        def force_process_all(self): return []
        def shutdown(self): pass

    class BatchConfig:
        def __init__(self, **kwargs): 
            for k, v in kwargs.items():
                setattr(self, k, v)

# Global variables for components
etl_engine: Optional[ETLEngine] = None
config_manager: Optional[ConfigManager] = None
performance_monitor: Optional[PerformanceMonitor] = None
batch_manager: Optional[BatchManager] = None
redis_client: Optional[redis.Redis] = None
contract_loader: Optional[ContractLoader] = None
contract_processor: Optional[ContractProcessor] = None

# State mapping configurations
state_text_to_category = {}
state_text_to_process_type = {}

def load_state_configurations():
    """Load state mapping configurations from YAML file"""
    global state_text_to_category, state_text_to_process_type
    try:
        states_config_path = ROOT_DIR / "config" / "states_config.yaml"
        if states_config_path.exists():
            with open(states_config_path, 'r') as file:
                states_config = yaml.safe_load(file)
                state_text_to_category = states_config.get("state_text_to_category", {})
                state_text_to_process_type = states_config.get("state_text_to_process_type", {})
                logger.info(f"Loaded {len(state_text_to_category)} state categories and {len(state_text_to_process_type)} process types")
        else:
            logger.warning("states_config.yaml not found, using empty mappings")
    except Exception as e:
        logger.error(f"Failed to load state configurations: {e}")
        state_text_to_category = {}
        state_text_to_process_type = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan manager for startup and shutdown"""
    global etl_engine, config_manager, performance_monitor, batch_manager, redis_client
    
    try:
        logger.info("ETL Service starting...")
        
        # Load state mapping configurations
        load_state_configurations()
        
        # Initialize configuration manager
        config_manager = ConfigManager()
        config = config_manager.load_configuration()
        logger.info("Configuration loaded")
        
        # Initialize performance monitor
        performance_monitor = PerformanceMonitor()
        performance_monitor.start_system_monitoring()
        logger.info("Performance monitoring started")
        
        # Initialize ETL engine
        etl_engine = ETLEngine(config)
        logger.info("ETL Engine initialized")

        # Initialize contract loader/processor
        global contract_loader, contract_processor
        contract_loader = ContractLoader()
        try:
            contract_processor = ContractProcessor(etl_engine.spark, etl_engine.delta_manager)
            logger.info("Contract Processor initialized")
        except Exception as e:
            logger.warning(f"Contract Processor init failed, continuing without: {e}")
            contract_processor = None
        
        # Initialize batch manager with processing callback
        def batch_processing_callback(data_type, items):
            """Callback function for batch processing"""
            try:
                # Generic path: expect data_type like "domain.product"
                if "." in data_type and contract_processor is not None and contract_loader is not None:
                    domain, product = data_type.split(".", 1)
                    contract = contract_loader.load(domain, product)
                    result = contract_processor.process(domain, product, items, contract)
                    return bool(result.get("success", False))
                else:
                    logger.error(f"Unknown data type or processor unavailable: {data_type}")
                    return False
            except Exception as e:
                logger.error(f"Batch processing failed for {data_type}: {e}")
                return False
        
        # Get batch configuration from ETL config
        etl_config = config.get('etl', {})
        batch_config = BatchConfig(
            max_items=etl_config.get('max_items', 400),
            max_wait_time_seconds=300,  # 5 minutes
            max_concurrent_batches=3,
            enable_auto_processing=True
        )
        
        batch_manager = BatchManager(batch_config, batch_processing_callback)
        logger.info("Batch Manager initialized")
        
        logger.info("Initializing Redis connection...")
        
        # Initialize Redis connection for batch operations
        try:
            global redis_client
            # Get Redis configuration from ETL config
            etl_config_root = config_manager.export_config() if hasattr(config_manager, 'export_config') else config
            redis_host = etl_config_root.get('redis_host', 'redis')
            redis_port = int(etl_config_root.get('redis_port', 6379))
            redis_db = int(etl_config_root.get('redis_db', 0))

            logger.info(f"Connecting to Redis {redis_host}:{redis_port}")
            
            redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            redis_client.ping()
            logger.info(f"Redis connected: {redis_host}:{redis_port}")
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}")
            redis_client = None
        
        logger.info("ETL Service ready")
        yield
        
    except Exception as e:
        logger.error(f"ETL Service startup failed: {e}")
        raise
    
    finally:
        # Cleanup
        try:
            if batch_manager:
                batch_manager.shutdown()
            if performance_monitor:
                performance_monitor.stop_system_monitoring()
            if etl_engine:
                etl_engine.shutdown()
            if redis_client:
                redis_client.close()
            logger.info("ETL Service shutdown complete")
        except Exception as e:
            logger.error(f"Shutdown error: {e}")

# Create FastAPI application with lifespan
app = FastAPI(
    title="ETL Processing Service",
    description="Production-ready ETL pipeline for high-volume data processing (2k-4k records/minute)",
    version="2.0.0",
    lifespan=lifespan
)

@app.get("/")
async def root():
    """Root endpoint with service information"""
    return {
        "service": "ETL Processing Service",
        "version": "2.0.0",
        "status": "running",
        "features": [
            "Delta Lake management",
            "Data validation and quality checks",
            "Performance monitoring",
            "Automatic error recovery",
            "High-volume processing (2k-4k records/minute)"
        ]
    }

@app.get("/delta/status")
async def get_delta_status():
    """Check Delta Lake availability and configuration"""
    try:
        status = {
            "service": "Delta Lake Status",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "delta_available": False,
            "spark_configured": False,
            "test_result": None,
            "error": None
        }
        
        if etl_engine and etl_engine.delta_manager:
            status["delta_available"] = etl_engine.delta_manager.delta_available
            
            # Try a simple Delta operation test
            try:
                from pyspark.sql.types import StructType, StructField, StringType
                from pyspark.sql import Row
                
                test_data = [Row(test_col="delta_test")]
                test_df = etl_engine.spark.createDataFrame(test_data)
                
                # Test Delta format creation (without saving)
                writer = test_df.write.format("delta")
                status["test_result"] = "Delta format creation successful"
                status["spark_configured"] = True
                
            except Exception as test_error:
                status["test_result"] = f"Delta test failed: {test_error}"
                status["error"] = str(test_error)
        
        return status
        
    except Exception as e:
        logger.error(f"Delta status check failed: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Delta status check failed: {e}"}
        )

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        health_status = {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "components": {
                "etl_engine": etl_engine is not None,
                "config_manager": config_manager is not None,
                "performance_monitor": performance_monitor is not None and performance_monitor.system_monitoring_active,
                "redis": redis_client is not None
            }
        }
        
        # Add performance metrics if available
        if performance_monitor:
            health_status["performance"] = performance_monitor.get_current_metrics()
        
        return health_status
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail="Health check failed")

@app.get("/metrics")
async def get_metrics():
    """Get current performance metrics"""
    try:
        if not performance_monitor:
            raise HTTPException(status_code=503, detail="Performance monitor not available")
        
        metrics = performance_monitor.get_detailed_metrics()
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metrics": metrics
        }
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve metrics")

@app.post("/ingest/{domain}/{product}")
async def ingest_contract_data(
    domain: str = Path(..., description="Domain name"),
    product: str = Path(..., description="Product name"),
    data: Dict[str, Any] = Body(...)
):
    """Generic contract-driven ingest endpoint (writes Delta partitioned by insertionTime)."""
    try:
        if not batch_manager:
            raise HTTPException(status_code=503, detail="Batch Manager not available")
        
        # Extract payload list
        items: List[Dict[str, Any]]
        if isinstance(data, dict) and "data" in data:
            items = data["data"] if isinstance(data["data"], list) else [data["data"]]
        elif isinstance(data, list):
            items = data
        else:
            items = [data]

        # Tag items with batch time; processor will ensure InsertionTime exists
        triggered_batch_ids: List[str] = []
        data_type = f"{domain}.{product}"
        for item in items:
            batch_id = batch_manager.add_item(data_type, item)
            if batch_id and batch_id not in triggered_batch_ids:
                triggered_batch_ids.append(batch_id)

        return {
            "message": f"Ingest accepted for {domain}/{product}",
            "items_processed": len(items),
            "status": "accepted",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "triggered_batches": triggered_batch_ids or None
        }
    
    except Exception as e:
        logger.error(f"Ingest request failed: {e}")
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")


@app.post("/ipp_psop/IncidentFile/")
async def legacy_ipp_psop_ingest(
    data: Dict[str, Any] = Body(...)
):
    """Legacy compatibility endpoint.

    Routes payloads to the contract-driven ingest pipeline while logging a warning for migration.
    """
    logger.warning("/ipp_psop/IncidentFile/ is deprecated; please migrate to /ingest/ipp_psop/IncidentFile")
    return await ingest_contract_data("ipp_psop", "IncidentFile", data)

@app.get("/batch/status")
async def get_batch_status():
    """Get current batch processing status and statistics"""
    try:
        if not batch_manager:
            raise HTTPException(status_code=503, detail="Batch Manager not available")
        
        statistics = batch_manager.get_statistics()
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "batch_statistics": statistics
        }
    
    except Exception as e:
        logger.error(f"Failed to get batch status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve batch status: {str(e)}")

@app.post("/batch/force_process")
async def force_process_batches():
    """Force processing of all active batches"""
    try:
        if not batch_manager:
            raise HTTPException(status_code=503, detail="Batch Manager not available")
        
        triggered_batch_ids = batch_manager.force_process_all()
        
        return {
            "message": f"Forced processing of {len(triggered_batch_ids)} batches",
            "triggered_batches": triggered_batch_ids,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    except Exception as e:
        logger.error(f"Failed to force process batches: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to force process: {str(e)}")

@app.post("/vitds/TrafficData/")
async def process_traffic(
    background_tasks: BackgroundTasks,
    data: Dict[str, Any] = Body(...)
):
    """Deprecated. Use /ingest/{domain}/{product}."""
    raise HTTPException(status_code=410, detail="Deprecated. Use /ingest/traffic/<product> instead.")

@app.post("/VPC/Events/")
async def process_vpc_events(
    background_tasks: BackgroundTasks,
    data: List[Dict[str, Any]] = Body(...)
):
    """Deprecated. Use /ingest/{domain}/{product}."""
    raise HTTPException(status_code=410, detail="Deprecated. Use /ingest/vpc/<product> instead.")


@app.post("/VPC/Clerks/")
async def process_vpc_clerks(
    background_tasks: BackgroundTasks,
    data: List[Dict[str, Any]] = Body(...)
):
    """Deprecated. Use /ingest/{domain}/{product}."""
    raise HTTPException(status_code=410, detail="Deprecated. Use /ingest/vpc/<product> instead.")

@app.post("/process/vpc")
async def process_vpc(
    background_tasks: BackgroundTasks,
    data: Dict[str, Any] = Body(...)
):
    """Deprecated. Use /ingest/{domain}/{product}."""
    raise HTTPException(status_code=410, detail="Deprecated. Use /ingest/vpc/<product> instead.")

async def _process_ipp_psop_background(processing_id: str, data: Dict[str, Any]):
    """Background task for IPP_PSOP processing"""
    try:
        logger.info(f"Processing IPP_PSOP: {processing_id}")
        
        # Extract the actual data from the request payload
        if isinstance(data, dict) and "data" in data:
            incident_data = data["data"]
        elif isinstance(data, list):
            incident_data = data
        else:
            incident_data = [data] if isinstance(data, dict) else []
        
        if not incident_data:
            logger.error(f"No data found in request: {processing_id}")
            return
        
        # Use ETL processing utils that handles bronze, lookup, and silver layers
        from utils.etl_processing_utils import process_ipp_psop_data
        
        try:
            process_ipp_psop_data(incident_data)
            success = True
        except Exception as e:
            logger.error(f"IPP_PSOP processing failed: {e}")
            success = False
        
        result = {
            "processing_id": processing_id,
            "status": "completed" if success else "failed",
            "records_processed": len(incident_data),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        # Store result in Redis if available
        if redis_client:
            redis_client.setex(
                f"result:{processing_id}",
                3600,
                json.dumps(result)
            )
        
        logger.info(f"IPP_PSOP processing completed: {processing_id}, success: {success}")
        
    except Exception as e:
        logger.error(f"IPP_PSOP processing failed {processing_id}: {e}")
        if redis_client:
            error_result = {
                "processing_id": processing_id,
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            redis_client.setex(
                f"result:{processing_id}",
                3600,
                json.dumps(error_result)
            )

async def _process_traffic_background(processing_id: str, data: Dict[str, Any]):
    """Deprecated background task - no-op."""
    logger.warning("_process_traffic_background is deprecated; use /ingest instead.")

async def _process_vpc_background(processing_id: str, data: Dict[str, Any]):
    """Deprecated background task - no-op."""
    logger.warning("_process_vpc_background is deprecated; use /ingest instead.")

@app.get("/result/{processing_id}")
async def get_processing_result(processing_id: str):
    """Get processing result by ID"""
    try:
        if not redis_client:
            raise HTTPException(status_code=503, detail="Result storage not available")
        
        result = redis_client.get(f"result:{processing_id}")
        if not result:
            raise HTTPException(status_code=404, detail="Processing result not found")
        
        return json.loads(result)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get processing result {processing_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve result")

if __name__ == "__main__":
    # This is for development only
    logger.info("Starting ETL Service in development mode")
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info"
    )

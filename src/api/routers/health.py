"""
Health check and monitoring endpoints.

Provides system health checks, readiness probes, and monitoring
endpoints for load balancers and orchestration systems.
"""

from typing import Dict, Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
import structlog

from src.core.database import db_manager
from src.core.cache import cache_manager


logger = structlog.get_logger()
router = APIRouter()


class HealthResponse(BaseModel):
    """Health check response model."""
    status: str
    timestamp: str
    version: str = "1.0.0"
    environment: str
    checks: Dict[str, Any]


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Comprehensive health check endpoint.
    
    Checks the status of all critical system components including
    database, cache, and external dependencies.
    """
    from datetime import datetime
    import os
    
    checks = {}
    overall_status = "healthy"
    
    # Database health check
    try:
        db_health = await db_manager.health_check()
        checks["database"] = db_health
        if db_health["status"] != "healthy":
            overall_status = "unhealthy"
    except Exception as e:
        checks["database"] = {"status": "unhealthy", "error": str(e)}
        overall_status = "unhealthy"
    
    # Cache health check
    try:
        cache_health = await cache_manager.health_check()
        checks["cache"] = cache_health
        if cache_health["status"] != "healthy":
            overall_status = "unhealthy"
    except Exception as e:
        checks["cache"] = {"status": "unhealthy", "error": str(e)}
        overall_status = "unhealthy"
    
    # System resources check
    try:
        import psutil
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        checks["system"] = {
            "status": "healthy",
            "cpu_percent": psutil.cpu_percent(interval=1),
            "memory_percent": memory.percent,
            "memory_available_gb": round(memory.available / (1024**3), 2),
            "disk_percent": disk.percent,
            "disk_free_gb": round(disk.free / (1024**3), 2)
        }
        
        # Mark unhealthy if resources are critically low
        if memory.percent > 90 or disk.percent > 90:
            checks["system"]["status"] = "warning"
            overall_status = "degraded" if overall_status == "healthy" else overall_status
            
    except ImportError:
        checks["system"] = {"status": "unknown", "message": "psutil not available"}
    except Exception as e:
        checks["system"] = {"status": "error", "error": str(e)}
    
    return HealthResponse(
        status=overall_status,
        timestamp=datetime.utcnow().isoformat(),
        environment=os.getenv("ENVIRONMENT", "development"),
        checks=checks
    )


@router.get("/ready")
async def readiness_check():
    """
    Kubernetes readiness probe endpoint.
    
    Checks if the application is ready to serve traffic.
    Returns 200 if ready, 503 if not ready.
    """
    try:
        # Test database connection
        db_health = await db_manager.health_check()
        if db_health["status"] != "healthy":
            raise HTTPException(status_code=503, detail="Database not ready")
        
        # Test cache connection  
        cache_health = await cache_manager.health_check()
        if cache_health["status"] != "healthy":
            raise HTTPException(status_code=503, detail="Cache not ready")
        
        return {"status": "ready"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        raise HTTPException(status_code=503, detail="Service not ready")


@router.get("/live")
async def liveness_check():
    """
    Kubernetes liveness probe endpoint.
    
    Simple check to verify the application process is alive.
    Should always return 200 unless the process is completely broken.
    """
    return {"status": "alive"}


@router.get("/startup")
async def startup_check():
    """
    Kubernetes startup probe endpoint.
    
    Checks if the application has completed startup initialization.
    """
    # Check if critical components are initialized
    try:
        # Verify database is initialized
        if not db_manager.async_engine:
            raise HTTPException(status_code=503, detail="Database not initialized")
        
        # Verify cache is initialized
        if not cache_manager.client:
            raise HTTPException(status_code=503, detail="Cache not initialized")
        
        return {"status": "started"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Startup check failed: {e}")
        raise HTTPException(status_code=503, detail="Startup not complete")


@router.get("/metrics/basic")
async def basic_metrics():
    """
    Basic application metrics endpoint.
    
    Provides simple metrics that can be consumed by monitoring systems
    without requiring Prometheus client library.
    """
    try:
        import psutil
        from datetime import datetime
        
        # System metrics
        memory = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent(interval=1)
        
        # Database connection info
        db_info = db_manager.get_connection_info()
        
        # Cache info
        cache_health = await cache_manager.health_check()
        
        metrics = {
            "timestamp": datetime.utcnow().isoformat(),
            "system": {
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "memory_used_mb": round(memory.used / (1024**2)),
                "memory_available_mb": round(memory.available / (1024**2))
            },
            "database": {
                "pool_size": db_info["async_engine"]["pool_size"],
                "connections_checked_out": db_info["async_engine"]["pool_status"]["checked_out"],
                "connections_checked_in": db_info["async_engine"]["pool_status"]["checked_in"]
            },
            "cache": {
                "status": cache_health.get("status", "unknown"),
                "connected_clients": cache_health.get("info", {}).get("connected_clients", 0),
                "used_memory": cache_health.get("info", {}).get("used_memory", "unknown")
            }
        }
        
        return metrics
        
    except Exception as e:
        logger.error(f"Basic metrics failed: {e}")
        return {"error": "Failed to collect metrics", "timestamp": datetime.utcnow().isoformat()}


@router.get("/version")
async def version_info():
    """
    Application version information.
    
    Returns version, build, and deployment information.
    """
    import os
    from datetime import datetime
    
    return {
        "version": "1.0.0",
        "build_date": os.getenv("BUILD_DATE", "unknown"),
        "commit_hash": os.getenv("COMMIT_HASH", "unknown"),
        "environment": os.getenv("ENVIRONMENT", "development"),
        "python_version": os.getenv("PYTHON_VERSION", "unknown"),
        "timestamp": datetime.utcnow().isoformat()
    }
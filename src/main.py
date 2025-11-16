"""
FastAPI application factory and main entry point.

This module sets up the FastAPI application with all necessary middleware,
routers, and configuration for the StreamlineHub Business Intelligence platform.
"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
import structlog

from src.api.middleware.auth import AuthMiddleware
from src.api.middleware.logging import LoggingMiddleware
from src.api.middleware.rate_limit import RateLimitMiddleware
from src.api.routers import (
    auth_router,
    health_router,
)
from src.api.routers.websocket import router as websocket_router
from src.api.routers.analytics import router as analytics_router
from src.core.config import get_settings
from src.core.database import init_database, close_database
from src.core.cache import init_cache, close_cache_connections
from src.core.logging_config import setup_logging


logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Application lifespan manager for startup and shutdown events.
    
    Handles database connections, cache initialization, Kafka integration, and cleanup.
    """
    logger.info("Starting StreamlineHub API server...")
    
    # Initialize connections
    await init_database()
    await init_cache()
    
    # Initialize Kafka integration
    try:
        from src.core.confluent_kafka_integration import kafka_manager
        from src.api.routers.websocket import manager as websocket_manager
        
        # Try to initialize Confluent Kafka (fallback gracefully if not available)
        kafka_success = await kafka_manager.initialize(websocket_manager)
        if kafka_success:
            logger.info("✅ Confluent Kafka integration ready")
        else:
            logger.warning("⚠️ Confluent Kafka initialization failed")
        
    except Exception as e:
        logger.warning(f"⚠️ Confluent Kafka integration failed, continuing without Kafka: {e}")
    
    logger.info("API server startup complete")
    
    yield
    
    # Cleanup on shutdown
    logger.info("Shutting down API server...")
    
    try:
        from src.core.confluent_kafka_integration import kafka_manager
        
        # Close Kafka connections
        await kafka_manager.close()
    except Exception as e:
        logger.warning(f"Error closing Kafka connections: {e}")
    
    # Cleanup
    logger.info("Shutting down API server...")
    await close_database()
    await close_cache_connections()
    logger.info("API server shutdown complete")


def create_app() -> FastAPI:
    """
    Create and configure the FastAPI application.
    
    Returns:
        FastAPI: Configured application instance
    """
    settings = get_settings()
    setup_logging(settings.log_level)
    
    app = FastAPI(
        title="StreamlineHub Business Intelligence API",
        description="Enterprise Business Intelligence Platform",
        version="1.0.0",
        docs_url="/docs" if settings.debug else None,
        redoc_url="/redoc" if settings.debug else None,
        lifespan=lifespan,
        openapi_tags=[
            {
                "name": "Authentication",
                "description": "User authentication and authorization endpoints"
            },
            {
                "name": "Analytics",
                "description": "Real-time analytics and reporting"
            },
            {
                "name": "Campaigns",
                "description": "Marketing campaign management"
            },
            {
                "name": "Machine Learning",
                "description": "ML model inference and predictions"
            },
            {
                "name": "Administration",
                "description": "System administration endpoints"
            },
            {
                "name": "Health",
                "description": "System health and monitoring"
            }
        ]
    )
    
    # Configure middleware
    configure_middleware(app, settings)
    
    # Register routers
    register_routers(app)
    
    # Add exception handlers
    configure_exception_handlers(app)
    
    return app


def configure_middleware(app: FastAPI, settings) -> None:
    """Configure application middleware."""
    
    # Trusted hosts
    if settings.allowed_hosts:
        app.add_middleware(
            TrustedHostMiddleware,
            allowed_hosts=settings.allowed_hosts
        )
    
    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Rate limiting
    app.add_middleware(RateLimitMiddleware)
    
    # Authentication (temporarily disabled for testing)
    # app.add_middleware(AuthMiddleware)
    
    # Logging
    app.add_middleware(LoggingMiddleware)


def register_routers(app: FastAPI) -> None:
    """Register API routers."""
    
    # Health check (no prefix, for load balancer)
    app.include_router(health_router, tags=["Health"])
    
    # API v1 routes
    api_prefix = "/api/v1"
    
    app.include_router(
        auth_router, 
        prefix=f"{api_prefix}/auth",
        tags=["Authentication"]
    )
    
    # WebSocket routes for real-time data
    app.include_router(websocket_router, tags=["WebSocket"])
    
    # Analytics API
    app.include_router(
        analytics_router,
        prefix=f"{api_prefix}/analytics",
        tags=["Analytics"]
    )
    
    # app.include_router(
    #     campaigns_router,
    #     prefix=f"{api_prefix}/campaigns",
    #     tags=["Campaigns"]
    # )
    
    # app.include_router(
    #     ml_router,
    #     prefix=f"{api_prefix}/ml",
    #     tags=["Machine Learning"]
    # )
    
    # app.include_router(
    #     admin_router,
    #     prefix=f"{api_prefix}/admin",
    #     tags=["Administration"]
    # )
    
    # # High-throughput events router
    # app.include_router(
    #     events_router,
    #     tags=["Events"]
    # )




def configure_exception_handlers(app: FastAPI) -> None:
    """Configure global exception handlers."""
    
    @app.exception_handler(ValueError)
    async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
        logger.warning(
            "Value error occurred",
            error=str(exc),
            path=request.url.path,
            method=request.method
        )
        return JSONResponse(
            status_code=400,
            content={"detail": str(exc)}
        )
    
    @app.exception_handler(PermissionError)
    async def permission_error_handler(request: Request, exc: PermissionError) -> JSONResponse:
        logger.warning(
            "Permission denied",
            error=str(exc),
            path=request.url.path,
            method=request.method
        )
        return JSONResponse(
            status_code=403,
            content={"detail": "Insufficient permissions"}
        )
    
    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.error(
            "Unhandled exception occurred",
            error=str(exc),
            error_type=type(exc).__name__,
            path=request.url.path,
            method=request.method,
            exc_info=exc
        )
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"}
        )


# Create the application instance
app = create_app()


if __name__ == "__main__":
    import uvicorn
    
    settings = get_settings()
    
    uvicorn.run(
        "src.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        workers=1 if settings.debug else settings.workers,
        log_level=settings.log_level.lower(),
        access_log=True
    )
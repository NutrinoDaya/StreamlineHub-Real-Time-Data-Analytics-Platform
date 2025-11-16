"""
Router module initialization.

This module exposes all API routers for the FastAPI application
with proper imports and organization.
"""

from .auth import router as auth_router
from .health import router as health_router
# Temporarily disabled during MongoDB migration
# from .customers import router as customers_router
# from .analytics import router as analytics_router
# from .campaigns import router as campaigns_router
# from .ml import router as ml_router
# from .admin import router as admin_router
# from .events import router as events_router

# Export all routers
__all__ = [
    "auth_router",
    "health_router", 
    # Temporarily disabled during MongoDB migration
    # "customers_router",
    # "analytics_router", 
    # "campaigns_router",
    # "ml_router",
    # "admin_router",
    # "events_router"
]
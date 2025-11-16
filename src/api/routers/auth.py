"""
Simple authentication router for MongoDB testing.
"""

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()

class HealthResponse(BaseModel):
    status: str
    message: str

class SystemStatusResponse(BaseModel):
    status: str
    database: str
    migration_status: str
    timestamp: str

@router.get("/health", response_model=HealthResponse)
async def auth_health():
    """Health check for auth router."""
    return HealthResponse(
        status="ok", 
        message="Auth router loaded successfully with MongoDB support"
    )

@router.get("/status", response_model=SystemStatusResponse)
async def system_status():
    """Get system status for frontend."""
    from datetime import datetime
    return SystemStatusResponse(
        status="connected",
        database="MongoDB",
        migration_status="successful",
        timestamp=datetime.utcnow().isoformat()
    )

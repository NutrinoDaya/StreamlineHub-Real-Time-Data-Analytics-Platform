"""
Administration API router.

This module provides endpoints for system administration,
user management, and configuration.
"""

from typing import List, Optional
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, status, Query
from pydantic import BaseModel, EmailStr
from enum import Enum
import random


router = APIRouter(tags=["Administration"])


# Enums
class UserRole(str, Enum):
    ADMIN = "admin"
    ANALYST = "analyst"
    MARKETER = "marketer"
    VIEWER = "viewer"


class UserStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    SUSPENDED = "suspended"


# Pydantic Models
class UserBase(BaseModel):
    username: str
    email: EmailStr
    first_name: str
    last_name: str
    role: UserRole


class UserCreate(UserBase):
    password: str


class UserUpdate(BaseModel):
    email: Optional[EmailStr] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    role: Optional[UserRole] = None
    status: Optional[UserStatus] = None


class UserResponse(UserBase):
    id: int
    status: UserStatus
    created_at: datetime
    updated_at: datetime
    last_login: Optional[datetime] = None
    login_count: int = 0
    
    class Config:
        from_attributes = True


class SystemStats(BaseModel):
    uptime_seconds: int
    cpu_usage_percent: float
    memory_usage_percent: float
    disk_usage_percent: float
    total_api_calls: int
    active_sessions: int
    database_size_mb: float
    cache_hit_rate: float


class AuditLog(BaseModel):
    id: int
    timestamp: datetime
    user_id: Optional[int]
    username: Optional[str]
    action: str
    resource: str
    resource_id: Optional[str]
    ip_address: str
    status: str
    details: Optional[str] = None


class SystemConfig(BaseModel):
    key: str
    value: str
    description: Optional[str] = None
    updated_at: datetime
    updated_by: str


# Empty data storage - no mock data
users_db = []

audit_logs = []

configs = []


@router.get("/users", response_model=List[UserResponse])
async def get_users(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    role: Optional[UserRole] = None,
    status: Optional[UserStatus] = None,
    search: Optional[str] = None
):
    """
    Get list of users with filtering and pagination.
    """
    filtered = users_db.copy()
    
    if role:
        filtered = [u for u in filtered if u["role"] == role]
    
    if status:
        filtered = [u for u in filtered if u["status"] == status]
    
    if search:
        search_lower = search.lower()
        filtered = [
            u for u in filtered
            if search_lower in u["username"].lower()
            or search_lower in u["email"].lower()
            or search_lower in f"{u['first_name']} {u['last_name']}".lower()
        ]
    
    return filtered[skip:skip + limit]


@router.get("/users/{user_id}", response_model=UserResponse)
async def get_user(user_id: int):
    """
    Get a specific user by ID.
    """
    user = next((u for u in users_db if u["id"] == user_id), None)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@router.post("/users", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def create_user(user: UserCreate):
    """
    Create a new user.
    """
    # Check if username or email already exists
    if any(u["username"] == user.username for u in users_db):
        raise HTTPException(status_code=400, detail="Username already exists")
    if any(u["email"] == user.email for u in users_db):
        raise HTTPException(status_code=400, detail="Email already exists")
    
    new_user = {
        "id": max(u["id"] for u in users_db) + 1,
        **user.dict(exclude={"password"}),
        "status": UserStatus.ACTIVE,
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
        "last_login": None,
        "login_count": 0
    }
    users_db.append(new_user)
    return new_user


@router.put("/users/{user_id}", response_model=UserResponse)
async def update_user(user_id: int, user: UserUpdate):
    """
    Update a user's information.
    """
    existing = next((u for u in users_db if u["id"] == user_id), None)
    if not existing:
        raise HTTPException(status_code=404, detail="User not found")
    
    update_data = user.dict(exclude_unset=True)
    for key, value in update_data.items():
        existing[key] = value
    existing["updated_at"] = datetime.now()
    
    return existing


@router.delete("/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(user_id: int):
    """
    Delete a user.
    """
    global users_db
    user = next((u for u in users_db if u["id"] == user_id), None)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    if user["id"] == 1:  # Prevent deleting admin
        raise HTTPException(status_code=400, detail="Cannot delete system administrator")
    
    users_db = [u for u in users_db if u["id"] != user_id]
    return None


@router.get("/system/stats", response_model=SystemStats)
async def get_system_stats():
    """
    Get system statistics and resource usage.
    Returns zero values - no mock data.
    """
    return SystemStats(
        uptime_seconds=0,
        cpu_usage_percent=0.0,
        memory_usage_percent=0.0,
        disk_usage_percent=0.0,
        total_api_calls=0,
        active_sessions=0,
        database_size_mb=0.0,
        cache_hit_rate=0.0
    )


@router.get("/audit-logs", response_model=List[AuditLog])
async def get_audit_logs(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    user_id: Optional[int] = None,
    action: Optional[str] = None,
    resource: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
):
    """
    Get audit logs with filtering and pagination.
    """
    filtered = audit_logs.copy()
    
    if user_id:
        filtered = [log for log in filtered if log["user_id"] == user_id]
    
    if action:
        filtered = [log for log in filtered if log["action"] == action.upper()]
    
    if resource:
        filtered = [log for log in filtered if log["resource"] == resource]
    
    if start_date:
        filtered = [log for log in filtered if log["timestamp"] >= start_date]
    
    if end_date:
        filtered = [log for log in filtered if log["timestamp"] <= end_date]
    
    # Sort by timestamp descending
    filtered.sort(key=lambda x: x["timestamp"], reverse=True)
    
    return filtered[skip:skip + limit]


@router.get("/config", response_model=List[SystemConfig])
async def get_system_config():
    """
    Get system configuration settings.
    """
    return configs


@router.put("/config/{key}", response_model=SystemConfig)
async def update_config(key: str, value: str):
    """
    Update a system configuration setting.
    """
    config = next((c for c in configs if c["key"] == key), None)
    if not config:
        raise HTTPException(status_code=404, detail="Configuration not found")
    
    config["value"] = value
    config["updated_at"] = datetime.now()
    
    return config


@router.get("/dashboard-metrics")
async def get_admin_dashboard_metrics():
    """
    Get metrics for admin dashboard.
    Returns zero values - no mock data.
    """
    return {
        "total_users": len(users_db),
        "active_users": 0,
        "new_users_week": 0,
        "total_api_calls_today": 0,
        "avg_response_time_ms": 0.0,
        "error_rate_percent": 0.0,
        "cache_hit_rate": 0.0,
        "database_connections": 0
    }

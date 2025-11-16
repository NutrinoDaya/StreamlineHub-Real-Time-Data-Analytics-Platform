"""
Campaign management API router.

This module provides endpoints for creating, managing, and analyzing
marketing campaigns.
"""

from typing import List, Optional
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, status, Query
from pydantic import BaseModel, Field
from enum import Enum
import random


router = APIRouter(tags=["Campaigns"])


# Enums
class CampaignStatus(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    ARCHIVED = "archived"


class CampaignType(str, Enum):
    EMAIL = "email"
    SMS = "sms"
    PUSH = "push"
    SOCIAL = "social"
    DISPLAY = "display"


# Pydantic Models
class CampaignBase(BaseModel):
    name: str
    description: Optional[str] = None
    type: CampaignType
    target_segment: str
    budget: float = 0.0
    start_date: datetime
    end_date: Optional[datetime] = None


class CampaignCreate(CampaignBase):
    pass


class CampaignUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[CampaignStatus] = None
    budget: Optional[float] = None
    end_date: Optional[datetime] = None


class CampaignMetrics(BaseModel):
    impressions: int = 0
    clicks: int = 0
    conversions: int = 0
    revenue: float = 0.0
    cost: float = 0.0
    ctr: float = 0.0  # Click-through rate
    cvr: float = 0.0  # Conversion rate
    roi: float = 0.0  # Return on investment
    cpc: float = 0.0  # Cost per click
    cpa: float = 0.0  # Cost per acquisition


class CampaignResponse(CampaignBase):
    id: int
    status: CampaignStatus
    created_at: datetime
    updated_at: datetime
    metrics: CampaignMetrics
    
    class Config:
        from_attributes = True


class CampaignStats(BaseModel):
    total_campaigns: int
    active_campaigns: int
    total_budget: float
    total_spent: float
    total_revenue: float
    total_roi: float
    avg_ctr: float
    avg_cvr: float


# Empty data storage - no mock data
campaigns_db = []


@router.get("", response_model=List[CampaignResponse])
async def get_campaigns(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    status: Optional[CampaignStatus] = None,
    type: Optional[CampaignType] = None,
    search: Optional[str] = None
):
    """
    Get list of campaigns with filtering and pagination.
    """
    filtered = campaigns_db.copy()
    
    if status:
        filtered = [c for c in filtered if c["status"] == status]
    
    if type:
        filtered = [c for c in filtered if c["type"] == type]
    
    if search:
        search_lower = search.lower()
        filtered = [c for c in filtered if search_lower in c["name"].lower()]
    
    return filtered[skip:skip + limit]


@router.get("/stats", response_model=CampaignStats)
async def get_campaign_stats():
    """
    Get campaign statistics and performance overview.
    """
    total = len(campaigns_db)
    active = len([c for c in campaigns_db if c["status"] == CampaignStatus.ACTIVE])
    total_budget = sum(c["budget"] for c in campaigns_db)
    total_spent = sum(c["metrics"]["cost"] for c in campaigns_db)
    total_revenue = sum(c["metrics"]["revenue"] for c in campaigns_db)
    total_roi = round(((total_revenue - total_spent) / total_spent * 100) if total_spent > 0 else 0, 2)
    avg_ctr = round(sum(c["metrics"]["ctr"] for c in campaigns_db) / total if total > 0 else 0, 2)
    avg_cvr = round(sum(c["metrics"]["cvr"] for c in campaigns_db) / total if total > 0 else 0, 2)
    
    return CampaignStats(
        total_campaigns=total,
        active_campaigns=active,
        total_budget=total_budget,
        total_spent=total_spent,
        total_revenue=total_revenue,
        total_roi=total_roi,
        avg_ctr=avg_ctr,
        avg_cvr=avg_cvr
    )


@router.get("/{campaign_id}", response_model=CampaignResponse)
async def get_campaign(campaign_id: int):
    """
    Get a specific campaign by ID.
    """
    campaign = next((c for c in campaigns_db if c["id"] == campaign_id), None)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return campaign


@router.post("", response_model=CampaignResponse, status_code=status.HTTP_201_CREATED)
async def create_campaign(campaign: CampaignCreate):
    """
    Create a new campaign.
    """
    new_campaign = {
        "id": max(c["id"] for c in campaigns_db) + 1,
        **campaign.dict(),
        "status": CampaignStatus.DRAFT,
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
        "metrics": CampaignMetrics().dict()
    }
    campaigns_db.append(new_campaign)
    return new_campaign


@router.put("/{campaign_id}", response_model=CampaignResponse)
async def update_campaign(campaign_id: int, campaign: CampaignUpdate):
    """
    Update a campaign's information.
    """
    existing = next((c for c in campaigns_db if c["id"] == campaign_id), None)
    if not existing:
        raise HTTPException(status_code=404, detail="Campaign not found")
    
    update_data = campaign.dict(exclude_unset=True)
    for key, value in update_data.items():
        existing[key] = value
    existing["updated_at"] = datetime.now()
    
    return existing


@router.delete("/{campaign_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_campaign(campaign_id: int):
    """
    Delete a campaign.
    """
    global campaigns_db
    campaign = next((c for c in campaigns_db if c["id"] == campaign_id), None)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    
    campaigns_db = [c for c in campaigns_db if c["id"] != campaign_id]
    return None


@router.post("/{campaign_id}/activate", response_model=CampaignResponse)
async def activate_campaign(campaign_id: int):
    """
    Activate a campaign.
    """
    campaign = next((c for c in campaigns_db if c["id"] == campaign_id), None)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    
    campaign["status"] = CampaignStatus.ACTIVE
    campaign["updated_at"] = datetime.now()
    return campaign


@router.post("/{campaign_id}/pause", response_model=CampaignResponse)
async def pause_campaign(campaign_id: int):
    """
    Pause a campaign.
    """
    campaign = next((c for c in campaigns_db if c["id"] == campaign_id), None)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    
    campaign["status"] = CampaignStatus.PAUSED
    campaign["updated_at"] = datetime.now()
    return campaign


@router.get("/{campaign_id}/performance", response_model=dict)
async def get_campaign_performance(campaign_id: int, period: str = Query("7d", regex="^(24h|7d|30d)$")):
    """
    Get detailed performance data for a campaign over time.
    Returns empty data - no mock values.
    """
    campaign = next((c for c in campaigns_db if c["id"] == campaign_id), None)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    
    return {
        "campaign_id": campaign_id,
        "period": period,
        "data": []
    }

"""
Model initialization and exports.

This module exports all Pydantic models for the StreamlineHub platform,
providing MongoDB document models for users, customers, and campaigns.
"""

# Import all Pydantic models
# from .user import (
#     User, UserCreate, UserUpdate, UserResponse, UserLogin, 
#     UserRegister, UserPasswordUpdate, UserStats, UserActivity,
#     UserRole, UserTheme
# )
from .customer import (
    Customer, CustomerCreate, CustomerUpdate, CustomerResponse,
    CustomerStats, CustomerAnalytics, CustomerSegment, CustomerEvent,
    CustomerStatus, LifecycleStage, Gender
)
from .campaign import (
    Campaign, CampaignCreate, CampaignUpdate, CampaignResponse,
    CampaignStats, CampaignPerformance, CampaignAudience, CampaignTemplate,
    CampaignStatus, CampaignType, CampaignPriority
)

# Export all models for easy importing
__all__ = [
    # User models
    "User", "UserCreate", "UserUpdate", "UserResponse", "UserLogin",
    "UserRegister", "UserPasswordUpdate", "UserStats", "UserActivity",
    "UserRole", "UserTheme",
    
    # Customer models
    "Customer", "CustomerCreate", "CustomerUpdate", "CustomerResponse",
    "CustomerStats", "CustomerAnalytics", "CustomerSegment", "CustomerEvent",
    "CustomerStatus", "LifecycleStage", "Gender",
    
    # Campaign models
    "Campaign", "CampaignCreate", "CampaignUpdate", "CampaignResponse",
    "CampaignStats", "CampaignPerformance", "CampaignAudience", "CampaignTemplate",
    "CampaignStatus", "CampaignType", "CampaignPriority"
]

# Model registry for dynamic access
MODEL_REGISTRY = {
    # "user": User,
    "customer": Customer,
    "customer_event": CustomerEvent,
    "campaign": Campaign,
    "campaign_audience": CampaignAudience,
    "campaign_template": CampaignTemplate,
    "customer_segment": CustomerSegment,
}


def get_model(model_name: str):
    """
    Get model class by name.
    
    Args:
        model_name: Name of the model
        
    Returns:
        Model class or None if not found
    """
    return MODEL_REGISTRY.get(model_name.lower())


def get_all_models():
    """
    Get all registered model classes.
    
    Returns:
        List of all model classes
    """
    return list(MODEL_REGISTRY.values())


def get_model_names():
    """
    Get all registered model names.
    
    Returns:
        List of all model names
    """
    return list(MODEL_REGISTRY.keys())
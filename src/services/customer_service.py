"""
Customer Service

Production-grade service for customer data operations with MongoDB integration.
Handles customer CRUD operations, analytics queries, and business logic.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from decimal import Decimal

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import DESCENDING, ASCENDING
from bson import ObjectId
import structlog

from src.core.database import get_database
from src.models.customer import Customer, CustomerStatus, LifecycleStage

logger = structlog.get_logger()


class CustomerService:
    """Production customer data service with MongoDB operations."""
    
    def __init__(self):
        self.collection_name = "customers"
    
    async def get_customers(
        self, 
        limit: int = 50, 
        offset: int = 0, 
        search: Optional[str] = None,
        status: Optional[CustomerStatus] = None,
        lifecycle_stage: Optional[LifecycleStage] = None
    ) -> Dict[str, Any]:
        """
        Get customers with filtering, pagination, and search capabilities.
        
        Args:
            limit: Maximum number of customers to return
            offset: Number of customers to skip
            search: Search term for name or email
            status: Filter by customer status
            lifecycle_stage: Filter by lifecycle stage
            
        Returns:
            Dict containing customers list, total count, and metadata
        """
        try:
            database = get_database()
            collection = database[self.collection_name]
            
            # Build query filter
            query_filter = {}
            
            if search:
                query_filter["$or"] = [
                    {"name": {"$regex": search, "$options": "i"}},
                    {"email": {"$regex": search, "$options": "i"}}
                ]
            
            if status:
                query_filter["status"] = status.value
                
            if lifecycle_stage:
                query_filter["lifecycle_stage"] = lifecycle_stage.value
            
            # Get total count for pagination
            total_customers = await collection.count_documents(query_filter)
            
            # Get paginated results
            cursor = collection.find(query_filter).skip(offset).limit(limit)
            customers = []
            
            async for document in cursor:
                customer_data = {
                    "id": str(document["_id"]),
                    "name": document.get("name", ""),
                    "email": document.get("email", ""),
                    "status": document.get("status", "active"),
                    "lifecycle_stage": document.get("lifecycle_stage", "prospect"),
                    "total_orders": document.get("total_orders", 0),
                    "total_spent": float(document.get("total_spent", 0.0)),
                    "last_order_date": document.get("last_order_date"),
                    "created_at": document.get("created_at"),
                    "updated_at": document.get("updated_at")
                }
                customers.append(customer_data)
            
            return {
                "customers": customers,
                "total": total_customers,
                "limit": limit,
                "offset": offset,
                "has_next": offset + limit < total_customers,
                "has_previous": offset > 0
            }
            
        except Exception as e:
            logger.error("Failed to get customers", error=str(e))
            raise
    
    async def get_customer_by_id(self, customer_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific customer by ID.
        
        Args:
            customer_id: Customer ObjectId or string ID
            
        Returns:
            Customer data dictionary or None if not found
        """
        try:
            database = get_database()
            collection = database[self.collection_name]
            
            # Handle both ObjectId and string IDs
            if isinstance(customer_id, str) and ObjectId.is_valid(customer_id):
                query = {"_id": ObjectId(customer_id)}
            else:
                query = {"customer_id": customer_id}
            
            document = await collection.find_one(query)
            
            if not document:
                return None
            
            return {
                "id": str(document["_id"]),
                "customer_id": document.get("customer_id"),
                "name": document.get("name", ""),
                "email": document.get("email", ""),
                "status": document.get("status", "active"),
                "lifecycle_stage": document.get("lifecycle_stage", "prospect"),
                "total_orders": document.get("total_orders", 0),
                "total_spent": float(document.get("total_spent", 0.0)),
                "last_order_date": document.get("last_order_date"),
                "created_at": document.get("created_at"),
                "updated_at": document.get("updated_at"),
                "profile": document.get("profile", {}),
                "behavioral_data": document.get("behavioral_data", {}),
                "segments": document.get("segments", [])
            }
            
        except Exception as e:
            logger.error("Failed to get customer by ID", customer_id=customer_id, error=str(e))
            raise
    
    async def get_customer_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive customer statistics and KPIs.
        
        Returns:
            Dictionary containing various customer metrics and statistics
        """
        try:
            database = get_database()
            collection = database[self.collection_name]
            
            # Pipeline for comprehensive statistics
            pipeline = [
                {
                    "$group": {
                        "_id": None,
                        "total_customers": {"$sum": 1},
                        "active_customers": {
                            "$sum": {"$cond": [{"$eq": ["$status", "active"]}, 1, 0]}
                        },
                        "churned_customers": {
                            "$sum": {"$cond": [{"$eq": ["$status", "churned"]}, 1, 0]}
                        },
                        "total_revenue": {"$sum": "$total_spent"},
                        "avg_order_value": {"$avg": "$total_spent"},
                        "total_orders": {"$sum": "$total_orders"}
                    }
                }
            ]
            
            result = await collection.aggregate(pipeline).to_list(1)
            
            if not result:
                return {
                    "total_customers": 0,
                    "active_customers": 0,
                    "churned_customers": 0,
                    "total_revenue": 0.0,
                    "avg_order_value": 0.0,
                    "total_orders": 0,
                    "churn_rate": 0.0,
                    "activity_rate": 0.0
                }
            
            stats = result[0]
            total = stats["total_customers"]
            
            # Calculate rates
            churn_rate = (stats["churned_customers"] / total * 100) if total > 0 else 0
            activity_rate = (stats["active_customers"] / total * 100) if total > 0 else 0
            
            return {
                "total_customers": stats["total_customers"],
                "active_customers": stats["active_customers"],
                "churned_customers": stats["churned_customers"],
                "total_revenue": float(stats["total_revenue"]),
                "avg_order_value": float(stats["avg_order_value"]) if stats["avg_order_value"] else 0.0,
                "total_orders": stats["total_orders"],
                "churn_rate": round(churn_rate, 2),
                "activity_rate": round(activity_rate, 2)
            }
            
        except Exception as e:
            logger.error("Failed to get customer statistics", error=str(e))
            raise
    
    async def get_customer_segments(self) -> Dict[str, Any]:
        """
        Get customer segmentation data.
        
        Returns:
            Customer segments with counts and percentages
        """
        try:
            database = get_database()
            collection = database[self.collection_name]
            
            # Lifecycle stage distribution
            lifecycle_pipeline = [
                {"$group": {"_id": "$lifecycle_stage", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            
            lifecycle_results = await collection.aggregate(lifecycle_pipeline).to_list(None)
            
            # Status distribution  
            status_pipeline = [
                {"$group": {"_id": "$status", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            
            status_results = await collection.aggregate(status_pipeline).to_list(None)
            
            # Calculate total for percentages
            total_customers = await collection.count_documents({})
            
            return {
                "lifecycle_stages": [
                    {
                        "stage": result["_id"] or "unknown",
                        "count": result["count"],
                        "percentage": round((result["count"] / total_customers * 100), 2) if total_customers > 0 else 0
                    }
                    for result in lifecycle_results
                ],
                "status_distribution": [
                    {
                        "status": result["_id"] or "unknown", 
                        "count": result["count"],
                        "percentage": round((result["count"] / total_customers * 100), 2) if total_customers > 0 else 0
                    }
                    for result in status_results
                ],
                "total_customers": total_customers
            }
            
        except Exception as e:
            logger.error("Failed to get customer segments", error=str(e))
            raise


# Global service instance
customer_service = CustomerService()

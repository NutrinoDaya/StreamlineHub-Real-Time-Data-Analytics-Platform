"""
Analytics Service

Production-grade service for analytics data operations with MongoDB integration.
Handles real-time metrics, historical analytics, and business intelligence queries.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from decimal import Decimal

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import DESCENDING, ASCENDING
from bson import ObjectId
import structlog

from src.core.database import get_database

logger = structlog.get_logger()


class AnalyticsService:
    """Production analytics service with MongoDB operations."""
    
    def __init__(self):
        self.transactions_collection = "transactions"
        self.events_collection = "events"
        self.customers_collection = "customers"
        self.products_collection = "products"
        self.campaigns_collection = "campaigns"
    
    async def get_realtime_metrics(self) -> Dict[str, Any]:
        """
        Get real-time analytics metrics from MongoDB.
        
        Returns:
            Dictionary containing current real-time metrics
        """
        try:
            database = get_database()
            
            # Get metrics from the last 5 minutes
            five_minutes_ago = datetime.utcnow() - timedelta(minutes=5)
            
            # Recent events count
            events_collection = database[self.events_collection]
            recent_events = await events_collection.count_documents({
                "timestamp": {"$gte": five_minutes_ago}
            })
            
            # Recent transactions
            transactions_collection = database[self.transactions_collection]
            recent_transactions_cursor = transactions_collection.find({
                "created_at": {"$gte": five_minutes_ago}
            })
            
            total_value = 0.0
            unique_customers = set()
            transaction_count = 0
            
            async for transaction in recent_transactions_cursor:
                transaction_count += 1
                total_value += float(transaction.get("amount", 0))
                customer_id = transaction.get("customer_id")
                if customer_id:
                    unique_customers.add(customer_id)
            
            # Calculate events per second
            events_per_second = recent_events / 300 if recent_events > 0 else 0.0
            
            return {
                "status": "active" if recent_events > 0 else "no_data",
                "message": "Real-time metrics available" if recent_events > 0 else "No recent activity",
                "data": {
                    "total_events": recent_events,
                    "events_per_second": round(events_per_second, 2),
                    "unique_customers": len(unique_customers),
                    "total_value": round(total_value, 2),
                    "transaction_count": transaction_count,
                    "data_source": "mongodb_realtime",
                    "timestamp": datetime.utcnow().isoformat()
                }
            }
            
        except Exception as e:
            logger.error("Failed to get realtime metrics", error=str(e))
            return {
                "status": "error",
                "message": "Failed to retrieve real-time metrics",
                "data": {
                    "total_events": 0,
                    "events_per_second": 0.0,
                    "unique_customers": 0,
                    "total_value": 0.0,
                    "data_source": "mongodb_realtime",
                    "timestamp": datetime.utcnow().isoformat()
                }
            }
    
    async def get_dashboard_summary(self) -> Dict[str, Any]:
        """
        Get dashboard summary metrics.
        
        Returns:
            Dictionary containing key dashboard metrics
        """
        try:
            database = get_database()
            
            # Get data for last 24 hours
            twenty_four_hours_ago = datetime.utcnow() - timedelta(hours=24)
            
            # Transaction metrics
            transactions_collection = database[self.transactions_collection]
            transactions_pipeline = [
                {"$match": {"created_at": {"$gte": twenty_four_hours_ago}}},
                {
                    "$group": {
                        "_id": None,
                        "total_transactions": {"$sum": 1},
                        "total_revenue": {"$sum": "$amount"},
                        "avg_transaction": {"$avg": "$amount"},
                        "unique_customers": {"$addToSet": "$customer_id"}
                    }
                }
            ]
            
            transaction_results = await transactions_collection.aggregate(transactions_pipeline).to_list(1)
            
            # Event metrics
            events_collection = database[self.events_collection]
            total_events = await events_collection.count_documents({
                "timestamp": {"$gte": twenty_four_hours_ago}
            })
            
            # Customer metrics
            customers_collection = database[self.customers_collection]
            active_customers = await customers_collection.count_documents({
                "status": "active",
                "last_activity": {"$gte": twenty_four_hours_ago}
            })
            
            if transaction_results:
                tx_data = transaction_results[0]
                return {
                    "total_transactions": tx_data["total_transactions"],
                    "total_revenue": round(float(tx_data["total_revenue"]), 2),
                    "avg_transaction_value": round(float(tx_data["avg_transaction"]), 2),
                    "unique_customers": len(tx_data["unique_customers"]),
                    "total_events": total_events,
                    "active_customers": active_customers,
                    "period": "24h",
                    "last_updated": datetime.utcnow().isoformat()
                }
            else:
                return {
                    "total_transactions": 0,
                    "total_revenue": 0.0,
                    "avg_transaction_value": 0.0,
                    "unique_customers": 0,
                    "total_events": total_events,
                    "active_customers": active_customers,
                    "period": "24h",
                    "last_updated": datetime.utcnow().isoformat()
                }
            
        except Exception as e:
            logger.error("Failed to get dashboard summary", error=str(e))
            raise
    
    async def get_pipeline_status(self) -> Dict[str, Any]:
        """
        Get data pipeline status from system metrics.
        
        Returns:
            Dictionary containing pipeline component statuses
        """
        try:
            # Check recent data ingestion activity
            database = get_database()
            five_minutes_ago = datetime.utcnow() - timedelta(minutes=5)
            
            # Check if data is being processed recently
            events_collection = database[self.events_collection]
            recent_activity = await events_collection.count_documents({
                "timestamp": {"$gte": five_minutes_ago}
            })
            
            kafka_status = "active" if recent_activity > 0 else "inactive"
            spark_status = "active" if recent_activity > 10 else "inactive"
            
            return {
                "status": "running" if recent_activity > 0 else "stopped",
                "components": {
                    "kafka_consumer": kafka_status,
                    "spark_streaming": spark_status,
                    "metrics_writer": kafka_status
                },
                "last_updated": datetime.utcnow().isoformat(),
                "health_score": 100 if recent_activity > 10 else 50 if recent_activity > 0 else 0,
                "recent_activity_count": recent_activity
            }
            
        except Exception as e:
            logger.error("Failed to get pipeline status", error=str(e))
            return {
                "status": "error",
                "components": {
                    "kafka_consumer": "error",
                    "spark_streaming": "error",
                    "metrics_writer": "error"
                },
                "last_updated": datetime.utcnow().isoformat(),
                "health_score": 0,
                "error": str(e)
            }
    
    async def get_historical_analytics(
        self, 
        days: int = 30,
        metric_type: str = "revenue"
    ) -> Dict[str, Any]:
        """
        Get historical analytics data.
        
        Args:
            days: Number of days to look back
            metric_type: Type of metric to analyze (revenue, transactions, customers)
            
        Returns:
            Dictionary containing historical analytics data
        """
        try:
            database = get_database()
            start_date = datetime.utcnow() - timedelta(days=days)
            
            transactions_collection = database[self.transactions_collection]
            
            # Build aggregation pipeline based on metric type
            if metric_type == "revenue":
                pipeline = [
                    {"$match": {"created_at": {"$gte": start_date}}},
                    {
                        "$group": {
                            "_id": {
                                "year": {"$year": "$created_at"},
                                "month": {"$month": "$created_at"},
                                "day": {"$dayOfMonth": "$created_at"}
                            },
                            "total_revenue": {"$sum": "$amount"},
                            "transaction_count": {"$sum": 1}
                        }
                    },
                    {"$sort": {"_id.year": 1, "_id.month": 1, "_id.day": 1}}
                ]
            else:
                pipeline = [
                    {"$match": {"created_at": {"$gte": start_date}}},
                    {
                        "$group": {
                            "_id": {
                                "year": {"$year": "$created_at"},
                                "month": {"$month": "$created_at"},
                                "day": {"$dayOfMonth": "$created_at"}
                            },
                            "transaction_count": {"$sum": 1},
                            "unique_customers": {"$addToSet": "$customer_id"}
                        }
                    },
                    {"$sort": {"_id.year": 1, "_id.month": 1, "_id.day": 1}}
                ]
            
            results = await transactions_collection.aggregate(pipeline).to_list(None)
            
            # Format results
            historical_data = []
            for result in results:
                date_obj = datetime(
                    result["_id"]["year"],
                    result["_id"]["month"], 
                    result["_id"]["day"]
                )
                
                data_point = {
                    "date": date_obj.strftime("%Y-%m-%d"),
                    "timestamp": date_obj.isoformat()
                }
                
                if metric_type == "revenue":
                    data_point["revenue"] = round(float(result["total_revenue"]), 2)
                    data_point["transactions"] = result["transaction_count"]
                else:
                    data_point["transactions"] = result["transaction_count"]
                    data_point["unique_customers"] = len(result.get("unique_customers", []))
                
                historical_data.append(data_point)
            
            return {
                "metric_type": metric_type,
                "period_days": days,
                "data": historical_data,
                "total_points": len(historical_data),
                "generated_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error("Failed to get historical analytics", error=str(e))
            raise
    
    async def get_top_products(self, limit: int = 10) -> Dict[str, Any]:
        """Get top performing products."""
        try:
            database = get_database()
            transactions_collection = database[self.transactions_collection]
            
            # Get top products by revenue in last 30 days
            thirty_days_ago = datetime.utcnow() - timedelta(days=30)
            
            pipeline = [
                {"$match": {"created_at": {"$gte": thirty_days_ago}}},
                {
                    "$group": {
                        "_id": "$product_id",
                        "total_revenue": {"$sum": "$amount"},
                        "total_orders": {"$sum": 1},
                        "avg_order_value": {"$avg": "$amount"}
                    }
                },
                {"$sort": {"total_revenue": -1}},
                {"$limit": limit}
            ]
            
            results = await transactions_collection.aggregate(pipeline).to_list(None)
            
            # Get product details
            products_collection = database[self.products_collection]
            top_products = []
            
            for result in results:
                product_id = result["_id"]
                product_info = await products_collection.find_one({"_id": ObjectId(product_id)})
                
                product_data = {
                    "product_id": str(product_id),
                    "name": product_info.get("name", f"Product {product_id}") if product_info else f"Product {product_id}",
                    "category": product_info.get("category", "Unknown") if product_info else "Unknown",
                    "total_revenue": round(float(result["total_revenue"]), 2),
                    "total_orders": result["total_orders"],
                    "avg_order_value": round(float(result["avg_order_value"]), 2)
                }
                top_products.append(product_data)
            
            return {
                "products": top_products,
                "period": "30_days",
                "limit": limit,
                "generated_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error("Failed to get top products", error=str(e))
            raise


# Global service instance
analytics_service = AnalyticsService()

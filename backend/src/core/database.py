"""
MongoDB database connection and management.

This module provides MongoDB connection management, session handling,
and database operations using Motor (async MongoDB driver).
"""

import logging
from typing import Any, Dict, Optional
from datetime import datetime

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
import pymongo
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

from src.core.config import get_settings
import structlog

logger = structlog.get_logger()

# Global MongoDB client and database instances
mongodb_client: Optional[AsyncIOMotorClient] = None
mongodb_database: Optional[AsyncIOMotorDatabase] = None


class MongoDBManager:
    """
    MongoDB connection manager with async support.
    
    Provides centralized MongoDB connection management, health checks,
    and database operations for the StreamlineHub platform.
    """

    def __init__(self):
        self.client: Optional[AsyncIOMotorClient] = None
        self.database: Optional[AsyncIOMotorDatabase] = None
        self.settings = get_settings()

    async def connect(self) -> None:
        """
        Establish connection to MongoDB.
        
        Creates async MongoDB client and database instances.
        """
        try:
            # Create MongoDB client with connection options
            self.client = AsyncIOMotorClient(
                self.settings.mongodb_url,
                maxPoolSize=self.settings.mongodb_max_connections,
                minPoolSize=self.settings.mongodb_min_connections,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
                socketTimeoutMS=10000,
                retryWrites=True,
                retryReads=True
            )
            
            # Get database instance
            self.database = self.client[self.settings.mongodb_database]
            
            # Test connection
            await self.client.admin.command('ping')
            
            logger.info(
                f"MongoDB connected successfully - Database: {self.settings.mongodb_database}"
            )
            
            # Set global instances
            global mongodb_client, mongodb_database
            mongodb_client = self.client
            mongodb_database = self.database

        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"MongoDB connection failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error connecting to MongoDB: {e}")
            raise

    async def disconnect(self) -> None:
        """
        Close MongoDB connection.
        """
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
            
            # Clear global instances
            global mongodb_client, mongodb_database
            mongodb_client = None
            mongodb_database = None

    async def health_check(self) -> Dict[str, Any]:
        """
        Perform MongoDB health check.
        
        Returns:
            Dict containing connection status and server info
        """
        try:
            if self.client is None or self.database is None:
                return {
                    "status": "disconnected",
                    "error": "No active MongoDB connection"
                }
                
            # Ping the database
            ping_result = await self.client.admin.command('ping')
            
            # Get server status
            server_status = await self.client.admin.command('serverStatus')
            
            return {
                "status": "connected",
                "database": self.database.name,
                "server_version": server_status.get("version"),
                "uptime": server_status.get("uptime"),
                "connections": server_status.get("connections"),
                "ping": ping_result.get("ok") == 1
            }
        except Exception as e:
            logger.error(f"MongoDB health check failed: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

    async def create_indexes(self) -> None:
        """
        Create database indexes for optimal performance.
        """
        try:
            # Users collection indexes
            users_collection = self.database.users
            await users_collection.create_index("email", unique=True)
            await users_collection.create_index("is_active")
            await users_collection.create_index("created_at")
            
            # Customers collection indexes
            customers_collection = self.database.customers
            await customers_collection.create_index("customer_id", unique=True)
            await customers_collection.create_index("email")
            await customers_collection.create_index("status")
            await customers_collection.create_index("customer_segment")
            await customers_collection.create_index("created_at")
            
            # Campaigns collection indexes
            campaigns_collection = self.database.campaigns
            await campaigns_collection.create_index("campaign_id", unique=True)
            await campaigns_collection.create_index("status")
            await campaigns_collection.create_index("campaign_type")
            await campaigns_collection.create_index([("start_date", 1), ("end_date", 1)])
            await campaigns_collection.create_index("created_at")
            
            logger.info("Database indexes created successfully")
        except Exception as e:
            logger.error(f"Failed to create indexes: {e}")
            raise

    async def get_collection_stats(self, collection_name: str) -> Dict[str, Any]:
        """
        Get statistics for a specific collection.
        
        Args:
            collection_name: Name of the collection
            
        Returns:
            Dict containing collection statistics
        """
        try:
            collection = self.database[collection_name]
            stats = await self.database.command("collStats", collection_name)
            
            return {
                "name": collection_name,
                "count": stats.get("count", 0),
                "size": stats.get("size", 0),
                "avg_obj_size": stats.get("avgObjSize", 0),
                "storage_size": stats.get("storageSize", 0),
                "indexes": stats.get("nindexes", 0),
                "total_index_size": stats.get("totalIndexSize", 0)
            }
        except Exception as e:
            logger.error(f"Failed to get collection stats for {collection_name}: {e}")
            return {"error": str(e)}


# Global database manager instance
db_manager = MongoDBManager()


async def init_database() -> None:
    """
    Initialize MongoDB connection and setup.
    
    This function should be called on application startup.
    """
    await db_manager.connect()
    await db_manager.create_indexes()


async def close_database() -> None:
    """
    Close MongoDB connection.
    
    This function should be called on application shutdown.
    """
    await db_manager.disconnect()


def get_database() -> AsyncIOMotorDatabase:
    """
    Get the MongoDB database instance.
    
    Returns:
        AsyncIOMotorDatabase instance
        
    Raises:
        RuntimeError: If database is not initialized
    """
    if mongodb_database is None:
        raise RuntimeError(
            "MongoDB database not initialized. Call init_database() first."
        )
    return mongodb_database


def get_collection(collection_name: str):
    """
    Get a MongoDB collection.
    
    Args:
        collection_name: Name of the collection
        
    Returns:
        MongoDB collection instance
    """
    database = get_database()
    return database[collection_name]


# Convenience functions for common collections
def get_users_collection():
    """Get users collection."""
    return get_collection("users")


def get_customers_collection():
    """Get customers collection."""
    return get_collection("customers")


def get_campaigns_collection():
    """Get campaigns collection."""
    return get_collection("campaigns")


# Health check function for API endpoints
async def mongodb_health_check() -> Dict[str, Any]:
    """
    MongoDB health check for API endpoints.
    
    Returns:
        Dict containing health status
    """
    return await db_manager.health_check()

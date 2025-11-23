"""
Redis cache connection and management.

This module provides Redis connection setup, caching utilities,
and session storage for the StreamLineHub Analytics application.
"""

import json
import pickle
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, List, Optional, Union
from datetime import datetime, timedelta

import redis.asyncio as redis
import structlog
from redis.asyncio import Redis, ConnectionPool

from src.core.config import get_settings


logger = structlog.get_logger()

# Global Redis connection pool and client
redis_pool: Optional[ConnectionPool] = None
redis_client: Optional[Redis] = None


async def init_cache() -> None:
    """
    Initialize Redis connection pool and client.
    
    Sets up connection pooling for efficient Redis usage across
    the application with proper error handling and monitoring.
    Continues with application even if Redis is unavailable.
    """
    global redis_pool, redis_client
    
    settings = get_settings()
    
    try:
        # Create connection pool
        redis_pool = ConnectionPool.from_url(
            str(settings.redis_url),
            db=settings.redis_db,
            max_connections=settings.redis_max_connections,
            retry_on_timeout=True,
            socket_keepalive=True,
            socket_keepalive_options={},
            health_check_interval=30,
        )
        
        # Create Redis client
        redis_client = Redis(
            connection_pool=redis_pool,
            decode_responses=False,  # We'll handle encoding/decoding manually
        )
        
        # Test connection
        await redis_client.ping()
        
        logger.info(
            "Redis cache initialized",
            url=str(settings.redis_url).split("@")[-1],  # Log without credentials
            db=settings.redis_db,
            max_connections=settings.redis_max_connections,
        )
        
    except Exception as e:
        logger.warning(f"Redis cache unavailable, continuing without caching: {e}")
        redis_client = None
        redis_pool = None
        # Don't raise - allow application to continue without cache


async def close_cache_connections() -> None:
    """Close Redis connections and cleanup resources."""
    global redis_pool, redis_client
    
    if redis_client:
        await redis_client.close()
        logger.info("Redis client closed")
    
    if redis_pool:
        await redis_pool.disconnect()
        logger.info("Redis connection pool closed")


class CacheManager:
    """
    Redis cache manager with advanced caching operations.
    
    Provides high-level caching operations including serialization,
    expiration, pattern matching, and cache invalidation.
    """
    
    def __init__(self):
        # Don't bind to the global redis_client at construction time; it may
        # be initialized later during app startup. Use the `client` property
        # to access the current global redis client.
        self.default_ttl = 3600  # 1 hour default TTL
        # NOTE: `client` is provided as a property to read the global redis_client
    @property
    def client(self) -> Optional[Redis]:
        """Return the current global Redis client instance.

        The global `redis_client` variable may be updated when `init_cache`
        runs during application startup. Using a property ensures we always
        use the most recent client instance.
        """
        return redis_client
    
    async def get(
        self, 
        key: str, 
        default: Any = None,
        deserialize: bool = True
    ) -> Any:
        """
        Get value from cache with optional deserialization.
        
        Args:
            key: Cache key
            default: Default value if key not found
            deserialize: Whether to deserialize the value
            
        Returns:
            Cached value or default
        """
        try:
            if self.client is None:
                logger.debug(f"Redis client not available, returning default for key '{key}'")
                return default
                
            value = await self.client.get(key)
            if value is None:
                return default
            
            if deserialize:
                return self._deserialize(value)
            return value
            
        except Exception as e:
            logger.error(f"Cache get error for key '{key}': {e}")
            return default
    
    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        serialize: bool = True
    ) -> bool:
        """
        Set value in cache with optional serialization and TTL.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds (None for default)
            serialize: Whether to serialize the value
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if serialize:
                value = self._serialize(value)
            
            expire_time = ttl or self.default_ttl
            result = await self.client.setex(key, expire_time, value)
            return result
            
        except Exception as e:
            logger.error(f"Cache set error for key '{key}': {e}")
            return False
    
    async def delete(self, *keys: str) -> int:
        """
        Delete one or more keys from cache.
        
        Args:
            keys: Cache keys to delete
            
        Returns:
            Number of keys deleted
        """
        try:
            if not keys:
                return 0
            return await self.client.delete(*keys)
            
        except Exception as e:
            logger.error(f"Cache delete error for keys {keys}: {e}")
            return 0
    
    async def exists(self, *keys: str) -> int:
        """
        Check if keys exist in cache.
        
        Args:
            keys: Cache keys to check
            
        Returns:
            Number of existing keys
        """
        try:
            return await self.client.exists(*keys)
        except Exception as e:
            logger.error(f"Cache exists error for keys {keys}: {e}")
            return 0
    
    async def expire(self, key: str, ttl: int) -> bool:
        """
        Set expiration time for a key.
        
        Args:
            key: Cache key
            ttl: Time to live in seconds
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if self.client is None:
                logger.debug(f"Redis client not available, skipping expire for key '{key}'")
                return False
            return await self.client.expire(key, ttl)
        except Exception as e:
            logger.error(f"Cache expire error for key '{key}': {e}")
            return False
    
    async def increment(self, key: str, amount: int = 1) -> int:
        """
        Increment numeric value in cache.
        
        Args:
            key: Cache key
            amount: Amount to increment by
            
        Returns:
            New value after increment
        """
        try:
            if self.client is None:
                logger.warning(f"Redis client not available, skipping increment for key '{key}'")
                return amount  # Return the increment amount as fallback
            return await self.client.incrby(key, amount)
        except Exception as e:
            logger.error(f"Cache increment error for key '{key}': {e}")
            return amount  # Return the increment amount as fallback
    
    async def get_many(self, *keys: str) -> List[Any]:
        """
        Get multiple values from cache.
        
        Args:
            keys: Cache keys to retrieve
            
        Returns:
            List of values (None for missing keys)
        """
        try:
            values = await self.client.mget(*keys)
            return [self._deserialize(v) if v is not None else None for v in values]
        except Exception as e:
            logger.error(f"Cache get_many error for keys {keys}: {e}")
            return [None] * len(keys)
    
    async def set_many(
        self, 
        mapping: Dict[str, Any], 
        ttl: Optional[int] = None
    ) -> bool:
        """
        Set multiple key-value pairs in cache.
        
        Args:
            mapping: Dictionary of key-value pairs
            ttl: Time to live in seconds
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Serialize all values
            serialized_mapping = {
                k: self._serialize(v) for k, v in mapping.items()
            }
            
            # Use pipeline for atomic operation
            async with self.client.pipeline() as pipe:
                await pipe.mset(serialized_mapping)
                
                # Set expiration for all keys if TTL provided
                if ttl:
                    for key in mapping.keys():
                        await pipe.expire(key, ttl)
                
                await pipe.execute()
            
            return True
            
        except Exception as e:
            logger.error(f"Cache set_many error: {e}")
            return False
    
    async def delete_pattern(self, pattern: str) -> int:
        """
        Delete keys matching a pattern.
        
        Args:
            pattern: Redis pattern (e.g., "user:*", "session:*")
            
        Returns:
            Number of keys deleted
        """
        try:
            keys = await self.client.keys(pattern)
            if keys:
                return await self.client.delete(*keys)
            return 0
            
        except Exception as e:
            logger.error(f"Cache delete_pattern error for pattern '{pattern}': {e}")
            return 0
    
    async def get_keys_by_pattern(self, pattern: str) -> List[str]:
        """
        Get all keys matching a pattern.
        
        Args:
            pattern: Redis pattern
            
        Returns:
            List of matching keys
        """
        try:
            keys = await self.client.keys(pattern)
            return [key.decode('utf-8') if isinstance(key, bytes) else key for key in keys]
        except Exception as e:
            logger.error(f"Cache get_keys_by_pattern error for pattern '{pattern}': {e}")
            return []
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform cache health check.
        
        Returns:
            Health status information
        """
        try:
            # Check if client is initialized
            if self.client is None:
                return {
                    "status": "unhealthy",
                    "error": "Redis client not initialized"
                }
            
            # Test ping
            ping_result = await self.client.ping()
            
            # Get info
            info = await self.client.info()
            
            # Test set/get operation
            test_key = "health_check_test"
            test_value = datetime.now().isoformat()
            await self.set(test_key, test_value, ttl=10)
            retrieved_value = await self.get(test_key)
            await self.delete(test_key)
            
            return {
                "status": "healthy" if ping_result else "unhealthy",
                "ping": ping_result,
                "test_operation": retrieved_value == test_value,
                "info": {
                    "connected_clients": info.get("connected_clients"),
                    "used_memory": info.get("used_memory_human"),
                    "uptime": info.get("uptime_in_seconds"),
                    "version": info.get("redis_version"),
                }
            }
            
        except Exception as e:
            logger.error(f"Cache health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    def _serialize(self, value: Any) -> bytes:
        """
        Serialize value for cache storage.
        
        Uses JSON for simple types and pickle for complex objects.
        """
        if isinstance(value, (str, int, float, bool, type(None))):
            return json.dumps(value).encode('utf-8')
        else:
            return pickle.dumps(value)
    
    def _deserialize(self, value: bytes) -> Any:
        """
        Deserialize value from cache storage.
        
        Attempts JSON first, falls back to pickle.
        """
        try:
            return json.loads(value.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return pickle.loads(value)


class SessionManager:
    """
    Redis-based session management for user sessions.
    
    Handles user session storage, validation, and cleanup
    with automatic expiration and security features.
    """
    
    def __init__(self):
        self.cache = CacheManager()
        self.session_prefix = "session:"
        self.user_session_prefix = "user_sessions:"
        self.session_ttl = 30 * 60  # 30 minutes default
    
    async def create_session(
        self,
        session_id: str,
        user_id: int,
        user_data: Dict[str, Any],
        ttl: Optional[int] = None
    ) -> bool:
        """
        Create a new user session.
        
        Args:
            session_id: Unique session identifier
            user_id: User ID
            user_data: Session data to store
            ttl: Session expiration time in seconds
            
        Returns:
            True if successful, False otherwise
        """
        try:
            session_key = f"{self.session_prefix}{session_id}"
            user_sessions_key = f"{self.user_session_prefix}{user_id}"
            
            session_data = {
                "user_id": user_id,
                "created_at": datetime.now().isoformat(),
                "last_accessed": datetime.now().isoformat(),
                **user_data
            }
            
            expire_time = ttl or self.session_ttl
            
            # Use pipeline for atomic operation
            async with self.cache.client.pipeline() as pipe:
                # Store session data
                await pipe.setex(
                    session_key, 
                    expire_time, 
                    self.cache._serialize(session_data)
                )
                
                # Add session to user's session set
                await pipe.sadd(user_sessions_key, session_id)
                await pipe.expire(user_sessions_key, expire_time)
                
                await pipe.execute()
            
            return True
            
        except Exception as e:
            logger.error(f"Session creation error for session '{session_id}': {e}")
            return False
    
    async def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get session data by session ID.
        
        Args:
            session_id: Session identifier
            
        Returns:
            Session data or None if not found
        """
        session_key = f"{self.session_prefix}{session_id}"
        session_data = await self.cache.get(session_key)
        
        if session_data:
            # Update last accessed time
            session_data["last_accessed"] = datetime.now().isoformat()
            await self.cache.set(session_key, session_data)
        
        return session_data
    
    async def update_session(
        self,
        session_id: str,
        data: Dict[str, Any]
    ) -> bool:
        """
        Update session data.
        
        Args:
            session_id: Session identifier
            data: Data to update
            
        Returns:
            True if successful, False otherwise
        """
        session_key = f"{self.session_prefix}{session_id}"
        session_data = await self.cache.get(session_key)
        
        if session_data:
            session_data.update(data)
            session_data["last_accessed"] = datetime.now().isoformat()
            return await self.cache.set(session_key, session_data)
        
        return False
    
    async def delete_session(self, session_id: str) -> bool:
        """
        Delete a session.
        
        Args:
            session_id: Session identifier
            
        Returns:
            True if successful, False otherwise
        """
        try:
            session_key = f"{self.session_prefix}{session_id}"
            
            # Get session data to find user ID
            session_data = await self.cache.get(session_key)
            
            if session_data:
                user_id = session_data.get("user_id")
                user_sessions_key = f"{self.user_session_prefix}{user_id}"
                
                # Use pipeline for atomic operation
                async with self.cache.client.pipeline() as pipe:
                    await pipe.delete(session_key)
                    await pipe.srem(user_sessions_key, session_id)
                    await pipe.execute()
            else:
                await self.cache.delete(session_key)
            
            return True
            
        except Exception as e:
            logger.error(f"Session deletion error for session '{session_id}': {e}")
            return False
    
    async def get_user_sessions(self, user_id: int) -> List[str]:
        """
        Get all session IDs for a user.
        
        Args:
            user_id: User ID
            
        Returns:
            List of session IDs
        """
        try:
            user_sessions_key = f"{self.user_session_prefix}{user_id}"
            sessions = await self.cache.client.smembers(user_sessions_key)
            return [s.decode('utf-8') if isinstance(s, bytes) else s for s in sessions]
        except Exception as e:
            logger.error(f"Get user sessions error for user {user_id}: {e}")
            return []
    
    async def delete_user_sessions(self, user_id: int) -> int:
        """
        Delete all sessions for a user.
        
        Args:
            user_id: User ID
            
        Returns:
            Number of sessions deleted
        """
        sessions = await self.get_user_sessions(user_id)
        if not sessions:
            return 0
        
        try:
            session_keys = [f"{self.session_prefix}{sid}" for sid in sessions]
            user_sessions_key = f"{self.user_session_prefix}{user_id}"
            
            # Delete all session data and user session set
            deleted_count = await self.cache.delete(*session_keys, user_sessions_key)
            return deleted_count
            
        except Exception as e:
            logger.error(f"Delete user sessions error for user {user_id}: {e}")
            return 0


# Global cache and session managers
cache_manager = CacheManager()
session_manager = SessionManager()


# Dependency injection for FastAPI
def get_cache() -> CacheManager:
    """FastAPI dependency for cache manager."""
    return cache_manager


async def get_session_manager() -> SessionManager:
    """FastAPI dependency for session manager."""
    return session_manager


def get_redis_client() -> Optional[Redis]:
    """Get the global Redis client instance."""
    return redis_client
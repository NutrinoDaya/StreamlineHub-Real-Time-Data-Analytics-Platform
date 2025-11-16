"""
Data Buffer Module
Redis-based buffering system for high-throughput event processing
"""

import asyncio
import json
import redis.asyncio as redis
from typing import Dict, List, Any, Optional
from datetime import datetime
import structlog

logger = structlog.get_logger("data_buffer")

class RedisDataBuffer:
    """
    Redis-based event buffering system with automatic batch processing
    """
    
    def __init__(self, 
                 redis_url: str = "redis://redis:6379",
                 buffer_key: str = "etl:event_buffer",
                 batch_threshold: int = 500,
                 max_buffer_size: int = 5000):
        self.redis_url = redis_url
        self.buffer_key = buffer_key
        self.batch_threshold = batch_threshold
        self.max_buffer_size = max_buffer_size
        self.redis_client: Optional[redis.Redis] = None
        self.stats = {
            "events_added": 0,
            "events_processed": 0,
            "batches_created": 0,
            "errors": 0,
            "last_flush": None
        }
    
    async def initialize(self) -> bool:
        """Initialize Redis connection"""
        try:
            self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
            
            # Test connection
            await self.redis_client.ping()
            logger.info(f"✅ Redis buffer connected to {self.redis_url}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Redis: {e}")
            return False
    
    async def add_event(self, event: Dict[str, Any]) -> bool:
        """Add event to buffer"""
        try:
            if not self.redis_client:
                logger.error("Redis client not initialized")
                return False
            
            # Add timestamp and unique ID
            enriched_event = {
                **event,
                "buffer_timestamp": datetime.now().isoformat(),
                "event_id": f"evt_{datetime.now().timestamp()}_{id(event)}"
            }
            
            # Convert to JSON and add to Redis list
            event_json = json.dumps(enriched_event)
            await self.redis_client.lpush(self.buffer_key, event_json)
            
            self.stats["events_added"] += 1
            
            # Check if we need to flush
            current_size = await self.get_buffer_size()
            if current_size >= self.batch_threshold:
                logger.info(f"🔄 Buffer threshold reached ({current_size} events), triggering flush")
                return True
                
            return True
            
        except Exception as e:
            logger.error(f"Failed to add event to buffer: {e}")
            self.stats["errors"] += 1
            return False
    
    async def get_buffer_size(self) -> int:
        """Get current buffer size"""
        try:
            if not self.redis_client:
                return 0
            return await self.redis_client.llen(self.buffer_key)
        except Exception as e:
            logger.error(f"Failed to get buffer size: {e}")
            return 0
    
    async def flush_buffer(self, batch_size: Optional[int] = None) -> List[Dict[str, Any]]:
        """Flush events from buffer for processing"""
        try:
            if not self.redis_client:
                logger.error("Redis client not initialized")
                return []
            
            # Determine batch size
            if batch_size is None:
                batch_size = self.batch_threshold
            
            events = []
            
            # Get events using RPOP (FIFO order)
            for _ in range(batch_size):
                event_json = await self.redis_client.rpop(self.buffer_key)
                if not event_json:
                    break
                
                try:
                    event = json.loads(event_json)
                    events.append(event)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode event JSON: {e}")
                    self.stats["errors"] += 1
            
            if events:
                self.stats["events_processed"] += len(events)
                self.stats["batches_created"] += 1
                self.stats["last_flush"] = datetime.now().isoformat()
                logger.info(f"✅ Flushed {len(events)} events from buffer")
            
            return events
            
        except Exception as e:
            logger.error(f"Failed to flush buffer: {e}")
            self.stats["errors"] += 1
            return []
    
    async def clear_buffer(self) -> int:
        """Clear all events from buffer"""
        try:
            if not self.redis_client:
                return 0
            
            cleared_count = await self.redis_client.delete(self.buffer_key)
            logger.info(f"🗑️  Cleared {cleared_count} events from buffer")
            return cleared_count
            
        except Exception as e:
            logger.error(f"Failed to clear buffer: {e}")
            return 0
    
    def get_buffer_stats(self) -> Dict[str, Any]:
        """Get buffer statistics"""
        return {
            **self.stats.copy(),
            "buffer_key": self.buffer_key,
            "batch_threshold": self.batch_threshold,
            "max_buffer_size": self.max_buffer_size,
            "timestamp": datetime.now().isoformat()
        }
    
    async def get_detailed_status(self) -> Dict[str, Any]:
        """Get detailed buffer status including Redis info"""
        try:
            if not self.redis_client:
                return {"status": "disconnected"}
            
            current_size = await self.get_buffer_size()
            
            return {
                "status": "connected",
                "current_size": current_size,
                "utilization_percent": (current_size / self.max_buffer_size) * 100,
                "threshold_reached": current_size >= self.batch_threshold,
                "redis_url": self.redis_url,
                "stats": self.get_buffer_stats()
            }
            
        except Exception as e:
            logger.error(f"Failed to get detailed status: {e}")
            return {"status": "error", "message": str(e)}
    
    async def close(self):
        """Close Redis connection"""
        if self.redis_client:
            await self.redis_client.aclose()
            logger.info("🔌 Redis buffer connection closed")

# Global buffer instance
_buffer_instance: Optional[RedisDataBuffer] = None

def get_data_buffer(redis_url: str = "redis://localhost:16379") -> RedisDataBuffer:
    """Get or create global data buffer instance"""
    global _buffer_instance
    if _buffer_instance is None:
        _buffer_instance = RedisDataBuffer(redis_url=redis_url)
    return _buffer_instance

async def initialize_data_buffer(redis_url: str = "redis://localhost:16379") -> bool:
    """Initialize global data buffer"""
    buffer = get_data_buffer(redis_url)
    return await buffer.initialize()
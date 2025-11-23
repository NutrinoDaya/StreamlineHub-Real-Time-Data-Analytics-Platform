#!/usr/bin/env python3
"""
Real-time Metrics Calculator
Calculates real analytics metrics from Bronze/Silver layer data and Redis event streams
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import redis.asyncio as redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as spark_sum, when
from pathlib import Path

from src.core.spark_session import get_spark_session

logger = logging.getLogger(__name__)

class RealTimeMetricsCalculator:
    """
    Calculates real-time analytics metrics from actual data sources
    """
    
    def __init__(self, 
                redis_url: str = None,
                bronze_path: str = "/opt/StreamlineHub/data/bronze",
                silver_path: str = "/opt/StreamlineHub/data/silver"):
        self.redis_url = redis_url
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.redis_client: Optional[redis.Redis] = None
        self.spark: Optional[SparkSession] = None
    
    async def initialize(self) -> bool:
        """Initialize connections"""
        try:
            # Initialize Redis
            if self.redis_url:
                self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
                await self.redis_client.ping()
                logger.info("[SUCCESS]  Redis connection established for metrics calculation")
            
            # Initialize Spark for Bronze/Silver data queries
            self.spark = get_spark_session("RealTimeMetrics")
            logger.info("[SUCCESS]  Spark session initialized for metrics calculation")
            
            return True
            
        except Exception as e:
            logger.error(f"[ERROR]  Failed to initialize metrics calculator: {e}")
            return False
    
    async def calculate_real_metrics(self) -> Dict[str, Any]:
        """
        Calculate real-time metrics from actual data sources
        """
        try:
            # Get current timestamp and calculate time windows
            now = datetime.now()
            hour_ago = now - timedelta(hours=1)
            day_ago = now - timedelta(days=1)
            
            # Get active users from Redis (real-time)
            active_users = await self._get_active_users_from_redis()
            
            # Get conversion rate from Bronze/Silver data
            conversion_rate = await self._calculate_conversion_rate(hour_ago, now)
            
            # Get average session duration from customer behavior data
            avg_session_duration = await self._calculate_avg_session_duration(day_ago, now)
            
            # Get bounce rate from customer behavior data
            bounce_rate = await self._calculate_bounce_rate(day_ago, now)
            
            # Get revenue per minute from transaction data
            revenue_per_minute = await self._calculate_revenue_per_minute(hour_ago, now)
            
            # Get events per second from Redis metrics
            events_per_second = await self._get_events_per_second_from_redis()
            
            return {
                'timestamp': now.isoformat(),
                'active_users': active_users,
                'events_per_second': events_per_second,
                'revenue_per_minute': revenue_per_minute,
                'conversion_rate': conversion_rate,
                'avg_session_duration': avg_session_duration,
                'bounce_rate': bounce_rate
            }
            
        except Exception as e:
            logger.error(f"Error calculating real metrics: {e}")
            # Return safe defaults when calculation fails
            return {
                'timestamp': datetime.now().isoformat(),
                'active_users': 0,
                'events_per_second': 0.0,
                'revenue_per_minute': 0.0,
                'conversion_rate': 0.0,
                'avg_session_duration': 0.0,
                'bounce_rate': 0.0
            }
    
    async def _get_active_users_from_redis(self) -> int:
        """Get active users count from Redis"""
        if not self.redis_client:
            return 0
        try:
            # Check recent activity in Redis
            active_count = await self.redis_client.hget('metrics:events', 'active_users')
            return int(active_count) if active_count else 0
        except Exception:
            return 0
    
    async def _get_events_per_second_from_redis(self) -> float:
        """Get events per second from Redis"""
        if not self.redis_client:
            return 0.0
        try:
            events_rate = await self.redis_client.hget('metrics:events', 'events_per_second')
            return float(events_rate) if events_rate else 0.0
        except Exception:
            return 0.0
    
    async def _calculate_conversion_rate(self, start_time: datetime, end_time: datetime) -> float:
        """Calculate conversion rate from customer behavior data"""
        if not self.spark:
            return 0.0
        
        try:
            # Check if Bronze customer behavior data exists
            bronze_behavior_path = f"{self.bronze_path}/customer_behavior_delta"
            if not Path(bronze_behavior_path).exists():
                logger.warning("Bronze customer behavior data not found")
                return 0.0
            
            # Read Bronze customer behavior data
            df = self.spark.read.format("delta").load(bronze_behavior_path)
            
            # Filter by time range (convert datetime to milliseconds)
            start_ms = int(start_time.timestamp() * 1000)
            end_ms = int(end_time.timestamp() * 1000)
            
            recent_df = df.filter(
                (col("timestamp") >= start_ms) & 
                (col("timestamp") <= end_ms)
            )
            
            if recent_df.count() == 0:
                return 0.0
            
            # Calculate conversion rate: purchases / page_views
            metrics = recent_df.agg(
                spark_sum(when(col("action") == "page_view", 1).otherwise(0)).alias("page_views"),
                spark_sum(when(col("action") == "purchase", 1).otherwise(0)).alias("purchases")
            ).collect()[0]
            
            page_views = metrics["page_views"] or 0
            purchases = metrics["purchases"] or 0
            
            if page_views > 0:
                return round((purchases / page_views) * 100, 2)
            else:
                return 0.0
                
        except Exception as e:
            logger.error(f"Error calculating conversion rate: {e}")
            return 0.0
    
    async def _calculate_avg_session_duration(self, start_time: datetime, end_time: datetime) -> float:
        """Calculate average session duration from customer behavior data"""
        if not self.spark:
            return 0.0
        
        try:
            # Try to use Silver analytics data first (pre-calculated)
            silver_behavior_path = f"{self.silver_path}/customer_behavior_analytics_delta"
            if Path(silver_behavior_path).exists():
                df = self.spark.read.format("delta").load(silver_behavior_path)
                
                # Filter by time range
                start_ms = int(start_time.timestamp() * 1000)
                end_ms = int(end_time.timestamp() * 1000)
                
                recent_df = df.filter(
                    (col("window_start") >= start_ms) & 
                    (col("window_end") <= end_ms)
                )
                
                if recent_df.count() > 0:
                    avg_duration = recent_df.agg(avg("avg_session_duration")).collect()[0][0]
                    return float(avg_duration) if avg_duration else 0.0
            
            # Fallback: calculate from Bronze data
            bronze_behavior_path = f"{self.bronze_path}/customer_behavior_delta"
            if Path(bronze_behavior_path).exists():
                df = self.spark.read.format("delta").load(bronze_behavior_path)
                
                start_ms = int(start_time.timestamp() * 1000)
                end_ms = int(end_time.timestamp() * 1000)
                
                # Group by session and calculate duration
                session_durations = df.filter(
                    (col("timestamp") >= start_ms) & 
                    (col("timestamp") <= end_ms)
                ).groupBy("session_id").agg(
                    (col("timestamp").cast("long").alias("max_time") - col("timestamp").cast("long").alias("min_time")).alias("duration")
                )
                
                avg_duration = session_durations.agg(avg("duration")).collect()[0][0]
                # Convert from milliseconds to seconds
                return float(avg_duration / 1000) if avg_duration else 0.0
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Error calculating session duration: {e}")
            return 0.0
    
    async def _calculate_bounce_rate(self, start_time: datetime, end_time: datetime) -> float:
        """Calculate bounce rate from customer behavior data"""
        if not self.spark:
            return 0.0
        
        try:
            bronze_behavior_path = f"{self.bronze_path}/customer_behavior_delta"
            if not Path(bronze_behavior_path).exists():
                return 0.0
            
            df = self.spark.read.format("delta").load(bronze_behavior_path)
            
            start_ms = int(start_time.timestamp() * 1000)
            end_ms = int(end_time.timestamp() * 1000)
            
            # Calculate sessions with only one interaction (bounce)
            session_interactions = df.filter(
                (col("timestamp") >= start_ms) & 
                (col("timestamp") <= end_ms)
            ).groupBy("session_id").agg(
                count("action").alias("interaction_count")
            )
            
            total_sessions = session_interactions.count()
            if total_sessions == 0:
                return 0.0
            
            bounce_sessions = session_interactions.filter(col("interaction_count") == 1).count()
            
            return round((bounce_sessions / total_sessions) * 100, 2)
            
        except Exception as e:
            logger.error(f"Error calculating bounce rate: {e}")
            return 0.0
    
    async def _calculate_revenue_per_minute(self, start_time: datetime, end_time: datetime) -> float:
        """Calculate revenue per minute from transaction data"""
        if not self.spark:
            return 0.0
        
        try:
            bronze_transaction_path = f"{self.bronze_path}/transaction_completed_delta"
            if not Path(bronze_transaction_path).exists():
                return 0.0
            
            df = self.spark.read.format("delta").load(bronze_transaction_path)
            
            start_ms = int(start_time.timestamp() * 1000)
            end_ms = int(end_time.timestamp() * 1000)
            
            # Calculate total revenue in time window
            total_revenue = df.filter(
                (col("timestamp") >= start_ms) & 
                (col("timestamp") <= end_ms) &
                (col("status") == "completed")
            ).agg(spark_sum("amount")).collect()[0][0]
            
            if total_revenue:
                # Convert to revenue per minute
                duration_minutes = (end_time - start_time).total_seconds() / 60
                return float(total_revenue / duration_minutes) if duration_minutes > 0 else 0.0
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Error calculating revenue per minute: {e}")
            return 0.0
"""
Stream Processor Module
Handles real-time stream processing using Spark
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path
import structlog

logger = structlog.get_logger("stream_processor")

class StreamProcessor:
    """
    Real-time stream processor for continuous data processing
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.is_running = False
        self.spark_session = None
        
    async def initialize(self) -> bool:
        """Initialize stream processor"""
        try:
            logger.info("🌊 Initializing Stream Processor...")
            
            # Initialize Spark session for streaming
            await self._initialize_spark_session()
            
            self.is_running = True
            logger.info("✅ Stream Processor initialized")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Stream Processor: {e}")
            return False
    
    async def _initialize_spark_session(self):
        """Initialize Spark session for streaming"""
        try:
            # Import here to avoid dependency issues
            from pyspark.sql import SparkSession
            
            self.spark_session = SparkSession.builder \
                .appName("StreamlineHub-StreamProcessor") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
            
            logger.info("✅ Spark session initialized for streaming")
            
        except Exception as e:
            logger.warning(f"Spark session initialization failed: {e}")
            self.spark_session = None
    
    async def process_stream(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process stream of events"""
        try:
            if not self.spark_session:
                logger.warning("Spark session not available, using fallback processing")
                return await self._fallback_processing(events)
            
            # Convert events to Spark DataFrame
            df = self.spark_session.createDataFrame(events)
            
            # Apply stream transformations
            processed_df = self._apply_stream_transformations(df)
            
            # Collect results
            processed_events = processed_df.collect()
            
            return {
                "status": "success",
                "events_processed": len(processed_events),
                "processor": "spark_stream",
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Stream processing failed: {e}")
            return await self._fallback_processing(events)
    
    def _apply_stream_transformations(self, df):
        """Apply Spark transformations to streaming data"""
        try:
            # Add processing metadata
            from pyspark.sql.functions import current_timestamp, lit
            
            processed_df = df.withColumn("stream_processed_at", current_timestamp()) \
                            .withColumn("processor_version", lit("1.0.0"))
            
            return processed_df
            
        except Exception as e:
            logger.error(f"Stream transformation failed: {e}")
            return df
    
    async def _fallback_processing(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Fallback processing when Spark is not available"""
        try:
            processed_events = []
            for event in events:
                processed_event = {
                    **event,
                    "stream_processed_at": datetime.now().isoformat(),
                    "processor_version": "1.0.0_fallback"
                }
                processed_events.append(processed_event)
            
            return {
                "status": "success",
                "events_processed": len(processed_events),
                "processor": "fallback",
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Fallback processing failed: {e}")
            return {
                "status": "failed",
                "error": str(e),
                "events_processed": 0,
                "timestamp": datetime.now().isoformat()
            }
    
    async def shutdown(self):
        """Shutdown stream processor"""
        if self.spark_session:
            self.spark_session.stop()
        self.is_running = False
        logger.info("🔌 Stream Processor shutdown complete")
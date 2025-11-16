"""
ETL Processor Module
Main processing engine for data transformation and loading
"""

import asyncio
import time
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
from pathlib import Path
import structlog

logger = structlog.get_logger("etl_processor")

class ETLProcessor:
    """
    Main ETL processing engine coordinating data transformation and loading
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.is_running = False
        self.processing_stats = {
            "jobs_started": 0,
            "jobs_completed": 0,
            "jobs_failed": 0,
            "total_records_processed": 0,
            "last_job_time": None
        }
        
        # Processing callbacks
        self.preprocessors: List[Callable] = []
        self.processors: List[Callable] = []
        self.postprocessors: List[Callable] = []
        
        # Storage paths
        self.bronze_path = Path(config.get("bronze_layer_path", "data/bronze"))
        self.silver_path = Path(config.get("silver_layer_path", "data/silver")) 
        self.gold_path = Path(config.get("gold_layer_path", "data/gold"))
    
    async def initialize(self) -> bool:
        """Initialize ETL processor"""
        try:
            logger.info("⚙️  Initializing ETL Processor...")
            
            # Create storage directories
            self.bronze_path.mkdir(parents=True, exist_ok=True)
            self.silver_path.mkdir(parents=True, exist_ok=True)
            self.gold_path.mkdir(parents=True, exist_ok=True)
            
            # Register default processors
            self._register_default_processors()
            
            self.is_running = True
            logger.info("✅ ETL Processor initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize ETL processor: {e}")
            return False
    
    def _register_default_processors(self):
        """Register default data processors"""
        
        # Preprocessor: Add processing metadata
        def add_metadata(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            processed_events = []
            for event in events:
                enhanced_event = {
                    **event,
                    "etl_processed_at": datetime.now().isoformat(),
                    "etl_batch_id": f"batch_{int(time.time())}",
                    "etl_version": "1.0.0"
                }
                processed_events.append(enhanced_event)
            return processed_events
        
        # Processor: Data validation and transformation
        def validate_and_transform(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            valid_events = []
            for event in events:
                try:
                    # Basic validation
                    if not event.get("event_type"):
                        event["event_type"] = "unknown"
                    
                    # Ensure required fields
                    if "timestamp" not in event:
                        event["timestamp"] = datetime.now().isoformat()
                    
                    valid_events.append(event)
                    
                except Exception as e:
                    logger.warning(f"Event validation failed: {e}")
                    continue
            
            return valid_events
        
        # Postprocessor: Generate summary statistics
        def generate_stats(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            if events:
                event_types = {}
                for event in events:
                    event_type = event.get("event_type", "unknown")
                    event_types[event_type] = event_types.get(event_type, 0) + 1
                
                logger.info(f"📊 Processed {len(events)} events: {dict(event_types)}")
            
            return events
        
        self.preprocessors.append(add_metadata)
        self.processors.append(validate_and_transform)
        self.postprocessors.append(generate_stats)
        
        logger.info(f"📝 Registered {len(self.preprocessors + self.processors + self.postprocessors)} processors")
    
    async def process_events(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process a batch of events through the ETL pipeline"""
        try:
            start_time = time.time()
            job_id = f"job_{int(start_time)}"
            
            logger.info(f"🚀 Starting ETL job {job_id} with {len(events)} events")
            
            self.processing_stats["jobs_started"] += 1
            processed_events = events.copy()
            
            # Run preprocessors
            for preprocessor in self.preprocessors:
                processed_events = preprocessor(processed_events)
                logger.debug(f"Preprocessor completed: {len(processed_events)} events")
            
            # Run main processors
            for processor in self.processors:
                processed_events = processor(processed_events)
                logger.debug(f"Processor completed: {len(processed_events)} events")
            
            # Run postprocessors
            for postprocessor in self.postprocessors:
                processed_events = postprocessor(processed_events)
                logger.debug(f"Postprocessor completed: {len(processed_events)} events")
            
            # Store to Bronze layer
            bronze_result = await self._store_to_bronze(processed_events, job_id)
            
            # Update statistics
            processing_time = time.time() - start_time
            self.processing_stats["jobs_completed"] += 1
            self.processing_stats["total_records_processed"] += len(processed_events)
            self.processing_stats["last_job_time"] = datetime.now().isoformat()
            
            logger.info(f"✅ ETL job {job_id} completed in {processing_time:.2f}s")
            
            return {
                "job_id": job_id,
                "status": "success",
                "events_processed": len(processed_events),
                "processing_time_seconds": processing_time,
                "bronze_storage": bronze_result,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.processing_stats["jobs_failed"] += 1
            logger.error(f"❌ ETL job failed: {e}")
            
            return {
                "job_id": f"job_{int(time.time())}",
                "status": "failed",
                "error": str(e),
                "events_processed": 0,
                "timestamp": datetime.now().isoformat()
            }
    
    async def _store_to_bronze(self, events: List[Dict[str, Any]], job_id: str) -> Dict[str, Any]:
        """Store processed events to Bronze layer using Delta processor"""
        try:
            # Use Delta processor for Bronze layer storage
            from src.processing.delta_processor import DeltaProcessor
            
            # Create Delta processor instance
            delta_config = {
                "bronze_layer_path": str(self.bronze_path),
                "silver_layer_path": str(self.silver_path),
                "gold_layer_path": str(self.gold_path)
            }
            
            delta_processor = DeltaProcessor(delta_config)
            
            # Initialize if not already done
            if not hasattr(self, '_delta_processor') or not self._delta_processor:
                await delta_processor.initialize()
                self._delta_processor = delta_processor
            
            # Write to Bronze layer using Delta processor
            bronze_result = await self._delta_processor.write_to_bronze(events, job_id)
            
            return bronze_result
            
        except Exception as e:
            logger.error(f"Failed to store to Bronze layer: {e}")
            return {
                "status": "failed",
                "error": str(e),
                "total_events": len(events),
                "job_id": job_id
            }
    
    def add_preprocessor(self, processor: Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]):
        """Add a custom preprocessor"""
        self.preprocessors.append(processor)
        logger.info("Added custom preprocessor")
    
    def add_processor(self, processor: Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]):
        """Add a custom processor"""
        self.processors.append(processor)
        logger.info("Added custom processor")
    
    def add_postprocessor(self, processor: Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]):
        """Add a custom postprocessor"""
        self.postprocessors.append(processor)
        logger.info("Added custom postprocessor")
    
    def get_status(self) -> Dict[str, Any]:
        """Get processor status and statistics"""
        return {
            "status": "running" if self.is_running else "stopped",
            "config": self.config,
            "stats": self.processing_stats.copy(),
            "processors_registered": {
                "preprocessors": len(self.preprocessors),
                "processors": len(self.processors),
                "postprocessors": len(self.postprocessors)
            },
            "storage_paths": {
                "bronze": str(self.bronze_path),
                "silver": str(self.silver_path),
                "gold": str(self.gold_path)
            },
            "timestamp": datetime.now().isoformat()
        }
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get detailed processing statistics"""
        total_jobs = self.processing_stats["jobs_completed"] + self.processing_stats["jobs_failed"]
        success_rate = 0.0
        
        if total_jobs > 0:
            success_rate = (self.processing_stats["jobs_completed"] / total_jobs) * 100
        
        return {
            **self.processing_stats.copy(),
            "total_jobs": total_jobs,
            "success_rate_percent": success_rate,
            "timestamp": datetime.now().isoformat()
        }
    
    async def shutdown(self):
        """Shutdown ETL processor"""
        self.is_running = False
        logger.info("🔌 ETL Processor shutdown complete")

# Global ETL processor instance
_etl_processor_instance: Optional[ETLProcessor] = None

def get_etl_processor(config: Optional[Dict[str, Any]] = None) -> ETLProcessor:
    """Get or create global ETL processor instance"""
    global _etl_processor_instance
    if _etl_processor_instance is None:
        if config is None:
            config = {}
        _etl_processor_instance = ETLProcessor(config)
    return _etl_processor_instance
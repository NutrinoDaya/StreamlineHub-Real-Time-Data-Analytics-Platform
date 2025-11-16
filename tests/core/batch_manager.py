#!/usr/bin/env python
"""
Batch Manager

This module provides batch processing capabilities for ETL operations:
- Collects incoming data until batch size is reached
- Triggers batch processing when max_items threshold is met
- Manages concurrent batch processing
- Provides batch statistics and monitoring
"""

import time
import uuid
from collections import deque
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from threading import Lock, Event
from concurrent.futures import ThreadPoolExecutor

from utils import LoggerManager

logger = LoggerManager.get_logger("ETL_Processing.log")


@dataclass
class BatchConfig:
    """Batch processing configuration"""
    max_items: int = 400
    max_wait_time_seconds: int = 300  # 5 minutes
    max_concurrent_batches: int = 3
    enable_auto_processing: bool = True


@dataclass
class BatchInfo:
    """Information about a processing batch"""
    batch_id: str
    data_type: str
    items: list = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    status: str = "collecting"  # collecting, processing, completed, failed
    processing_start: Optional[datetime] = None
    processing_end: Optional[datetime] = None
    error_message: Optional[str] = None
    
    @property
    def item_count(self) -> int:
        return len(self.items)
    
    @property
    def age_seconds(self) -> float:
        return (datetime.now() - self.created_at).total_seconds()
    
    @property
    def processing_duration(self) -> Optional[timedelta]:
        if self.processing_start and self.processing_end:
            return self.processing_end - self.processing_start
        return None


class BatchManager:
    """
    batch manager for high-throughput data processing
    """
    
    def __init__(self, config: BatchConfig, processing_callback):
        self.config = config
        self.processing_callback = processing_callback
        
        # Batch storage by data type
        self.active_batches: Dict[str, BatchInfo] = {}
        self.completed_batches: deque = deque(maxlen=100)  # Keep last 100 batches for monitoring
        
        # Thread safety
        self.lock = Lock()
        self.shutdown_event = Event()
        
        # Batch processing executor
        self.executor = ThreadPoolExecutor(max_workers=self.config.max_concurrent_batches)
        
        # Statistics
        self.stats = {
            "total_items_processed": 0,
            "total_batches_processed": 0,
            "total_batches_failed": 0,
            "average_batch_size": 0,
            "average_processing_time": 0
        }
        
        # Auto-processing timer
        if self.config.enable_auto_processing:
            self._start_auto_processing_timer()
        
        logger.info(f"Batch Manager initialized - max_items: {self.config.max_items}, "
                   f"max_wait: {self.config.max_wait_time_seconds}s, "
                   f"max_concurrent: {self.config.max_concurrent_batches}")
    
    def add_item(self, data_type: str, item: Dict[str, Any]) -> Optional[str]:
        """
        Add an item to the appropriate batch buffer
        Items accumulate in buffer until EXACTLY max_items is reached
        Returns batch_id if a batch was triggered for processing
        
        Logic: Buffer accumulates items and only processes when reaching max_items.
        If receiving 10k items with max_items=50k, it will NOT process early.
        Processing only triggers when buffer size == max_items.
        """
        with self.lock:
            # Find or create active batch for this data type
            batch_key = f"{data_type}_current"
            
            if batch_key not in self.active_batches:
                batch_id = f"{data_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
                self.active_batches[batch_key] = BatchInfo(
                    batch_id=batch_id,
                    data_type=data_type
                )
                logger.debug(f"Created new batch buffer {batch_id} for {data_type}")
            
            batch = self.active_batches[batch_key]
            batch.items.append(item)
            
            # Log progress towards max_items threshold
            if batch.item_count % 1000 == 0:  # Log every 1000 items
                logger.debug(f"Batch buffer {batch.batch_id}: {batch.item_count}/{self.config.max_items} items accumulated")
            
            # ONLY trigger processing when EXACTLY reaching max_items
            # This prevents premature processing of partial batches
            if batch.item_count >= self.config.max_items:
                logger.info(f"Batch buffer {batch.batch_id} reached threshold: {batch.item_count}/{self.config.max_items} - triggering processing")
                return self._trigger_batch_processing(batch_key)
            
            return None
    
    def _trigger_batch_processing(self, batch_key: str) -> str:
        """
        Trigger processing for a specific batch
        Must be called within lock context to prevent race conditions
        Ensures no concurrent processing of same batch
        """
        if batch_key not in self.active_batches:
            logger.warning(f"Attempted to trigger processing for non-existent batch: {batch_key}")
            return None
        
        batch = self.active_batches.pop(batch_key)
        
        # Validate batch before processing
        if batch.status == "processing":
            logger.warning(f"Batch {batch.batch_id} is already being processed - skipping to prevent duplication")
            return None
        
        batch.status = "processing"
        batch.processing_start = datetime.now()
        
        logger.info(f"Triggering batch processing: {batch.batch_id} with {batch.item_count} items (threshold: {self.config.max_items})")
        
        # Submit to executor for async processing with error handling
        future = self.executor.submit(self._process_batch, batch)
        
        # Store future for monitoring (optional)
        # future.add_done_callback(lambda f: self._on_batch_complete(f, batch))
        
        return batch.batch_id
    
    def _process_batch(self, batch: BatchInfo) -> bool:
        """
        Process a batch of items
        Runs in background thread
        """
        try:
            logger.info(f"Starting batch processing: {batch.batch_id}")
            
            # Call the processing callback
            success = self.processing_callback(batch.data_type, batch.items)
            
            # Update batch status
            batch.processing_end = datetime.now()
            batch.status = "completed" if success else "failed"
            
            if not success:
                batch.error_message = "Processing callback returned False"
            
            # Update statistics
            with self.lock:
                self.completed_batches.append(batch)
                if success:
                    self.stats["total_items_processed"] += batch.item_count
                    self.stats["total_batches_processed"] += 1
                    
                    # Update averages
                    total_batches = self.stats["total_batches_processed"]
                    self.stats["average_batch_size"] = self.stats["total_items_processed"] / total_batches
                    
                    if batch.processing_duration:
                        current_avg = self.stats["average_processing_time"]
                        new_duration = batch.processing_duration.total_seconds()
                        self.stats["average_processing_time"] = (current_avg * (total_batches - 1) + new_duration) / total_batches
                else:
                    self.stats["total_batches_failed"] += 1
            
            duration = batch.processing_duration.total_seconds() if batch.processing_duration else 0
            rate = batch.item_count / duration if duration > 0 else 0
            
            logger.info(f"Batch processing completed: {batch.batch_id}, "
                       f"success: {success}, duration: {duration:.2f}s, "
                       f"rate: {rate:.1f} items/sec")
            
            return success
            
        except Exception as e:
            batch.processing_end = datetime.now()
            batch.status = "failed"
            batch.error_message = str(e)
            
            with self.lock:
                self.completed_batches.append(batch)
                self.stats["total_batches_failed"] += 1
            
            logger.error(f"Batch processing failed: {batch.batch_id}: {e}")
            return False
    
    def _start_auto_processing_timer(self):
        """Start background timer for auto-processing old batches"""
        def auto_process_timer():
            while not self.shutdown_event.is_set():
                try:
                    time.sleep(30)  # Check every 30 seconds
                    self._process_aged_batches()
                except Exception as e:
                    logger.error(f"Auto-processing timer error: {e}")
        
        # Start timer in background thread
        import threading
        timer_thread = threading.Thread(target=auto_process_timer, daemon=True)
        timer_thread.start()
        logger.debug("Auto-processing timer started")
    
    def _process_aged_batches(self):
        """Process batches that have exceeded max wait time"""
        with self.lock:
            aged_batches = []
            for batch_key, batch in list(self.active_batches.items()):
                if batch.age_seconds >= self.config.max_wait_time_seconds:
                    aged_batches.append(batch_key)
            
            for batch_key in aged_batches:
                logger.info(f"Auto-processing aged batch: {self.active_batches[batch_key].batch_id}")
                self._trigger_batch_processing(batch_key)
    
    def force_process_all(self):
        """
        Force processing of all active batches
        Returns list of batch IDs that were triggered
        """
        triggered_batches = []
        
        with self.lock:
            for batch_key in list(self.active_batches.keys()):
                batch_id = self._trigger_batch_processing(batch_key)
                if batch_id:
                    triggered_batches.append(batch_id)
        
        logger.info(f"Force processed {len(triggered_batches)} batches: {triggered_batches}")
        return triggered_batches
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get current processing statistics"""
        with self.lock:
            active_batches_info = {}
            for batch_key, batch in self.active_batches.items():
                active_batches_info[batch_key] = {
                    "batch_id": batch.batch_id,
                    "item_count": batch.item_count,
                    "age_seconds": batch.age_seconds,
                    "status": batch.status
                }
            
            recent_batches = []
            for batch in list(self.completed_batches)[-10:]:  # Last 10 batches
                recent_batches.append({
                    "batch_id": batch.batch_id,
                    "data_type": batch.data_type,
                    "item_count": batch.item_count,
                    "status": batch.status,
                    "duration_seconds": batch.processing_duration.total_seconds() if batch.processing_duration else None,
                    "error_message": batch.error_message
                })
            
            return {
                "config": {
                    "max_items": self.config.max_items,
                    "max_wait_time_seconds": self.config.max_wait_time_seconds,
                    "max_concurrent_batches": self.config.max_concurrent_batches
                },
                "active_batches": active_batches_info,
                "recent_batches": recent_batches,
                "statistics": self.stats.copy()
            }
    
    def shutdown(self):
        """Shutdown the batch manager and process remaining batches"""
        logger.info("Shutting down Batch Manager...")
        
        # Stop auto-processing timer
        self.shutdown_event.set()
        
        # Process all remaining batches
        self.force_process_all()
        
        # Wait for all processing to complete
        self.executor.shutdown(wait=True)
        
        logger.info("Batch Manager shutdown complete")

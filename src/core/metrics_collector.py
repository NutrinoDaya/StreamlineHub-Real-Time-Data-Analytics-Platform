"""
Metrics Collector Module
Collects, aggregates and provides system performance metrics
"""

import asyncio
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from collections import deque, defaultdict
import psutil
import structlog

logger = structlog.get_logger("metrics_collector")

class MetricsCollector:
    """
    Advanced metrics collection system for ETL pipeline monitoring
    """
    
    def __init__(self, 
                 history_size: int = 1000,
                 collection_interval: int = 5):
        self.history_size = history_size
        self.collection_interval = collection_interval
        
        # Metrics storage
        self.metrics_history = deque(maxlen=history_size)
        self.counters = defaultdict(int)
        self.gauges = defaultdict(float)
        self.timers = defaultdict(list)
        self.rates = defaultdict(lambda: deque(maxlen=60))  # 5-minute rolling window
        
        # System monitoring
        self.system_metrics = {}
        self.monitoring_active = False
        self.monitoring_task: Optional[asyncio.Task] = None
        
        # Performance tracking
        self.start_time = datetime.now()
        self.last_collection = None
    
    async def initialize(self) -> bool:
        """Initialize metrics collector"""
        try:
            logger.info("📊 Initializing Metrics Collector...")
            
            # Initialize counters
            self.counters.update({
                "events_received": 0,
                "events_processed": 0,
                "events_buffered": 0,
                "batches_created": 0,
                "errors_total": 0
            })
            
            # Initialize gauges  
            self.gauges.update({
                "buffer_utilization": 0.0,
                "processing_rate": 0.0,
                "system_cpu_percent": 0.0,
                "system_memory_percent": 0.0,
                "system_disk_percent": 0.0
            })
            
            # Start background collection
            await self.start_collection()
            
            logger.info("✅ Metrics Collector initialized")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Metrics Collector: {e}")
            return False
    
    async def start_collection(self):
        """Start background metrics collection"""
        if not self.monitoring_active:
            self.monitoring_active = True
            self.monitoring_task = asyncio.create_task(self._collection_loop())
            logger.info("🔍 Started metrics collection")
    
    async def stop_collection(self):
        """Stop background metrics collection"""
        if self.monitoring_task:
            self.monitoring_active = False
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
            logger.info("⏹️  Stopped metrics collection")
    
    async def _collection_loop(self):
        """Background metrics collection loop"""
        while self.monitoring_active:
            try:
                await self._collect_system_metrics()
                await self._update_rates()
                await self._store_snapshot()
                
                self.last_collection = datetime.now()
                await asyncio.sleep(self.collection_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in metrics collection: {e}")
                await asyncio.sleep(5)
    
    async def _collect_system_metrics(self):
        """Collect system performance metrics"""
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=0.1)
            self.gauges["system_cpu_percent"] = cpu_percent
            
            # Memory usage
            memory = psutil.virtual_memory()
            self.gauges["system_memory_percent"] = memory.percent
            
            # Disk usage
            disk = psutil.disk_usage('/')
            self.gauges["system_disk_percent"] = (disk.used / disk.total) * 100
            
            # Store in system metrics
            self.system_metrics = {
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "memory_total_gb": memory.total / (1024**3),
                "memory_used_gb": memory.used / (1024**3),
                "disk_percent": self.gauges["system_disk_percent"],
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to collect system metrics: {e}")
    
    async def _update_rates(self):
        """Update rate calculations"""
        try:
            current_time = time.time()
            
            # Calculate processing rate (events per second)
            events_processed = self.counters.get("events_processed", 0)
            
            # Add to rate history
            self.rates["processing_rate"].append({
                "timestamp": current_time,
                "value": events_processed
            })
            
            # Calculate rate from last minute of data
            if len(self.rates["processing_rate"]) >= 2:
                recent_data = list(self.rates["processing_rate"])[-12:]  # Last minute (5s intervals)
                if len(recent_data) >= 2:
                    time_diff = recent_data[-1]["timestamp"] - recent_data[0]["timestamp"]
                    value_diff = recent_data[-1]["value"] - recent_data[0]["value"]
                    
                    if time_diff > 0:
                        rate = value_diff / time_diff
                        self.gauges["processing_rate"] = max(0, rate)
            
        except Exception as e:
            logger.error(f"Failed to update rates: {e}")
    
    async def _store_snapshot(self):
        """Store current metrics snapshot"""
        try:
            snapshot = {
                "timestamp": datetime.now().isoformat(),
                "counters": dict(self.counters),
                "gauges": dict(self.gauges),
                "system": self.system_metrics.copy()
            }
            
            self.metrics_history.append(snapshot)
            
        except Exception as e:
            logger.error(f"Failed to store metrics snapshot: {e}")
    
    # Counter operations
    def increment_counter(self, name: str, value: int = 1):
        """Increment a counter"""
        self.counters[name] += value
    
    def set_gauge(self, name: str, value: float):
        """Set a gauge value"""
        self.gauges[name] = value
    
    def record_timer(self, name: str, duration: float):
        """Record a timer value"""
        self.timers[name].append(duration)
        # Keep only last 100 measurements
        if len(self.timers[name]) > 100:
            self.timers[name] = self.timers[name][-100:]
    
    def get_current_metrics(self) -> Dict[str, Any]:
        """Get current metric values"""
        return {
            "counters": dict(self.counters),
            "gauges": dict(self.gauges),
            "system": self.system_metrics.copy(),
            "uptime_seconds": (datetime.now() - self.start_time).total_seconds(),
            "last_collection": self.last_collection.isoformat() if self.last_collection else None,
            "timestamp": datetime.now().isoformat()
        }
    
    def get_processing_metrics(self) -> Dict[str, Any]:
        """Get ETL processing specific metrics"""
        try:
            # Calculate success rate
            events_received = self.counters.get("events_received", 0)
            events_processed = self.counters.get("events_processed", 0)
            errors_total = self.counters.get("errors_total", 0)
            
            success_rate = 0.0
            if events_received > 0:
                success_rate = ((events_received - errors_total) / events_received) * 100
            
            # Calculate average processing time from timers
            avg_processing_time = 0.0
            if "processing_time" in self.timers and self.timers["processing_time"]:
                avg_processing_time = sum(self.timers["processing_time"]) / len(self.timers["processing_time"])
            
            return {
                "events_received": events_received,
                "events_processed": events_processed,
                "events_buffered": self.counters.get("events_buffered", 0),
                "batches_created": self.counters.get("batches_created", 0),
                "processing_rate": self.gauges.get("processing_rate", 0.0),
                "success_rate_percent": success_rate,
                "error_count": errors_total,
                "avg_processing_time_ms": avg_processing_time * 1000,
                "buffer_utilization_percent": self.gauges.get("buffer_utilization", 0.0),
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get processing metrics: {e}")
            return {}
    
    def get_historical_metrics(self, 
                             hours: int = 1,
                             metric_type: str = "all") -> List[Dict[str, Any]]:
        """Get historical metrics"""
        try:
            cutoff_time = datetime.now() - timedelta(hours=hours)
            
            filtered_history = []
            for snapshot in self.metrics_history:
                snapshot_time = datetime.fromisoformat(snapshot["timestamp"])
                if snapshot_time >= cutoff_time:
                    if metric_type == "all":
                        filtered_history.append(snapshot)
                    elif metric_type in snapshot:
                        filtered_history.append({
                            "timestamp": snapshot["timestamp"],
                            metric_type: snapshot[metric_type]
                        })
            
            return filtered_history
            
        except Exception as e:
            logger.error(f"Failed to get historical metrics: {e}")
            return []
    
    def get_health_summary(self) -> Dict[str, Any]:
        """Get health summary based on metrics"""
        try:
            current = self.get_current_metrics()
            
            # Determine health status
            health_status = "healthy"
            warnings = []
            
            # Check CPU usage
            cpu_percent = current["system"].get("cpu_percent", 0)
            if cpu_percent > 90:
                health_status = "critical"
                warnings.append(f"High CPU usage: {cpu_percent:.1f}%")
            elif cpu_percent > 70:
                health_status = "warning"
                warnings.append(f"Elevated CPU usage: {cpu_percent:.1f}%")
            
            # Check memory usage  
            memory_percent = current["system"].get("memory_percent", 0)
            if memory_percent > 90:
                health_status = "critical" 
                warnings.append(f"High memory usage: {memory_percent:.1f}%")
            elif memory_percent > 80:
                health_status = "warning"
                warnings.append(f"Elevated memory usage: {memory_percent:.1f}%")
            
            # Check error rate
            error_count = self.counters.get("errors_total", 0)
            events_received = self.counters.get("events_received", 0)
            
            if events_received > 0:
                error_rate = (error_count / events_received) * 100
                if error_rate > 10:
                    health_status = "critical"
                    warnings.append(f"High error rate: {error_rate:.1f}%")
                elif error_rate > 5:
                    health_status = "warning" 
                    warnings.append(f"Elevated error rate: {error_rate:.1f}%")
            
            return {
                "health_status": health_status,
                "warnings": warnings,
                "metrics_snapshot": current,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get health summary: {e}")
            return {
                "health_status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def shutdown(self):
        """Shutdown metrics collector"""
        await self.stop_collection()
        logger.info("📊 Metrics Collector shutdown complete")

# Global metrics collector instance
_metrics_collector_instance: Optional[MetricsCollector] = None

def get_metrics_collector() -> MetricsCollector:
    """Get or create global metrics collector instance"""
    global _metrics_collector_instance
    if _metrics_collector_instance is None:
        _metrics_collector_instance = MetricsCollector()
    return _metrics_collector_instance
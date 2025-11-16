#!/usr/bin/env python
"""
ETL Performance Monitor

This module provides comprehensive performance monitoring for ETL operations:
- Real-time metrics collection
- Performance bottleneck detection
- Resource utilization tracking
- Alerting for performance degradation
"""

import time
import psutil
import threading
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
from collections import deque

from utils import LoggerManager

logger = LoggerManager.get_logger("ETL_Processing.log")


@dataclass
class PerformanceMetric:
    """Performance metric data structure"""
    timestamp: datetime
    metric_name: str
    value: float
    unit: str
    metadata: Dict[str, Any] = None


@dataclass
class ProcessingStats:
    """Processing statistics for a batch"""
    batch_id: str
    table_name: str
    record_count: int
    processing_start: datetime
    processing_end: Optional[datetime] = None
    bytes_processed: int = 0
    spark_job_ids: List[str] = None
    performance_metrics: List[PerformanceMetric] = None
    
    @property
    def processing_duration(self) -> Optional[float]:
        """Processing duration in seconds"""
        if self.processing_end:
            return (self.processing_end - self.processing_start).total_seconds()
        return None
    
    @property
    def records_per_second(self) -> Optional[float]:
        """Records processed per second"""
        if self.processing_duration and self.processing_duration > 0:
            return self.record_count / self.processing_duration
        return None


class PerformanceMonitor:
    """
    performance monitoring system
    """
    
    def __init__(self, max_history_size: int = 1000):
        self.max_history_size = max_history_size
        self.metrics_history = deque(maxlen=max_history_size)
        self.processing_stats = deque(maxlen=max_history_size)
        self.active_batches = {}
        self.system_monitoring_active = False
        self.system_metrics_thread = None
        self.performance_thresholds = {
            "cpu_threshold": 90.0,
            "memory_threshold": 85.0,
            "processing_time_threshold": 300.0,  # 5 minutes
            "records_per_second_minimum": 10.0,
            "disk_io_threshold": 100.0  # MB/s
        }
        
    def start_system_monitoring(self, interval_seconds: int = 30):
        """Start background system monitoring"""
        if not self.system_monitoring_active:
            self.system_monitoring_active = True
            self.system_metrics_thread = threading.Thread(
                target=self._system_monitoring_loop,
                args=(interval_seconds,),
                daemon=True
            )
            self.system_metrics_thread.start()
            logger.info("Started system performance monitoring")
    
    def stop_system_monitoring(self):
        """Stop background system monitoring"""
        self.system_monitoring_active = False
        if self.system_metrics_thread:
            self.system_metrics_thread.join(timeout=5)
        logger.info("Stopped system performance monitoring")
    
    def _system_monitoring_loop(self, interval_seconds: int):
        """Background system monitoring loop"""
        while self.system_monitoring_active:
            try:
                self._collect_system_metrics()
                time.sleep(interval_seconds)
            except Exception as e:
                logger.error(f"System monitoring error: {e}")
                time.sleep(interval_seconds)
    
    def _collect_system_metrics(self):
        """Collect system performance metrics"""
        now = datetime.now()
        
        try:
            # CPU metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            self._add_metric("system.cpu.usage_percent", cpu_percent, "percent", now)
            
            # Memory metrics
            memory = psutil.virtual_memory()
            self._add_metric("system.memory.usage_percent", memory.percent, "percent", now)
            self._add_metric("system.memory.available_gb", memory.available / (1024**3), "GB", now)
            
            # Disk I/O metrics
            disk_io = psutil.disk_io_counters()
            if disk_io:
                self._add_metric("system.disk.read_mb_per_sec", disk_io.read_bytes / (1024**2), "MB/s", now)
                self._add_metric("system.disk.write_mb_per_sec", disk_io.write_bytes / (1024**2), "MB/s", now)
            
            # Check thresholds and alert
            self._check_performance_thresholds(cpu_percent, memory.percent)
            
        except Exception as e:
            logger.error(f"Failed to collect system metrics: {e}")
    
    def _add_metric(self, name: str, value: float, unit: str, timestamp: datetime = None, metadata: Dict = None):
        """Add performance metric to history"""
        if timestamp is None:
            timestamp = datetime.now()
        
        metric = PerformanceMetric(
            timestamp=timestamp,
            metric_name=name,
            value=value,
            unit=unit,
            metadata=metadata or {}
        )
        self.metrics_history.append(metric)
    
    def _check_performance_thresholds(self, cpu_percent: float, memory_percent: float):
        """Check performance thresholds and alert if exceeded"""
        if cpu_percent > self.performance_thresholds["cpu_threshold"]:
            logger.warning(f"High CPU usage detected: {cpu_percent:.1f}%")
        
        if memory_percent > self.performance_thresholds["memory_threshold"]:
            logger.warning(f"High memory usage detected: {memory_percent:.1f}%")
    
    def start_batch_processing(self, batch_id: str, table_name: str, record_count: int) -> ProcessingStats:
        """Start tracking a batch processing operation"""
        stats = ProcessingStats(
            batch_id=batch_id,
            table_name=table_name,
            record_count=record_count,
            processing_start=datetime.now(),
            performance_metrics=[]
        )
        
        self.active_batches[batch_id] = stats
        
        self._add_metric(
            f"batch.{table_name}.started",
            record_count,
            "records",
            metadata={"batch_id": batch_id, "table_name": table_name}
        )
        
        logger.info(f"Started processing batch {batch_id} for {table_name} with {record_count} records")
        return stats
    
    def update_batch_progress(self, batch_id: str, bytes_processed: int = None, spark_job_ids: List[str] = None):
        """Update batch processing progress"""
        if batch_id in self.active_batches:
            stats = self.active_batches[batch_id]
            
            if bytes_processed is not None:
                stats.bytes_processed = bytes_processed
                self._add_metric(
                    f"batch.{stats.table_name}.bytes_processed",
                    bytes_processed / (1024**2),  # Convert to MB
                    "MB",
                    metadata={"batch_id": batch_id}
                )
            
            if spark_job_ids is not None:
                stats.spark_job_ids = spark_job_ids
    
    def complete_batch_processing(self, batch_id: str, success: bool = True) -> Optional[ProcessingStats]:
        """Complete batch processing tracking"""
        if batch_id not in self.active_batches:
            logger.warning(f"Batch {batch_id} not found in active batches")
            return None
        
        stats = self.active_batches.pop(batch_id)
        stats.processing_end = datetime.now()
        
        # Calculate performance metrics
        duration = stats.processing_duration
        records_per_sec = stats.records_per_second
        
        self._add_metric(
            f"batch.{stats.table_name}.duration_seconds",
            duration,
            "seconds",
            metadata={"batch_id": batch_id, "success": success}
        )
        
        if records_per_sec:
            self._add_metric(
                f"batch.{stats.table_name}.records_per_second",
                records_per_sec,
                "records/sec",
                metadata={"batch_id": batch_id}
            )
        
        # Check performance thresholds
        if duration and duration > self.performance_thresholds["processing_time_threshold"]:
            logger.warning(f"Slow batch processing detected: {duration:.1f}s for {stats.record_count} records")
        
        if records_per_sec and records_per_sec < self.performance_thresholds["records_per_second_minimum"]:
            logger.warning(f"Low processing throughput: {records_per_sec:.1f} records/sec")
        
        self.processing_stats.append(stats)
        
        status = "completed" if success else "failed"
        logger.info(f"Batch {batch_id} {status}: {stats.record_count} records in {duration:.2f}s "
                   f"({records_per_sec:.1f} records/sec)")
        
        return stats
    
    def get_performance_summary(self, time_window_minutes: int = 60) -> Dict[str, Any]:
        """Get performance summary for the specified time window"""
        cutoff_time = datetime.now() - timedelta(minutes=time_window_minutes)
        
        # Filter recent metrics
        recent_metrics = [m for m in self.metrics_history if m.timestamp >= cutoff_time]
        recent_batches = [s for s in self.processing_stats if s.processing_start >= cutoff_time]
        
        summary = {
            "time_window_minutes": time_window_minutes,
            "summary_timestamp": datetime.now().isoformat(),
            "system_metrics": self._summarize_system_metrics(recent_metrics),
            "processing_metrics": self._summarize_processing_metrics(recent_batches),
            "active_batches": len(self.active_batches),
            "alerts": self._get_recent_alerts(recent_metrics, recent_batches)
        }
        
        return summary
    
    def _summarize_system_metrics(self, metrics: List[PerformanceMetric]) -> Dict[str, Any]:
        """Summarize system metrics"""
        system_summary = {}
        
        metric_groups = {}
        for metric in metrics:
            if metric.metric_name.startswith("system."):
                if metric.metric_name not in metric_groups:
                    metric_groups[metric.metric_name] = []
                metric_groups[metric.metric_name].append(metric.value)
        
        for metric_name, values in metric_groups.items():
            if values:
                system_summary[metric_name] = {
                    "current": values[-1],
                    "average": sum(values) / len(values),
                    "max": max(values),
                    "min": min(values),
                    "samples": len(values)
                }
        
        return system_summary
    
    def _summarize_processing_metrics(self, batches: List[ProcessingStats]) -> Dict[str, Any]:
        """Summarize processing metrics"""
        if not batches:
            return {"total_batches": 0}
        
        total_records = sum(b.record_count for b in batches)
        total_duration = sum(b.processing_duration for b in batches if b.processing_duration)
        successful_batches = len([b for b in batches if b.processing_end is not None])
        
        processing_summary = {
            "total_batches": len(batches),
            "successful_batches": successful_batches,
            "total_records_processed": total_records,
            "total_processing_time_seconds": total_duration,
            "average_records_per_batch": total_records / len(batches) if batches else 0,
            "average_processing_time_seconds": total_duration / successful_batches if successful_batches > 0 else 0
        }
        
        # Calculate throughput
        if total_duration > 0:
            processing_summary["overall_records_per_second"] = total_records / total_duration
        
        # Per-table breakdown
        table_stats = {}
        for batch in batches:
            table = batch.table_name
            if table not in table_stats:
                table_stats[table] = {"batches": 0, "records": 0, "duration": 0}
            
            table_stats[table]["batches"] += 1
            table_stats[table]["records"] += batch.record_count
            if batch.processing_duration:
                table_stats[table]["duration"] += batch.processing_duration
        
        processing_summary["per_table_stats"] = table_stats
        
        return processing_summary
    
    def _get_recent_alerts(self, metrics: List[PerformanceMetric], batches: List[ProcessingStats]) -> List[Dict[str, Any]]:
        """Get recent performance alerts"""
        alerts = []
        
        # System alerts
        for metric in metrics:
            if metric.metric_name == "system.cpu.usage_percent" and metric.value > self.performance_thresholds["cpu_threshold"]:
                alerts.append({
                    "type": "high_cpu",
                    "timestamp": metric.timestamp.isoformat(),
                    "value": metric.value,
                    "threshold": self.performance_thresholds["cpu_threshold"]
                })
            
            if metric.metric_name == "system.memory.usage_percent" and metric.value > self.performance_thresholds["memory_threshold"]:
                alerts.append({
                    "type": "high_memory",
                    "timestamp": metric.timestamp.isoformat(),
                    "value": metric.value,
                    "threshold": self.performance_thresholds["memory_threshold"]
                })
        
        # Processing alerts
        for batch in batches:
            if batch.processing_duration and batch.processing_duration > self.performance_thresholds["processing_time_threshold"]:
                alerts.append({
                    "type": "slow_processing",
                    "timestamp": batch.processing_start.isoformat(),
                    "batch_id": batch.batch_id,
                    "table_name": batch.table_name,
                    "duration": batch.processing_duration,
                    "threshold": self.performance_thresholds["processing_time_threshold"]
                })
            
            if batch.records_per_second and batch.records_per_second < self.performance_thresholds["records_per_second_minimum"]:
                alerts.append({
                    "type": "low_throughput",
                    "timestamp": batch.processing_start.isoformat(),
                    "batch_id": batch.batch_id,
                    "table_name": batch.table_name,
                    "records_per_second": batch.records_per_second,
                    "threshold": self.performance_thresholds["records_per_second_minimum"]
                })
        
        return alerts
    
    def export_metrics(self, format: str = "json") -> str:
        """Export performance metrics in specified format"""
        if format.lower() == "json":
            import json
            data = {
                "export_timestamp": datetime.now().isoformat(),
                "metrics": [
                    {
                        "timestamp": m.timestamp.isoformat(),
                        "name": m.metric_name,
                        "value": m.value,
                        "unit": m.unit,
                        "metadata": m.metadata
                    } for m in self.metrics_history
                ],
                "processing_stats": [
                    {
                        "batch_id": s.batch_id,
                        "table_name": s.table_name,
                        "record_count": s.record_count,
                        "processing_start": s.processing_start.isoformat(),
                        "processing_end": s.processing_end.isoformat() if s.processing_end else None,
                        "duration_seconds": s.processing_duration,
                        "records_per_second": s.records_per_second,
                        "bytes_processed": s.bytes_processed
                    } for s in self.processing_stats
                ]
            }
            return json.dumps(data, indent=2)
        else:
            raise ValueError(f"Unsupported export format: {format}")
    
    def get_current_metrics(self) -> Dict[str, Any]:
        """Get current system metrics summary"""
        if not self.metrics_history:
            return {"status": "no_metrics_available"}
        
        # Get latest metrics
        latest_metrics = {}
        for metric in reversed(self.metrics_history):
            if metric.metric_name not in latest_metrics:
                latest_metrics[metric.metric_name] = {
                    "value": metric.value,
                    "unit": metric.unit,
                    "timestamp": metric.timestamp.isoformat()
                }
            if len(latest_metrics) >= 10:  # Limit to last 10 unique metrics
                break
        
        return {
            "timestamp": datetime.now().isoformat(),
            "system_monitoring_active": self.system_monitoring_active,
            "latest_metrics": latest_metrics,
            "active_batches": len(self.active_batches)
        }
    
    def get_detailed_metrics(self) -> Dict[str, Any]:
        """Get detailed performance metrics"""
        return {
            "timestamp": datetime.now().isoformat(),
            "system_monitoring_active": self.system_monitoring_active,
            "metrics_history_count": len(self.metrics_history),
            "processing_stats_count": len(self.processing_stats),
            "active_batches": len(self.active_batches),
            "performance_summary": self.get_performance_summary()
        }

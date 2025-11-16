"""
Fast Metrics Service
High-performance metrics storage optimized for real-time dashboard needs.

Comparison of storage approaches:
1. JSON: Simple, human-readable, but slower for large datasets
2. Parquet + PyArrow: Columnar, compressed, very fast for analytics
3. In-memory + periodic flush: Fastest for reads, risk of data loss

For dashboard metrics (small dataset, frequent updates, no historical buildup):
- PyArrow + Parquet is optimal: fast read/write, compression, structured
"""

import pyarrow as pa
import pyarrow.parquet as pq
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from pathlib import Path
import json
import logging

logger = logging.getLogger(__name__)

class FastMetricsService:
    """High-performance metrics service using PyArrow + Parquet."""
    
    def __init__(self, metrics_file: str = "/tmp/dashboard_metrics.parquet"):
        self.metrics_file = Path(metrics_file)
        self.metrics_file.parent.mkdir(exist_ok=True)
        self._lock = threading.Lock()
        
        # Define schema for consistent structure
        self.schema = pa.schema([
            ('timestamp', pa.timestamp('s')),
            ('total_events', pa.int64()),
            ('events_per_second', pa.float64()),
            ('total_customers', pa.int64()),
            ('total_revenue', pa.float64()),
            ('producer_sent', pa.int64()),
            ('producer_delivered', pa.int64()),
            ('producer_errors', pa.int32()),
            ('consumer_consumed', pa.int64()),
            ('consumer_errors', pa.int32()),
            ('success_rate', pa.float64()),
            ('data_source', pa.string())
        ])
    
    def write_metrics(self, metrics: Dict[str, Any]) -> bool:
        """Write single metrics record to Parquet file."""
        try:
            with self._lock:
                # Convert metrics to PyArrow record
                data = {
                    'timestamp': [pa.scalar(datetime.now())],
                    'total_events': [metrics.get('total_events', 0)],
                    'events_per_second': [metrics.get('events_per_second', 0.0)],
                    'total_customers': [metrics.get('total_customers', 0)],
                    'total_revenue': [metrics.get('total_revenue', 0.0)],
                    'producer_sent': [metrics.get('producer_sent', 0)],
                    'producer_delivered': [metrics.get('producer_delivered', 0)],
                    'producer_errors': [metrics.get('producer_errors', 0)],
                    'consumer_consumed': [metrics.get('consumer_consumed', 0)],
                    'consumer_errors': [metrics.get('consumer_errors', 0)],
                    'success_rate': [metrics.get('success_rate', 0.0)],
                    'data_source': [metrics.get('data_source', 'unknown')]
                }
                
                table = pa.table(data, schema=self.schema)
                
                # Write single record (overwrites previous - no buildup)
                pq.write_table(table, self.metrics_file)
                return True
                
        except Exception as e:
            logger.error(f"Error writing metrics: {e}")
            return False
    
    def read_metrics(self) -> Optional[Dict[str, Any]]:
        """Read latest metrics record from Parquet file."""
        try:
            with self._lock:
                if not self.metrics_file.exists():
                    return None
                
                table = pq.read_table(self.metrics_file)
                if len(table) == 0:
                    return None
                
                # Convert to Python dict (latest record)
                df = table.to_pandas()
                latest = df.iloc[-1].to_dict()
                
                # Convert timestamp to string for JSON serialization
                if 'timestamp' in latest:
                    latest['timestamp'] = latest['timestamp'].isoformat()
                
                return latest
                
        except Exception as e:
            logger.error(f"Error reading metrics: {e}")
            return None
    
    def get_file_size(self) -> int:
        """Get metrics file size in bytes."""
        try:
            if self.metrics_file.exists():
                return self.metrics_file.stat().st_size
            return 0
        except Exception:
            return 0


class JSONMetricsService:
    """JSON-based metrics service for comparison."""
    
    def __init__(self, metrics_file: str = "/tmp/dashboard_metrics.json"):
        self.metrics_file = Path(metrics_file)
        self.metrics_file.parent.mkdir(exist_ok=True)
        self._lock = threading.Lock()
    
    def write_metrics(self, metrics: Dict[str, Any]) -> bool:
        try:
            with self._lock:
                metrics['timestamp'] = datetime.now().isoformat()
                with open(self.metrics_file, 'w') as f:
                    json.dump(metrics, f)
                return True
        except Exception as e:
            logger.error(f"Error writing JSON metrics: {e}")
            return False
    
    def read_metrics(self) -> Optional[Dict[str, Any]]:
        try:
            with self._lock:
                if not self.metrics_file.exists():
                    return None
                with open(self.metrics_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Error reading JSON metrics: {e}")
            return None
    
    def get_file_size(self) -> int:
        try:
            if self.metrics_file.exists():
                return self.metrics_file.stat().st_size
            return 0
        except Exception:
            return 0


def benchmark_storage_methods():
    """Benchmark different storage approaches."""
    print("🚀 Benchmarking Metrics Storage Methods")
    print("=" * 50)
    
    # Test data
    test_metrics = {
        'total_events': 150000,
        'events_per_second': 2500.0,
        'total_customers': 50000,
        'total_revenue': 1875000.0,
        'producer_sent': 150000,
        'producer_delivered': 149950,
        'producer_errors': 50,
        'consumer_consumed': 149950,
        'consumer_errors': 25,
        'success_rate': 99.97,
        'data_source': 'kafka_real_time'
    }
    
    # Test PyArrow/Parquet
    print("\n📊 Testing PyArrow + Parquet:")
    parquet_service = FastMetricsService("/tmp/test_parquet_metrics.parquet")
    
    start_time = time.time()
    for _ in range(1000):
        parquet_service.write_metrics(test_metrics)
    parquet_write_time = time.time() - start_time
    
    start_time = time.time()
    for _ in range(1000):
        parquet_service.read_metrics()
    parquet_read_time = time.time() - start_time
    
    parquet_size = parquet_service.get_file_size()
    
    print(f"  Write 1000 records: {parquet_write_time:.4f}s")
    print(f"  Read 1000 times: {parquet_read_time:.4f}s")
    print(f"  File size: {parquet_size} bytes")
    
    # Test JSON
    print("\n📝 Testing JSON:")
    json_service = JSONMetricsService("/tmp/test_json_metrics.json")
    
    start_time = time.time()
    for _ in range(1000):
        json_service.write_metrics(test_metrics)
    json_write_time = time.time() - start_time
    
    start_time = time.time()
    for _ in range(1000):
        json_service.read_metrics()
    json_read_time = time.time() - start_time
    
    json_size = json_service.get_file_size()
    
    print(f"  Write 1000 records: {json_write_time:.4f}s")
    print(f"  Read 1000 times: {json_read_time:.4f}s")
    print(f"  File size: {json_size} bytes")
    
    # Comparison
    print(f"\n🏆 Performance Comparison:")
    print(f"  Parquet vs JSON write speed: {json_write_time/parquet_write_time:.2f}x")
    print(f"  Parquet vs JSON read speed: {json_read_time/parquet_read_time:.2f}x")
    print(f"  Parquet vs JSON file size: {json_size/parquet_size:.2f}x")
    
    # Cleanup
    Path("/tmp/test_parquet_metrics.parquet").unlink(missing_ok=True)
    Path("/tmp/test_json_metrics.json").unlink(missing_ok=True)
    
    return {
        'parquet_write_time': parquet_write_time,
        'parquet_read_time': parquet_read_time,
        'parquet_size': parquet_size,
        'json_write_time': json_write_time,
        'json_read_time': json_read_time,
        'json_size': json_size
    }


# Default service instance
fast_metrics = FastMetricsService()

if __name__ == "__main__":
    benchmark_storage_methods()
"""
File-based Dashboard Service
Simple fallback service that tracks events in a file when Spark is not available.
"""

import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, Any
from pathlib import Path

# Data directory for simple file storage (container-compatible path)
try:
    # Try container path first
    DATA_DIR = Path("/tmp/dashboard_data")
    DATA_DIR.mkdir(exist_ok=True)
except Exception:
    # Fallback to local path
    try:
        DATA_DIR = Path("data")
        DATA_DIR.mkdir(exist_ok=True)
    except Exception:
        # Last resort - use temp directory
        import tempfile
        DATA_DIR = Path(tempfile.gettempdir()) / "dashboard_data"
        DATA_DIR.mkdir(exist_ok=True)

METRICS_FILE = DATA_DIR / "dashboard_metrics.json"

class FileDashboardService:
    """Simple file-based dashboard service."""
    
    def __init__(self):
        self.metrics_file = METRICS_FILE
    
    def increment_events(self, event_count: int = 1):
        """Increment event count in metrics file."""
        try:
            # Read existing metrics
            metrics = self.get_metrics()
            
            # Update counts
            metrics["total_events"] += event_count
            metrics["last_update"] = datetime.now().isoformat()
            
            # Calculate events per second (last 60 seconds)
            current_time = time.time()
            
            # Add to recent events list
            if "recent_events" not in metrics:
                metrics["recent_events"] = []
            
            metrics["recent_events"].append({
                "timestamp": current_time,
                "count": event_count
            })
            
            # Keep only last 60 seconds of events
            cutoff_time = current_time - 60
            metrics["recent_events"] = [
                event for event in metrics["recent_events"] 
                if event["timestamp"] > cutoff_time
            ]
            
            # Calculate events per second
            recent_total = sum(event["count"] for event in metrics["recent_events"])
            time_span = max(1, current_time - metrics["recent_events"][0]["timestamp"]) if metrics["recent_events"] else 1
            metrics["events_per_second"] = recent_total / time_span
            
            # Save metrics
            with open(self.metrics_file, 'w') as f:
                json.dump(metrics, f, indent=2)
                
        except Exception as e:
            print(f"⚠️ Error updating metrics: {e}")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics from file."""
        default_metrics = {
            "total_events": 0,
            "events_per_second": 0.0,
            "total_customers": 0,
            "total_revenue": 0.0,
            "last_update": datetime.now().isoformat(),
            "recent_events": []
        }
        
        try:
            if self.metrics_file.exists():
                with open(self.metrics_file, 'r') as f:
                    metrics = json.load(f)
                    
                # Clean old events (older than 60 seconds)
                current_time = time.time()
                cutoff_time = current_time - 60
                
                if "recent_events" in metrics:
                    metrics["recent_events"] = [
                        event for event in metrics["recent_events"] 
                        if event["timestamp"] > cutoff_time
                    ]
                    
                    # Recalculate events per second
                    recent_total = sum(event["count"] for event in metrics["recent_events"])
                    time_span = max(1, current_time - metrics["recent_events"][0]["timestamp"]) if metrics["recent_events"] else 1
                    metrics["events_per_second"] = recent_total / time_span
                
                return metrics
            else:
                # Create file with defaults
                with open(self.metrics_file, 'w') as f:
                    json.dump(default_metrics, f, indent=2)
                return default_metrics
                
        except Exception as e:
            print(f"⚠️ Error reading metrics: {e}")
            return default_metrics
    
    def get_dashboard_metrics(self) -> Dict[str, Any]:
        """Get formatted metrics for dashboard."""
        metrics = self.get_metrics()
        
        return {
            "source": "file_based",
            "timestamp": datetime.now().isoformat(),
            "total_events": metrics.get("total_events", 0),
            "events_per_second": round(metrics.get("events_per_second", 0.0), 2),
            "total_customers": max(1, metrics.get("total_events", 0) // 3),  # Estimate
            "total_revenue": metrics.get("total_events", 0) * 12.5,  # Estimate
            "avg_event_value": 12.5,
            "data_freshness": "file_based",
            "table_status": "file_storage",
            "last_updated": metrics.get("last_update")
        }

# Global instance
file_dashboard_service = FileDashboardService()

# Export functions for compatibility
def get_file_dashboard_metrics() -> Dict[str, Any]:
    """Get dashboard metrics from file storage."""
    return file_dashboard_service.get_dashboard_metrics()

def increment_file_events(count: int = 1):
    """Increment event count in file storage."""
    file_dashboard_service.increment_events(count)

if __name__ == "__main__":
    # Test the service
    print("🧪 Testing File Dashboard Service")
    print("=" * 40)
    
    service = FileDashboardService()
    
    # Simulate some events
    for i in range(10):
        service.increment_events(5)
        time.sleep(0.1)
    
    metrics = service.get_dashboard_metrics()
    print("📊 File Metrics:")
    for key, value in metrics.items():
        print(f"  {key}: {value}")
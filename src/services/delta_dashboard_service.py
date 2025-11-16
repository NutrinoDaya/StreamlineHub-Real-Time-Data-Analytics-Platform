"""
Delta Lake Dashboard Service
Reads processed data from Delta Lake Gold tables for real-time dashboard metrics.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.core.spark_session import get_spark_session, get_path
from pyspark.sql.functions import *

class DeltaLakeDashboardService:
    """Service to read dashboard metrics from Delta Lake Gold tables."""
    
    def __init__(self):
        """Initialize Spark session for reading Delta tables."""
        self.spark = None
        self._initialize_spark()
    
    def _initialize_spark(self):
        """Initialize Spark session if not already done."""
        try:
            if not self.spark or self.spark._jsparkSession.sparkContext().isStopped():
                self.spark = get_spark_session("DashboardMetrics")
                print("✅ Delta Lake Dashboard Service initialized")
        except Exception as e:
            print(f"⚠️ Error initializing Spark session: {e}")
            self.spark = None
    
    def get_real_time_metrics(self) -> Dict[str, Any]:
        """Get real-time metrics from Gold Delta table."""
        try:
            if not self.spark:
                self._initialize_spark()
                if not self.spark:
                    return self._get_fallback_metrics()
            
            # Read from Gold dashboard metrics table
            gold_path = get_path("gold", "dashboard_metrics")
            
            # Check if table exists
            try:
                df = self.spark.read.format("delta").load(gold_path)
                
                # Get latest metrics (last 5 minutes)
                latest_metrics = df.filter(
                    col("dashboard_timestamp") >= (current_timestamp() - expr("INTERVAL 5 MINUTES"))
                ).orderBy(col("dashboard_timestamp").desc()).limit(10)
                
                if latest_metrics.count() > 0:
                    # Collect recent data
                    metrics_data = latest_metrics.collect()
                    
                    # Calculate aggregated metrics
                    total_events = sum(row["total_events_per_minute"] or 0 for row in metrics_data)
                    total_customers = sum(row["total_unique_customers"] or 0 for row in metrics_data)
                    total_revenue = sum(row["total_revenue"] or 0 for row in metrics_data)
                    
                    # Calculate real-time events per second (from last minute)
                    latest_row = metrics_data[0] if metrics_data else None
                    events_per_second = latest_row["events_per_second"] if latest_row else 0
                    
                    return {
                        "source": "delta_lake_gold",
                        "timestamp": datetime.now().isoformat(),
                        "total_events": int(total_events),
                        "events_per_second": float(events_per_second or 0),
                        "total_customers": int(total_customers),
                        "total_revenue": float(total_revenue or 0),
                        "avg_event_value": float(total_revenue / total_events) if total_events > 0 else 0,
                        "data_freshness": "real_time",
                        "table_status": "active"
                    }
                else:
                    return self._get_empty_metrics("No recent data in Gold table")
                    
            except Exception as table_error:
                print(f"📊 Gold table not ready yet: {table_error}")
                return self._get_bronze_fallback_metrics()
                
        except Exception as e:
            print(f"❌ Error reading Delta Lake metrics: {e}")
            return self._get_fallback_metrics()
    
    def _get_bronze_fallback_metrics(self) -> Dict[str, Any]:
        """Fallback to Bronze table if Gold is not ready."""
        try:
            bronze_path = get_path("bronze", "customer_behavior")
            df = self.spark.read.format("delta").load(bronze_path)
            
            # Get recent events (last 5 minutes)
            recent_events = df.filter(
                col("ingestion_timestamp") >= (current_timestamp() - expr("INTERVAL 5 MINUTES"))
            )
            
            total_events = recent_events.count()
            unique_customers = recent_events.select("customer_id").distinct().count()
            total_value = recent_events.agg(sum(coalesce(col("value"), lit(0))).alias("total")).collect()[0]["total"] or 0
            
            # Estimate events per second (events in last 5 minutes / 300 seconds)
            events_per_second = total_events / 300 if total_events > 0 else 0
            
            return {
                "source": "delta_lake_bronze",
                "timestamp": datetime.now().isoformat(),
                "total_events": total_events,
                "events_per_second": events_per_second,
                "total_customers": unique_customers,
                "total_revenue": float(total_value),
                "avg_event_value": float(total_value / total_events) if total_events > 0 else 0,
                "data_freshness": "bronze_fallback",
                "table_status": "bronze_only"
            }
            
        except Exception as e:
            print(f"⚠️ Bronze fallback failed: {e}")
            return self._get_empty_metrics("Bronze table not available")
    
    def _get_empty_metrics(self, reason: str) -> Dict[str, Any]:
        """Return empty metrics structure."""
        return {
            "source": "empty",
            "timestamp": datetime.now().isoformat(),
            "total_events": 0,
            "events_per_second": 0.0,
            "total_customers": 0,
            "total_revenue": 0.0,
            "avg_event_value": 0.0,
            "data_freshness": "no_data",
            "table_status": f"unavailable: {reason}"
        }
    
    def _get_fallback_metrics(self) -> Dict[str, Any]:
        """Fallback metrics when Spark is unavailable."""
        return {
            "source": "fallback",
            "timestamp": datetime.now().isoformat(),
            "total_events": 0,
            "events_per_second": 0.0,
            "total_customers": 0,
            "total_revenue": 0.0,
            "avg_event_value": 0.0,
            "data_freshness": "spark_unavailable",
            "table_status": "spark_connection_failed"
        }
    
    def get_historical_trends(self, hours: int = 1) -> Dict[str, Any]:
        """Get historical trends from Gold table."""
        try:
            if not self.spark:
                self._initialize_spark()
                if not self.spark:
                    return {"trends": [], "status": "spark_unavailable"}
            
            gold_path = get_path("gold", "dashboard_metrics")
            df = self.spark.read.format("delta").load(gold_path)
            
            # Get trends for specified hours
            trends = df.filter(
                col("dashboard_timestamp") >= (current_timestamp() - expr(f"INTERVAL {hours} HOURS"))
            ).select(
                "dashboard_timestamp",
                "total_events_per_minute",
                "events_per_second",
                "total_unique_customers",
                "total_revenue"
            ).orderBy("dashboard_timestamp")
            
            trends_data = [
                {
                    "timestamp": row["dashboard_timestamp"].isoformat(),
                    "events_per_minute": row["total_events_per_minute"] or 0,
                    "events_per_second": row["events_per_second"] or 0,
                    "customers": row["total_unique_customers"] or 0,
                    "revenue": row["total_revenue"] or 0
                }
                for row in trends.collect()
            ]
            
            return {
                "trends": trends_data,
                "status": "success",
                "data_points": len(trends_data),
                "time_range_hours": hours
            }
            
        except Exception as e:
            print(f"📈 Error getting trends: {e}")
            return {"trends": [], "status": f"error: {e}"}

# Global instance
delta_dashboard_service = DeltaLakeDashboardService()

def get_delta_dashboard_metrics() -> Dict[str, Any]:
    """Get real-time dashboard metrics from Delta Lake."""
    return delta_dashboard_service.get_real_time_metrics()

def get_delta_historical_trends(hours: int = 1) -> Dict[str, Any]:
    """Get historical trends from Delta Lake."""
    return delta_dashboard_service.get_historical_trends(hours)

if __name__ == "__main__":
    # Test the service
    print("🧪 Testing Delta Lake Dashboard Service")
    print("=" * 50)
    
    service = DeltaLakeDashboardService()
    metrics = service.get_real_time_metrics()
    
    print("📊 Real-time Metrics:")
    for key, value in metrics.items():
        print(f"  {key}: {value}")
    
    trends = service.get_historical_trends()
    print(f"\n📈 Historical Trends: {len(trends.get('trends', []))} data points")
"""
Analytics API router.

This module provides endpoints for real-time analytics, metrics,
and historical data analysis using Elasticsearch and aggregated data.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel
import random
import logging
import json
from elasticsearch import Elasticsearch, exceptions as es_exceptions

logger = logging.getLogger(__name__)


router = APIRouter(tags=["Analytics"])

# Elasticsearch client (configured to connect to local instance)
def get_elasticsearch_client():
    """Get Elasticsearch client instance."""
    try:
        es = Elasticsearch(
            [{'host': 'localhost', 'port': 9200, 'scheme': 'http'}],
            request_timeout=30,
            max_retries=3,
            retry_on_timeout=True
        )
        
        if not es.ping():
            logger.warning("Elasticsearch is not available, using mock data")
            return None
        
        return es
    except Exception as e:
        logger.warning(f"Could not connect to Elasticsearch: {e}, using mock data")
        return None


# Pydantic Models
class RealtimeMetrics(BaseModel):
    timestamp: datetime
    active_users: int
    events_per_second: float
    revenue_per_minute: float
    conversion_rate: float
    avg_session_duration: float
    bounce_rate: float


class HistoricalDataPoint(BaseModel):
    timestamp: datetime
    revenue: float
    orders: int
    customers: int
    conversion_rate: float
    avg_order_value: float


class TopProduct(BaseModel):
    product_id: str
    product_name: str
    category: str
    sales: int
    revenue: float
    growth_rate: float


class TopCustomer(BaseModel):
    customer_id: int
    customer_name: str
    total_spent: float
    total_orders: int
    last_order_date: datetime


class ChannelPerformance(BaseModel):
    channel: str
    visitors: int
    conversions: int
    revenue: float
    conversion_rate: float
    avg_order_value: float


class GeographicData(BaseModel):
    country: str
    region: str
    customers: int
    revenue: float
    avg_order_value: float


class DashboardSummary(BaseModel):
    realtime_metrics: RealtimeMetrics
    today_revenue: float
    today_orders: int
    today_customers: int
    revenue_change_percent: float
    orders_change_percent: float
    customers_change_percent: float
    top_products: List[TopProduct]
    top_customers: List[TopCustomer]


@router.get("/realtime", response_model=RealtimeMetrics)
async def get_realtime_metrics():
    """
    Get real-time analytics metrics from Kafka/Redis cache.
    """
    # Try to get real-time data from Redis directly
    try:
        import redis
        redis_client = redis.Redis(host='redis', port=6379, password='redis_secret', decode_responses=True)
        
        # Get aggregated metrics from Redis
        total_events = int(redis_client.hget('metrics:events', 'total') or 0)
        
        # Calculate events per second using recent events rate
        # Get the last stored rate or calculate from recent events
        events_per_second = 0.0
        
        # Try to get stored realtime metrics first
        cached_metrics = redis_client.get('realtime_metrics')
        if cached_metrics:
            import json
            data = json.loads(cached_metrics)
            return RealtimeMetrics(
                timestamp=datetime.fromisoformat(data.get('timestamp', datetime.now().isoformat())),
                active_users=int(data.get('active_users', total_events)),
                events_per_second=float(data.get('events_per_second', 0.0)),
                revenue_per_minute=float(data.get('revenue_per_minute', 0.0)),
                conversion_rate=float(data.get('conversion_rate', 2.5)),
                avg_session_duration=float(data.get('avg_session_duration', 180.0)),
                bounce_rate=float(data.get('bounce_rate', 0.35))
            )
            
        # Fallback to basic metrics
        return RealtimeMetrics(
            timestamp=datetime.now(),
            active_users=total_events,
            events_per_second=events_per_second,
            revenue_per_minute=0.0,
            conversion_rate=2.5,
            avg_session_duration=180.0,
            bounce_rate=0.35
        )
        
    except Exception as e:
        logger.warning(f"Could not fetch real-time metrics from Redis: {e}")
    
    # Fallback: Get data from Kafka consumer stats (no estimations)
    try:
        from src.core.confluent_kafka_integration import kafka_manager
        if kafka_manager and kafka_manager.consumer:
            stats = kafka_manager.get_stats()
            return RealtimeMetrics(
                timestamp=datetime.now(),
                active_users=stats.get('processed_events', 0),
                events_per_second=stats.get('consume_rate_per_second', 0.0),
                revenue_per_minute=0.0,
                conversion_rate=0.0,
                avg_session_duration=0.0,
                bounce_rate=0.0
            )
    except Exception as e:
        logger.warning(f"Could not fetch metrics from Kafka: {e}")
    
    # Final fallback: Return zero metrics with timestamp
    return RealtimeMetrics(
        timestamp=datetime.now(),
        active_users=0,
        events_per_second=0.0,
        revenue_per_minute=0.0,
        conversion_rate=0.0,
        avg_session_duration=0.0,
        bounce_rate=0.0
    )


@router.get("/historical", response_model=List[HistoricalDataPoint])
async def get_historical_analytics(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    granularity: str = Query("day", regex="^(hour|day|week|month)$")
):
    """
    Get historical analytics data with specified granularity.
    """
    es = get_elasticsearch_client()
    if not es:
        return []
    
    try:
        # Set default date range (last 7 days)
        if not start_date:
            start_date = datetime.now() - timedelta(days=7)
        if not end_date:
            end_date = datetime.now()
        
        # Query both customer and transaction indices
        customer_query = {
            "query": {
                "range": {
                    "hour": {
                        "gte": start_date.isoformat(),
                        "lte": end_date.isoformat()
                    }
                }
            },
            "aggs": {
                "by_time": {
                    "date_histogram": {
                        "field": "hour",
                        "calendar_interval": granularity,
                        "format": "yyyy-MM-dd'T'HH:mm:ss"
                    },
                    "aggs": {
                        "total_customers": {"sum": {"field": "total_customers"}},
                        "total_events": {"sum": {"field": "total_events"}}
                    }
                }
            },
            "size": 0
        }
        
        transaction_query = {
            "query": {
                "range": {
                    "hour": {
                        "gte": start_date.isoformat(),
                        "lte": end_date.isoformat()
                    }
                }
            },
            "aggs": {
                "by_time": {
                    "date_histogram": {
                        "field": "hour",
                        "calendar_interval": granularity,
                        "format": "yyyy-MM-dd'T'HH:mm:ss"
                    },
                    "aggs": {
                        "total_revenue": {"sum": {"field": "total_revenue"}},
                        "total_orders": {"sum": {"field": "total_transactions"}},
                        "avg_order_value": {"avg": {"field": "avg_transaction_value"}}
                    }
                }
            },
            "size": 0
        }
        
        # Execute queries
        customer_result = es.search(index="analytics_customer_behavior", body=customer_query)
        transaction_result = es.search(index="analytics_transaction_summary", body=transaction_query)
        
        # Combine results
        historical_data = []
        customer_buckets = customer_result.get("aggregations", {}).get("by_time", {}).get("buckets", [])
        transaction_buckets = transaction_result.get("aggregations", {}).get("by_time", {}).get("buckets", [])
        
        # Create lookup for transaction data
        transaction_lookup = {bucket["key_as_string"]: bucket for bucket in transaction_buckets}
        
        for bucket in customer_buckets:
            timestamp = bucket["key_as_string"]
            transaction_data = transaction_lookup.get(timestamp, {})
            
            revenue = transaction_data.get("total_revenue", {}).get("value", 0) or 0
            orders = transaction_data.get("total_orders", {}).get("value", 0) or 0
            customers = bucket.get("total_customers", {}).get("value", 0) or 0
            avg_order_value = transaction_data.get("avg_order_value", {}).get("value", 0) or 0
            
            historical_data.append(HistoricalDataPoint(
                timestamp=datetime.fromisoformat(timestamp),
                revenue=float(revenue),
                orders=int(orders),
                customers=int(customers),
                conversion_rate=float((orders / max(customers, 1)) * 100),
                avg_order_value=float(avg_order_value)
            ))
        
        return sorted(historical_data, key=lambda x: x.timestamp)
        
    except Exception as e:
        logger.warning(f"Error fetching historical data from Elasticsearch: {e}")
        return []


@router.get("/dashboard", response_model=DashboardSummary)
async def get_dashboard_summary():
    """
    Get comprehensive dashboard summary with real metrics from database and cache.
    """
    # Get real-time metrics
    realtime_data = await get_realtime_metrics()
    
    # Try to get cached dashboard metrics using CacheManager
    from src.core.cache import get_cache
    
    try:
        cache = get_cache()
        cached_dashboard = await cache.get("dashboard_summary")
        if cached_dashboard:
            data = cached_dashboard
            return DashboardSummary(
                    realtime_metrics=realtime_data,
                    today_revenue=data.get('today_revenue', 0),
                    today_orders=data.get('today_orders', 0),
                    today_customers=data.get('today_customers', 0),
                    revenue_change_percent=data.get('revenue_change_percent', 0),
                    orders_change_percent=data.get('orders_change_percent', 0),
                    customers_change_percent=data.get('customers_change_percent', 0),
                    top_products=[TopProduct(**p) for p in data.get('top_products', [])],
                    top_customers=[TopCustomer(**c) for c in data.get('top_customers', [])]
                )
    except Exception as e:
        logger.warning(f"Could not fetch dashboard metrics from cache: {e}")
    
    # Fallback: Calculate metrics from recent Kafka events (no estimates)
    try:
        from src.core.confluent_kafka_integration import kafka_manager
        stats = kafka_manager.get_stats() if kafka_manager else {}
        
        # Use only real Kafka metrics - no estimates
        processed_events = stats.get('processed_events', 0)
        
        return DashboardSummary(
            realtime_metrics=realtime_data,
            today_revenue=0.0,
            today_orders=0,
            today_customers=0,
            revenue_change_percent=0.0,
            orders_change_percent=0.0,
            customers_change_percent=0.0,
            top_products=[],
            top_customers=[]
        )
        
    except Exception as e:
        logger.warning(f"Could not calculate metrics from Kafka: {e}")
    
    # Final fallback: Return minimal data
    return DashboardSummary(
        realtime_metrics=realtime_data,
        today_revenue=0.0,
        today_orders=0,
        today_customers=0,
        revenue_change_percent=0.0,
        orders_change_percent=0.0,
        customers_change_percent=0.0,
        top_products=[],
        top_customers=[]
    )


@router.get("/channels", response_model=List[ChannelPerformance])
async def get_channel_performance():
    """
    Get performance metrics by marketing channel.
    Returns empty data - no mock values.
    """
    # Return empty list - no mock data
    return []


@router.get("/geography", response_model=List[GeographicData])
async def get_geographic_analytics():
    """
    Get analytics data by geographic location.
    Returns empty data - no mock values.
    """
    # Return empty list - no mock data
    return []


@router.get("/trends", response_model=dict)
async def get_trends(
    metric: str = Query("revenue", regex="^(revenue|orders|customers|conversion_rate)$"),
    period: str = Query("7d", regex="^(24h|7d|30d|90d)$")
):
    """
    Get trend analysis for specific metrics.
    Returns empty data - no mock values.
    """
    return {
        "metric": metric,
        "period": period,
        "trend_percent": 0.0,
        "trend_direction": "neutral",
        "data": []
    }


# New Elasticsearch-based Analytics Endpoints

@router.get("/aggregations/customer-actions")
async def get_customer_actions_aggregations():
    """Get customer action aggregations from Elasticsearch."""
    es = get_elasticsearch_client()
    
    if not es:
        # Mock data when ES is unavailable
        return {
            "success": True,
            "data": [
                {"action": "view_product", "event_count": 150, "total_value": 12500.0, "avg_value": 83.33, "unique_customers": 45},
                {"action": "add_to_cart", "event_count": 75, "total_value": 8750.0, "avg_value": 116.67, "unique_customers": 35},
                {"action": "purchase", "event_count": 25, "total_value": 5250.0, "avg_value": 210.0, "unique_customers": 20}
            ],
            "message": "Mock data - Elasticsearch unavailable"
        }
    
    try:
        # Query customer actions index
        response = es.search(
            index="customer_actions",
            body={
                "size": 0,
                "aggs": {
                    "actions": {
                        "terms": {"field": "action"},
                        "aggs": {
                            "total_events": {"sum": {"field": "event_count"}},
                            "total_value": {"sum": {"field": "total_value"}},
                            "avg_value": {"avg": {"field": "avg_value"}},
                            "unique_customers": {"sum": {"field": "unique_customers"}}
                        }
                    }
                }
            }
        )
        
        data = []
        for bucket in response['aggregations']['actions']['buckets']:
            data.append({
                "action": bucket['key'],
                "event_count": int(bucket['total_events']['value'] or 0),
                "total_value": bucket['total_value']['value'] or 0,
                "avg_value": bucket['avg_value']['value'] or 0,
                "unique_customers": int(bucket['unique_customers']['value'] or 0)
            })
        
        return {"success": True, "data": data}
        
    except es_exceptions.NotFoundError:
        return {"success": True, "data": [], "message": "No customer actions data found"}
    except Exception as e:
        logger.error(f"Error querying customer actions: {e}")
        return {"success": False, "error": str(e), "data": []}


@router.get("/aggregations/revenue-hourly")
async def get_revenue_hourly_aggregations():
    """Get hourly revenue aggregations from Elasticsearch."""
    es = get_elasticsearch_client()
    
    if not es:
        # Mock data when ES is unavailable
        hours = []
        for i in range(24):
            hours.append({
                "hour": f"2025-11-15 {i:02d}:00:00",
                "transaction_count": random.randint(10, 50),
                "total_revenue": round(random.uniform(1000, 5000), 2),
                "avg_transaction_value": round(random.uniform(50, 200), 2),
                "unique_customers": random.randint(5, 25)
            })
        
        return {
            "success": True,
            "data": hours[-12:],  # Last 12 hours
            "message": "Mock data - Elasticsearch unavailable"
        }
    
    try:
        # Query revenue hourly index for last 24 hours
        response = es.search(
            index="revenue_hourly",
            body={
                "size": 100,
                "sort": [{"hour": {"order": "desc"}}],
                "query": {
                    "range": {
                        "hour": {
                            "gte": "now-24h/h",
                            "lte": "now/h"
                        }
                    }
                }
            }
        )
        
        data = []
        for hit in response['hits']['hits']:
            source = hit['_source']
            data.append({
                "hour": source['hour'],
                "transaction_count": source['transaction_count'],
                "total_revenue": source['total_revenue'],
                "avg_transaction_value": source['avg_transaction_value'],
                "unique_customers": source['unique_customers']
            })
        
        return {"success": True, "data": data}
        
    except es_exceptions.NotFoundError:
        return {"success": True, "data": [], "message": "No revenue data found"}
    except Exception as e:
        logger.error(f"Error querying revenue data: {e}")
        return {"success": False, "error": str(e), "data": []}


@router.get("/aggregations/system-metrics")
async def get_system_metrics_aggregations():
    """Get system metrics aggregations from Elasticsearch."""
    es = get_elasticsearch_client()
    
    if not es:
        # Mock data when ES is unavailable
        return {
            "success": True,
            "data": [
                {"metric_name": "cpu_usage_percent", "avg_value": 65.5, "min_value": 45.2, "max_value": 89.1, "measurement_count": 120},
                {"metric_name": "memory_usage_mb", "avg_value": 2048.7, "min_value": 1800.0, "max_value": 2500.3, "measurement_count": 120},
                {"metric_name": "active_users", "avg_value": 156.3, "min_value": 89.0, "max_value": 245.0, "measurement_count": 120}
            ],
            "message": "Mock data - Elasticsearch unavailable"
        }
    
    try:
        # Query system metrics index
        response = es.search(
            index="system_metrics",
            body={
                "size": 0,
                "aggs": {
                    "metrics": {
                        "terms": {"field": "metric_name"},
                        "aggs": {
                            "avg_value": {"avg": {"field": "avg_value"}},
                            "min_value": {"min": {"field": "min_value"}},
                            "max_value": {"max": {"field": "max_value"}},
                            "total_measurements": {"sum": {"field": "measurement_count"}}
                        }
                    }
                }
            }
        )
        
        data = []
        for bucket in response['aggregations']['metrics']['buckets']:
            data.append({
                "metric_name": bucket['key'],
                "avg_value": bucket['avg_value']['value'] or 0,
                "min_value": bucket['min_value']['value'] or 0,
                "max_value": bucket['max_value']['value'] or 0,
                "measurement_count": int(bucket['total_measurements']['value'] or 0)
            })
        
        return {"success": True, "data": data}
        
    except es_exceptions.NotFoundError:
        return {"success": True, "data": [], "message": "No system metrics data found"}
    except Exception as e:
        logger.error(f"Error querying system metrics: {e}")
        return {"success": False, "error": str(e), "data": []}


@router.get("/aggregations/metrics-timeseries")
async def get_metrics_timeseries():
    """Get metrics timeseries data from Elasticsearch."""
    es = get_elasticsearch_client()
    
    if not es:
        # Mock timeseries data when ES is unavailable
        import datetime as dt
        now = dt.datetime.now()
        data = []
        
        for i in range(12):  # Last 12 time buckets (1 hour if 5-minute intervals)
            time_bucket = (now - dt.timedelta(minutes=i*5)).strftime("%Y-%m-%d %H:%M:%S")
            for metric in ["cpu_usage_percent", "memory_usage_mb", "active_users"]:
                data.append({
                    "time_bucket": time_bucket,
                    "metric_name": metric,
                    "avg_value": round(random.uniform(50, 100), 2),
                    "measurement_count": random.randint(5, 15)
                })
        
        return {
            "success": True,
            "data": sorted(data, key=lambda x: x['time_bucket']),
            "message": "Mock data - Elasticsearch unavailable"
        }
    
    try:
        # Query metrics timeseries for last hour
        response = es.search(
            index="metrics_timeseries",
            body={
                "size": 1000,
                "sort": [{"time_bucket": {"order": "desc"}}],
                "query": {
                    "range": {
                        "time_bucket": {
                            "gte": "now-1h/m",
                            "lte": "now/m"
                        }
                    }
                }
            }
        )
        
        data = []
        for hit in response['hits']['hits']:
            source = hit['_source']
            data.append({
                "time_bucket": source['time_bucket'],
                "metric_name": source['metric_name'],
                "avg_value": source['avg_value'],
                "measurement_count": source['measurement_count']
            })
        
        return {"success": True, "data": data}
        
    except es_exceptions.NotFoundError:
        return {"success": True, "data": [], "message": "No timeseries data found"}
    except Exception as e:
        logger.error(f"Error querying timeseries data: {e}")
        return {"success": False, "error": str(e), "data": []}


@router.get("/aggregations/pipeline-status")
async def get_pipeline_status():
    """Get pipeline run status from Elasticsearch."""
    es = get_elasticsearch_client()
    
    if not es:
        # Mock data when ES is unavailable
        return {
            "success": True,
            "data": {
                "last_run": {
                    "pipeline_run_id": "mock-run-12345",
                    "start_time": "2025-11-15T17:00:00Z",
                    "end_time": "2025-11-15T17:02:30Z",
                    "processing_duration_seconds": 150.5,
                    "total_events_processed": 1748,
                    "status": "completed"
                },
                "runs_today": 144,
                "success_rate": 98.6,
                "avg_processing_time": 145.3
            },
            "message": "Mock data - Elasticsearch unavailable"
        }
    
    try:
        # Get latest pipeline run
        latest_response = es.search(
            index="pipeline_runs",
            body={
                "size": 1,
                "sort": [{"created_at": {"order": "desc"}}]
            }
        )
        
        # Get today's runs
        today_response = es.search(
            index="pipeline_runs",
            body={
                "size": 0,
                "query": {
                    "range": {
                        "created_at": {
                            "gte": "now/d",
                            "lte": "now/d+1d"
                        }
                    }
                },
                "aggs": {
                    "total_runs": {"value_count": {"field": "pipeline_run_id"}},
                    "success_count": {
                        "filter": {"term": {"status": "completed"}},
                        "aggs": {"count": {"value_count": {"field": "pipeline_run_id"}}}
                    },
                    "avg_duration": {"avg": {"field": "processing_duration_seconds"}}
                }
            }
        )
        
        last_run = None
        if latest_response['hits']['hits']:
            last_run = latest_response['hits']['hits'][0]['_source']
        
        total_runs = today_response['aggregations']['total_runs']['value']
        success_count = today_response['aggregations']['success_count']['count']['value']
        success_rate = (success_count / total_runs * 100) if total_runs > 0 else 0
        avg_duration = today_response['aggregations']['avg_duration']['value'] or 0
        
        return {
            "success": True,
            "data": {
                "last_run": last_run,
                "runs_today": int(total_runs),
                "success_rate": round(success_rate, 2),
                "avg_processing_time": round(avg_duration, 2)
            }
        }
        
    except es_exceptions.NotFoundError:
        return {"success": True, "data": {"last_run": None, "runs_today": 0, "success_rate": 0, "avg_processing_time": 0}}
    except Exception as e:
        logger.error(f"Error querying pipeline status: {e}")
        return {"success": False, "error": str(e)}


@router.post("/refresh-cache")
async def refresh_analytics_cache():
    """Refresh analytics cache (called by Airflow after data ingestion)."""
    try:
        # Clear any cached analytics data
        from src.core.cache import get_cache
        cache = get_cache()
        
        # Clear relevant cache keys
        cache_keys = [
            "customer_actions_cache",
            "revenue_hourly_cache", 
            "system_metrics_cache",
            "metrics_timeseries_cache",
            "pipeline_status_cache"
        ]
        
        for key in cache_keys:
            await cache.delete(key)
        
        logger.info("Analytics cache refreshed successfully")
        return {"success": True, "message": "Analytics cache refreshed"}
        
    except Exception as e:
        logger.error(f"Error refreshing analytics cache: {e}")
        return {"success": False, "error": str(e)}

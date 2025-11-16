"""
Pipeline Manager Module
Orchestrates data processing pipelines and monitors health
"""

import asyncio
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime, timedelta
import structlog
from pathlib import Path

logger = structlog.get_logger("pipeline_manager")

class PipelineComponent:
    """Represents a single pipeline component"""
    
    def __init__(self, name: str, check_function: Callable[[], bool]):
        self.name = name
        self.check_function = check_function
        self.last_check = None
        self.status = "unknown"
        self.message = ""
    
    async def check_health(self) -> Dict[str, Any]:
        """Check component health"""
        try:
            is_healthy = await asyncio.create_task(
                asyncio.to_thread(self.check_function)
            ) if asyncio.iscoroutinefunction(self.check_function) else self.check_function()
            
            self.status = "healthy" if is_healthy else "error"
            self.message = f"{self.name} is {'operational' if is_healthy else 'experiencing issues'}"
            self.last_check = datetime.now()
            
            return {
                "status": self.status,
                "message": self.message,
                "last_check": self.last_check.isoformat() if self.last_check else None
            }
            
        except Exception as e:
            self.status = "error"
            self.message = f"{self.name} check failed: {str(e)}"
            self.last_check = datetime.now()
            
            return {
                "status": self.status,
                "message": self.message,
                "last_check": self.last_check.isoformat()
            }

class StreamlineHubPipelineManager:
    """
    Manages and monitors all data processing pipeline components
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.components: Dict[str, PipelineComponent] = {}
        self.monitoring_active = False
        self.monitoring_task: Optional[asyncio.Task] = None
        self.health_cache = {}
        self.cache_ttl = 30  # seconds
    
    async def initialize(self) -> bool:
        """Initialize pipeline manager and register components"""
        try:
            logger.info("🚀 Initializing Pipeline Manager...")
            
            # Register pipeline components
            self._register_components()
            
            # Start health monitoring
            await self.start_monitoring()
            
            logger.info("✅ Pipeline Manager initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Pipeline Manager: {e}")
            return False
    
    def _register_components(self):
        """Register all pipeline components"""
        
        # Kafka component
        self.components["kafka"] = PipelineComponent(
            name="Kafka",
            check_function=self._check_kafka_health
        )
        
        # Delta Lake component  
        self.components["delta_lake"] = PipelineComponent(
            name="Delta Lake",
            check_function=self._check_delta_lake_health
        )
        
        # Spark component
        self.components["spark"] = PipelineComponent(
            name="Spark",
            check_function=self._check_spark_health
        )
        
        # Elasticsearch component
        self.components["elasticsearch"] = PipelineComponent(
            name="Elasticsearch", 
            check_function=self._check_elasticsearch_health
        )
        
        # Airflow component
        self.components["airflow"] = PipelineComponent(
            name="Airflow",
            check_function=self._check_airflow_health
        )
        
        logger.info(f"📋 Registered {len(self.components)} pipeline components")
    
    def _check_kafka_health(self) -> bool:
        """Check Kafka broker health"""
        try:
            # Check if Kafka container is running
            import docker
            client = docker.from_env()
            
            containers = client.containers.list(filters={"name": "kafka"})
            if containers:
                container = containers[0]
                return container.status == "running"
            return False
            
        except Exception as e:
            logger.debug(f"Kafka health check failed: {e}")
            return False
    
    def _check_delta_lake_health(self) -> bool:
        """Check Delta Lake accessibility"""
        try:
            # Check if bronze layer directory exists and is accessible
            bronze_path = Path(self.config.get("bronze_layer_path", "data/bronze"))
            bronze_path.mkdir(parents=True, exist_ok=True)
            
            # Try to write a test file
            test_file = bronze_path / ".health_check"
            test_file.write_text("health_check")
            test_file.unlink()
            
            return True
            
        except Exception as e:
            logger.debug(f"Delta Lake health check failed: {e}")
            return False
    
    def _check_spark_health(self) -> bool:
        """Check Spark cluster health"""
        try:
            import docker
            client = docker.from_env()
            
            # Check spark-master container
            containers = client.containers.list(filters={"name": "spark-master"})
            if containers:
                container = containers[0]
                return container.status == "running"
            return False
            
        except Exception as e:
            logger.debug(f"Spark health check failed: {e}")
            return False
    
    def _check_elasticsearch_health(self) -> bool:
        """Check Elasticsearch health"""
        try:
            import requests
            response = requests.get("http://localhost:9200/_cluster/health", timeout=5)
            return response.status_code == 200
            
        except Exception as e:
            logger.debug(f"Elasticsearch health check failed: {e}")
            return False
    
    def _check_airflow_health(self) -> bool:
        """Check Airflow health"""
        try:
            import docker
            client = docker.from_env()
            
            # Check if any Airflow containers are running
            containers = client.containers.list(filters={"name": "airflow"})
            return len(containers) > 0 and any(c.status == "running" for c in containers)
            
        except Exception as e:
            logger.debug(f"Airflow health check failed: {e}")
            return True  # Assume healthy if we can't check (optional component)
    
    async def start_monitoring(self):
        """Start background health monitoring"""
        if not self.monitoring_active:
            self.monitoring_active = True
            self.monitoring_task = asyncio.create_task(self._monitoring_loop())
            logger.info("🔍 Started pipeline health monitoring")
    
    async def stop_monitoring(self):
        """Stop background health monitoring"""
        if self.monitoring_task:
            self.monitoring_active = False
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
            logger.info("⏹️  Stopped pipeline health monitoring")
    
    async def _monitoring_loop(self):
        """Background monitoring loop"""
        while self.monitoring_active:
            try:
                # Update health status for all components
                for component_name, component in self.components.items():
                    health_status = await component.check_health()
                    self.health_cache[component_name] = {
                        **health_status,
                        "cached_at": datetime.now()
                    }
                
                # Wait before next check
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(10)
    
    async def get_pipeline_health(self) -> Dict[str, Any]:
        """Get health status of all pipeline components"""
        try:
            health_status = {}
            
            for component_name, component in self.components.items():
                # Use cached status if recent, otherwise do fresh check
                if (component_name in self.health_cache and 
                    (datetime.now() - self.health_cache[component_name]["cached_at"]).seconds < self.cache_ttl):
                    
                    cached_status = self.health_cache[component_name]
                    health_status[component_name] = {
                        "status": cached_status["status"],
                        "message": cached_status["message"]
                    }
                else:
                    # Do fresh check
                    fresh_status = await component.check_health()
                    health_status[component_name] = {
                        "status": fresh_status["status"],
                        "message": fresh_status["message"]
                    }
            
            # Determine overall pipeline status
            overall_status = "healthy"
            if any(status["status"] == "error" for status in health_status.values()):
                overall_status = "error"
            elif any(status["status"] == "unknown" for status in health_status.values()):
                overall_status = "warning"
            
            return {
                "pipeline_status": overall_status,
                "last_checked": datetime.now().isoformat(),
                "components": health_status
            }
            
        except Exception as e:
            logger.error(f"Failed to get pipeline health: {e}")
            return {
                "pipeline_status": "error",
                "last_checked": datetime.now().isoformat(),
                "error": str(e),
                "components": {}
            }
    
    async def get_pipeline_metrics(self) -> Dict[str, Any]:
        """Get pipeline performance metrics"""
        try:
            return {
                "monitoring_active": self.monitoring_active,
                "components_registered": len(self.components),
                "cache_ttl_seconds": self.cache_ttl,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Failed to get pipeline metrics: {e}")
            return {}
    
    async def shutdown(self):
        """Shutdown pipeline manager"""
        await self.stop_monitoring()
        logger.info("🔌 Pipeline Manager shutdown complete")

# Global pipeline manager instance
_pipeline_manager_instance: Optional[StreamlineHubPipelineManager] = None

def get_pipeline_manager(config: Optional[Dict[str, Any]] = None) -> StreamlineHubPipelineManager:
    """Get or create global pipeline manager instance"""
    global _pipeline_manager_instance
    if _pipeline_manager_instance is None:
        if config is None:
            config = {}
        _pipeline_manager_instance = StreamlineHubPipelineManager(config)
    return _pipeline_manager_instance
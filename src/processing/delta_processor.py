"""
Delta Processor Module
Handles Delta Lake operations for Bronze, Silver, and Gold layers
"""

import json
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path
import structlog

logger = structlog.get_logger("delta_processor")

class DeltaProcessor:
    """
    Delta Lake processor for managing Bronze, Silver, and Gold layers
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.is_running = False
        self.spark_session = None
        
        # Storage paths
        self.bronze_path = Path(config.get("bronze_layer_path", "data/bronze"))
        self.silver_path = Path(config.get("silver_layer_path", "data/silver"))
        self.gold_path = Path(config.get("gold_layer_path", "data/gold"))
        
        self.delta_stats = {
            "bronze_writes": 0,
            "silver_writes": 0,
            "gold_writes": 0,
            "last_write_time": None
        }
    
    async def initialize(self) -> bool:
        """Initialize Delta processor"""
        try:
            logger.info("🔺 Initializing Delta Processor...")
            
            # Create layer directories
            self.bronze_path.mkdir(parents=True, exist_ok=True)
            self.silver_path.mkdir(parents=True, exist_ok=True)
            self.gold_path.mkdir(parents=True, exist_ok=True)
            
            # Initialize Spark session with Delta Lake
            await self._initialize_spark_session()
            
            self.is_running = True
            logger.info("✅ Delta Processor initialized")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Delta Processor: {e}")
            return False
    
    async def _initialize_spark_session(self):
        """Initialize Spark session with Delta Lake support"""
        try:
            from pyspark.sql import SparkSession
            
            self.spark_session = SparkSession.builder \
                .appName("StreamlineHub-DeltaProcessor") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.adaptive.enabled", "true") \
                .getOrCreate()
            
            # Test Delta Lake functionality
            test_df = self.spark_session.createDataFrame([(1, "test")], ["id", "value"])
            test_path = str(self.bronze_path / "_test_delta")
            
            # Write test Delta table
            test_df.write.format("delta").mode("overwrite").save(test_path)
            
            # Clean up test
            import shutil
            shutil.rmtree(test_path, ignore_errors=True)
            
            logger.info("✅ Spark session with Delta Lake initialized")
            
        except Exception as e:
            logger.warning(f"Spark/Delta initialization failed, using file fallback: {e}")
            self.spark_session = None
    
    async def write_to_bronze(self, events: List[Dict[str, Any]], job_id: str = None) -> Dict[str, Any]:
        """Write events to Bronze layer"""
        try:
            if not job_id:
                job_id = f"job_{int(datetime.now().timestamp())}"
            
            logger.info(f"💾 Writing {len(events)} events to Bronze layer (job: {job_id})")
            
            if self.spark_session:
                return await self._write_delta_bronze(events, job_id)
            else:
                return await self._write_file_bronze(events, job_id)
            
        except Exception as e:
            logger.error(f"Failed to write to Bronze layer: {e}")
            return {
                "status": "failed",
                "error": str(e),
                "events_written": 0,
                "job_id": job_id
            }
    
    async def _write_delta_bronze(self, events: List[Dict[str, Any]], job_id: str) -> Dict[str, Any]:
        """Write events to Bronze layer using Delta Lake"""
        try:
            # Convert events to Spark DataFrame
            df = self.spark_session.createDataFrame(events)
            
            # Add partitioning columns
            from pyspark.sql.functions import current_date, col, lit
            
            partitioned_df = df.withColumn("partition_date", current_date()) \
                              .withColumn("job_id", lit(job_id)) \
                              .withColumn("bronze_written_at", current_date())
            
            # Write to Delta Lake Bronze layer partitioned by date and event type
            bronze_table_path = str(self.bronze_path / "events_delta")
            
            partitioned_df.write \
                .format("delta") \
                .mode("append") \
                .partitionBy("partition_date", "event_type") \
                .option("path", bronze_table_path) \
                .saveAsTable("bronze.events")
            
            self.delta_stats["bronze_writes"] += 1
            self.delta_stats["last_write_time"] = datetime.now().isoformat()
            
            logger.info(f"✅ Wrote {len(events)} events to Delta Bronze layer")
            
            return {
                "status": "success",
                "storage_type": "delta_lake",
                "events_written": len(events),
                "job_id": job_id,
                "table_path": bronze_table_path,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Delta Bronze write failed: {e}")
            # Fall back to file storage
            return await self._write_file_bronze(events, job_id)
    
    async def _write_file_bronze(self, events: List[Dict[str, Any]], job_id: str) -> Dict[str, Any]:
        """Write events to Bronze layer using file storage (fallback)"""
        try:
            current_date = datetime.now().strftime("%Y-%m-%d")
            
            storage_results = {}
            
            # Group events by type for partitioning
            events_by_type = {}
            for event in events:
                event_type = event.get("event_type", "unknown")
                if event_type not in events_by_type:
                    events_by_type[event_type] = []
                events_by_type[event_type].append(event)
            
            # Write each event type to separate partitioned files
            for event_type, type_events in events_by_type.items():
                # Create partitioned directory structure
                bronze_dir = self.bronze_path / "events_files" / f"event_type={event_type}" / f"date={current_date}"
                bronze_dir.mkdir(parents=True, exist_ok=True)
                
                # Write events as JSONL file
                bronze_file = bronze_dir / f"{job_id}.jsonl"
                
                with open(bronze_file, 'w') as f:
                    for event in type_events:
                        enhanced_event = {
                            **event,
                            "job_id": job_id,
                            "bronze_written_at": datetime.now().isoformat(),
                            "partition_date": current_date
                        }
                        f.write(json.dumps(enhanced_event) + '\n')
                
                storage_results[event_type] = {
                    "file_path": str(bronze_file),
                    "event_count": len(type_events),
                    "file_size_bytes": bronze_file.stat().st_size
                }
                
                logger.info(f"💾 Stored {len(type_events)} {event_type} events to Bronze files")
            
            self.delta_stats["bronze_writes"] += 1
            self.delta_stats["last_write_time"] = datetime.now().isoformat()
            
            return {
                "status": "success",
                "storage_type": "file_system",
                "events_written": len(events),
                "job_id": job_id,
                "storage_results": storage_results,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"File Bronze write failed: {e}")
            return {
                "status": "failed",
                "error": str(e),
                "events_written": 0,
                "job_id": job_id
            }
    
    async def read_from_bronze(self, 
                             event_type: Optional[str] = None,
                             date_filter: Optional[str] = None,
                             limit: int = 1000) -> Dict[str, Any]:
        """Read events from Bronze layer"""
        try:
            if self.spark_session:
                return await self._read_delta_bronze(event_type, date_filter, limit)
            else:
                return await self._read_file_bronze(event_type, date_filter, limit)
                
        except Exception as e:
            logger.error(f"Failed to read from Bronze layer: {e}")
            return {
                "status": "failed",
                "error": str(e),
                "events": []
            }
    
    async def _read_delta_bronze(self, event_type: Optional[str], date_filter: Optional[str], limit: int) -> Dict[str, Any]:
        """Read from Delta Lake Bronze layer"""
        try:
            bronze_table_path = str(self.bronze_path / "events_delta")
            
            # Read Delta table
            df = self.spark_session.read.format("delta").load(bronze_table_path)
            
            # Apply filters
            if event_type:
                df = df.filter(df.event_type == event_type)
            
            if date_filter:
                df = df.filter(df.partition_date == date_filter)
            
            # Apply limit
            df = df.limit(limit)
            
            # Collect results
            events = [row.asDict() for row in df.collect()]
            
            return {
                "status": "success",
                "storage_type": "delta_lake",
                "events": events,
                "count": len(events)
            }
            
        except Exception as e:
            logger.error(f"Delta Bronze read failed: {e}")
            return await self._read_file_bronze(event_type, date_filter, limit)
    
    async def _read_file_bronze(self, event_type: Optional[str], date_filter: Optional[str], limit: int) -> Dict[str, Any]:
        """Read from file-based Bronze layer"""
        try:
            bronze_files_path = self.bronze_path / "events_files"
            events = []
            
            if not bronze_files_path.exists():
                return {
                    "status": "success",
                    "storage_type": "file_system",
                    "events": [],
                    "count": 0
                }
            
            # Find matching files
            for event_type_dir in bronze_files_path.iterdir():
                if event_type_dir.is_dir():
                    # Check event type filter
                    current_event_type = event_type_dir.name.replace("event_type=", "")
                    if event_type and current_event_type != event_type:
                        continue
                    
                    for date_dir in event_type_dir.iterdir():
                        if date_dir.is_dir():
                            # Check date filter
                            current_date = date_dir.name.replace("date=", "")
                            if date_filter and current_date != date_filter:
                                continue
                            
                            # Read JSONL files
                            for file_path in date_dir.glob("*.jsonl"):
                                try:
                                    with open(file_path, 'r') as f:
                                        for line in f:
                                            if len(events) >= limit:
                                                break
                                            
                                            event = json.loads(line.strip())
                                            events.append(event)
                                            
                                    if len(events) >= limit:
                                        break
                                        
                                except Exception as file_error:
                                    logger.warning(f"Failed to read file {file_path}: {file_error}")
                            
                            if len(events) >= limit:
                                break
                        
                        if len(events) >= limit:
                            break
            
            return {
                "status": "success",
                "storage_type": "file_system",
                "events": events,
                "count": len(events)
            }
            
        except Exception as e:
            logger.error(f"File Bronze read failed: {e}")
            return {
                "status": "failed",
                "error": str(e),
                "events": []
            }
    
    def get_delta_stats(self) -> Dict[str, Any]:
        """Get Delta Lake processing statistics"""
        return {
            **self.delta_stats.copy(),
            "storage_paths": {
                "bronze": str(self.bronze_path),
                "silver": str(self.silver_path),
                "gold": str(self.gold_path)
            },
            "spark_available": self.spark_session is not None,
            "timestamp": datetime.now().isoformat()
        }
    
    async def shutdown(self):
        """Shutdown Delta processor"""
        if self.spark_session:
            self.spark_session.stop()
        self.is_running = False
        logger.info("🔌 Delta Processor shutdown complete")
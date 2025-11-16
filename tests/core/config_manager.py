#!/usr/bin/env python
"""
Configuration Manager

This module provides centralized configuration management for the ETL system:
- Configuration validation
- Environment-specific settings
- Performance tuning parameters
- Error handling settings
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass

from utils import readConfig, LoggerManager

logger = LoggerManager.get_logger("ETL_Processing.log")


@dataclass
class SparkConfig:
    """Spark configuration parameters"""
    app_name: str = "Big Data ETL"
    master_url: str = "spark://spark-master:7077"
    executor_cores: int = 1
    executor_memory: str = "1g"
    executor_instances: int = 3
    cores_max: int = 3
    serializer_buffer: str = "64m"
    serializer_buffer_max: str = "512m"
    packages: str = "org.elasticsearch:elasticsearch-spark-30_2.12:7.17.27,com.databricks:spark-xml_2.12:0.13.0,io.delta:delta-spark_2.12:3.3.0"


@dataclass
class ETLConfig:
    """ETL processing configuration"""
    max_attempts: int = 5
    max_items: int = 400
    partitions: int = 600
    compression_algorithm: str = "gzip"
    ipp_psop_prefix: str = "IPP_PSOP"
    traffic_prefix: str = "Traffic"
    enable_validation: bool = True
    enable_performance_monitoring: bool = True
    backup_corrupted_tables: bool = True


@dataclass
class DataLayerConfig:
    """Data layer configuration"""
    bronze_layer: str = "bronze"
    silver_layer: str = "silver"
    gold_layer: str = "aggregated_data"
    warehouse: str = "data"


@dataclass
class PerformanceConfig:
    """Performance tuning configuration"""
    records_per_partition: int = 1000
    enable_adaptive_partitioning: bool = True
    enable_z_order_optimization: bool = True
    auto_optimize_writes: bool = True
    auto_compact_enabled: bool = True
    vacuum_retention_hours: int = 168


@dataclass
class ErrorHandlingConfig:
    """Error handling configuration"""
    max_retries: int = 5
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0
    exponential_backoff: bool = True
    enable_jitter: bool = True
    corruption_patterns: list = None
    
    def __post_init__(self):
        if self.corruption_patterns is None:
            self.corruption_patterns = [
                "sparkfilenotfoundexception",
                "delta_log",
                "file does not exist",
                "deltaanalysisexception", 
                "protocol mismatch",
                "checkpoint",
                "transaction log",
                "concurrent"
            ]


class ConfigManager:
    """
    Centralized configuration management for ETL
    """
    
    def __init__(self, config_dir: Optional[Path] = None):
        self.config_dir = config_dir or Path(__file__).resolve().parent.parent.parent / "config"
        
        # Load all configurations
        self.spark_config = self._load_spark_config()
        self.etl_config = self._load_etl_config()
        self.data_layer_config = self._load_data_layer_config()
        self.performance_config = self._load_performance_config()
        self.error_handling_config = self._load_error_handling_config()
        
        # Validate configurations
        self._validate_configurations()
        
        logger.info("Configuration loaded successfully")
    
    def load_configuration(self) -> Dict[str, Any]:
        """Load and return all configuration as dictionary"""
        return {
            'spark': self.get_spark_conf_dict(),
            'etl': self.etl_config.__dict__,
            'data_layers': self.data_layer_config.__dict__,
            'performance': self.performance_config.__dict__,
            'error_handling': self.error_handling_config.__dict__
        }
    
    def _load_spark_config(self) -> SparkConfig:
        """Load Spark configuration"""
        try:
            spark_xml = readConfig(self.config_dir / "xml/Spark.xml")
            spark_session = spark_xml.get("SparkSession", {})
            
            return SparkConfig(
                app_name=spark_session.get("app_name", "Big Data ETL"),
                master_url=spark_session.get("master_url", "spark://spark-master:7077"),
                executor_cores=int(spark_session.get("spark_executor_cores", 1)),
                executor_memory=spark_session.get("spark_executor_memory", "1g"),
                executor_instances=int(spark_session.get("spark_executor_instances", 3)),
                cores_max=int(spark_session.get("spark_cores_max", 3)),
                serializer_buffer=spark_session.get("spark_kryoserializer_buffer", "64m"),
                serializer_buffer_max=spark_session.get("spark_kryoserializer_buffer_max", "512m"),
                packages=spark_session.get("spark_packages")
            )
        except Exception as e:
            logger.warning(f"Failed to load Spark config: {e}, using defaults")
            return SparkConfig()
    
    def _load_etl_config(self) -> ETLConfig:
        """Load ETL configuration"""
        try:
            etl_xml = readConfig(self.config_dir / "xml/ETL.xml")
            write_config = etl_xml.get("Write", {})
            
            return ETLConfig(
                max_attempts=int(write_config.get("max_attempts", 5)),
                max_items=int(write_config.get("max_items", 400)),
                partitions=int(write_config.get("partitions", 600)),
                compression_algorithm=write_config.get("compression_algorithm", "gzip"),
                ipp_psop_prefix=write_config.get("ipp_psop_prefix", "IPP_PSOP"),
                traffic_prefix=write_config.get("traffic_prefix", "Traffic"),
                enable_validation=etl_xml.get("enable_validation", "true").lower() == "true",
                enable_performance_monitoring=etl_xml.get("enable_performance_monitoring", "true").lower() == "true",
                backup_corrupted_tables=etl_xml.get("backup_corrupted_tables", "true").lower() == "true"
            )
        except Exception as e:
            logger.warning(f"Failed to load ETL config: {e}, using defaults")
            return ETLConfig()
    
    def _load_data_layer_config(self) -> DataLayerConfig:
        """Load data layer configuration"""
        try:
            dml_xml = readConfig(self.config_dir / "DML.xml")
            file_config = dml_xml.get("FILE", {})
            
            return DataLayerConfig(
                bronze_layer=file_config.get("bronze_layer", "bronze"),
                silver_layer=file_config.get("silver_layer", "silver"),
                gold_layer=file_config.get("gold_layer", "aggregated_data"),
                warehouse=file_config.get("warehouse", "data")
            )
        except Exception as e:
            logger.warning(f"Failed to load data layer config: {e}, using defaults")
            return DataLayerConfig()
    
    def _load_performance_config(self) -> PerformanceConfig:
        """Load performance configuration"""
        try:
            # Try to load from performance.yaml if it exists
            perf_config_path = self.config_dir / "performance.yaml"
            if perf_config_path.exists():
                with open(perf_config_path, 'r') as f:
                    perf_data = yaml.safe_load(f)
                
                return PerformanceConfig(
                    records_per_partition=perf_data.get("records_per_partition", 1000),
                    enable_adaptive_partitioning=perf_data.get("enable_adaptive_partitioning", True),
                    enable_z_order_optimization=perf_data.get("enable_z_order_optimization", True),
                    auto_optimize_writes=perf_data.get("auto_optimize_writes", True),
                    auto_compact_enabled=perf_data.get("auto_compact_enabled", True),
                    vacuum_retention_hours=perf_data.get("vacuum_retention_hours", 168)
                )
            else:
                return PerformanceConfig()
        except Exception as e:
            logger.warning(f"Failed to load performance config: {e}, using defaults")
            return PerformanceConfig()
    
    def _load_error_handling_config(self) -> ErrorHandlingConfig:
        """Load error handling configuration"""
        try:
            # Try to load from error_handling.yaml if it exists
            error_config_path = self.config_dir / "error_handling.yaml"
            if error_config_path.exists():
                with open(error_config_path, 'r') as f:
                    error_data = yaml.safe_load(f)
                
                return ErrorHandlingConfig(
                    max_retries=error_data.get("max_retries", 5),
                    base_delay_seconds=error_data.get("base_delay_seconds", 1.0),
                    max_delay_seconds=error_data.get("max_delay_seconds", 30.0),
                    exponential_backoff=error_data.get("exponential_backoff", True),
                    enable_jitter=error_data.get("enable_jitter", True),
                    corruption_patterns=error_data.get("corruption_patterns")
                )
            else:
                return ErrorHandlingConfig()
        except Exception as e:
            logger.warning(f"Failed to load error handling config: {e}, using defaults")
            return ErrorHandlingConfig()
    
    def _validate_configurations(self):
        """Validate all configurations"""
        validation_errors = []
        
        # Validate Spark config
        if self.spark_config.executor_cores < 1:
            validation_errors.append("Spark executor cores must be >= 1")
        
        if self.spark_config.executor_instances < 1:
            validation_errors.append("Spark executor instances must be >= 1")
        
        # Validate ETL config
        if self.etl_config.max_attempts < 1:
            validation_errors.append("ETL max attempts must be >= 1")
        
        if self.etl_config.max_items < 1:
            validation_errors.append("ETL max items must be >= 1")
        
        # Validate performance config
        if self.performance_config.records_per_partition < 1:
            validation_errors.append("Records per partition must be >= 1")
        
        if self.performance_config.vacuum_retention_hours < 0:
            validation_errors.append("Vacuum retention hours must be >= 0")
        
        # Validate error handling config
        if self.error_handling_config.max_retries < 1:
            validation_errors.append("Error handling max retries must be >= 1")
        
        if self.error_handling_config.base_delay_seconds < 0:
            validation_errors.append("Error handling base delay must be >= 0")
        
        if validation_errors:
            error_msg = "Configuration validation failed: " + "; ".join(validation_errors)
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info("All configurations validated successfully")
    
    def get_spark_conf_dict(self) -> Dict[str, str]:
        """Get Spark configuration as dictionary"""
        return {
            "spark.app.name": self.spark_config.app_name,
            "spark.master": self.spark_config.master_url,
            "spark.executor.cores": str(self.spark_config.executor_cores),
            "spark.executor.memory": self.spark_config.executor_memory,
            "spark.executor.instances": str(self.spark_config.executor_instances),
            "spark.cores.max": str(self.spark_config.cores_max),
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.kryoserializer.buffer": self.spark_config.serializer_buffer,
            "spark.kryoserializer.buffer.max": self.spark_config.serializer_buffer_max,
            "spark.jars.packages": self.spark_config.packages,
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.delta.autoCompact.enabled": "true",
            "spark.databricks.delta.autoOptimize.optimizeWrite": str(self.performance_config.auto_optimize_writes).lower(),
            "spark.databricks.delta.autoOptimize.autoCompact": str(self.performance_config.auto_compact_enabled).lower(),
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true"
        }
    
    def get_table_path(self, table_name: str, layer: str = "bronze", prefix: str = None) -> str:
        """Get standardized table path"""
        if layer == "bronze":
            base_path = Path(self.data_layer_config.warehouse) / self.data_layer_config.bronze_layer
            # Add date partitioning for bronze layer
            from datetime import datetime
            today = datetime.now().strftime("%Y-%m-%d")
            if prefix:
                return str(base_path / prefix / today / table_name)
            else:
                return str(base_path / today / table_name)
        elif layer == "silver":
            base_path = Path(self.data_layer_config.warehouse) / self.data_layer_config.silver_layer
            return str(base_path / table_name)
        elif layer == "gold":
            base_path = Path(self.data_layer_config.warehouse) / self.data_layer_config.gold_layer
            return str(base_path / table_name)
        else:
            raise ValueError(f"Unknown layer: {layer}")
    
    def export_config(self) -> Dict[str, Any]:
        """Export all configuration as dictionary"""
        return {
            "spark": {
                "app_name": self.spark_config.app_name,
                "master_url": self.spark_config.master_url,
                "executor_cores": self.spark_config.executor_cores,
                "executor_memory": self.spark_config.executor_memory,
                "executor_instances": self.spark_config.executor_instances,
                "cores_max": self.spark_config.cores_max,
                "packages": self.spark_config.packages
            },
            "etl": {
                "max_attempts": self.etl_config.max_attempts,
                "max_items": self.etl_config.max_items,
                "partitions": self.etl_config.partitions,
                "compression_algorithm": self.etl_config.compression_algorithm,
                "ipp_psop_prefix": self.etl_config.ipp_psop_prefix,
                "traffic_prefix": self.etl_config.traffic_prefix,
                "enable_validation": self.etl_config.enable_validation,
                "enable_performance_monitoring": self.etl_config.enable_performance_monitoring
            },
            "data_layers": {
                "bronze_layer": self.data_layer_config.bronze_layer,
                "silver_layer": self.data_layer_config.silver_layer,
                "gold_layer": self.data_layer_config.gold_layer,
                "warehouse": self.data_layer_config.warehouse
            },
            "performance": {
                "records_per_partition": self.performance_config.records_per_partition,
                "enable_adaptive_partitioning": self.performance_config.enable_adaptive_partitioning,
                "enable_z_order_optimization": self.performance_config.enable_z_order_optimization,
                "auto_optimize_writes": self.performance_config.auto_optimize_writes,
                "auto_compact_enabled": self.performance_config.auto_compact_enabled,
                "vacuum_retention_hours": self.performance_config.vacuum_retention_hours
            },
            "error_handling": {
                "max_retries": self.error_handling_config.max_retries,
                "base_delay_seconds": self.error_handling_config.base_delay_seconds,
                "max_delay_seconds": self.error_handling_config.max_delay_seconds,
                "exponential_backoff": self.error_handling_config.exponential_backoff,
                "enable_jitter": self.error_handling_config.enable_jitter,
                "corruption_patterns": self.error_handling_config.corruption_patterns
            }
        }


# Global configuration instance
_config_manager = None

def get_config_manager() -> ConfigManager:
    """Get global configuration manager instance"""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager()
    return _config_manager

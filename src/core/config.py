"""
Application configuration management.

This module handles all application settings using Pydantic settings
with environment variable support for different deployment environments.
"""

import os
from functools import lru_cache
from typing import List, Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings
from pydantic import AnyHttpUrl, RedisDsn


class Settings(BaseSettings):
    """
    Application settings with environment variable support.
    
    All settings can be overridden via environment variables with
    the prefix STREAMLINEHUB_
    """
    
    # Application
    app_name: str = "StreamLineHub Analytics"
    version: str = "1.0.0"
    debug: bool = Field(default=False, env="DEBUG")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    
    # Server configuration
    host: str = Field(default="0.0.0.0", env="HOST")
    port: int = Field(default=8000, env="PORT")
    workers: int = Field(default=4, env="WORKERS")
    
    # Security
    secret_key: str = Field(..., env="SECRET_KEY")
    algorithm: str = Field(default="HS256", env="ALGORITHM")
    access_token_expire_minutes: int = Field(default=30, env="ACCESS_TOKEN_EXPIRE_MINUTES")
    refresh_token_expire_days: int = Field(default=7, env="REFRESH_TOKEN_EXPIRE_DAYS")
    
    # CORS and security headers
    cors_origins: List[str] = Field(
        default=["http://localhost:3000", "http://localhost:3001"], 
        env="CORS_ORIGINS"
    )
    allowed_hosts: Optional[List[str]] = Field(default=None, env="ALLOWED_HOSTS")
    
    # MongoDB configuration
    mongodb_url: str = Field(default="mongodb://localhost:27017", env="MONGODB_URL")
    mongodb_database: str = Field(default="streamlinehub", env="MONGODB_DATABASE")
    mongodb_max_connections: int = Field(default=100, env="MONGODB_MAX_CONNECTIONS")
    mongodb_min_connections: int = Field(default=10, env="MONGODB_MIN_CONNECTIONS")
    
    # Redis configuration
    redis_url: RedisDsn = Field(..., env="REDIS_URL")
    redis_db: int = Field(default=0, env="REDIS_DB")
    redis_max_connections: int = Field(default=100, env="REDIS_MAX_CONNECTIONS")
    
    # Kafka configuration
    kafka_bootstrap_servers: str = Field(..., env="KAFKA_BOOTSTRAP_SERVERS")
    kafka_group_id: str = Field(default="streamlinehub-analytics", env="KAFKA_GROUP_ID")
    kafka_auto_offset_reset: str = Field(default="latest", env="KAFKA_AUTO_OFFSET_RESET")
    
    # Spark configuration
    spark_master: str = Field(default="local[*]", env="SPARK_MASTER")
    spark_app_name: str = Field(default="StreamLineHub Analytics", env="SPARK_APP_NAME")
    spark_driver_memory: str = Field(default="2g", env="SPARK_DRIVER_MEMORY")
    spark_executor_memory: str = Field(default="2g", env="SPARK_EXECUTOR_MEMORY")
    
    # Delta Lake configuration
    delta_lake_path: str = Field(..., env="DELTA_LAKE_PATH")
    checkpoint_location: str = Field(..., env="CHECKPOINT_LOCATION")
    
    # Elasticsearch configuration
    elasticsearch_url: str = Field(..., env="ELASTICSEARCH_URL")
    elasticsearch_index_prefix: str = Field(
        default="streamlinehub",
        env="ELASTICSEARCH_INDEX_PREFIX"
    )
    
    # Monitoring and metrics
    enable_metrics: bool = Field(default=True, env="ENABLE_METRICS")
    metrics_port: int = Field(default=9090, env="METRICS_PORT")
    
    # Rate limiting
    rate_limit_requests: int = Field(default=100, env="RATE_LIMIT_REQUESTS")
    rate_limit_window: int = Field(default=60, env="RATE_LIMIT_WINDOW")
    
    # File storage
    file_storage_path: str = Field(default="./storage", env="FILE_STORAGE_PATH")
    max_file_size: int = Field(default=10 * 1024 * 1024, env="MAX_FILE_SIZE")  # 10MB
    
    # Email configuration (for notifications)
    smtp_host: Optional[str] = Field(default=None, env="SMTP_HOST")
    smtp_port: Optional[int] = Field(default=587, env="SMTP_PORT")
    smtp_user: Optional[str] = Field(default=None, env="SMTP_USER")
    smtp_password: Optional[str] = Field(default=None, env="SMTP_PASSWORD")
    smtp_tls: bool = Field(default=True, env="SMTP_TLS")
    
    # External API keys
    openai_api_key: Optional[str] = Field(default=None, env="OPENAI_API_KEY")
    sendgrid_api_key: Optional[str] = Field(default=None, env="SENDGRID_API_KEY")
    
    # Feature flags
    enable_ml_inference: bool = Field(default=True, env="ENABLE_ML_INFERENCE")
    enable_real_time_processing: bool = Field(default=True, env="ENABLE_REAL_TIME_PROCESSING")
    enable_email_notifications: bool = Field(default=True, env="ENABLE_EMAIL_NOTIFICATIONS")
    
    @field_validator("cors_origins", mode='before')
    @classmethod
    def assemble_cors_origins(cls, v):
        """Parse CORS origins from environment variable."""
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)
    
    @field_validator("allowed_hosts", mode='before')
    @classmethod
    def assemble_allowed_hosts(cls, v):
        """Parse allowed hosts from environment variable."""
        if isinstance(v, str):
            return [i.strip() for i in v.split(",")]
        return v
    
    @field_validator("file_storage_path")
    @classmethod
    def create_storage_directory(cls, v):
        """Ensure file storage directory exists."""
        if not os.path.exists(v):
            os.makedirs(v, exist_ok=True)
        return v
    
    model_config = {
        "env_prefix": "STREAMLINEHUB_",
        "env_file": ".env",
        "case_sensitive": False
    }


class DevelopmentSettings(Settings):
    """Development environment settings."""
    debug: bool = True
    log_level: str = "DEBUG"
    secret_key: str = "dev_secret_key_change_in_production"
    
    # Development defaults
    mongodb_url: str = "mongodb://host.docker.internal:27017"
    mongodb_database: str = "streamlinehub_dev"
    redis_url: RedisDsn = "redis://localhost:6379/0"
    kafka_bootstrap_servers: str = "localhost:9093"
    delta_lake_path: str = "./data/delta-lake"
    checkpoint_location: str = "./data/checkpoints"
    elasticsearch_url: str = "http://localhost:9200"


class TestingSettings(Settings):
    """Testing environment settings."""
    debug: bool = True
    log_level: str = "WARNING"
    
    # Test database (separate MongoDB database)
    mongodb_url: str = "mongodb://host.docker.internal:27017"
    mongodb_database: str = "streamlinehub_test"
    redis_url: RedisDsn = "redis://localhost:6379/1"
    
    # Disable external services in tests
    enable_ml_inference: bool = False
    enable_real_time_processing: bool = False
    enable_email_notifications: bool = False


class ProductionSettings(Settings):
    """Production environment settings."""
    debug: bool = False
    log_level: str = "INFO"
    
    # Production requires all settings from environment
    # No defaults for sensitive production values


@lru_cache()
def get_settings() -> Settings:
    """
    Get application settings based on environment.
    
    Returns cached settings instance based on ENVIRONMENT variable.
    """
    environment = os.getenv("ENVIRONMENT", "development").lower()
    
    if environment == "development":
        return DevelopmentSettings()
    elif environment == "testing":
        return TestingSettings()
    elif environment == "production":
        return ProductionSettings()
    else:
        # Default to development
        return DevelopmentSettings()


# Convenience function for common usage
settings = get_settings()
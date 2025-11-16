#!/usr/bin/env python
"""
Minimal test runner for StreamLineHub API
Runs the API with mock configurations for testing purposes
"""

import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

# Set minimal environment variables
os.environ["STREAMLINEHUB_SECRET_KEY"] = "test_secret_key_for_development"
os.environ["STREAMLINEHUB_DATABASE_URL"] = "postgresql://user:pass@localhost:5432/testdb"
os.environ["STREAMLINEHUB_REDIS_URL"] = "redis://localhost:6379/0"
os.environ["STREAMLINEHUB_KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
os.environ["STREAMLINEHUB_DELTA_LAKE_PATH"] = "./data/delta"
os.environ["STREAMLINEHUB_CHECKPOINT_LOCATION"] = "./data/checkpoints"
import os
import sys
import asyncio
import uvicorn
os.environ["STREAMLINEHUB_ELASTICSEARCH_URL"] = "http://localhost:9200"
os.environ["STREAMLINEHUB_DEBUG"] = "true"
os.environ["STREAMLINEHUB_LOG_LEVEL"] = "INFO"

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Mock database and cache connections
async def mock_init_database():
    print("Mock: Database initialization skipped (test mode)")
    pass

async def mock_init_cache():
    print("Mock: Cache initialization skipped (test mode)")
    pass

async def mock_close_database():
    print("Mock: Database connections closed")
    pass

async def mock_close_cache():
    print("Mock: Cache connections closed")
    pass

# Apply mocks before importing the app
import src.core.database
import src.core.cache
src.core.database.init_database = mock_init_database
src.core.database.close_database_connections = mock_close_database
src.core.cache.init_cache = mock_init_cache
src.core.cache.close_cache_connections = mock_close_cache

if __name__ == "__main__":
    import uvicorn
    
    print("=" * 60)
    print("StreamLineHub API - Test Mode")
    print("=" * 60)
    print("\nStarting API server on http://localhost:8001")
    print("API Docs: http://localhost:8001/docs")
    print("Health Check: http://localhost:8001/health")
    print("\nPress CTRL+C to stop\n")
    
    try:
        uvicorn.run(
            "src.main:app",
            host="0.0.0.0",
            port=8001,
            reload=True,
            log_level="info"
        )
    except KeyboardInterrupt:
        print("\n\nShutting down...")
        sys.exit(0)

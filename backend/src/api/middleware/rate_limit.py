"""
Rate limiting middleware for API protection.

This middleware implements rate limiting based on user ID, IP address,
and API endpoints to prevent abuse and ensure fair usage.
"""

import asyncio
import time
from typing import Dict, Tuple, Optional
from collections import defaultdict

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
import structlog

from src.core.cache import get_cache
from src.core.config import get_settings
from src.core.logging_config import security_logger


logger = structlog.get_logger()


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Rate limiting middleware using sliding window algorithm.
    
    Implements rate limiting with different limits for authenticated
    vs anonymous users, and different limits per endpoint type.
    """
    
    def __init__(self, app):
        super().__init__(app)
        self.settings = get_settings()
        
        # Rate limit configurations (requests per minute)
        self.rate_limits = {
            # Default limits
            "anonymous": {"requests": 100, "window": 60},
            "authenticated": {"requests": 1000, "window": 60},
            
            # Endpoint-specific limits
            "auth": {"requests": 20, "window": 60},  # Login/register endpoints
            "upload": {"requests": 10, "window": 60},  # File upload endpoints
            "ml": {"requests": 50, "window": 60},  # ML inference endpoints
        }
        
        # Endpoints that have special rate limiting
        self.endpoint_limits = {
            "/api/v1/auth/login": "auth",
            "/api/v1/auth/register": "auth",
            "/api/v1/auth/password-reset": "auth",
            "/api/v1/ml/": "ml",  # All ML endpoints
            "/upload": "upload"
        }
        
        # In-memory rate limit store (fallback if Redis unavailable)
        self._memory_store = defaultdict(list)
        self._memory_lock = asyncio.Lock()
    
    async def dispatch(self, request: Request, call_next):
        """
        Process request through rate limiting middleware.
        
        Args:
            request: FastAPI request object
            call_next: Next middleware/endpoint in chain
            
        Returns:
            Response or rate limit error
        """
        # Skip rate limiting for health checks and static files
        if self._should_skip_rate_limiting(request.url.path):
            return await call_next(request)
        
        try:
            # Get rate limiting identifier and limits
            identifier, limit_config = await self._get_rate_limit_config(request)
            
            # Check rate limit
            is_allowed, remaining, reset_time = await self._check_rate_limit(
                identifier, limit_config
            )
            
            if not is_allowed:
                # Log rate limit exceeded
                security_logger.log_rate_limit_exceeded(
                    identifier=identifier,
                    endpoint=request.url.path,
                    limit=limit_config["requests"],
                    window=f"{limit_config['window']}s",
                    ip_address=self._get_client_ip(request)
                )
                
                return self._create_rate_limit_error(
                    limit_config["requests"],
                    limit_config["window"],
                    reset_time
                )
            
            # Process request
            response = await call_next(request)
            
            # Add rate limit headers
            response.headers["X-RateLimit-Limit"] = str(limit_config["requests"])
            response.headers["X-RateLimit-Remaining"] = str(remaining)
            response.headers["X-RateLimit-Reset"] = str(reset_time)
            
            return response
            
        except Exception as e:
            logger.error(f"Rate limiting middleware error: {e}")
            # Continue without rate limiting if there's an error
            return await call_next(request)
    
    def _should_skip_rate_limiting(self, path: str) -> bool:
        """
        Check if path should skip rate limiting.
        
        Args:
            path: Request path
            
        Returns:
            True if should skip, False otherwise
        """
        skip_paths = {
            "/health", "/ready", "/live", "/startup",
            "/metrics", "/metrics/basic", "/version",
            "/docs", "/redoc", "/openapi.json"
        }
        
        skip_prefixes = ["/static/", "/assets/"]
        
        if path in skip_paths:
            return True
        
        for prefix in skip_prefixes:
            if path.startswith(prefix):
                return True
        
        return False
    
    async def _get_rate_limit_config(self, request: Request) -> Tuple[str, Dict]:
        """
        Get rate limiting identifier and configuration.
        
        Args:
            request: FastAPI request object
            
        Returns:
            Tuple of (identifier, limit_config)
        """
        # Determine endpoint-specific limits
        endpoint_type = "default"
        for endpoint_path, limit_type in self.endpoint_limits.items():
            if request.url.path.startswith(endpoint_path):
                endpoint_type = limit_type
                break
        
        # Get user information if authenticated
        user_id = getattr(request.state, "user_id", None)
        
        if user_id:
            # Authenticated user - use user ID as identifier
            identifier = f"user:{user_id}"
            base_config = self.rate_limits["authenticated"]
        else:
            # Anonymous user - use IP address as identifier
            ip_address = self._get_client_ip(request)
            identifier = f"ip:{ip_address}"
            base_config = self.rate_limits["anonymous"]
        
        # Apply endpoint-specific limits if they're more restrictive
        if endpoint_type != "default" and endpoint_type in self.rate_limits:
            endpoint_config = self.rate_limits[endpoint_type]
            limit_config = {
                "requests": min(base_config["requests"], endpoint_config["requests"]),
                "window": endpoint_config["window"]
            }
        else:
            limit_config = base_config.copy()
        
        return identifier, limit_config
    
    async def _check_rate_limit(
        self, 
        identifier: str, 
        limit_config: Dict
    ) -> Tuple[bool, int, int]:
        """
        Check if request is within rate limits.
        
        Args:
            identifier: Rate limit identifier
            limit_config: Rate limit configuration
            
        Returns:
            Tuple of (is_allowed, remaining_requests, reset_time)
        """
        try:
            # Try to use Redis cache first
            cache = get_cache()
            return await self._check_rate_limit_redis(identifier, limit_config, cache)
        except Exception:
            # Fallback to in-memory storage
            return await self._check_rate_limit_memory(identifier, limit_config)
    
    async def _check_rate_limit_redis(
        self, 
        identifier: str, 
        limit_config: Dict, 
        cache
    ) -> Tuple[bool, int, int]:
        """
        Check rate limit using Redis sliding window.
        
        Args:
            identifier: Rate limit identifier
            limit_config: Rate limit configuration
            cache: Cache manager instance
            
        Returns:
            Tuple of (is_allowed, remaining_requests, reset_time)
        """
        current_time = int(time.time())
        window = limit_config["window"]
        max_requests = limit_config["requests"]
        
        # Redis key for the sliding window
        key = f"rate_limit:{identifier}:{current_time // window}"
        
        # Get current count and increment
        current_count = await cache.increment(key, 1)
        
        # Set expiration if this is the first request in the window
        if current_count == 1:
            await cache.expire(key, window)
        
        # Calculate remaining requests and reset time
        remaining = max(0, max_requests - current_count)
        reset_time = ((current_time // window) + 1) * window
        
        is_allowed = current_count <= max_requests
        
        return is_allowed, remaining, reset_time
    
    async def _check_rate_limit_memory(
        self, 
        identifier: str, 
        limit_config: Dict
    ) -> Tuple[bool, int, int]:
        """
        Check rate limit using in-memory sliding window.
        
        Args:
            identifier: Rate limit identifier
            limit_config: Rate limit configuration
            
        Returns:
            Tuple of (is_allowed, remaining_requests, reset_time)
        """
        async with self._memory_lock:
            current_time = time.time()
            window = limit_config["window"]
            max_requests = limit_config["requests"]
            
            # Clean old entries
            cutoff_time = current_time - window
            requests = self._memory_store[identifier]
            self._memory_store[identifier] = [
                req_time for req_time in requests if req_time > cutoff_time
            ]
            
            # Add current request
            self._memory_store[identifier].append(current_time)
            
            # Check if within limit
            current_count = len(self._memory_store[identifier])
            remaining = max(0, max_requests - current_count)
            reset_time = int(current_time + window)
            
            is_allowed = current_count <= max_requests
            
            return is_allowed, remaining, reset_time
    
    def _get_client_ip(self, request: Request) -> str:
        """
        Get client IP address from request.
        
        Args:
            request: FastAPI request object
            
        Returns:
            Client IP address
        """
        # Check for forwarded headers
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()
        
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip
        
        if request.client:
            return request.client.host
        
        return "unknown"
    
    def _create_rate_limit_error(
        self, 
        limit: int, 
        window: int, 
        reset_time: int
    ) -> JSONResponse:
        """
        Create rate limit exceeded error response.
        
        Args:
            limit: Request limit
            window: Time window in seconds
            reset_time: Reset time timestamp
            
        Returns:
            JSON error response
        """
        return JSONResponse(
            status_code=429,
            content={
                "detail": "Rate limit exceeded",
                "type": "rate_limit_error",
                "limit": limit,
                "window": window,
                "retry_after": reset_time - int(time.time())
            },
            headers={
                "X-RateLimit-Limit": str(limit),
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Reset": str(reset_time),
                "Retry-After": str(reset_time - int(time.time()))
            }
        )
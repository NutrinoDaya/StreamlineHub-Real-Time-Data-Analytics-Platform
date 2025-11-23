"""
Logging middleware for request/response tracking.

This middleware provides structured logging of all HTTP requests
and responses with timing, user context, and error tracking.
"""

import time
import uuid
from typing import Dict, Any

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
import structlog

from src.core.logging_config import request_logger


logger = structlog.get_logger()


class LoggingMiddleware(BaseHTTPMiddleware):
    """
    Request/response logging middleware.
    
    Logs all HTTP requests and responses with structured data
    including timing, user context, and error information.
    """
    
    def __init__(self, app):
        super().__init__(app)
        self.request_logger = request_logger
        
        # Paths to exclude from detailed logging
        self.exclude_paths = {
            "/health",
            "/ready", 
            "/live",
            "/startup",
            "/metrics",
            "/metrics/basic"
        }
    
    async def dispatch(self, request: Request, call_next):
        """
        Process request through logging middleware.
        
        Args:
            request: FastAPI request object
            call_next: Next middleware/endpoint in chain
            
        Returns:
            Response with logging side effects
        """
        # Generate unique request ID
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        
        # Add request ID to context variables for structured logging
        try:
            import contextvars
            ctx = contextvars.copy_context()
            ctx.run(lambda: setattr(ctx, "request_id", request_id))
        except Exception:
            pass  # Context vars might not be available
        
        start_time = time.time()
        
        # Log request start (skip for health check endpoints)
        if not self._should_skip_logging(request.url.path):
            await self._log_request_start(request, request_id)
        
        # Process request
        response = None
        error = None
        
        try:
            response = await call_next(request)
            return response
            
        except Exception as e:
            error = str(e)
            logger.error(
                "Request processing error",
                request_id=request_id,
                method=request.method,
                path=request.url.path,
                error=error,
                exc_info=e
            )
            raise
            
        finally:
            # Log request completion
            if not self._should_skip_logging(request.url.path):
                duration_ms = (time.time() - start_time) * 1000
                await self._log_request_end(
                    request, 
                    response, 
                    request_id, 
                    duration_ms, 
                    error
                )
    
    async def _log_request_start(self, request: Request, request_id: str):
        """
        Log the start of a request.
        
        Args:
            request: FastAPI request object
            request_id: Unique request identifier
        """
        # Extract user context if available
        user_id = getattr(request.state, "user_id", None)
        
        # Get query parameters (exclude sensitive data)
        query_params = dict(request.query_params) if request.query_params else None
        query_params = self._filter_sensitive_params(query_params) if query_params else None
        
        # Get safe headers (exclude sensitive data)
        headers = self._get_safe_headers(dict(request.headers))
        
        self.request_logger.log_request_start(
            method=request.method,
            path=request.url.path,
            query_params=query_params,
            headers=headers,
            user_id=user_id,
            request_id=request_id
        )
    
    async def _log_request_end(
        self, 
        request: Request, 
        response: Response, 
        request_id: str,
        duration_ms: float, 
        error: str = None
    ):
        """
        Log the completion of a request.
        
        Args:
            request: FastAPI request object
            response: FastAPI response object
            request_id: Unique request identifier
            duration_ms: Request duration in milliseconds
            error: Error message if request failed
        """
        # Extract user context if available
        user_id = getattr(request.state, "user_id", None)
        
        # Get response size if available
        response_size = None
        if response and hasattr(response, "headers"):
            content_length = response.headers.get("content-length")
            if content_length:
                try:
                    response_size = int(content_length)
                except ValueError:
                    pass
        
        # Get status code
        status_code = response.status_code if response else 500
        
        self.request_logger.log_request_end(
            method=request.method,
            path=request.url.path,
            status_code=status_code,
            duration_ms=duration_ms,
            response_size=response_size,
            user_id=user_id,
            request_id=request_id,
            error=error
        )
    
    def _should_skip_logging(self, path: str) -> bool:
        """
        Check if path should skip detailed logging.
        
        Args:
            path: Request path
            
        Returns:
            True if should skip, False otherwise
        """
        return path in self.exclude_paths or path.startswith("/static/")
    
    def _filter_sensitive_params(self, params: Dict[str, str]) -> Dict[str, str]:
        """
        Filter sensitive parameters from query string.
        
        Args:
            params: Query parameters dictionary
            
        Returns:
            Filtered parameters dictionary
        """
        sensitive_keys = {
            "password", "token", "secret", "key", "auth", 
            "api_key", "access_token", "refresh_token"
        }
        
        filtered = {}
        for key, value in params.items():
            if any(sensitive in key.lower() for sensitive in sensitive_keys):
                filtered[key] = "***"
            else:
                filtered[key] = value
        
        return filtered
    
    def _get_safe_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        """
        Get safe headers for logging (exclude sensitive data).
        
        Args:
            headers: Request headers dictionary
            
        Returns:
            Safe headers dictionary
        """
        # Headers to include in logs
        safe_headers = {
            "content-type", "content-length", "user-agent", 
            "accept", "accept-language", "accept-encoding",
            "x-forwarded-for", "x-real-ip", "x-request-id"
        }
        
        # Headers to exclude (sensitive)
        sensitive_headers = {
            "authorization", "cookie", "x-api-key", 
            "x-auth-token", "proxy-authorization"
        }
        
        filtered = {}
        for key, value in headers.items():
            key_lower = key.lower()
            
            if key_lower in sensitive_headers:
                # Mask sensitive headers
                if key_lower == "authorization" and value.startswith("Bearer "):
                    filtered[key] = "Bearer ***"
                else:
                    filtered[key] = "***"
            elif key_lower in safe_headers:
                filtered[key] = value
        
        return filtered
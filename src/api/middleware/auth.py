"""
Authentication middleware for request processing.

This middleware handles JWT token validation, user authentication,
and request context setup for authenticated endpoints.
"""

import time
from typing import Optional, Dict, Any
from urllib.parse import urlparse

from fastapi import Request, Response, HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.security.utils import get_authorization_scheme_param
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
import structlog

from src.core.security import get_security_manager, AuthenticationError
from src.core.logging_config import security_logger


logger = structlog.get_logger()


class AuthMiddleware(BaseHTTPMiddleware):
    """
    Authentication middleware for JWT token validation.
    
    Handles JWT token extraction, validation, and user context
    setup for protected endpoints. Skips authentication for
    public endpoints and health checks.
    """
    
    def __init__(self, app):
        super().__init__(app)
        self.security_manager = get_security_manager()
        
        # Endpoints that don't require authentication
        self.public_paths = {
            "/docs",
            "/redoc", 
            "/openapi.json",
            "/health",
            "/ready",
            "/live",
            "/startup",
            "/metrics",
            "/metrics/basic",
            "/version",
            "/api/v1/auth/register",
            "/api/v1/auth/login",
            "/api/v1/auth/refresh",
            "/api/v1/auth/password-reset",
            "/api/v1/auth/password-reset/confirm",
            "/api/v1/analytics/realtime",
            "/api/v1/events/batch",
            "/api/v1/events/single",
            "/api/v1/events/stats"
        }
        
        # Paths that start with these prefixes are public
        self.public_prefixes = [
            "/static/",
            "/assets/",
            "/favicon.ico",
            "/api/v1/events/"  # All events endpoints are public for high-throughput ingestion
        ]
    
    async def dispatch(self, request: Request, call_next):
        """
        Process request through authentication middleware.
        
        Args:
            request: FastAPI request object
            call_next: Next middleware/endpoint in chain
            
        Returns:
            Response from next middleware or authentication error
        """
        start_time = time.time()
        
        # Check if path requires authentication
        if self._is_public_path(request.url.path):
            response = await call_next(request)
            return response
        
        try:
            # Extract and validate token
            token = self._extract_token(request)
            if not token:
                return self._create_auth_error("Missing authentication token")
            
            # Validate JWT token
            try:
                payload = self.security_manager.verify_access_token(token)
            except AuthenticationError as e:
                security_logger.log_authentication_attempt(
                    username="unknown",
                    ip_address=self._get_client_ip(request),
                    user_agent=request.headers.get("user-agent", ""),
                    success=False,
                    reason=str(e)
                )
                return self._create_auth_error(str(e))
            
            # Add user context to request state
            request.state.user_id = payload.get("user_id")
            request.state.user_email = payload.get("email")
            request.state.user_role = payload.get("role")
            request.state.token_payload = payload
            
            # Add to context variables for logging
            try:
                import contextvars
                contextvars.copy_context().run(
                    lambda: setattr(contextvars.copy_context(), "user_id", payload.get("user_id"))
                )
            except Exception:
                pass  # Context vars might not be available
            
            # Process request
            response = await call_next(request)
            
            # Log successful authenticated request
            duration_ms = (time.time() - start_time) * 1000
            logger.info(
                "Authenticated request completed",
                method=request.method,
                path=request.url.path,
                user_id=payload.get("user_id"),
                status_code=response.status_code,
                duration_ms=duration_ms
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Authentication middleware error: {e}")
            return self._create_auth_error("Authentication failed")
    
    def _is_public_path(self, path: str) -> bool:
        """
        Check if the request path is public (no auth required).
        
        Args:
            path: Request path
            
        Returns:
            True if path is public, False otherwise
        """
        # Check exact matches
        if path in self.public_paths:
            return True
        
        # Check prefix matches
        for prefix in self.public_prefixes:
            if path.startswith(prefix):
                return True
        
        return False
    
    def _extract_token(self, request: Request) -> Optional[str]:
        """
        Extract JWT token from Authorization header.
        
        Args:
            request: FastAPI request object
            
        Returns:
            JWT token string or None if not found
        """
        authorization = request.headers.get("Authorization")
        if not authorization:
            return None
        
        scheme, token = get_authorization_scheme_param(authorization)
        if scheme.lower() != "bearer":
            return None
        
        return token
    
    def _get_client_ip(self, request: Request) -> str:
        """
        Get client IP address from request headers.
        
        Checks X-Forwarded-For and X-Real-IP headers for
        proxy/load balancer scenarios.
        
        Args:
            request: FastAPI request object
            
        Returns:
            Client IP address string
        """
        # Check for forwarded headers (load balancer/proxy)
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            # Take the first IP in the chain
            return forwarded_for.split(",")[0].strip()
        
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip
        
        # Fallback to direct connection
        if request.client:
            return request.client.host
        
        return "unknown"
    
    def _create_auth_error(self, detail: str) -> JSONResponse:
        """
        Create authentication error response.
        
        Args:
            detail: Error detail message
            
        Returns:
            JSON error response
        """
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={
                "detail": detail,
                "type": "authentication_error"
            },
            headers={"WWW-Authenticate": "Bearer"}
        )


# Security scheme for dependency injection
security = HTTPBearer()
security_manager = get_security_manager()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> Dict[str, Any]:
    """
    Dependency to get current authenticated user from JWT token.
    
    Args:
        credentials: HTTP Bearer authorization credentials
        
    Returns:
        Dictionary containing user information from JWT payload
        
    Raises:
        HTTPException: If token is invalid or user not found
    """
    try:
        # Verify the JWT token
        payload = security_manager.verify_access_token(credentials.credentials)
        
        # Return user information
        return {
            "user_id": payload.get("user_id"),
            "email": payload.get("email"), 
            "role": payload.get("role", "user"),
            "username": payload.get("username"),
            "permissions": payload.get("permissions", [])
        }
        
    except AuthenticationError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"}
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"}
        )
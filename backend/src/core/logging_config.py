"""
Structured logging configuration for StreamLineHub Analytics.

This module sets up structured logging using structlog with proper
formatting, error tracking, and integration with monitoring systems.
"""

import logging
import logging.config
import sys
from typing import Any, Dict

import structlog
from structlog.processors import JSONRenderer, TimeStamper
from structlog.stdlib import add_logger_name, add_log_level, PositionalArgumentsFormatter


def setup_logging(log_level: str = "INFO") -> None:
    """
    Setup structured logging configuration.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper(), logging.INFO),
    )
    
    # Configure structlog
    structlog.configure(
        processors=[
            # Add logger name and level to log records
            add_logger_name,
            add_log_level,
            # Add timestamp
            TimeStamper(fmt="ISO", utc=True),
            # Handle positional arguments
            PositionalArgumentsFormatter(),
            # Add request context processor
            add_request_context,
            # Filter sensitive data
            filter_sensitive_data,
            # JSON output for structured logging
            JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Set levels for noisy libraries
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("fastapi").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("aiokafka").setLevel(logging.WARNING)


def add_request_context(logger, method_name: str, event_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Add request context to log entries.
    
    Extracts request information from context variables and adds them
    to the log record for better traceability.
    """
    # Try to get request context from context variables
    try:
        from contextvars import copy_context
        ctx = copy_context()
        
        # Add request ID if available
        request_id = ctx.get("request_id", None)
        if request_id:
            event_dict["request_id"] = request_id
        
        # Add user ID if available
        user_id = ctx.get("user_id", None)
        if user_id:
            event_dict["user_id"] = user_id
        
        # Add session ID if available
        session_id = ctx.get("session_id", None)
        if session_id:
            event_dict["session_id"] = session_id
        
        # Add trace ID for distributed tracing
        trace_id = ctx.get("trace_id", None)
        if trace_id:
            event_dict["trace_id"] = trace_id
            
    except Exception:
        # Context variables might not be available in all environments
        pass
    
    return event_dict


def filter_sensitive_data(logger, method_name: str, event_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Filter sensitive data from log entries.
    
    Removes or masks sensitive information like passwords, tokens,
    and personal data to prevent security leaks in logs.
    """
    sensitive_fields = {
        "password", "token", "secret", "key", "auth", "authorization",
        "cookie", "session", "ssn", "social_security", "credit_card",
        "api_key", "access_token", "refresh_token", "jwt", "bearer"
    }
    
    def mask_value(key: str, value: Any) -> Any:
        """Mask sensitive values."""
        if isinstance(key, str) and any(field in key.lower() for field in sensitive_fields):
            if isinstance(value, str) and len(value) > 4:
                return f"***{value[-4:]}"
            else:
                return "***"
        return value
    
    def clean_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively clean dictionary of sensitive data."""
        if not isinstance(data, dict):
            return data
        
        cleaned = {}
        for key, value in data.items():
            if isinstance(value, dict):
                cleaned[key] = clean_dict(value)
            elif isinstance(value, list):
                cleaned[key] = [clean_dict(item) if isinstance(item, dict) else mask_value(key, item) for item in value]
            else:
                cleaned[key] = mask_value(key, value)
        
        return cleaned
    
    # Clean the main event dict
    for key, value in list(event_dict.items()):
        if isinstance(value, dict):
            event_dict[key] = clean_dict(value)
        else:
            event_dict[key] = mask_value(key, value)
    
    return event_dict


class RequestLogger:
    """
    Request logging utility for tracking API requests.
    
    Provides structured logging for HTTP requests with timing,
    status codes, and error tracking.
    """
    
    def __init__(self):
        self.logger = structlog.get_logger("api.requests")
    
    def log_request_start(
        self,
        method: str,
        path: str,
        query_params: Dict[str, Any] = None,
        headers: Dict[str, str] = None,
        user_id: int = None,
        request_id: str = None
    ) -> None:
        """
        Log the start of an API request.
        
        Args:
            method: HTTP method
            path: Request path
            query_params: Query parameters
            headers: Request headers (sensitive data will be filtered)
            user_id: User ID if authenticated
            request_id: Unique request identifier
        """
        self.logger.info(
            "Request started",
            method=method,
            path=path,
            query_params=query_params,
            headers=headers,
            user_id=user_id,
            request_id=request_id,
            event_type="request_start"
        )
    
    def log_request_end(
        self,
        method: str,
        path: str,
        status_code: int,
        duration_ms: float,
        response_size: int = None,
        user_id: int = None,
        request_id: str = None,
        error: str = None
    ) -> None:
        """
        Log the completion of an API request.
        
        Args:
            method: HTTP method
            path: Request path
            status_code: HTTP status code
            duration_ms: Request duration in milliseconds
            response_size: Response size in bytes
            user_id: User ID if authenticated
            request_id: Unique request identifier
            error: Error message if request failed
        """
        log_level = "error" if status_code >= 500 else "warning" if status_code >= 400 else "info"
        
        getattr(self.logger, log_level)(
            "Request completed",
            method=method,
            path=path,
            status_code=status_code,
            duration_ms=duration_ms,
            response_size=response_size,
            user_id=user_id,
            request_id=request_id,
            error=error,
            event_type="request_end"
        )


class BusinessLogger:
    """
    Business logic logging utility.
    
    Provides structured logging for business events, user actions,
    and system operations for audit and monitoring purposes.
    """
    
    def __init__(self):
        self.logger = structlog.get_logger("business")
    
    def log_user_action(
        self,
        action: str,
        user_id: int,
        resource_type: str = None,
        resource_id: str = None,
        details: Dict[str, Any] = None,
        success: bool = True,
        error: str = None
    ) -> None:
        """
        Log user actions for audit trail.
        
        Args:
            action: Action performed (create, read, update, delete, etc.)
            user_id: User performing the action
            resource_type: Type of resource affected
            resource_id: ID of affected resource
            details: Additional action details
            success: Whether action was successful
            error: Error message if action failed
        """
        self.logger.info(
            "User action",
            action=action,
            user_id=user_id,
            resource_type=resource_type,
            resource_id=resource_id,
            details=details,
            success=success,
            error=error,
            event_type="user_action"
        )
    
    def log_system_event(
        self,
        event: str,
        component: str,
        details: Dict[str, Any] = None,
        success: bool = True,
        error: str = None
    ) -> None:
        """
        Log system events for monitoring and debugging.
        
        Args:
            event: Event name
            component: System component generating the event
            details: Event details
            success: Whether event was successful
            error: Error message if event failed
        """
        log_level = "error" if not success else "info"
        
        getattr(self.logger, log_level)(
            "System event",
            event=event,
            component=component,
            details=details,
            success=success,
            error=error,
            event_type="system_event"
        )
    
    def log_data_processing(
        self,
        operation: str,
        dataset: str,
        records_processed: int = None,
        duration_seconds: float = None,
        success: bool = True,
        error: str = None
    ) -> None:
        """
        Log data processing operations.
        
        Args:
            operation: Processing operation (ingest, transform, aggregate, etc.)
            dataset: Dataset name or identifier
            records_processed: Number of records processed
            duration_seconds: Processing duration
            success: Whether operation was successful
            error: Error message if operation failed
        """
        log_level = "error" if not success else "info"
        
        getattr(self.logger, log_level)(
            "Data processing",
            operation=operation,
            dataset=dataset,
            records_processed=records_processed,
            duration_seconds=duration_seconds,
            success=success,
            error=error,
            event_type="data_processing"
        )
    
    def log_ml_operation(
        self,
        operation: str,
        model_name: str,
        model_version: str = None,
        metrics: Dict[str, float] = None,
        success: bool = True,
        error: str = None
    ) -> None:
        """
        Log machine learning operations.
        
        Args:
            operation: ML operation (training, inference, evaluation, etc.)
            model_name: Name of the model
            model_version: Version of the model
            metrics: Performance metrics
            success: Whether operation was successful
            error: Error message if operation failed
        """
        log_level = "error" if not success else "info"
        
        getattr(self.logger, log_level)(
            "ML operation",
            operation=operation,
            model_name=model_name,
            model_version=model_version,
            metrics=metrics,
            success=success,
            error=error,
            event_type="ml_operation"
        )


class SecurityLogger:
    """
    Security-focused logging utility.
    
    Provides logging for security events, authentication attempts,
    authorization failures, and potential security threats.
    """
    
    def __init__(self):
        self.logger = structlog.get_logger("security")
    
    def log_authentication_attempt(
        self,
        username: str,
        ip_address: str,
        user_agent: str = None,
        success: bool = True,
        reason: str = None
    ) -> None:
        """
        Log authentication attempts.
        
        Args:
            username: Username or email used for authentication
            ip_address: Client IP address
            user_agent: Client user agent
            success: Whether authentication was successful
            reason: Reason for failure if unsuccessful
        """
        log_level = "warning" if not success else "info"
        
        getattr(self.logger, log_level)(
            "Authentication attempt",
            username=username,
            ip_address=ip_address,
            user_agent=user_agent,
            success=success,
            reason=reason,
            event_type="authentication"
        )
    
    def log_authorization_failure(
        self,
        user_id: int,
        action: str,
        resource: str,
        required_permissions: str,
        ip_address: str = None
    ) -> None:
        """
        Log authorization failures.
        
        Args:
            user_id: User attempting the action
            action: Action that was attempted
            resource: Resource that was accessed
            required_permissions: Permissions required for the action
            ip_address: Client IP address
        """
        self.logger.warning(
            "Authorization failure",
            user_id=user_id,
            action=action,
            resource=resource,
            required_permissions=required_permissions,
            ip_address=ip_address,
            event_type="authorization_failure"
        )
    
    def log_suspicious_activity(
        self,
        activity_type: str,
        description: str,
        user_id: int = None,
        ip_address: str = None,
        details: Dict[str, Any] = None,
        severity: str = "medium"
    ) -> None:
        """
        Log suspicious security activities.
        
        Args:
            activity_type: Type of suspicious activity
            description: Description of the activity
            user_id: User ID if applicable
            ip_address: Source IP address
            details: Additional details
            severity: Severity level (low, medium, high, critical)
        """
        log_level = "critical" if severity == "critical" else "error" if severity == "high" else "warning"
        
        getattr(self.logger, log_level)(
            "Suspicious activity detected",
            activity_type=activity_type,
            description=description,
            user_id=user_id,
            ip_address=ip_address,
            details=details,
            severity=severity,
            event_type="suspicious_activity"
        )
    
    def log_rate_limit_exceeded(
        self,
        identifier: str,
        endpoint: str,
        limit: int,
        window: str,
        ip_address: str = None
    ) -> None:
        """
        Log rate limit violations.
        
        Args:
            identifier: Client identifier (user ID, API key, IP)
            endpoint: API endpoint that was rate limited
            limit: Rate limit that was exceeded
            window: Time window for the rate limit
            ip_address: Client IP address
        """
        self.logger.warning(
            "Rate limit exceeded",
            identifier=identifier,
            endpoint=endpoint,
            limit=limit,
            window=window,
            ip_address=ip_address,
            event_type="rate_limit_exceeded"
        )


# Global logger instances
request_logger = RequestLogger()
business_logger = BusinessLogger()
security_logger = SecurityLogger()


# Utility functions
def get_logger(name: str = None) -> structlog.stdlib.BoundLogger:
    """
    Get a structured logger instance.
    
    Args:
        name: Logger name (defaults to caller module)
        
    Returns:
        Configured structlog logger
    """
    return structlog.get_logger(name)


def log_exception(logger: structlog.stdlib.BoundLogger, exc: Exception, context: Dict[str, Any] = None) -> None:
    """
    Log an exception with full context.
    
    Args:
        logger: Logger instance
        exc: Exception to log
        context: Additional context information
    """
    logger.error(
        "Exception occurred",
        error=str(exc),
        error_type=type(exc).__name__,
        context=context or {},
        exc_info=exc
    )
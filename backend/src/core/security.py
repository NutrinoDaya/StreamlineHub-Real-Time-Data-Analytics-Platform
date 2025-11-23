"""
Authentication and authorization utilities.

This module provides JWT token handling, password hashing,
and user authentication/authorization for the StreamLineHub Analytics platform.
"""

import hashlib
import secrets
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Union

import bcrypt
import jwt
from fastapi import HTTPException, status
from passlib.context import CryptContext
import structlog

from src.core.config import get_settings


logger = structlog.get_logger()

# Password hashing context with bcrypt configuration
pwd_context = CryptContext(
    schemes=["bcrypt"], 
    deprecated="auto",
    bcrypt__rounds=12,
    bcrypt__ident="2b"
)

# JWT algorithm
ALGORITHM = "HS256"


class AuthenticationError(Exception):
    """Raised when authentication fails."""
    pass


class AuthorizationError(Exception):
    """Raised when user lacks required permissions."""
    pass


class SecurityManager:
    """
    Security manager for authentication and authorization operations.
    
    Handles JWT tokens, password hashing, and security utilities
    for the StreamLineHub Analytics platform.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.secret_key = self.settings.secret_key
        self.algorithm = self.settings.algorithm
        self.access_token_expire_minutes = self.settings.access_token_expire_minutes
        self.refresh_token_expire_days = self.settings.refresh_token_expire_days
    
    # Password operations
    def hash_password(self, password: str) -> str:
        """
        Hash a password using bcrypt directly.
        
        Args:
            password: Plain text password
            
        Returns:
            Hashed password string
        """
        # Debug: Log password details
        password_bytes = password.encode('utf-8')
        logger.info(f"Hashing password: length={len(password)}, bytes_length={len(password_bytes)}, password='{password}'")
        
        # bcrypt has a maximum password length of 72 bytes
        # Truncate if necessary to avoid errors
        if len(password_bytes) > 72:
            password_bytes = password_bytes[:72]
            logger.info(f"Truncated password bytes length to: {len(password_bytes)}")
        
        try:
            # Use bcrypt directly to avoid PassLib issues
            salt = bcrypt.gensalt()
            hashed = bcrypt.hashpw(password_bytes, salt)
            result = hashed.decode('utf-8')
            logger.info("Password hashed successfully with bcrypt")
            return result
        except Exception as e:
            logger.error(f"Password hashing failed: {e}")
            raise
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """
        Verify a password against its hash.
        
        Args:
            plain_password: Plain text password to verify
            hashed_password: Stored password hash
            
        Returns:
            True if password matches, False otherwise
        """
        try:
            # Use bcrypt directly for verification
            password_bytes = plain_password.encode('utf-8')
            if len(password_bytes) > 72:
                password_bytes = password_bytes[:72]
            return bcrypt.checkpw(password_bytes, hashed_password.encode('utf-8'))
        except Exception as e:
            logger.error(f"Direct bcrypt verification failed: {e}")
            try:
                # Fallback to PassLib if the hash was created with it
                return pwd_context.verify(plain_password, hashed_password)
            except Exception as e2:
                logger.error(f"PassLib verification also failed: {e2}")
                return False
    
    def generate_password_reset_token(self, email: str) -> str:
        """
        Generate a password reset token.
        
        Args:
            email: User email address
            
        Returns:
            Password reset token
        """
        data = {
            "email": email,
            "type": "password_reset",
            "exp": datetime.utcnow() + timedelta(hours=1),
            "iat": datetime.utcnow(),
        }
        return jwt.encode(data, self.secret_key, algorithm=self.algorithm)
    
    def verify_password_reset_token(self, token: str) -> Optional[str]:
        """
        Verify and decode password reset token.
        
        Args:
            token: Password reset token
            
        Returns:
            Email if token is valid, None otherwise
        """
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            if payload.get("type") != "password_reset":
                return None
            return payload.get("email")
        except jwt.PyJWTError as e:
            logger.warning(f"Invalid password reset token: {e}")
            return None
    
    # JWT Token operations
    def create_access_token(
        self, 
        data: Dict[str, Any], 
        expires_delta: Optional[timedelta] = None
    ) -> str:
        """
        Create a JWT access token.
        
        Args:
            data: Token payload data
            expires_delta: Custom expiration delta
            
        Returns:
            Encoded JWT token
        """
        to_encode = data.copy()
        
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=self.access_token_expire_minutes)
        
        to_encode.update({
            "exp": expire,
            "iat": datetime.utcnow(),
            "type": "access"
        })
        
        encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
        return encoded_jwt
    
    def create_refresh_token(
        self, 
        data: Dict[str, Any], 
        expires_delta: Optional[timedelta] = None
    ) -> str:
        """
        Create a JWT refresh token.
        
        Args:
            data: Token payload data
            expires_delta: Custom expiration delta
            
        Returns:
            Encoded JWT refresh token
        """
        to_encode = data.copy()
        
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(days=self.refresh_token_expire_days)
        
        to_encode.update({
            "exp": expire,
            "iat": datetime.utcnow(),
            "type": "refresh"
        })
        
        encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
        return encoded_jwt
    
    def decode_token(self, token: str) -> Dict[str, Any]:
        """
        Decode and validate JWT token.
        
        Args:
            token: JWT token to decode
            
        Returns:
            Token payload
            
        Raises:
            AuthenticationError: If token is invalid
        """
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            return payload
        except jwt.ExpiredSignatureError:
            raise AuthenticationError("Token has expired")
        except jwt.JWTError as e:
            raise AuthenticationError(f"Token validation failed: {str(e)}")
    
    def refresh_access_token(self, refresh_token: str) -> str:
        """
        Create new access token from refresh token.
        
        Args:
            refresh_token: Valid refresh token
            
        Returns:
            New access token
            
        Raises:
            AuthenticationError: If refresh token is invalid
        """
        payload = self.decode_token(refresh_token)
        
        if payload.get("type") != "refresh":
            raise AuthenticationError("Invalid token type for refresh")
        
        # Create new access token with user data
        user_data = {
            "user_id": payload.get("user_id"),
            "email": payload.get("email"),
            "role": payload.get("role"),
        }
        
        return self.create_access_token(user_data)
    
    def verify_access_token(self, token: str) -> Dict[str, Any]:
        """
        Verify access token and extract user information.
        
        Args:
            token: Access token to verify
            
        Returns:
            User information from token
            
        Raises:
            AuthenticationError: If token is invalid
        """
        payload = self.decode_token(token)
        
        if payload.get("type") != "access":
            raise AuthenticationError("Invalid token type")
        
        return payload
    
    # API Key operations
    def generate_api_key(self, user_id: int, purpose: str = "api_access") -> str:
        """
        Generate API key for service authentication.
        
        Args:
            user_id: User ID
            purpose: API key purpose/description
            
        Returns:
            Generated API key
        """
        # Create a unique API key
        key_data = f"{user_id}:{purpose}:{datetime.utcnow().isoformat()}:{secrets.token_hex(16)}"
        api_key = hashlib.sha256(key_data.encode()).hexdigest()
        return f"streamlinehub_{api_key}"
    
    def verify_api_key(self, api_key: str) -> bool:
        """
        Verify API key format and structure.
        
        Args:
            api_key: API key to verify
            
        Returns:
            True if format is valid, False otherwise
        """
        if not api_key.startswith("streamlinehub_"):
            return False
        
        key_part = api_key.replace("streamlinehub_", "")
        if len(key_part) != 64:  # SHA256 hex length
            return False
        
        try:
            int(key_part, 16)  # Check if it's valid hex
            return True
        except ValueError:
            return False
    
    # Session security
    def generate_session_id(self) -> str:
        """
        Generate secure session ID.
        
        Returns:
            Random session ID
        """
        return secrets.token_urlsafe(32)
    
    def generate_csrf_token(self) -> str:
        """
        Generate CSRF protection token.
        
        Returns:
            Random CSRF token
        """
        return secrets.token_urlsafe(32)
    
    # Permission checking
    def check_permissions(
        self, 
        user_role: str, 
        required_permissions: Union[str, list], 
        resource_owner_id: Optional[int] = None, 
        user_id: Optional[int] = None
    ) -> bool:
        """
        Check if user has required permissions.
        
        Args:
            user_role: User's role
            required_permissions: Required permission(s)
            resource_owner_id: ID of resource owner (for ownership checks)
            user_id: Current user ID
            
        Returns:
            True if user has permissions, False otherwise
        """
        # Role hierarchy
        role_hierarchy = {
            "admin": 100,
            "manager": 80,
            "analyst": 60,
            "viewer": 40,
            "user": 20,
            "guest": 0
        }
        
        # Permission requirements
        permission_requirements = {
            "read": 20,
            "write": 40,
            "delete": 60,
            "admin": 80,
            "super_admin": 100
        }
        
        user_level = role_hierarchy.get(user_role, 0)
        
        # Handle single permission or list of permissions
        if isinstance(required_permissions, str):
            required_permissions = [required_permissions]
        
        # Check if user meets any of the required permissions
        for permission in required_permissions:
            required_level = permission_requirements.get(permission, 100)
            
            # Admin bypass
            if user_level >= 100:
                return True
            
            # Resource ownership check
            if resource_owner_id and user_id and resource_owner_id == user_id:
                # Resource owners can read/write their own resources
                if permission in ["read", "write"]:
                    return True
            
            # Check role-based permission
            if user_level >= required_level:
                return True
        
        return False
    
    def require_permissions(
        self, 
        user_role: str, 
        required_permissions: Union[str, list],
        resource_owner_id: Optional[int] = None,
        user_id: Optional[int] = None
    ) -> None:
        """
        Require permissions or raise authorization error.
        
        Args:
            user_role: User's role
            required_permissions: Required permission(s)
            resource_owner_id: ID of resource owner
            user_id: Current user ID
            
        Raises:
            AuthorizationError: If user lacks required permissions
        """
        if not self.check_permissions(
            user_role, required_permissions, resource_owner_id, user_id
        ):
            raise AuthorizationError(
                f"Insufficient permissions. Required: {required_permissions}, "
                f"User role: {user_role}"
            )
    
    # Security utilities
    def mask_sensitive_data(self, data: str, mask_char: str = "*", show_last: int = 4) -> str:
        """
        Mask sensitive data for logging/display.
        
        Args:
            data: Sensitive data to mask
            mask_char: Character to use for masking
            show_last: Number of characters to show at end
            
        Returns:
            Masked string
        """
        if len(data) <= show_last:
            return mask_char * len(data)
        
        return mask_char * (len(data) - show_last) + data[-show_last:]
    
    def generate_secure_filename(self, filename: str) -> str:
        """
        Generate secure filename by removing dangerous characters.
        
        Args:
            filename: Original filename
            
        Returns:
            Secure filename
        """
        import re
        import os
        
        # Remove path components
        filename = os.path.basename(filename)
        
        # Remove dangerous characters
        filename = re.sub(r'[^\w\-_\.]', '', filename)
        
        # Prevent hidden files
        if filename.startswith('.'):
            filename = 'file' + filename
        
        # Ensure we have a filename
        if not filename:
            filename = f"file_{secrets.token_hex(8)}"
        
        return filename
    
    def validate_password_strength(self, password: str) -> Dict[str, Any]:
        """
        Validate password strength.
        
        Args:
            password: Password to validate
            
        Returns:
            Validation result with score and feedback
        """
        score = 0
        feedback = []
        
        # Length check
        if len(password) >= 8:
            score += 2
        else:
            feedback.append("Password should be at least 8 characters long")
        
        if len(password) >= 12:
            score += 1
        
        # Character variety
        if any(c.islower() for c in password):
            score += 1
        else:
            feedback.append("Include lowercase letters")
        
        if any(c.isupper() for c in password):
            score += 1
        else:
            feedback.append("Include uppercase letters")
        
        if any(c.isdigit() for c in password):
            score += 1
        else:
            feedback.append("Include numbers")
        
        if any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in password):
            score += 2
        else:
            feedback.append("Include special characters")
        
        # Common patterns
        common_patterns = ["123", "abc", "password", "admin", "user"]
        if any(pattern in password.lower() for pattern in common_patterns):
            score -= 2
            feedback.append("Avoid common patterns")
        
        strength_levels = {
            0: "Very Weak",
            1: "Very Weak", 
            2: "Weak",
            3: "Fair",
            4: "Good",
            5: "Strong",
            6: "Very Strong",
            7: "Excellent",
            8: "Excellent"
        }
        
        return {
            "score": max(0, min(8, score)),
            "strength": strength_levels.get(max(0, min(8, score)), "Unknown"),
            "is_strong": score >= 5,
            "feedback": feedback
        }


# Global security manager instance
security_manager = SecurityManager()


# FastAPI dependency
def get_security_manager() -> SecurityManager:
    """FastAPI dependency for security manager."""
    return security_manager
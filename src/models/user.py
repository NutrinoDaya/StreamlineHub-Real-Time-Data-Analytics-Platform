""""""

User model for MongoDB document storage.User model for authentication and user management.



This module defines the User Pydantic model for MongoDB document operationsThis module defines the User model with authentication fields,

with authentication fields, profile information, and validation.profile information, and relationship management.

""""""



from datetime import datetimefrom datetime import datetime

from typing import Optional, Dict, Anyfrom typing import Optional

from enum import Enum

from sqlalchemy import (

from pydantic import BaseModel, Field, EmailStr, validator    Boolean, Column, DateTime, Integer, String, Text, 

from bson import ObjectId    ForeignKey, Index, UniqueConstraint

)

from sqlalchemy.orm import relationship

class PyObjectId(ObjectId):from sqlalchemy.sql import func

    """Custom ObjectId class for Pydantic compatibility."""

    from src.core.database import Base

    @classmethod

    def __get_validators__(cls):

        yield cls.validateclass User(Base):

    """

    @classmethod    User model for authentication and profile management.

    def validate(cls, v):    

        if not ObjectId.is_valid(v):    Stores user account information, authentication data,

            raise ValueError("Invalid objectid")    and profile details for the StreamLineHub Analytics platform.

        return ObjectId(v)    """

    

    @classmethod    __tablename__ = "users"

    def __modify_schema__(cls, field_schema):    

        field_schema.update(type="string")    # Primary key

    id = Column(Integer, primary_key=True, index=True)

    

class UserRole(str, Enum):    # Authentication fields

    """User role enumeration."""    email = Column(String(255), unique=True, nullable=False, index=True)

    USER = "user"    password_hash = Column(String(255), nullable=False)

    ADMIN = "admin"    is_active = Column(Boolean, default=True, nullable=False)

    MANAGER = "manager"    is_verified = Column(Boolean, default=False, nullable=False)

    ANALYST = "analyst"    

    # Profile information

    full_name = Column(String(255), nullable=False)

class UserTheme(str, Enum):    company = Column(String(255), nullable=True)

    """User theme enumeration."""    role = Column(String(50), default="user", nullable=False)

    LIGHT = "light"    avatar_url = Column(String(500), nullable=True)

    DARK = "dark"    bio = Column(Text, nullable=True)

    AUTO = "auto"    

    # Preferences

    timezone = Column(String(50), default="UTC", nullable=False)

class UserBase(BaseModel):    language = Column(String(10), default="en", nullable=False)

    """Base user model with common fields."""    theme = Column(String(20), default="light", nullable=False)

        

    # Authentication fields    # Email verification

    email: EmailStr    email_verification_token = Column(String(255), nullable=True)

    full_name: str = Field(..., min_length=1, max_length=255)    email_verified_at = Column(DateTime(timezone=True), nullable=True)

    is_active: bool = Field(default=True)    

    is_verified: bool = Field(default=False)    # Password reset

        password_reset_token = Column(String(255), nullable=True)

    # Profile information    password_reset_expires = Column(DateTime(timezone=True), nullable=True)

    company: Optional[str] = Field(None, max_length=255)    

    role: UserRole = Field(default=UserRole.USER)    # Activity tracking

    avatar_url: Optional[str] = Field(None, max_length=500)    last_login = Column(DateTime(timezone=True), nullable=True)

    bio: Optional[str] = Field(None, max_length=1000)    login_count = Column(Integer, default=0, nullable=False)

        failed_login_attempts = Column(Integer, default=0, nullable=False)

    # Preferences    last_failed_login = Column(DateTime(timezone=True), nullable=True)

    timezone: str = Field(default="UTC", max_length=50)    account_locked_until = Column(DateTime(timezone=True), nullable=True)

    language: str = Field(default="en", max_length=10)    

    theme: UserTheme = Field(default=UserTheme.LIGHT)    # Timestamps

        created_at = Column(

    # Notification preferences        DateTime(timezone=True), 

    email_notifications: bool = Field(default=True)        server_default=func.now(), 

    push_notifications: bool = Field(default=True)        nullable=False

    marketing_emails: bool = Field(default=False)    )

        updated_at = Column(

    # Security settings        DateTime(timezone=True), 

    two_factor_enabled: bool = Field(default=False)        server_default=func.now(), 

    password_changed_at: Optional[datetime] = None        onupdate=func.now(), 

            nullable=False

    # Activity tracking    )

    last_login_at: Optional[datetime] = None    deleted_at = Column(DateTime(timezone=True), nullable=True)

    last_seen_at: Optional[datetime] = None    

    login_count: int = Field(default=0)    # Relationships

        customers = relationship("Customer", back_populates="created_by_user", cascade="all, delete-orphan")

    # Metadata    campaigns = relationship("Campaign", back_populates="created_by_user", cascade="all, delete-orphan")

    tags: Optional[Dict[str, Any]] = Field(default=None)    api_keys = relationship("APIKey", back_populates="user", cascade="all, delete-orphan")

    custom_fields: Optional[Dict[str, Any]] = Field(default=None)    user_sessions = relationship("UserSession", back_populates="user", cascade="all, delete-orphan")

    

    # Indexes

class UserCreate(UserBase):    __table_args__ = (

    """User creation model."""        Index("idx_users_email_active", "email", "is_active"),

            Index("idx_users_role", "role"),

    password: str = Field(..., min_length=8, max_length=100)        Index("idx_users_created_at", "created_at"),

            Index("idx_users_last_login", "last_login"),

    @validator('password')    )

    def validate_password(cls, v):    

        if len(v) < 8:    def __repr__(self):

            raise ValueError('Password must be at least 8 characters long')        return f"<User(id={self.id}, email='{self.email}', role='{self.role}')>"

        if not any(c.isupper() for c in v):    

            raise ValueError('Password must contain at least one uppercase letter')    @property

        if not any(c.islower() for c in v):    def is_admin(self) -> bool:

            raise ValueError('Password must contain at least one lowercase letter')        """Check if user has admin role."""

        if not any(c.isdigit() for c in v):        return self.role in ["admin", "super_admin"]

            raise ValueError('Password must contain at least one digit')    

        return v    @property 

    def is_manager(self) -> bool:

        """Check if user has manager role or higher."""

class UserUpdate(BaseModel):        return self.role in ["admin", "super_admin", "manager"]

    """User update model with optional fields."""    

        @property

    full_name: Optional[str] = Field(None, min_length=1, max_length=255)    def is_locked(self) -> bool:

    company: Optional[str] = Field(None, max_length=255)        """Check if user account is currently locked."""

    avatar_url: Optional[str] = Field(None, max_length=500)        if not self.account_locked_until:

    bio: Optional[str] = Field(None, max_length=1000)            return False

    timezone: Optional[str] = Field(None, max_length=50)        return datetime.utcnow() < self.account_locked_until

    language: Optional[str] = Field(None, max_length=10)    

    theme: Optional[UserTheme] = None    def to_dict(self, include_sensitive: bool = False) -> dict:

    email_notifications: Optional[bool] = None        """

    push_notifications: Optional[bool] = None        Convert user to dictionary representation.

    marketing_emails: Optional[bool] = None        

    tags: Optional[Dict[str, Any]] = None        Args:

    custom_fields: Optional[Dict[str, Any]] = None            include_sensitive: Whether to include sensitive fields

            

        Returns:

class UserPasswordUpdate(BaseModel):            Dictionary representation of user

    """User password update model."""        """

            data = {

    current_password: str            "id": self.id,

    new_password: str = Field(..., min_length=8, max_length=100)            "email": self.email,

                "full_name": self.full_name,

    @validator('new_password')            "company": self.company,

    def validate_new_password(cls, v):            "role": self.role,

        if len(v) < 8:            "avatar_url": self.avatar_url,

            raise ValueError('Password must be at least 8 characters long')            "bio": self.bio,

        if not any(c.isupper() for c in v):            "timezone": self.timezone,

            raise ValueError('Password must contain at least one uppercase letter')            "language": self.language,

        if not any(c.islower() for c in v):            "theme": self.theme,

            raise ValueError('Password must contain at least one lowercase letter')            "is_active": self.is_active,

        if not any(c.isdigit() for c in v):            "is_verified": self.is_verified,

            raise ValueError('Password must contain at least one digit')            "last_login": self.last_login.isoformat() if self.last_login else None,

        return v            "login_count": self.login_count,

            "created_at": self.created_at.isoformat(),

            "updated_at": self.updated_at.isoformat(),

class User(UserBase):        }

    """Complete user model for database storage."""        

            if include_sensitive:

    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")            data.update({

    password_hash: str                "failed_login_attempts": self.failed_login_attempts,

                    "last_failed_login": self.last_failed_login.isoformat() if self.last_failed_login else None,

    # Timestamps                "account_locked_until": self.account_locked_until.isoformat() if self.account_locked_until else None,

    created_at: datetime = Field(default_factory=datetime.utcnow)                "email_verification_token": self.email_verification_token,

    updated_at: datetime = Field(default_factory=datetime.utcnow)                "email_verified_at": self.email_verified_at.isoformat() if self.email_verified_at else None,

                })

    class Config:        

        """Pydantic config for MongoDB compatibility."""        return data

        allow_population_by_field_name = True

        arbitrary_types_allowed = True

        json_encoders = {ObjectId: str}class APIKey(Base):

        schema_extra = {    """

            "example": {    API Key model for programmatic access.

                "email": "user@example.com",    

                "full_name": "John Doe",    Stores API keys for service-to-service communication

                "company": "Acme Corp",    and programmatic access to the platform.

                "role": "user",    """

                "timezone": "UTC",    

                "language": "en",    __tablename__ = "api_keys"

                "theme": "light",    

                "is_active": True,    # Primary key

                "is_verified": False    id = Column(Integer, primary_key=True, index=True)

            }    

        }    # API key information

    key_id = Column(String(32), unique=True, nullable=False, index=True)

    key_hash = Column(String(255), nullable=False)

class UserResponse(UserBase):    name = Column(String(255), nullable=False)

    """User response model (excludes sensitive fields)."""    description = Column(Text, nullable=True)

        

    id: PyObjectId = Field(alias="_id")    # Permissions and scopes

    created_at: datetime    scopes = Column(String(1000), nullable=False, default="read")

    updated_at: datetime    is_active = Column(Boolean, default=True, nullable=False)

        

    class Config:    # Usage tracking

        allow_population_by_field_name = True    last_used = Column(DateTime(timezone=True), nullable=True)

        arbitrary_types_allowed = True    usage_count = Column(Integer, default=0, nullable=False)

        json_encoders = {ObjectId: str}    

    # Expiration

    expires_at = Column(DateTime(timezone=True), nullable=True)

class UserLogin(BaseModel):    

    """User login model."""    # Foreign keys

        user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)

    email: EmailStr    

    password: str    # Timestamps

    created_at = Column(

        DateTime(timezone=True), 

class UserRegister(UserCreate):        server_default=func.now(), 

    """User registration model."""        nullable=False

        )

    confirm_password: str    updated_at = Column(

    accept_terms: bool = Field(..., description="Must accept terms and conditions")        DateTime(timezone=True), 

            server_default=func.now(), 

    @validator('confirm_password')        onupdate=func.now(), 

    def passwords_match(cls, v, values, **kwargs):        nullable=False

        if 'password' in values and v != values['password']:    )

            raise ValueError('Passwords do not match')    

        return v    # Relationships

        user = relationship("User", back_populates="api_keys")

    @validator('accept_terms')    

    def must_accept_terms(cls, v):    # Indexes

        if not v:    __table_args__ = (

            raise ValueError('You must accept the terms and conditions')        Index("idx_api_keys_user_id", "user_id"),

        return v        Index("idx_api_keys_active", "is_active"),

        Index("idx_api_keys_expires", "expires_at"),

    )

class UserStats(BaseModel):    

    """User statistics model."""    def __repr__(self):

            return f"<APIKey(id={self.id}, key_id='{self.key_id}', user_id={self.user_id})>"

    total_logins: int    

    last_login: Optional[datetime]    @property

    days_since_registration: int    def is_expired(self) -> bool:

    profile_completion: float  # Percentage        """Check if API key has expired."""

    activity_score: int        if not self.expires_at:

            return False

        return datetime.utcnow() > self.expires_at

class UserActivity(BaseModel):    

    """User activity tracking model."""    def to_dict(self) -> dict:

            """Convert API key to dictionary representation."""

    user_id: PyObjectId        return {

    action: str            "id": self.id,

    resource: Optional[str] = None            "key_id": self.key_id,

    resource_id: Optional[str] = None            "name": self.name,

    metadata: Optional[Dict[str, Any]] = None            "description": self.description,

    timestamp: datetime = Field(default_factory=datetime.utcnow)            "scopes": self.scopes.split(",") if self.scopes else [],

    ip_address: Optional[str] = None            "is_active": self.is_active,

    user_agent: Optional[str] = None            "last_used": self.last_used.isoformat() if self.last_used else None,
            "usage_count": self.usage_count,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


class UserSession(Base):
    """
    User session model for tracking active sessions.
    
    Stores information about user sessions for security
    monitoring and session management.
    """
    
    __tablename__ = "user_sessions"
    
    # Primary key
    id = Column(Integer, primary_key=True, index=True)
    
    # Session information
    session_id = Column(String(255), unique=True, nullable=False, index=True)
    ip_address = Column(String(45), nullable=True)  # IPv6 compatible
    user_agent = Column(Text, nullable=True)
    device_info = Column(Text, nullable=True)
    
    # Session tracking
    is_active = Column(Boolean, default=True, nullable=False)
    last_activity = Column(DateTime(timezone=True), nullable=True)
    expires_at = Column(DateTime(timezone=True), nullable=True)
    
    # Security information
    login_method = Column(String(50), default="password", nullable=False)
    two_factor_verified = Column(Boolean, default=False, nullable=False)
    
    # Foreign keys
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    
    # Timestamps
    created_at = Column(
        DateTime(timezone=True), 
        server_default=func.now(), 
        nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True), 
        server_default=func.now(), 
        onupdate=func.now(), 
        nullable=False
    )
    ended_at = Column(DateTime(timezone=True), nullable=True)
    
    # Relationships
    user = relationship("User", back_populates="user_sessions")
    
    # Indexes
    __table_args__ = (
        Index("idx_user_sessions_user_id", "user_id"),
        Index("idx_user_sessions_active", "is_active"),
        Index("idx_user_sessions_expires", "expires_at"),
        Index("idx_user_sessions_ip", "ip_address"),
    )
    
    def __repr__(self):
        return f"<UserSession(id={self.id}, session_id='{self.session_id}', user_id={self.user_id})>"
    
    @property
    def is_expired(self) -> bool:
        """Check if session has expired."""
        if not self.expires_at:
            return False
        return datetime.utcnow() > self.expires_at
    
    def to_dict(self) -> dict:
        """Convert session to dictionary representation."""
        return {
            "id": self.id,
            "session_id": self.session_id,
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
            "device_info": self.device_info,
            "is_active": self.is_active,
            "last_activity": self.last_activity.isoformat() if self.last_activity else None,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "login_method": self.login_method,
            "two_factor_verified": self.two_factor_verified,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
        }
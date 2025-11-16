""""""

User service for MongoDB user management and authentication.User service for user management and authentication.



This service handles user creation, authentication, profile management,This service handles user creation, authentication, profile management,

and user-related business logic using MongoDB operations.and user-related business logic for the StreamLineHub Analytics platform.

""""""



from datetime import datetime, timedeltafrom datetime import datetime, timedelta

from typing import Optional, List, Dict, Anyfrom typing import Optional, List, Dict, Any

import logging

from sqlalchemy import and_, or_, desc

from bson import ObjectIdfrom sqlalchemy.ext.asyncio import AsyncSession

from pymongo.errors import DuplicateKeyErrorfrom sqlalchemy.future import select

import structlogfrom sqlalchemy.orm import selectinload

import structlog

from src.models.user import (

    User, UserCreate, UserUpdate, UserResponse, UserLogin, from src.models.user import User, APIKey, UserSession

    UserRegister, UserPasswordUpdate, UserRolefrom src.core.security import get_security_manager, SecurityManager

)from src.core.logging_config import business_logger

from src.core.database import get_users_collection

from src.core.security import get_security_manager

logger = structlog.get_logger()

logger = structlog.get_logger()



class UserService:

class UserService:    """

    """    User service for managing user accounts and authentication.

    User service for managing user accounts and authentication with MongoDB.    

        Provides high-level operations for user management including

    Provides high-level operations for user management including    registration, authentication, profile updates, and access control.

    registration, authentication, profile updates, and access control.    """

    """    

        def __init__(self, session: AsyncSession):

    def __init__(self):        self.session = session

        self.users_collection = get_users_collection()        self.security_manager = get_security_manager()

        self.security_manager = get_security_manager()    

        async def create_user(

    async def create_user(self, user_data: UserCreate) -> User:        self,

        """        email: str,

        Create a new user account.        password: str,

                full_name: str,

        Args:        company: Optional[str] = None,

            user_data: User creation data        role: str = "user"

                ) -> User:

        Returns:        """

            Created user object        Create a new user account.

                    

        Raises:        Args:

            ValueError: If user already exists            email: User email address

        """            password: Plain text password

        try:            full_name: User's full name

            # Check if user already exists            company: Company name (optional)

            existing_user = await self.users_collection.find_one(            role: User role (default: user)

                {"email": user_data.email}            

            )        Returns:

            if existing_user:            Created User instance

                raise ValueError(f"User with email {user_data.email} already exists")            

                    Raises:

            # Hash password            ValueError: If email already exists or invalid data

            password_hash = self.security_manager.get_password_hash(user_data.password)        """

                    # Check if user already exists

            # Create user document        existing_user = await self.get_user_by_email(email)

            user_dict = user_data.dict(exclude={"password"})        if existing_user:

            user_dict.update({            raise ValueError(f"User with email {email} already exists")

                "password_hash": password_hash,        

                "created_at": datetime.utcnow(),        # Validate role

                "updated_at": datetime.utcnow()        valid_roles = ["user", "analyst", "manager", "admin", "super_admin"]

            })        if role not in valid_roles:

                        raise ValueError(f"Invalid role: {role}. Must be one of {valid_roles}")

            # Insert user        

            result = await self.users_collection.insert_one(user_dict)        # Hash password

                    password_hash = self.security_manager.hash_password(password)

            # Retrieve and return created user        

            user_doc = await self.users_collection.find_one({"_id": result.inserted_id})        # Create user

                    user = User(

            logger.info("User created successfully", email=user_data.email, user_id=str(result.inserted_id))            email=email.lower().strip(),

                        password_hash=password_hash,

            return User(**user_doc)            full_name=full_name.strip(),

                        company=company.strip() if company else None,

        except DuplicateKeyError:            role=role,

            raise ValueError(f"User with email {user_data.email} already exists")            is_active=True,

        except Exception as e:            is_verified=False,  # Requires email verification

            logger.error("Failed to create user", email=user_data.email, error=str(e))        )

            raise        

        self.session.add(user)

    async def authenticate_user(self, email: str, password: str) -> Optional[User]:        await self.session.commit()

        """        await self.session.refresh(user)

        Authenticate user with email and password.        

                business_logger.log_user_action(

        Args:            action="create_user",

            email: User email            user_id=user.id,

            password: Plain text password            details={"email": email, "role": role},

                        success=True

        Returns:        )

            User object if authentication successful, None otherwise        

        """        return user

        try:    

            # Find user by email    async def get_user_by_id(self, user_id: int) -> Optional[User]:

            user_doc = await self.users_collection.find_one(        """

                {"email": email, "is_active": True}        Get user by ID.

            )        

                    Args:

            if not user_doc:            user_id: User ID

                logger.warning("Authentication failed - user not found", email=email)            

                return None        Returns:

                        User instance or None if not found

            # Verify password        """

            if not self.security_manager.verify_password(password, user_doc["password_hash"]):        result = await self.session.execute(

                logger.warning("Authentication failed - invalid password", email=email)            select(User).where(User.id == user_id)

                return None        )

                    return result.scalar_one_or_none()

            # Update last login    

            await self.users_collection.update_one(    async def get_user_by_email(self, email: str) -> Optional[User]:

                {"_id": user_doc["_id"]},        """

                {        Get user by email address.

                    "$set": {"last_login_at": datetime.utcnow()},        

                    "$inc": {"login_count": 1}        Args:

                }            email: Email address

            )            

                    Returns:

            logger.info("User authenticated successfully", email=email, user_id=str(user_doc["_id"]))            User instance or None if not found

                    """

            return User(**user_doc)        result = await self.session.execute(

                        select(User).where(User.email == email.lower().strip())

        except Exception as e:        )

            logger.error("Authentication error", email=email, error=str(e))        return result.scalar_one_or_none()

            return None    

    async def authenticate_user(self, email: str, password: str) -> Optional[User]:

    async def get_user_by_id(self, user_id: str) -> Optional[User]:        """

        """        Authenticate user with email and password.

        Get user by ID.        

                Args:

        Args:            email: Email address

            user_id: User ID string            password: Plain text password

                        

        Returns:        Returns:

            User object if found, None otherwise            User instance if authentication successful, None otherwise

        """        """

        try:        user = await self.get_user_by_email(email)

            user_doc = await self.users_collection.find_one({"_id": ObjectId(user_id)})        if not user:

                        return None

            if user_doc:        

                return User(**user_doc)        # Check if account is locked

            return None        if user.is_locked:

                        business_logger.log_user_action(

        except Exception as e:                action="login_attempt",

            logger.error("Failed to get user by ID", user_id=user_id, error=str(e))                user_id=user.id,

            return None                details={"reason": "account_locked"},

                success=False

    async def get_user_by_email(self, email: str) -> Optional[User]:            )

        """            return None

        Get user by email.        

                # Verify password

        Args:        if not self.security_manager.verify_password(password, user.password_hash):

            email: User email            # Increment failed login attempts

                        await self._handle_failed_login(user)

        Returns:            return None

            User object if found, None otherwise        

        """        # Reset failed login attempts on successful login

        try:        user.failed_login_attempts = 0

            user_doc = await self.users_collection.find_one({"email": email})        user.last_failed_login = None

                    user.login_count += 1

            if user_doc:        

                return User(**user_doc)        await self.session.commit()

            return None        

                    business_logger.log_user_action(

        except Exception as e:            action="login",

            logger.error("Failed to get user by email", email=email, error=str(e))            user_id=user.id,

            return None            success=True

        )

    async def update_user(self, user_id: str, user_data: UserUpdate) -> Optional[User]:        

        """        return user

        Update user profile.    

            async def update_last_login(self, user_id: int) -> bool:

        Args:        """

            user_id: User ID string        Update user's last login timestamp.

            user_data: User update data        

                    Args:

        Returns:            user_id: User ID

            Updated user object if successful, None otherwise            

        """        Returns:

        try:            True if updated successfully

            # Prepare update data (exclude None values)        """

            update_data = {        user = await self.get_user_by_id(user_id)

                k: v for k, v in user_data.dict(exclude_unset=True).items()         if not user:

                if v is not None            return False

            }        

                    user.last_login = datetime.utcnow()

            if not update_data:        await self.session.commit()

                # No data to update        

                return await self.get_user_by_id(user_id)        return True

                

            update_data["updated_at"] = datetime.utcnow()    async def update_password(self, user_id: int, new_password: str) -> bool:

                    """

            # Update user        Update user password.

            result = await self.users_collection.update_one(        

                {"_id": ObjectId(user_id)},        Args:

                {"$set": update_data}            user_id: User ID

            )            new_password: New plain text password

                        

            if result.matched_count == 0:        Returns:

                logger.warning("User not found for update", user_id=user_id)            True if updated successfully

                return None        """

                    user = await self.get_user_by_id(user_id)

            # Return updated user        if not user:

            updated_user = await self.get_user_by_id(user_id)            return False

                    

            logger.info("User updated successfully", user_id=user_id, fields=list(update_data.keys()))        # Hash new password

                    password_hash = self.security_manager.hash_password(new_password)

            return updated_user        user.password_hash = password_hash

                    

        except Exception as e:        await self.session.commit()

            logger.error("Failed to update user", user_id=user_id, error=str(e))        

            return None        business_logger.log_user_action(

            action="password_update",

    async def change_password(self, user_id: str, password_data: UserPasswordUpdate) -> bool:            user_id=user_id,

        """            success=True

        Change user password.        )

                

        Args:        return True

            user_id: User ID string    

            password_data: Password change data    async def update_profile(

                    self,

        Returns:        user_id: int,

            True if successful, False otherwise        **updates: Dict[str, Any]

        """    ) -> Optional[User]:

        try:        """

            # Get user        Update user profile information.

            user = await self.get_user_by_id(user_id)        

            if not user:        Args:

                return False            user_id: User ID

                        updates: Dictionary of fields to update

            # Verify current password            

            if not self.security_manager.verify_password(        Returns:

                password_data.current_password,             Updated User instance or None if not found

                user.password_hash        """

            ):        user = await self.get_user_by_id(user_id)

                logger.warning("Password change failed - invalid current password", user_id=user_id)        if not user:

                return False            return None

                    

            # Hash new password        # Allowed fields for profile updates

            new_password_hash = self.security_manager.get_password_hash(password_data.new_password)        allowed_fields = {

                        "full_name", "company", "bio", "avatar_url",

            # Update password            "timezone", "language", "theme"

            result = await self.users_collection.update_one(        }

                {"_id": ObjectId(user_id)},        

                {        # Apply updates

                    "$set": {        updated_fields = []

                        "password_hash": new_password_hash,        for field, value in updates.items():

                        "password_changed_at": datetime.utcnow(),            if field in allowed_fields and hasattr(user, field):

                        "updated_at": datetime.utcnow()                setattr(user, field, value)

                    }                updated_fields.append(field)

                }        

            )        if updated_fields:

                        await self.session.commit()

            if result.modified_count > 0:            await self.session.refresh(user)

                logger.info("Password changed successfully", user_id=user_id)            

                return True            business_logger.log_user_action(

                            action="profile_update",

            return False                user_id=user_id,

                            details={"updated_fields": updated_fields},

        except Exception as e:                success=True

            logger.error("Failed to change password", user_id=user_id, error=str(e))            )

            return False        

        return user

    async def deactivate_user(self, user_id: str) -> bool:    

        """    async def deactivate_user(self, user_id: int) -> bool:

        Deactivate user account.        """

                Deactivate user account.

        Args:        

            user_id: User ID string        Args:

                        user_id: User ID

        Returns:            

            True if successful, False otherwise        Returns:

        """            True if deactivated successfully

        try:        """

            result = await self.users_collection.update_one(        user = await self.get_user_by_id(user_id)

                {"_id": ObjectId(user_id)},        if not user:

                {            return False

                    "$set": {        

                        "is_active": False,        user.is_active = False

                        "updated_at": datetime.utcnow()        user.deleted_at = datetime.utcnow()

                    }        

                }        await self.session.commit()

            )        

                    business_logger.log_user_action(

            if result.modified_count > 0:            action="deactivate_user",

                logger.info("User deactivated successfully", user_id=user_id)            user_id=user_id,

                return True            success=True

                    )

            return False        

                    return True

        except Exception as e:    

            logger.error("Failed to deactivate user", user_id=user_id, error=str(e))    async def reactivate_user(self, user_id: int) -> bool:

            return False        """

        Reactivate user account.

    async def activate_user(self, user_id: str) -> bool:        

        """        Args:

        Activate user account.            user_id: User ID

                    

        Args:        Returns:

            user_id: User ID string            True if reactivated successfully

                    """

        Returns:        user = await self.get_user_by_id(user_id)

            True if successful, False otherwise        if not user:

        """            return False

        try:        

            result = await self.users_collection.update_one(        user.is_active = True

                {"_id": ObjectId(user_id)},        user.deleted_at = None

                {        

                    "$set": {        await self.session.commit()

                        "is_active": True,        

                        "updated_at": datetime.utcnow()        business_logger.log_user_action(

                    }            action="reactivate_user",

                }            user_id=user_id,

            )            success=True

                    )

            if result.modified_count > 0:        

                logger.info("User activated successfully", user_id=user_id)        return True

                return True    

                async def list_users(

            return False        self,

                    offset: int = 0,

        except Exception as e:        limit: int = 100,

            logger.error("Failed to activate user", user_id=user_id, error=str(e))        search: Optional[str] = None,

            return False        role: Optional[str] = None,

        is_active: Optional[bool] = None

    async def list_users(    ) -> List[User]:

        self,         """

        skip: int = 0,         List users with optional filtering.

        limit: int = 100,        

        filters: Optional[Dict[str, Any]] = None        Args:

    ) -> List[UserResponse]:            offset: Number of records to skip

        """            limit: Maximum number of records to return

        List users with pagination and filtering.            search: Search term for name/email

                    role: Filter by role

        Args:            is_active: Filter by active status

            skip: Number of records to skip            

            limit: Maximum number of records to return        Returns:

            filters: Optional filters to apply            List of User instances

                    """

        Returns:        query = select(User)

            List of user response objects        

        """        # Apply filters

        try:        conditions = []

            # Build query        

            query = filters or {}        if search:

                        search_term = f"%{search.lower()}%"

            # Execute query with pagination            conditions.append(

            cursor = self.users_collection.find(query).skip(skip).limit(limit)                or_(

            users = await cursor.to_list(length=limit)                    User.full_name.ilike(search_term),

                                User.email.ilike(search_term),

            # Convert to response models (exclude sensitive fields)                    User.company.ilike(search_term)

            user_responses = []                )

            for user_doc in users:            )

                user = User(**user_doc)        

                user_response = UserResponse(        if role:

                    id=user.id,            conditions.append(User.role == role)

                    email=user.email,        

                    full_name=user.full_name,        if is_active is not None:

                    is_active=user.is_active,            conditions.append(User.is_active == is_active)

                    is_verified=user.is_verified,        

                    company=user.company,        if conditions:

                    role=user.role,            query = query.where(and_(*conditions))

                    avatar_url=user.avatar_url,        

                    bio=user.bio,        # Order by creation date (newest first)

                    timezone=user.timezone,        query = query.order_by(desc(User.created_at))

                    language=user.language,        

                    theme=user.theme,        # Apply pagination

                    email_notifications=user.email_notifications,        query = query.offset(offset).limit(limit)

                    push_notifications=user.push_notifications,        

                    marketing_emails=user.marketing_emails,        result = await self.session.execute(query)

                    two_factor_enabled=user.two_factor_enabled,        return result.scalars().all()

                    password_changed_at=user.password_changed_at,    

                    last_login_at=user.last_login_at,    async def get_user_stats(self, user_id: Optional[int] = None) -> Dict[str, Any]:

                    last_seen_at=user.last_seen_at,        """

                    login_count=user.login_count,        Get user statistics.

                    tags=user.tags,        

                    custom_fields=user.custom_fields,        Args:

                    created_at=user.created_at,            user_id: Specific user ID, or None for all users

                    updated_at=user.updated_at            

                )        Returns:

                user_responses.append(user_response)            Dictionary of user statistics

                    """

            return user_responses        if user_id:

                        # Stats for specific user

        except Exception as e:            user = await self.get_user_by_id(user_id)

            logger.error("Failed to list users", error=str(e))            if not user:

            return []                return {}

            

    async def count_users(self, filters: Optional[Dict[str, Any]] = None) -> int:            return {

        """                "user_id": user_id,

        Count users matching filters.                "login_count": user.login_count,

                        "last_login": user.last_login.isoformat() if user.last_login else None,

        Args:                "account_age_days": (datetime.utcnow() - user.created_at).days,

            filters: Optional filters to apply                "is_active": user.is_active,

                            "is_verified": user.is_verified,

        Returns:                "role": user.role,

            Number of users matching filters            }

        """        else:

        try:            # Global user stats

            query = filters or {}            from sqlalchemy import func, case

            return await self.users_collection.count_documents(query)            

                        result = await self.session.execute(

        except Exception as e:                select([

            logger.error("Failed to count users", error=str(e))                    func.count(User.id).label("total_users"),

            return 0                    func.count(case([(User.is_active == True, 1)])).label("active_users"),

                    func.count(case([(User.is_verified == True, 1)])).label("verified_users"),

    async def search_users(self, search_term: str, limit: int = 20) -> List[UserResponse]:                    func.count(case([(User.last_login >= datetime.utcnow() - timedelta(days=30), 1)])).label("active_30d"),

        """                    func.count(case([(User.created_at >= datetime.utcnow() - timedelta(days=7), 1)])).label("new_7d"),

        Search users by name or email.                ])

                    )

        Args:            

            search_term: Search term            stats = result.first()

            limit: Maximum number of results            

                        return {

        Returns:                "total_users": stats.total_users,

            List of matching user response objects                "active_users": stats.active_users,

        """                "verified_users": stats.verified_users,

        try:                "active_last_30_days": stats.active_30d,

            # Create text search query                "new_last_7_days": stats.new_7d,

            query = {                "activation_rate": (stats.active_users / stats.total_users * 100) if stats.total_users > 0 else 0,

                "$or": [                "verification_rate": (stats.verified_users / stats.total_users * 100) if stats.total_users > 0 else 0,

                    {"full_name": {"$regex": search_term, "$options": "i"}},            }

                    {"email": {"$regex": search_term, "$options": "i"}},    

                    {"company": {"$regex": search_term, "$options": "i"}}    async def _handle_failed_login(self, user: User) -> None:

                ]        """

            }        Handle failed login attempt.

                    

            # Execute search        Args:

            cursor = self.users_collection.find(query).limit(limit)            user: User instance

            users = await cursor.to_list(length=limit)        """

                    user.failed_login_attempts += 1

            # Convert to response models        user.last_failed_login = datetime.utcnow()

            user_responses = []        

            for user_doc in users:        # Lock account after 5 failed attempts for 30 minutes

                user = User(**user_doc)        if user.failed_login_attempts >= 5:

                user_response = UserResponse(            user.account_locked_until = datetime.utcnow() + timedelta(minutes=30)

                    id=user.id,            

                    email=user.email,            business_logger.log_user_action(

                    full_name=user.full_name,                action="account_locked",

                    is_active=user.is_active,                user_id=user.id,

                    is_verified=user.is_verified,                details={"failed_attempts": user.failed_login_attempts},

                    company=user.company,                success=False

                    role=user.role,            )

                    avatar_url=user.avatar_url,        

                    bio=user.bio,        await self.session.commit()

                    timezone=user.timezone,

                    language=user.language,

                    theme=user.theme,class APIKeyService:

                    email_notifications=user.email_notifications,    """

                    push_notifications=user.push_notifications,    API Key service for managing programmatic access.

                    marketing_emails=user.marketing_emails,    

                    two_factor_enabled=user.two_factor_enabled,    Handles API key creation, validation, and management for

                    password_changed_at=user.password_changed_at,    service-to-service authentication.

                    last_login_at=user.last_login_at,    """

                    last_seen_at=user.last_seen_at,    

                    login_count=user.login_count,    def __init__(self, session: AsyncSession):

                    tags=user.tags,        self.session = session

                    custom_fields=user.custom_fields,        self.security_manager = get_security_manager()

                    created_at=user.created_at,    

                    updated_at=user.updated_at    async def create_api_key(

                )        self,

                user_responses.append(user_response)        user_id: int,

                    name: str,

            return user_responses        description: Optional[str] = None,

                    scopes: List[str] = None,

        except Exception as e:        expires_at: Optional[datetime] = None

            logger.error("Failed to search users", search_term=search_term, error=str(e))    ) -> Dict[str, Any]:

            return []        """

        Create a new API key.

        

# Global user service instance        Args:

user_service = UserService()            user_id: Owner user ID

            name: API key name

            description: Optional description

def get_user_service() -> UserService:            scopes: List of permission scopes

    """            expires_at: Optional expiration date

    Get user service instance.            

            Returns:

    Returns:            Dictionary with API key information including the actual key

        UserService instance        """

    """        # Generate API key

    return user_service        api_key_value = self.security_manager.generate_api_key(user_id, name)
        key_id = api_key_value.replace("streamlinehub_", "")[:32]
        
        # Hash the key for storage
        key_hash = self.security_manager.hash_password(api_key_value)
        
        # Create API key record
        api_key = APIKey(
            key_id=key_id,
            key_hash=key_hash,
            name=name,
            description=description,
            scopes=",".join(scopes) if scopes else "read",
            expires_at=expires_at,
            user_id=user_id
        )
        
        self.session.add(api_key)
        await self.session.commit()
        await self.session.refresh(api_key)
        
        business_logger.log_user_action(
            action="create_api_key",
            user_id=user_id,
            details={"key_id": key_id, "name": name},
            success=True
        )
        
        return {
            "id": api_key.id,
            "key_id": key_id,
            "api_key": api_key_value,  # Only returned once!
            "name": name,
            "scopes": scopes or ["read"],
            "expires_at": expires_at.isoformat() if expires_at else None,
            "created_at": api_key.created_at.isoformat()
        }
    
    async def list_api_keys(self, user_id: int) -> List[Dict[str, Any]]:
        """
        List API keys for a user.
        
        Args:
            user_id: User ID
            
        Returns:
            List of API key information (without actual keys)
        """
        result = await self.session.execute(
            select(APIKey).where(APIKey.user_id == user_id).order_by(desc(APIKey.created_at))
        )
        
        api_keys = result.scalars().all()
        return [key.to_dict() for key in api_keys]
    
    async def revoke_api_key(self, user_id: int, key_id: str) -> bool:
        """
        Revoke (deactivate) an API key.
        
        Args:
            user_id: User ID (for authorization)
            key_id: API key ID
            
        Returns:
            True if revoked successfully
        """
        result = await self.session.execute(
            select(APIKey).where(
                and_(APIKey.key_id == key_id, APIKey.user_id == user_id)
            )
        )
        
        api_key = result.scalar_one_or_none()
        if not api_key:
            return False
        
        api_key.is_active = False
        await self.session.commit()
        
        business_logger.log_user_action(
            action="revoke_api_key",
            user_id=user_id,
            details={"key_id": key_id},
            success=True
        )
        
        return True
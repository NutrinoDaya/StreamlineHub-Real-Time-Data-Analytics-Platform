#!/usr/bin/env python#!/usr/bin/env python

"""""

Script to create a seed admin user in MongoDB for testing.Script to create a seed admin user in the database for testing.

"""""



import async ioimport asyncio

import sys import sys

from pathlib import Pathfrom pathlib import Path

from datetime import datetimefrom datetime import datetime



# Add parent directory to path# Add parent directory to path

sys.path.insert(0, str(Path(__file__).parent.parent))sys.path.insert(0, str(Path(__file__).parent.parent))



from src.core.database import init_database, get_users_collectionfrom sqlalchemy import text

from src.core.security import get_security_managerfrom src.core.database import init_database, get_async_session

from src.models.user import UserCreate, UserRolefrom src.core.security import get_security_manager





async def create_seed_user():async def create_seed_user():

    """Create a test admin user in MongoDB"""    """Create a test admin user"""

    print("=" * 60)    print("=" * 60)

    print("Creating Seed User (MongoDB)")    print("Creating Seed User")

    print("=" * 60)    print("=" * 60)

        

    try:    try:

        # Initialize database connection        await init_database()

        await init_database()        print("✓ Database initialized")

        print("✓ MongoDB connected")        

                email = "admin@streamlinehub.com"

        # Get users collection        password = "Admin123!"

        users_collection = get_users_collection()        full_name = "System Administrator"

                

        # User details        security_manager = get_security_manager()

        email = "admin@streamlinehub.com"        password_hash = security_manager.hash_password(password)

        password = "Admin123!"        

        full_name = "System Administrator"        async with get_async_session() as session:

                    # Check if user already exists

        # Check if user already exists            result = await session.execute(

        existing_user = await users_collection.find_one({"email": email})                text("SELECT id, email FROM users WHERE email = :email"),

        if existing_user:                {"email": email}

            print(f"✓ User already exists: {existing_user['email']} (ID: {existing_user['_id']})")            )

            print(f"\nLogin with:")            existing_user = result.first()

            print(f"  Email: {email}")            

            print(f"  Password: {password}")            if existing_user:

            return                print(f"✓ User already exists: {existing_user[1]} (ID: {existing_user[0]})")

                        print(f"\nLogin with:")

        # Hash password                print(f"  Email: {email}")

        security_manager = get_security_manager()                print(f"  Password: {password}")

        password_hash = security_manager.get_password_hash(password)                return

                    

        # Create user document            # Create new user

        user_data = {            await session.execute(

            "email": email,                text("""

            "full_name": full_name,                    INSERT INTO users 

            "password_hash": password_hash,                    (email, password_hash, full_name, role, is_active, is_verified, created_at, updated_at)

            "role": UserRole.ADMIN,                    VALUES (:email, :password_hash, :full_name, :role, :is_active, :is_verified, :created_at, :updated_at)

            "is_active": True,                """),

            "is_verified": True,                {

            "company": "StreamlineHub",                    "email": email,

            "timezone": "UTC",                    "password_hash": password_hash,

            "language": "en",                    "full_name": full_name,

            "theme": "light",                    "role": "admin",

            "email_notifications": True,                    "is_active": True,

            "push_notifications": True,                    "is_verified": True,

            "marketing_emails": False,                    "created_at": datetime.utcnow(),

            "two_factor_enabled": False,                    "updated_at": datetime.utcnow()

            "login_count": 0,                }

            "created_at": datetime.utcnow(),            )

            "updated_at": datetime.utcnow()            await session.commit()

        }            

                    print(f"✓ Created seed user: {email}")

        # Insert user            print(f"\nLogin with:")

        result = await users_collection.insert_one(user_data)            print(f"  Email: {email}")

        print(f"✓ Admin user created successfully")            print(f"  Password: {password}")

        print(f"  User ID: {result.inserted_id}")            

        print(f"  Email: {email}")    except Exception as e:

        print(f"  Role: {UserRole.ADMIN}")        print(f"✗ Failed to create seed user: {e}")

                import traceback

        print(f"\nLogin with:")        traceback.print_exc()

        print(f"  Email: {email}")        return False

        print(f"  Password: {password}")    

        print(f"  Role: {UserRole.ADMIN}")    return True

        

        # Create additional test users

        print(f"\nCreating additional test users...")if __name__ == "__main__":

            success = asyncio.run(create_seed_user())

        test_users = [    sys.exit(0 if success else 1)

            {
                "email": "user@streamlinehub.com",
                "full_name": "Test User",
                "role": UserRole.USER,
                "company": "Test Corp"
            },
            {
                "email": "analyst@streamlinehub.com",
                "full_name": "Data Analyst",
                "role": UserRole.ANALYST,
                "company": "Analytics Inc"
            },
            {
                "email": "manager@streamlinehub.com",
                "full_name": "Project Manager",
                "role": UserRole.MANAGER,
                "company": "Management Co"
            }
        ]
        
        for test_user in test_users:
            # Check if user exists
            existing = await users_collection.find_one({"email": test_user["email"]})
            if existing:
                print(f"  ✓ {test_user['email']} already exists")
                continue
                
            # Create test user
            test_password = "Test123!"
            test_password_hash = security_manager.get_password_hash(test_password)
            
            test_user_data = {
                "email": test_user["email"],
                "full_name": test_user["full_name"],
                "password_hash": test_password_hash,
                "role": test_user["role"],
                "company": test_user["company"],
                "is_active": True,
                "is_verified": True,
                "timezone": "UTC",
                "language": "en",
                "theme": "light",
                "email_notifications": True,
                "push_notifications": True,
                "marketing_emails": False,
                "two_factor_enabled": False,
                "login_count": 0,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
            
            result = await users_collection.insert_one(test_user_data)
            print(f"  ✓ {test_user['email']} created (ID: {result.inserted_id})")
        
        print(f"\n✓ All seed users created successfully!")
        print(f"\nTest user credentials:")
        print(f"  Password for all test users: Test123!")
        
    except Exception as e:
        print(f"✗ Failed to create seed user: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(create_seed_user())
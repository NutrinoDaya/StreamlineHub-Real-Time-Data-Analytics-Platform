#!/usr/bin/env python3
"""
Airflow User Initialization Script
Creates default admin user and fixes authentication issues
"""

import sys
from airflow import settings
from airflow.models import User
from flask_appbuilder.security.sqla.models import Role

def create_admin_user():
    """Create default admin user for Airflow"""
    try:
        # Get session
        session = settings.Session()
        
        # Check if admin user already exists
        existing_user = session.query(User).filter(User.username == 'admin').first()
        
        if existing_user:
            print("Admin user already exists")
            # Ensure user is active
            existing_user.active = True
            session.commit()
            session.close()
            return True
        
        # Get admin role
        admin_role = session.query(Role).filter(Role.name == 'Admin').first()
        
        if not admin_role:
            print("Admin role not found - database may not be initialized")
            session.close()
            return False
        
        # Create admin user
        admin_user = User(
            username='admin',
            email='admin@streamlinehub.local',
            first_name='Admin',
            last_name='User',
            active=True,
            password='admin123'  # Change this in production
        )
        
        # Add admin role to user
        admin_user.roles = [admin_role]
        
        # Add to session and commit
        session.add(admin_user)
        session.commit()
        session.close()
        
        print("Admin user created successfully")
        print("Username: admin")
        print("Password: admin123")
        print("Email: admin@streamlinehub.local")
        
        return True
        
    except Exception as e:
        print(f"Failed to create admin user: {e}")
        return False

def fix_user_permissions():
    """Fix user permissions and active status"""
    try:
        session = settings.Session()
        
        # Get all users and ensure they are active
        users = session.query(User).all()
        
        for user in users:
            if not user.active:
                user.active = True
                print(f"Activated user: {user.username}")
        
        session.commit()
        session.close()
        
        print("User permissions fixed")
        return True
        
    except Exception as e:
        print(f"Failed to fix user permissions: {e}")
        return False

def main():
    """Main execution function"""
    print("Initializing Airflow user management...")
    
    # Create admin user
    if create_admin_user():
        print("Admin user initialization completed")
    else:
        print("Admin user initialization failed")
        sys.exit(1)
    
    # Fix user permissions
    if fix_user_permissions():
        print("User permissions fixed")
    else:
        print("Failed to fix user permissions")
    
    print("Airflow user initialization completed successfully")

if __name__ == '__main__':
    main()
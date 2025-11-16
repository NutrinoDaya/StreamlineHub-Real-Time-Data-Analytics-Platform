#!/usr/bin/env python3
"""
Database seeding script for StreamLineHub Analytics
Populates the database with sample data for testing
"""

import asyncio
import sys
import os
from datetime import datetime, timedelta
import random
from faker import Faker

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.database import init_database, get_async_session
from src.models.user import User
from src.models.customer import Customer
from src.models.campaign import Campaign, CampaignInteraction
from src.core.security import SecurityManager

fake = Faker()
security_manager = SecurityManager()

async def seed_database():
    """Seed database with sample data"""
    
    print("🌱 Starting database seeding...")
    
    # Initialize database
    await init_database()
    
    async with get_async_session() as session:
        try:
            # 1. Create admin user
            print("👤 Creating admin user...")
            admin_password_hash = security_manager.hash_password("admin123!")
            admin_user = User(
                email="admin@streamlinehub.com",
                password_hash=admin_password_hash,
                full_name="StreamLineHub Admin",
                role="admin",
                is_active=True,
                is_verified=True,
                company="StreamLineHub Analytics",
            )
            session.add(admin_user)
            
            # 2. Create sample users
            print("👥 Creating sample users...")
            sample_users = []
            for i in range(5):
                password_hash = security_manager.hash_password("user123!")
                user = User(
                    email=fake.email(),
                    password_hash=password_hash,
                    full_name=fake.name(),
                    role="user",
                    is_active=True,
                    is_verified=True,
                    company=fake.company(),
                )
                sample_users.append(user)
                session.add(user)
            
            await session.commit()
            print(f"✅ Created {len(sample_users) + 1} users")
            
            # 3. Create sample customers
            print("🏢 Creating sample customers...")
            customers = []
            for i in range(50):
                customer = Customer(
                    email=fake.email(),
                    first_name=fake.first_name(),
                    last_name=fake.last_name(),
                    phone=fake.phone_number(),
                    company=fake.company(),
                    industry=fake.random_element(elements=[
                        'Technology', 'Healthcare', 'Finance', 'Retail', 'Manufacturing',
                        'Education', 'Real Estate', 'Automotive', 'Media', 'Energy'
                    ]),
                    annual_revenue=fake.random_int(min=10000, max=10000000),
                    employee_count=fake.random_int(min=1, max=5000),
                    website=fake.url(),
                    address=fake.address(),
                    city=fake.city(),
                    state=fake.state(),
                    country=fake.country(),
                    postal_code=fake.postcode(),
                    lead_source=fake.random_element(elements=[
                        'Website', 'Social Media', 'Email Campaign', 'Referral', 
                        'Trade Show', 'Cold Call', 'Advertisement'
                    ]),
                    lead_score=fake.random_int(min=0, max=100),
                    lifecycle_stage=fake.random_element(elements=[
                        'lead', 'prospect', 'customer', 'churned'
                    ]),
                    created_at=fake.date_time_between(start_date='-2y', end_date='now')
                )
                customers.append(customer)
                session.add(customer)
            
            await session.commit()
            print(f"✅ Created {len(customers)} customers")
            
            # 4. Create sample campaigns
            print("📧 Creating sample campaigns...")
            campaigns = []
            for i in range(15):
                start_date = fake.date_time_between(start_date='-1y', end_date='now')
                end_date = start_date + timedelta(days=fake.random_int(min=7, max=90))
                
                campaign = Campaign(
                    name=f"Campaign {i+1}: {fake.catch_phrase()}",
                    description=fake.text(max_nb_chars=200),
                    campaign_type=fake.random_element(elements=[
                        'email', 'social_media', 'paid_ads', 'content_marketing', 'webinar'
                    ]),
                    status=fake.random_element(elements=[
                        'draft', 'active', 'paused', 'completed'
                    ]),
                    budget=fake.random_int(min=1000, max=100000),
                    start_date=start_date,
                    end_date=end_date,
                    target_audience=fake.random_element(elements=[
                        'enterprise', 'smb', 'startups', 'healthcare', 'fintech'
                    ]),
                    goals=fake.random_element(elements=[
                        'lead_generation', 'brand_awareness', 'customer_retention', 'upsell'
                    ]),
                    created_by=admin_user.id,
                )
                campaigns.append(campaign)
                session.add(campaign)
            
            await session.commit()
            print(f"✅ Created {len(campaigns)} campaigns")
            
            # 5. Create campaign interactions
            print("📊 Creating campaign interactions...")
            interactions_count = 0
            for campaign in campaigns:
                # Create interactions for random customers
                num_interactions = fake.random_int(min=5, max=20)
                selected_customers = fake.random_elements(
                    elements=customers, length=num_interactions, unique=True
                )
                
                for customer in selected_customers:
                    interaction = CampaignInteraction(
                        campaign_id=campaign.id,
                        customer_id=customer.id,
                        interaction_type=fake.random_element(elements=[
                            'email_open', 'email_click', 'website_visit', 'form_submit',
                            'download', 'social_share', 'conversion'
                        ]),
                        channel=fake.random_element(elements=[
                            'email', 'social_media', 'website', 'mobile_app'
                        ]),
                        interaction_date=fake.date_time_between(
                            start_date=campaign.start_date,
                            end_date=campaign.end_date or datetime.now()
                        ),
                        value=fake.random_int(min=0, max=1000) if fake.boolean(chance_of_getting_true=30) else None,
                        metadata={
                            'device': fake.random_element(elements=['desktop', 'mobile', 'tablet']),
                            'browser': fake.random_element(elements=['chrome', 'firefox', 'safari', 'edge']),
                            'location': fake.city(),
                        }
                    )
                    session.add(interaction)
                    interactions_count += 1
            
            await session.commit()
            print(f"✅ Created {interactions_count} campaign interactions")
            
            print("\n🎉 Database seeding completed successfully!")
            print("\n📝 Login credentials:")
            print("Admin: admin@streamlinehub.com / admin123!")
            print("Users: Check database for generated emails / user123!")
            
        except Exception as e:
            print(f"❌ Error during seeding: {e}")
            await session.rollback()
            raise

if __name__ == "__main__":
    asyncio.run(seed_database())
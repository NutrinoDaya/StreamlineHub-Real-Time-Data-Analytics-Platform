#!/usr/bin/env python3
"""
Customer Data Seeder

Populates the database with sample customer data for testing and demonstration.
This creates realistic customer profiles with proper relationships and analytics data.
"""

import asyncio
import random
from datetime import datetime, timedelta, date
from faker import Faker
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from src.core.database import get_async_session, init_database
from src.models.customer import Customer

fake = Faker()


async def create_sample_customers(db: AsyncSession, num_customers: int = 100) -> None:
    """
    Create sample customers with realistic data.
    
    Args:
        db: Database session
        num_customers: Number of customers to create
    """
    print(f"Creating {num_customers} sample customers...")
    
    segments = ['standard', 'premium', 'vip', 'basic']
    acquisition_sources = ['website', 'social_media', 'referral', 'advertising', 'organic']
    
    customers = []
    
    for i in range(num_customers):
        # Generate realistic customer data
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name.lower()}.{last_name.lower()}{i}@{fake.domain_name()}"
        
        # Generate birth date (18-80 years old)
        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=80)
        
        # Generate address
        address = fake.address().split('\n')
        
        # Random segment with weighted distribution
        segment = random.choices(
            segments, 
            weights=[50, 30, 10, 10],  # Most customers are standard
            k=1
        )[0]
        
        # Lifetime value based on segment
        if segment == 'vip':
            lifetime_value = round(random.uniform(5000, 50000), 2)
        elif segment == 'premium':
            lifetime_value = round(random.uniform(1000, 10000), 2)
        elif segment == 'standard':
            lifetime_value = round(random.uniform(100, 2000), 2)
        else:  # basic
            lifetime_value = round(random.uniform(10, 500), 2)
        
        # Churn probability (inversely related to segment quality)
        if segment == 'vip':
            churn_prob = round(random.uniform(0.05, 0.2), 2)
        elif segment == 'premium':
            churn_prob = round(random.uniform(0.1, 0.4), 2)
        elif segment == 'standard':
            churn_prob = round(random.uniform(0.2, 0.6), 2)
        else:  # basic
            churn_prob = round(random.uniform(0.4, 0.8), 2)
        
        # Random created date (last 2 years)
        created_date = fake.date_time_between(start_date='-2y', end_date='now')
        
        customer = Customer(
            customer_id=f"CUST_{i+1:06d}",
            email=email,
            first_name=first_name,
            last_name=last_name,
            full_name=f"{first_name} {last_name}",
            phone=fake.phone_number(),
            date_of_birth=birth_date,
            address_line1=address[0] if address else fake.street_address(),
            city=fake.city(),
            state=fake.state_abbr(),
            zip_code=fake.zipcode(),
            country=fake.country_code(),
            segment=segment,
            acquisition_source=random.choice(acquisition_sources),
            lifetime_value=lifetime_value,
            churn_probability=churn_prob,
            total_orders=random.randint(1, 50) if segment != 'basic' else random.randint(0, 5),
            total_spent=lifetime_value * random.uniform(0.6, 0.9),  # Spent amount related to LTV
            last_order_date=fake.date_time_between(start_date='-1y', end_date='now') if random.random() > 0.1 else None,
            created_at=created_date,
            updated_at=created_date + timedelta(days=random.randint(0, 30))
        )
        
        customers.append(customer)
        
        if (i + 1) % 10 == 0:
            print(f"Generated {i + 1} customers...")
    
    # Bulk insert
    print("Inserting customers into database...")
    db.add_all(customers)
    await db.commit()
    
    print(f"✓ Successfully created {num_customers} customers!")


async def get_customer_stats(db: AsyncSession) -> None:
    """Display customer statistics."""
    result = await db.execute(select(func.count(Customer.id)))
    total = result.scalar()
    
    if total == 0:
        print("No customers found in database.")
        return
    
    print(f"\nCustomer Statistics:")
    print(f"Total customers: {total}")
    
    # Segment breakdown
    result = await db.execute(
        select(Customer.segment, func.count(Customer.id).label('count'))
        .group_by(Customer.segment)
    )
    segment_stats = result.all()
    
    print("\nSegment breakdown:")
    for seg, count in segment_stats:
        percentage = (count / total) * 100
        print(f"  {seg}: {count} ({percentage:.1f}%)")
    
    # Recent customers
    thirty_days_ago = datetime.now() - timedelta(days=30)
    result = await db.execute(
        select(func.count(Customer.id)).filter(Customer.created_at >= thirty_days_ago)
    )
    recent_count = result.scalar()
    print(f"\nCustomers created in last 30 days: {recent_count}")


async def main():
    """Main function to seed customer data."""
    print("=" * 60)
    print("StreamLineHub Analytics - Customer Data Seeder")
    print("=" * 60)
    
    # Initialize database
    await init_database()
    
    # Get database session
    async with get_async_session() as db:
        try:
            # Check if customers already exist
            result = await db.execute(select(func.count(Customer.id)))
            existing_count = result.scalar()
            
            if existing_count > 0:
                print(f"Found {existing_count} existing customers.")
                response = input("Do you want to add more customers? (y/N): ")
                
                if response.lower() != 'y':
                    print("Skipping customer creation.")
                    await get_customer_stats(db)
                    return
            
            # Get number of customers to create
            try:
                num_customers = int(input("How many customers to create? (default: 100): ") or "100")
            except ValueError:
                num_customers = 100
            
            # Create customers
            await create_sample_customers(db, num_customers)
            
            # Display final stats
            await get_customer_stats(db)
            
        except Exception as e:
            print(f"Error: {e}")
            await db.rollback()
            raise


if __name__ == "__main__":
    asyncio.run(main())
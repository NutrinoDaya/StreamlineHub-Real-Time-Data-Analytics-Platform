"""
Customer Events Producer

Publishes customer-related events such as registrations, profile updates,
purchases, and behavioral analytics to Kafka for real-time processing.
"""

import random
import uuid
from typing import Dict, Any, List
from datetime import datetime, timedelta
try:
    from producers.kafka_base import BaseKafkaProducer, EventMetrics, KafkaConfig
except ImportError:
    from kafka_base import BaseKafkaProducer, EventMetrics, KafkaConfig


class CustomerEventProducer(BaseKafkaProducer):
    """
    Producer for customer-related events.
    
    Handles customer registration, profile updates, behavioral tracking,
    and customer lifecycle events for real-time analytics and ML processing.
    """
    
    def __init__(self):
        super().__init__(KafkaConfig.TOPICS['customer_events']['name'])
        
    def send_registration_event(self, customer_data: Dict[str, Any]) -> bool:
        """
        Send customer registration event.
        
        Args:
            customer_data: Customer registration information
            
        Returns:
            bool: Success status
        """
        event = {
            **EventMetrics.create_event_metadata('customer_registration', 'user_service'),
            'customer_id': customer_data.get('id'),
            'email': customer_data.get('email'),
            'full_name': customer_data.get('full_name'),
            'segment': customer_data.get('segment', 'standard'),
            'acquisition_source': customer_data.get('acquisition_source', 'website'),
            'registration_timestamp': datetime.now().isoformat(),
            'initial_attributes': {
                'first_name': customer_data.get('first_name'),
                'last_name': customer_data.get('last_name'),
                'phone': customer_data.get('phone'),
                'city': customer_data.get('city'),
                'state': customer_data.get('state'),
                'country': customer_data.get('country', 'USA')
            }
        }
        
        return self.send_message(event, key=str(customer_data.get('id')))
        
    def send_profile_update_event(self, customer_id: int, old_data: Dict, new_data: Dict) -> bool:
        """
        Send customer profile update event.
        
        Args:
            customer_id: Customer ID
            old_data: Previous customer data
            new_data: Updated customer data
            
        Returns:
            bool: Success status
        """
        # Calculate what changed
        changes = {}
        for key in new_data:
            if key in old_data and old_data[key] != new_data[key]:
                changes[key] = {
                    'old_value': old_data[key],
                    'new_value': new_data[key]
                }
        
        event = {
            **EventMetrics.create_event_metadata('customer_profile_update', 'customer_service'),
            'customer_id': customer_id,
            'changes': changes,
            'update_timestamp': datetime.now().isoformat(),
            'updated_fields': list(changes.keys())
        }
        
        return self.send_message(event, key=str(customer_id))
        
    def send_behavioral_event(self, customer_id: int, action: str, context: Dict[str, Any]) -> bool:
        """
        Send customer behavioral event.
        
        Args:
            customer_id: Customer ID
            action: Action performed (view_product, add_to_cart, etc.)
            context: Additional context about the action
            
        Returns:
            bool: Success status
        """
        event = {
            **EventMetrics.create_event_metadata('customer_behavior', 'web_analytics'),
            'customer_id': customer_id,
            'action': action,
            'context': context,
            'session_id': context.get('session_id', str(uuid.uuid4())),
            'user_agent': context.get('user_agent', 'unknown'),
            'ip_address': context.get('ip_address', '0.0.0.0'),
            'page_url': context.get('page_url'),
            'referrer': context.get('referrer'),
            'device_type': context.get('device_type', 'desktop'),
            'timestamp': datetime.now().isoformat()
        }
        
        return self.send_message(event, key=str(customer_id))


class TransactionEventProducer(BaseKafkaProducer):
    """
    Producer for transaction and purchase events.
    
    Handles order creation, payment processing, refunds, and
    transaction-related analytics events.
    """
    
    def __init__(self):
        super().__init__(KafkaConfig.TOPICS['transaction_events']['name'])
        
    def send_transaction_event(self, transaction_data: Dict[str, Any]) -> bool:
        """
        Send transaction event.
        
        Args:
            transaction_data: Transaction information
            
        Returns:
            bool: Success status
        """
        event = {
            **EventMetrics.create_event_metadata('transaction_completed', 'payment_service'),
            'transaction_id': transaction_data.get('transaction_id'),
            'customer_id': transaction_data.get('customer_id'),
            'amount': transaction_data.get('amount'),
            'currency': transaction_data.get('currency', 'USD'),
            'payment_method': transaction_data.get('payment_method'),
            'products': transaction_data.get('products', []),
            'discount_applied': transaction_data.get('discount_applied', 0),
            'tax_amount': transaction_data.get('tax_amount', 0),
            'shipping_cost': transaction_data.get('shipping_cost', 0),
            'total_amount': transaction_data.get('total_amount'),
            'transaction_status': transaction_data.get('status', 'completed'),
            'timestamp': datetime.now().isoformat()
        }
        
        return self.send_message(event, key=str(transaction_data.get('customer_id')))


class AnalyticsEventProducer(BaseKafkaProducer):
    """
    Producer for analytics and metrics events.
    
    Publishes system metrics, performance data, and analytical
    events for monitoring and business intelligence.
    """
    
    def __init__(self):
        super().__init__(KafkaConfig.TOPICS['analytics_events']['name'])
        
    def send_metric_event(self, metric_name: str, value: float, dimensions: Dict[str, Any] = None) -> bool:
        """
        Send system metric event.
        
        Args:
            metric_name: Name of the metric
            value: Metric value
            dimensions: Additional metric dimensions/tags
            
        Returns:
            bool: Success status
        """
        event = {
            **EventMetrics.create_event_metadata('system_metric', 'analytics_service'),
            'metric_name': metric_name,
            'value': value,
            'dimensions': dimensions or {},
            'timestamp': datetime.now().isoformat(),
            'unit': dimensions.get('unit', 'count') if dimensions else 'count'
        }
        
        return self.send_message(event, key=metric_name)


def generate_sample_events(num_events: int = 100) -> None:
    """
    Generate sample events for testing and demonstration.
    
    Creates realistic customer, transaction, and analytics events
    to populate the Kafka topics with meaningful data.
    
    Args:
        num_events: Number of events to generate
    """
    customer_producer = CustomerEventProducer()
    transaction_producer = TransactionEventProducer()
    analytics_producer = AnalyticsEventProducer()
    
    print(f"Generating {num_events} sample events...")
    
    try:
        for i in range(num_events):
            customer_id = random.randint(1, 1000)
            
            # Generate customer behavioral event
            actions = ['view_product', 'add_to_cart', 'remove_from_cart', 'checkout_start', 'purchase_complete']
            action = random.choice(actions)
            
            customer_producer.send_behavioral_event(
                customer_id=customer_id,
                action=action,
                context={
                    'session_id': str(uuid.uuid4()),
                    'product_id': random.randint(1, 100),
                    'category': random.choice(['electronics', 'clothing', 'books', 'home']),
                    'device_type': random.choice(['desktop', 'mobile', 'tablet']),
                    'page_url': f'/products/{random.randint(1, 100)}'
                }
            )
            
            # Generate transaction event (30% chance)
            if random.random() < 0.3:
                transaction_producer.send_transaction_event({
                    'transaction_id': f'TXN_{random.randint(100000, 999999)}',
                    'customer_id': customer_id,
                    'amount': round(random.uniform(10, 500), 2),
                    'currency': 'USD',
                    'payment_method': random.choice(['credit_card', 'debit_card', 'paypal', 'apple_pay']),
                    'products': [
                        {
                            'id': random.randint(1, 100),
                            'name': f'Product {random.randint(1, 100)}',
                            'price': round(random.uniform(5, 200), 2),
                            'quantity': random.randint(1, 3)
                        }
                    ],
                    'status': 'completed'
                })
            
            # Generate analytics metric events
            metrics = [
                ('active_users', random.randint(500, 2000)),
                ('page_views', random.randint(1000, 5000)),
                ('api_response_time', round(random.uniform(50, 500), 2)),
                ('error_rate', round(random.uniform(0.1, 2.0), 2))
            ]
            
            for metric_name, value in metrics:
                analytics_producer.send_metric_event(
                    metric_name=metric_name,
                    value=value,
                    dimensions={'source': 'sample_generator', 'environment': 'development'}
                )
            
            if (i + 1) % 10 == 0:
                print(f"Generated {i + 1} events...")
                
    finally:
        customer_producer.flush_and_close()
        transaction_producer.flush_and_close()
        analytics_producer.flush_and_close()
        
    print(f"Successfully generated {num_events} events!")


if __name__ == "__main__":
    # Generate sample events for testing
    generate_sample_events(50)
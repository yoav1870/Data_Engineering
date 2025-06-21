#!/usr/bin/env python3
"""
Test script for the Nail Salon streaming pipeline
"""

import json
import time
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime

def test_kafka_connection():
    """Test basic Kafka connectivity"""
    print("🔍 Testing Kafka connection...")
    
    try:
        # Test producer
        producer = KafkaProducer(
            bootstrap_servers=['localhost:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Test consumer
        consumer = KafkaConsumer(
            'nail_salon_ratings',
            bootstrap_servers=['localhost:29092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='test_group'
        )
        
        print("✅ Kafka connection successful")
        producer.close()
        consumer.close()
        return True
        
    except Exception as e:
        print(f"❌ Kafka connection failed: {e}")
        return False

def send_test_ratings():
    """Send a few test ratings to Kafka"""
    print("📝 Sending test ratings...")
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8')
    )
    
    test_ratings = [
        {
            'customer_id': 1,
            'branch_id': 1,
            'employee_id': 5,
            'treatment_id': 1,
            'rating_value': 4.5,
            'comment': 'Excellent service!',
            'timestamp': datetime.now().isoformat()
        },
        {
            'customer_id': 2,
            'branch_id': 2,
            'employee_id': 3,
            'treatment_id': 2,
            'rating_value': 3.8,
            'comment': 'Good experience',
            'timestamp': datetime.now().isoformat()
        },
        {
            'customer_id': 3,
            'branch_id': 1,
            'employee_id': 7,
            'treatment_id': 3,
            'rating_value': 5.0,
            'comment': 'Perfect!',
            'timestamp': datetime.now().isoformat()
        }
    ]
    
    for i, rating in enumerate(test_ratings):
        future = producer.send('nail_salon_ratings', key=rating['customer_id'], value=rating)
        record_metadata = future.get(timeout=10)
        print(f"✅ Test rating {i+1} sent to partition {record_metadata.partition}, offset {record_metadata.offset}")
    
    producer.flush()
    producer.close()
    print("✅ All test ratings sent successfully")

def consume_test_ratings():
    """Consume and display test ratings"""
    print("📖 Consuming test ratings...")
    
    consumer = KafkaConsumer(
        'nail_salon_ratings',
        bootstrap_servers=['localhost:29092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='test_consumer_group'
    )
    
    messages_received = 0
    max_messages = 5  # Limit to prevent infinite loop
    
    for message in consumer:
        if messages_received >= max_messages:
            break
            
        rating = json.loads(message.value.decode('utf-8'))
        print(f"📊 Received rating: Customer {rating['customer_id']} gave {rating['rating_value']} stars")
        messages_received += 1
    
    consumer.close()
    print(f"✅ Consumed {messages_received} messages")

if __name__ == "__main__":
    print("🚀 Starting Nail Salon Streaming Pipeline Test")
    print("=" * 50)
    
    # Test 1: Kafka connection
    if not test_kafka_connection():
        print("❌ Cannot proceed without Kafka connection")
        exit(1)
    
    # Test 2: Send test ratings
    send_test_ratings()
    
    # Wait a moment for processing
    print("⏳ Waiting 3 seconds for processing...")
    time.sleep(3)
    
    # Test 3: Consume test ratings
    consume_test_ratings()
    
    print("=" * 50)
    print("✅ Streaming pipeline test completed!") 
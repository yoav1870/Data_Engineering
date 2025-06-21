import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NailSalonRatingsProducer:
    def __init__(self, bootstrap_servers=None):
        if bootstrap_servers is None:
            # Use environment variable or default to 'kafka:9092'
            bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",")
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8')
        )
        
        # Sample data for realistic ratings
        self.branches = [1, 2, 3, 4, 5]
        self.employees = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        self.treatments = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        self.customers = list(range(1, 21))  # Customer IDs 1-20
        
        self.comments = [
            "Great service!", "Amazing work!", "Very satisfied", "Excellent experience",
            "Good job", "Okay service", "Could be better", "Not happy", "Terrible experience",
            "Love the results!", "Professional service", "Friendly staff", "Clean environment",
            "Will come back", "Disappointed", "Perfect!", "Average service", "Outstanding!"
        ]
        
        self.topic = 'nail_salon_ratings'
        
    def generate_rating(self):
        """Generate a realistic customer rating"""
        rating = {
            'customer_id': random.choice(self.customers),
            'branch_id': random.choice(self.branches),
            'employee_id': random.choice(self.employees),
            'treatment_id': random.choice(self.treatments),
            'rating_value': round(random.uniform(1.0, 5.0), 1),  # Rating 1.0-5.0
            'comment': random.choice(self.comments),
            'timestamp': datetime.now().isoformat()
        }
        return rating
    
    def send_rating(self, rating):
        """Send a rating to Kafka topic"""
        try:
            # Use customer_id as the key for partitioning
            future = self.producer.send(
                self.topic, 
                key=rating['customer_id'],
                value=rating
            )
            
            # Wait for the send to complete
            record_metadata = future.get(timeout=10)
            logger.info(f"✅ Rating sent successfully to {self.topic} [partition: {record_metadata.partition}, offset: {record_metadata.offset}]")
            return True
            
        except KafkaError as e:
            logger.error(f"❌ Failed to send rating: {e}")
            return False
    
    def run_streaming_simulation(self, duration_minutes=5, interval_seconds=2):
        """Run the streaming simulation for specified duration"""
        logger.info(f"🚀 Starting ratings stream simulation for {duration_minutes} minutes...")
        logger.info(f"📊 Sending ratings every {interval_seconds} seconds to topic: {self.topic}")
        
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)
        ratings_sent = 0
        
        try:
            while datetime.now() < end_time:
                rating = self.generate_rating()
                if self.send_rating(rating):
                    ratings_sent += 1
                    logger.info(f"📝 Rating #{ratings_sent}: Customer {rating['customer_id']} gave {rating['rating_value']} stars")
                
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("⏹️  Simulation stopped by user")
        
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info(f"✅ Simulation completed! Total ratings sent: {ratings_sent}")
    
    def send_single_rating(self):
        """Send a single rating for testing"""
        rating = self.generate_rating()
        success = self.send_rating(rating)
        if success:
            logger.info(f"📝 Single rating sent: {rating}")
        self.producer.flush()
        self.producer.close()
        return success

if __name__ == "__main__":
    # Create producer instance
    producer = NailSalonRatingsProducer()
    
    # Run streaming simulation
    producer.run_streaming_simulation(duration_minutes=10, interval_seconds=3) 
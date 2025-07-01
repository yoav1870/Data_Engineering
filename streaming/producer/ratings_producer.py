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
        
    def generate_clean_rating(self):
        """Generate a clean, realistic customer rating"""
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
    
    def generate_dirty_rating(self):
        """Generate a rating with various data quality issues"""
        # Make 'invalid_rating' and 'null_values' more likely
        dirty_type = random.choices(
            [
                'invalid_rating', 'null_values', 'future_timestamp', 'old_timestamp', 
                'empty_comment', 'special_chars', 'duplicate', 'clean'
            ],
            weights=[3, 3, 1, 1, 1, 1, 1, 1],  # Higher weight for invalid_rating and null_values
            k=1
        )[0]
        
        if dirty_type == 'clean':
            return self.generate_clean_rating()
        
        # Base rating
        rating = {
            'customer_id': random.choice(self.customers),
            'branch_id': random.choice(self.branches),
            'employee_id': random.choice(self.employees),
            'treatment_id': random.choice(self.treatments),
            'rating_value': round(random.uniform(1.0, 5.0), 1),
            'comment': random.choice(self.comments),
            'timestamp': datetime.now().isoformat()
        }
        
        # Apply dirty data based on type
        if dirty_type == 'invalid_rating':
            # Invalid rating values, more likely to be above 5 or below 1
            invalid_ratings = [0.0, 0.5, 5.5, 6.0, 10.0, -1.0, 999.0, None]
            rating['rating_value'] = random.choice(invalid_ratings)
            logger.info(f"🧪 Generated invalid rating: {rating['rating_value']}")
            
        elif dirty_type == 'null_values':
            # Randomly null out some critical fields, more likely to hit critical ones
            null_fields = random.choices(
                ['customer_id', 'branch_id', 'employee_id', 'treatment_id', 'rating_value', 'comment', 'timestamp'],
                k=random.randint(1, 3)
            )
            for field in null_fields:
                rating[field] = None
            logger.info(f"🧪 Generated null values for: {null_fields}")
            
        elif dirty_type == 'future_timestamp':
            # Future timestamp (1-30 days in the future)
            future_days = random.randint(1, 30)
            rating['timestamp'] = (datetime.now() + timedelta(days=future_days)).isoformat()
            logger.info(f"🧪 Generated future timestamp: {future_days} days ahead")
            
        elif dirty_type == 'old_timestamp':
            # Very old timestamp (3-5 years ago)
            old_years = random.randint(3, 5)
            rating['timestamp'] = (datetime.now() - timedelta(days=old_years*365)).isoformat()
            logger.info(f"🧪 Generated old timestamp: {old_years} years ago")
            
        elif dirty_type == 'empty_comment':
            # Empty or whitespace-only comments
            empty_comments = ["", "   ", "\n", "\t", "   \n   "]
            rating['comment'] = random.choice(empty_comments)
            logger.info(f"🧪 Generated empty comment: '{rating['comment']}'")
            
        elif dirty_type == 'special_chars':
            # Comments with special characters that should be cleaned
            dirty_comments = [
                "Great service! 🎉🎊",
                "Amazing work!!! $$$",
                "Very satisfied @#$%^&*()",
                "Excellent experience <script>alert('xss')</script>",
                "Good job &amp; &lt; &gt;",
                "Okay service | \\ / : * ? \" < >",
                "Could be better [ ] { } ( )",
                "Not happy ~ ` ! @ # $ % ^ & * ( ) _ + = -",
                "Terrible experience \x00\x01\x02\x03",
                "Love the results! \n\r\t\b\f"
            ]
            rating['comment'] = random.choice(dirty_comments)
            logger.info(f"🧪 Generated dirty comment with special chars: '{rating['comment']}'")
            
        elif dirty_type == 'duplicate':
            # This will create exact duplicates that should be deduplicated
            # We'll generate the same rating multiple times
            pass  # Just use the base rating as-is, duplicates will be created by sending same data multiple times
        
        return rating
    
    def generate_rating(self):
        """Generate a rating (with 30% chance of dirty data for testing)"""
        if random.random() < 0.3:  # 30% chance of dirty data
            return self.generate_dirty_rating()
        else:
            return self.generate_clean_rating()
    
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
        logger.info(f"🧪 30% of ratings will contain dirty data for testing cleaning logic")
        
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)
        ratings_sent = 0
        dirty_ratings_sent = 0
        
        try:
            while datetime.now() < end_time:
                rating = self.generate_rating()
                
                # Check if this is dirty data
                is_dirty = (
                    rating.get('rating_value') is None or
                    (isinstance(rating.get('rating_value'), (int, float)) and (rating['rating_value'] < 1.0 or rating['rating_value'] > 5.0)) or
                    any(rating.get(field) is None for field in ['customer_id', 'branch_id', 'employee_id', 'treatment_id', 'timestamp']) or
                    rating.get('comment') in ["", "   ", "\n", "\t", "   \n   "] or
                    (rating.get('timestamp') and datetime.fromisoformat(rating['timestamp'].replace('Z', '+00:00')) > datetime.now()) or
                    (rating.get('timestamp') and datetime.fromisoformat(rating['timestamp'].replace('Z', '+00:00')) < datetime.now() - timedelta(days=730))
                )
                
                if self.send_rating(rating):
                    ratings_sent += 1
                    if is_dirty:
                        dirty_ratings_sent += 1
                        logger.info(f"🧪 Dirty rating #{ratings_sent}: {rating}")
                    else:
                        logger.info(f"📝 Clean rating #{ratings_sent}: Customer {rating['customer_id']} gave {rating['rating_value']} stars")
                
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("⏹️  Simulation stopped by user")
        
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info(f"✅ Simulation completed!")
            logger.info(f"📊 Total ratings sent: {ratings_sent}")
            logger.info(f"🧪 Dirty ratings sent: {dirty_ratings_sent}")
            logger.info(f"📝 Clean ratings sent: {ratings_sent - dirty_ratings_sent}")
    
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
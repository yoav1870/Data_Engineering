import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NailSalonRatingsConsumer:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Nail Salon Ratings Consumer") \
            .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.my_catalog.type", "hadoop") \
            .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "password") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.sql.streaming.checkpointLocation", "s3a://warehouse/checkpoints/ratings") \
            .getOrCreate()
        
        # Define schema for ratings data
        self.ratings_schema = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("branch_id", IntegerType(), True),
            StructField("employee_id", IntegerType(), True),
            StructField("treatment_id", IntegerType(), True),
            StructField("rating_value", FloatType(), True),
            StructField("comment", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])
        
        self.kafka_topic = "nail_salon_ratings"
        self.bootstrap_servers = "kafka:9092"
        
    def start_streaming(self):
        """Start the streaming job to consume ratings from Kafka"""
        logger.info("🚀 Starting Nail Salon Ratings Consumer...")
        
        try:
            # Read from Kafka
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.bootstrap_servers) \
                .option("subscribe", self.kafka_topic) \
                .option("startingOffsets", "latest") \
                .load()
            
            logger.info("✅ Successfully connected to Kafka topic")
            
            # Parse JSON data
            parsed_df = df \
                .selectExpr("CAST(value AS STRING) as json_data") \
                .select(from_json(col("json_data"), self.ratings_schema).alias("data")) \
                .select("data.*") \
                .withColumn("processed_timestamp", current_timestamp())
            
            logger.info("✅ Successfully parsed JSON data")
            
            # Write to Iceberg table using foreachBatch
            def write_to_iceberg(batch_df, batch_id):
                if batch_df.count() > 0:
                    logger.info(f"📝 Processing batch {batch_id} with {batch_df.count()} records")
                    
                    # Write to Bronze layer
                    batch_df.writeTo("my_catalog.bronze_ratings") \
                        .append()
                    
                    logger.info(f"✅ Batch {batch_id} written to Bronze layer")
            
            # Start the streaming query
            query = parsed_df \
                .writeStream \
                .foreachBatch(write_to_iceberg) \
                .outputMode("append") \
                .start()
            
            logger.info("✅ Streaming query started successfully")
            logger.info("📊 Waiting for data from Kafka...")
            
            # Wait for termination
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"❌ Error in streaming job: {str(e)}")
            raise
        finally:
            self.spark.stop()
    
    def create_bronze_table(self):
        """Create the Bronze table if it doesn't exist"""
        try:
            # Create Bronze table with proper schema
            self.spark.sql("""
                CREATE TABLE IF NOT EXISTS my_catalog.bronze_ratings (
                    customer_id INT,
                    branch_id INT,
                    employee_id INT,
                    treatment_id INT,
                    rating_value FLOAT,
                    comment STRING,
                    timestamp STRING,
                    processed_timestamp TIMESTAMP,
                    _kafka_topic STRING,
                    _kafka_partition INT,
                    _kafka_offset BIGINT
                ) USING iceberg
                PARTITIONED BY (days(timestamp))
            """)
            
            logger.info("✅ Bronze table created successfully")
            
        except Exception as e:
            logger.error(f"❌ Error creating Bronze table: {str(e)}")
            raise

if __name__ == "__main__":
    consumer = NailSalonRatingsConsumer()
    
    # Create Bronze table first
    consumer.create_bronze_table()
    
    # Start streaming
    consumer.start_streaming() 
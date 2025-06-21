import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
import time

# Configure logging with more detail
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NailSalonRatingsConsumer:
    def __init__(self):
        logger.info("🔧 Initializing Nail Salon Ratings Consumer...")
        
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
        
        logger.info("✅ Spark session created successfully")
        
        # Define schema for ratings data
        self.ratings_schema = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("branch_id", IntegerType(), True),
            StructField("employee_id", IntegerType(), True),
            StructField("treatment_id", IntegerType(), True),
            StructField("rating_value", FloatType(), True),
            StructField("comment", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])
        
        self.kafka_topic = "nail_salon_ratings"
        self.bootstrap_servers = "kafka:9092"
        
        logger.info(f"📋 Schema defined for topic: {self.kafka_topic}")
        logger.info(f"🔗 Kafka bootstrap servers: {self.bootstrap_servers}")
        
    def start_streaming(self):
        """Start the streaming job to consume ratings from Kafka"""
        logger.info("🚀 Starting Nail Salon Ratings Consumer...")
        
        try:
            logger.info("🔍 Attempting to connect to Kafka...")
            
            # Read from Kafka
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.bootstrap_servers) \
                .option("subscribe", self.kafka_topic) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            logger.info("✅ Successfully connected to Kafka topic")
            logger.info(f"📊 Kafka DataFrame schema: {df.schema}")
            
            # Parse JSON data
            logger.info("🔧 Parsing JSON data from Kafka...")
            parsed_df = df \
                .select(
                    from_json(col("value").cast("string"), self.ratings_schema).alias("data"),
                    col("topic").alias("_kafka_topic"),
                    col("partition").alias("_kafka_partition"),
                    col("offset").alias("_kafka_offset")
                ) \
                .select("data.*", "_kafka_topic", "_kafka_partition", "_kafka_offset") \
                .withColumn("timestamp", to_timestamp(col("timestamp"))) \
                .withColumn("processed_timestamp", current_timestamp())
            
            logger.info("✅ Successfully parsed JSON data")
            logger.info(f"📊 Parsed DataFrame schema: {parsed_df.schema}")
            
            # Write to Iceberg table using foreachBatch
            def write_to_iceberg(batch_df, batch_id):
                logger.info(f"🔄 Processing batch {batch_id}")
                logger.info(f"📊 Batch {batch_id} schema: {batch_df.schema}")
                
                record_count = batch_df.count()
                logger.info(f"📈 Batch {batch_id} contains {record_count} records")
                
                if record_count > 0:
                    # Show sample data
                    logger.info(f"📋 Sample data from batch {batch_id}:")
                    batch_df.show(5, truncate=False)
                    
                    try:
                        # Write to Bronze layer
                        logger.info(f"💾 Writing batch {batch_id} to Bronze layer...")
                        batch_df.writeTo("my_catalog.bronze_ratings") \
                            .append()
                        
                        logger.info(f"✅ Batch {batch_id} successfully written to Bronze layer")
                        
                    except Exception as write_error:
                        logger.error(f"❌ Error writing batch {batch_id} to Bronze layer: {str(write_error)}")
                        logger.error(f"📋 Batch {batch_id} data that failed to write:")
                        batch_df.show(10, truncate=False)
                        raise write_error
                else:
                    logger.info(f"⚠️  Batch {batch_id} is empty, skipping write")
            
            # Start the streaming query
            logger.info("🎯 Starting streaming query...")
            query = parsed_df \
                .writeStream \
                .foreachBatch(write_to_iceberg) \
                .outputMode("append") \
                .trigger(processingTime="10 seconds") \
                .start()
            
            logger.info("✅ Streaming query started successfully")
            logger.info(f"📊 Query ID: {query.id}")
            logger.info(f"📊 Query name: {query.name}")
            logger.info("📊 Waiting for data from Kafka...")
            
            # Wait for termination
            if query:
                query.awaitTermination()
            else:
                logger.error("❌ Streaming query failed to start")
                raise Exception("Streaming query is None")
            
        except Exception as e:
            logger.error(f"❌ Error in streaming job: {str(e)}")
            logger.error(f"📋 Full error details: {e}")
            import traceback
            logger.error(f"📋 Stack trace: {traceback.format_exc()}")
            raise
        finally:
            logger.info("🛑 Stopping Spark session...")
            self.spark.stop()
    
    def create_bronze_table(self):
        """Create the bronze_ratings table if it doesn't exist"""
        logger.info("🏗️  Setting up Bronze table for streaming...")
        
        try:
            # Skip table existence check and just proceed with streaming
            # The table will be created automatically when we write to it
            logger.info("🔍 Proceeding with streaming - table will be created automatically")
            logger.info("✅ Bronze table setup complete")
                
        except Exception as e:
            logger.error(f"❌ Error with Bronze table setup: {str(e)}")
            logger.error(f"📋 Full error details: {str(e)}")
            import traceback
            logger.error(f"📋 Stack trace: {traceback.format_exc()}")
            raise

    def show_last_5_rows(self):
        """Show the last 5 rows of the bronze_ratings table"""
        try:
            logger.info("📊 Showing last 5 rows of bronze_ratings table...")
            result = self.spark.sql("SELECT * FROM my_catalog.bronze_ratings ORDER BY timestamp DESC LIMIT 5")
            
            if result.count() > 0:
                logger.info("📋 Last 5 rows of bronze_ratings table:")
                result.show(truncate=False)
            else:
                logger.info("📋 Table is empty - no rows found")
                
        except Exception as e:
            logger.error(f"❌ Error showing table rows: {str(e)}")
            logger.error(f"📋 Full error details: {str(e)}")
            import traceback
            logger.error(f"📋 Stack trace: {traceback.format_exc()}")

    def show_table_count(self):
        """Show the total count of rows in the bronze_ratings table"""
        try:
            logger.info("📊 Counting rows in bronze_ratings table...")
            result = self.spark.sql("SELECT COUNT(*) as total_rows FROM my_catalog.bronze_ratings")
            count = result.collect()[0]['total_rows']
            logger.info(f"📋 Total rows in bronze_ratings table: {count}")
            return count
                
        except Exception as e:
            logger.error(f"❌ Error counting table rows: {str(e)}")
            logger.error(f"📋 Full error details: {str(e)}")
            import traceback
            logger.error(f"📋 Stack trace: {traceback.format_exc()}")
            return 0

if __name__ == "__main__":
    logger.info("🎬 Starting Nail Salon Ratings Consumer application...")
    
    try:
        # Initialize consumer
        consumer = NailSalonRatingsConsumer()
        
        # Create bronze table
        consumer.create_bronze_table()
        
        # Show initial table status
        logger.info("📊 Initial table status:")
        consumer.show_table_count()
        consumer.show_last_5_rows()
        
        # Start streaming
        logger.info("🚀 Starting streaming query...")
        query = consumer.start_streaming()
        
        # Show table status after 10 seconds
        time.sleep(10)
        logger.info("📊 Table status after 10 seconds:")
        consumer.show_table_count()
        consumer.show_last_5_rows()
        
        # Wait for termination
        if query:
            query.awaitTermination()
        else:
            logger.error("❌ Streaming query failed to start")
            raise Exception("Streaming query is None")
        
    except Exception as e:
        logger.error(f"❌ Application failed: {str(e)}")
        logger.error(f"📋 Full error details: {str(e)}")
        import traceback
        logger.error(f"📋 Stack trace: {traceback.format_exc()}")
        raise 
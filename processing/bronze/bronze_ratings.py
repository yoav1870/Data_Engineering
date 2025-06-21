from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Bronze Ratings Streaming Setup") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

print("✅ Setting up Bronze Ratings Streaming Table")

try:
    # Create the bronze table for streaming ratings
    # This table will be populated by the streaming consumer from Kafka
    spark.sql("""
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
    
    print("✅ Bronze ratings table created successfully")
    print("📝 Note: This table will be populated by the streaming consumer from Kafka")
    print("📊 Real-time ratings will flow: Kafka → Spark Streaming → bronze_ratings")

except Exception as e:
    print("❌ ERROR during table creation:", str(e))

spark.stop()

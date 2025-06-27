from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number, col, to_date, current_timestamp

# Spark session config
spark = SparkSession.builder \
    .appName("Silver Ratings Transformation") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

print("✅ Spark session created!")

# Read from bronze_ratings
bronze_df = spark.read.format("iceberg").load("my_catalog.bronze_ratings")

# Deduplicate (remove exact duplicates except Kafka metadata)
dedup_cols = [
    "customer_id", "branch_id", "employee_id", "treatment_id",
    "rating_value", "comment", "timestamp"
]
deduped_df = bronze_df.dropDuplicates(dedup_cols)

# Assign row_number as rating_id
window = Window.orderBy(col("timestamp"), col("customer_id"))
silver_df = deduped_df.withColumn("rating_id", row_number().over(window))

# Extract rating_date from timestamp
silver_df = silver_df.withColumn("rating_date", to_date(col("timestamp")))

# Add ingestion_time (current time)
silver_df = silver_df.withColumn("ingestion_time", current_timestamp())

# Create the silver_ratings table if it doesn't exist
spark.sql("""
    CREATE TABLE IF NOT EXISTS my_catalog.silver_ratings (
        rating_id INT,
        customer_id INT,
        branch_id INT,
        employee_id INT,
        treatment_id INT,
        rating_value FLOAT,
        comment STRING,
        rating_date DATE,
        timestamp TIMESTAMP,
        ingestion_time TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (rating_date)
""")

# Select and reorder columns for silver schema
silver_df = silver_df.select(
    "rating_id", "customer_id", "branch_id", "employee_id", "treatment_id",
    "rating_value", "comment", "rating_date", "timestamp", "ingestion_time"
)

# Write to silver_ratings (overwrite for demo; use .append() for incremental)
silver_df.writeTo("my_catalog.silver_ratings").overwritePartitions()

print("✅ Silver ratings table created and populated!")

spark.stop() 
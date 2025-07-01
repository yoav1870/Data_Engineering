from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max as spark_max, last, when

# Create SparkSession
spark = SparkSession.builder \
    .appName("Gold Customer Metrics") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

print(" Loading silver_ratings...")
df = spark.table("my_catalog.silver_ratings")

# Create table customer metrics
print(" Aggregating customer metrics...")
gold_df = df.groupBy("customer_id").agg(
    avg("rating_value").alias("avg_rating"),
    count("rating_id").alias("total_ratings"),
    last("rating_value").alias("last_rating")
).withColumn("is_vip", when(col("total_ratings") >= 10, True).otherwise(False))

print(" Writing to gold_customer_metrics table...")
# Create table if it doesn't exist
spark.sql("""
    CREATE TABLE IF NOT EXISTS my_catalog.gold_customer_metrics (
        customer_id INT,
        avg_rating FLOAT,
        total_ratings INT,
        last_rating FLOAT,
        is_vip BOOLEAN
    )
    USING iceberg
""")

# Write to the gold_customer_metrics table
gold_df.writeTo("my_catalog.gold_customer_metrics").overwritePartitions()

print(" --> Done writing gold_customer_metrics.")
spark.stop()

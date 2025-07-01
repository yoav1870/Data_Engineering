from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max as spark_max, datediff, current_date, last, when

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

print(" Loading silver_sessions...")
df = spark.table("my_catalog.silver_sessions")

# Create table customer metrics
print(" Aggregating customer metrics...")
gold_df = df.groupBy("customer_id").agg(
    avg("rating_value").alias("avg_rating"),
    count("session_id").alias("total_sessions"),
    spark_max("timestamp").alias("last_visit_date"),
    last("rating_value").alias("last_rating")
).withColumn("time_since_last_visit", datediff(current_date(), col("last_visit_date"))) \
 .withColumn("is_vip", when(col("total_sessions") >= 10, True).otherwise(False))

print(" Writing to gold_customer_metrics table...")
# Create table if it doesn't exist
spark.sql("""
    CREATE TABLE IF NOT EXISTS my_catalog.gold_customer_metrics (
        customer_id INT,
        avg_rating FLOAT,
        total_sessions INT,
        last_rating FLOAT,
        time_since_last_visit INT,
        is_vip BOOLEAN,
        last_visit_date DATE
    )
    USING iceberg
    PARTITIONED BY (last_visit_date)
""")

# Write to the gold_customer_metrics table
gold_df.writeTo("my_catalog.gold_customer_metrics").overwritePartitions()

print(" --> Done writing gold_customer_metrics.")
spark.stop()

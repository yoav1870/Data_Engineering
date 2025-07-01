from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum

# Create SparkSession
spark = SparkSession.builder \
    .appName("Gold Color Engagement") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

print(" >> Loading silver_inventory and silver_instagram data...")
inventory_df = spark.table("my_catalog.silver_inventory")
instagram_df = spark.table("my_catalog.silver_instagram")

# Calculate color usage from inventory
color_usage = inventory_df.groupBy("color_id").agg(
    count("inventory_id").alias("times_used")
)

# Instagram engagement aggregation
instagram_agg = instagram_df.groupBy("color_id").agg(
    spark_sum("likes").alias("total_likes"),
    spark_sum("comments").alias("total_comments")
)

# Join and calculate engagement score
# Avoid division by zero
gold_df = color_usage \
    .join(instagram_agg, on="color_id", how="left") \
    .withColumn("engagement_score", 
                (col("total_likes") + col("total_comments")) / (col("times_used") + 1e-6)) \
    .fillna({"total_likes": 0, "total_comments": 0, "engagement_score": 0.0})

print("✨ Writing gold_color_engagement table...")
gold_df.writeTo("my_catalog.gold_color_engagement").createOrReplace()

print(" --> Done.")
spark.stop()

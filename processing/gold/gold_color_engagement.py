from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count, sum as spark_sum, split, expr, row_number
from pyspark.sql.window import Window

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

print(" >> Loading silver_sessions and instagram data...")
sessions_df = spark.table("my_catalog.silver_sessions")
instagram_df = spark.read.option("header", True).csv("processing/dimensions/instagram.csv")

# convert colors_used to array and explode
sessions_exploded = sessions_df.withColumn("color", explode(split(col("colors_used"), ";")))

# calculate color usage
color_usage = sessions_exploded.groupBy("color").agg(
    count("*").alias("times_used")
)

# per month season calculation
sessions_with_season = sessions_exploded.withColumn("month", expr("month(timestamp)")) \
    .withColumn("season", expr("""
        CASE
            WHEN month IN (12, 1, 2) THEN 'Winter'
            WHEN month IN (3, 4, 5) THEN 'Spring'
            WHEN month IN (6, 7, 8) THEN 'Summer'
            WHEN month IN (9, 10, 11) THEN 'Fall'
        END
    """))

top_season_window = Window.partitionBy("color").orderBy(count("*").desc())
top_season = sessions_with_season.groupBy("color", "season").count() \
    .withColumn("rank", row_number().over(top_season_window)) \
    .filter(col("rank") == 1) \
    .select("color", col("season").alias("top_season"))

# instagram data aggregation
instagram_agg = instagram_df.groupBy("color_id").agg(
    spark_sum("likes").alias("total_likes"),
    spark_sum("comments").alias("total_comments")
).withColumnRenamed("color_id", "color")

# the final gold table
gold_df = color_usage \
    .join(instagram_agg, on="color", how="left") \
    .join(top_season, on="color", how="left") \
    .withColumn("engagement_score", 
                (col("total_likes") + col("total_comments")) / col("times_used")) \
    .fillna({"total_likes": 0, "total_comments": 0, "top_season": "Unknown", "engagement_score": 0.0}) \
    .withColumnRenamed("color", "color_id") \
    .withColumn("color_id", col("color_id").cast("int"))

# write the gold table
print("✨ Writing gold_color_engagement table...")
gold_df.writeTo("my_catalog.gold_color_engagement").createOrReplace()

print(" --> Done.")

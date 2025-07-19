import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lit, row_number, lead, to_date
from datetime import datetime

# Config
CATALOG = "my_catalog"
DIM_TABLE = f"{CATALOG}.dim_customers"
SILVER_RATINGS = f"{CATALOG}.silver_ratings"

# Spark session
spark = SparkSession.builder \
    .appName("Update dim_customers SCD2 from silver_ratings") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

print("\n🚀 Starting SCD2 update for dim_customers from silver_ratings...")

# 1. Read all ratings (ordered by customer and rating date)
ratings_df = spark.read.format("iceberg").load(SILVER_RATINGS)
ratings_df = ratings_df.withColumn("rating_date", to_date(col("timestamp")))

# 2. For each customer, order by rating_date, assign version_id, and calculate end_date
window = Window.partitionBy("customer_id").orderBy(col("rating_date"))

scd2_df = ratings_df \
    .select("customer_id", "rating_date") \
    .withColumn("version_id", row_number().over(window)) \
    .withColumn("start_date", col("rating_date")) \
    .withColumn("end_date", lead("rating_date").over(window))

# 3. Set is_current flag and null out end_date for the latest record
scd2_df = scd2_df.withColumn(
    "is_current",
    col("end_date").isNull()
)
scd2_df = scd2_df.withColumn(
    "end_date",
    col("end_date").cast("date")
)

# 4. Add null/defaults for other attributes
scd2_df = scd2_df \
    .withColumn("first_name", lit(None).cast("string")) \
    .withColumn("last_name", lit(None).cast("string")) \
    .withColumn("email", lit(None).cast("string")) \
    .withColumn("phone", lit(None).cast("string")) \
    .withColumn("address", lit(None).cast("string")) \
    .withColumn("city", lit(None).cast("string")) \
    .withColumn("created_timestamp", lit(datetime.now()))

# 5. Reorder columns to match dim_customers schema
final_cols = [
    "customer_id", "version_id", "first_name", "last_name", "email", "phone", "address", "city",
    "start_date", "end_date", "is_current", "created_timestamp"
]
scd2_df = scd2_df.select(final_cols)

# 6. Overwrite dim_customers with new SCD2 records
scd2_df.writeTo(DIM_TABLE).overwritePartitions()
print(f"✅ SCD2 update complete. {scd2_df.count()} records now in {DIM_TABLE}.")

spark.stop()
print("🏁 SCD2 update finished.") 
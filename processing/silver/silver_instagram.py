from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, isnan, count
from pyspark.sql.types import IntegerType, StringType, DateType

# Spark session config
spark = SparkSession.builder \
    .appName("Silver Instagram Transformation") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

print("✅ Spark session created successfully")

# Read bronze_instagram table
bronze_df = spark.read.format("iceberg").load("my_catalog.bronze_instagram")
print(f"📥 Read {bronze_df.count()} records from bronze_instagram")

# Read date_dim from Iceberg table (MinIO)
date_dim = spark.read.format("iceberg").load("my_catalog.dim_date")
date_dim = date_dim.withColumn("date_id", col("date_id").cast(IntegerType())) \
                     .withColumn("date", col("date").cast(DateType()))

# Join on post_date = date to get date_id and season
joined = bronze_df.join(date_dim, bronze_df.post_date == date_dim.date, "left")

# Select and reorder columns
silver_df = joined.select(
    col("post_id").cast(IntegerType()),
    col("color_id").cast(IntegerType()),
    col("date_id").cast(IntegerType()),
    col("likes").cast(IntegerType()),
    col("comments").cast(IntegerType()),
    col("post_date").cast(DateType()),
    col("ingestion_date").cast(DateType()),
    col("season").cast(StringType())
)

# ===== DATA CLEANING =====
print("🧹 Cleaning data...")
# Remove records with nulls in key fields
key_fields = ["post_id", "color_id", "date_id", "likes", "comments", "post_date", "ingestion_date", "season"]
cleaned = silver_df
for c in key_fields:
    cleaned = cleaned.filter(col(c).isNotNull())
# Filter out negative likes/comments
cleaned = cleaned.filter((col("likes") >= 0) & (col("comments") >= 0))
# Drop duplicates (keep latest by ingestion_date)
window_cols = ["post_id", "color_id", "date_id"]
cleaned = cleaned.dropDuplicates(window_cols)

# ===== DATA VERIFICATION =====
print("\n📊 Data Quality Summary:")
total = silver_df.count()
cleaned_count = cleaned.count()
dropped = total - cleaned_count
print(f"  - Total records in bronze: {total}")
print(f"  - Records after cleaning: {cleaned_count}")
print(f"  - Records written to silver: {cleaned_count}")
print(f"  - Dropped records: {dropped}")
# Null counts per column
for c in key_fields:
    nulls = cleaned.filter(col(c).isNull()).count()
    print(f"  - {c}: {nulls} nulls after cleaning")

# ===== WRITE TO SILVER TABLE =====
print("🏗️ Writing to silver_instagram table...")
try:
    spark.sql("DROP TABLE IF EXISTS my_catalog.silver_instagram")
    cleaned.writeTo("my_catalog.silver_instagram") \
        .tableProperty("write.format.default", "parquet") \
        .create()
    print(f"✅ Silver instagram table created and populated with {cleaned_count} records!")
except Exception as e:
    print(f"❌ ERROR during table creation: {str(e)}")
finally:
    spark.stop()
    print("🛑 Spark session stopped") 
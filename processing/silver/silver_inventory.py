from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, current_timestamp, col, isnan, count
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
# Spark session config
spark = SparkSession.builder \
    .appName("Silver Inventory Transformation") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

print("✅ Spark session created!")

# Read from bronze_inventory
df = spark.read.format("iceberg").load("my_catalog.bronze_inventory")

total_records = df.count()
print(f"📊 Found {total_records} records in bronze_inventory")

# ===== DATA CLEANING =====
print("🧹 Starting data cleaning...")

# Remove records with nulls in key fields
cleaned_df = df.dropna(subset=["record_id", "branch_id", "color_id", "quantity_used", "timestamp"])

# Remove records with negative or zero quantity_used
cleaned_df = cleaned_df.filter(col("quantity_used") > 0)


window = Window.partitionBy("record_id", "branch_id", "color_id").orderBy(col("timestamp").desc())
cleaned_df = cleaned_df.withColumn("row_num", row_number().over(window)).filter(col("row_num") == 1).drop("row_num")

cleaned_count = cleaned_df.count()
dropped_count = total_records - cleaned_count
print(f"✅ Data cleaning completed! {cleaned_count} records after cleaning, {dropped_count} records dropped.")

# ===== DATA VERIFICATION =====
print("\n🔍 Data Quality Verification:")
print("=" * 50)

# Check null counts for each column
for c in cleaned_df.columns:
    if c in ['quantity_used']:  # Only numeric columns
        nulls = cleaned_df.filter(col(c).isNull() | isnan(col(c))).count()
    else:  # Non-numeric columns
        nulls = cleaned_df.filter(col(c).isNull()).count()
    print(f"  {c}: {nulls} null values")

# Check minimum quantity_used value
min_quantity = cleaned_df.agg({"quantity_used": "min"}).collect()[0][0]
print(f"  Minimum quantity_used: {min_quantity}")

# Transform to silver schema
silver_df = cleaned_df.withColumn("report_date", to_date(col("timestamp"))) \
    .withColumn("ingestion_time", current_timestamp()) \
    .select(
        "record_id",
        "branch_id",
        "color_id",
        col("quantity_used"),
        col("report_date"),
        col("ingestion_time")
    )

# Drop and recreate the silver_inventory table for schema consistency
try:
    spark.sql("DROP TABLE IF EXISTS my_catalog.silver_inventory")
    print("  🔄 Dropped existing silver_inventory table")
except Exception as e:
    print(f"  ⚠️  Warning when dropping table: {e}")

spark.sql("""
    CREATE TABLE my_catalog.silver_inventory (
        record_id INT,
        branch_id INT,
        color_id INT,
        quantity_used INT,
        report_date DATE,
        ingestion_time TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (report_date)
""")
print("  ✅ Created silver_inventory table with correct schema")

# Write to silver_inventory
generated_count = silver_df.count()
silver_df.writeTo("my_catalog.silver_inventory").overwritePartitions()
print(f"✅ Silver inventory table created and populated with {generated_count} records!")

# ===== DATA QUALITY SUMMARY =====
print("\n📊 Data Quality Summary:")
print(f"  - Total records in bronze: {total_records}")
print(f"  - Records after cleaning: {cleaned_count}")
print(f"  - Records written to silver: {generated_count}")
print(f"  - Dropped records: {dropped_count}")

spark.stop() 
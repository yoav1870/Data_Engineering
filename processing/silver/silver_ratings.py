from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number, col, to_date, current_timestamp, when, isnan, trim, length, regexp_replace

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
print("📖 Reading from bronze_ratings table...")
bronze_df = spark.read.format("iceberg").load("my_catalog.bronze_ratings")

if bronze_df.count() == 0:
    print("⚠️  No data found in bronze_ratings table.")
    spark.stop()
    exit(1)

print(f"📊 Found {bronze_df.count()} records in bronze_ratings")

# ===== DATA CLEANING STEP =====
print("🧹 Starting data cleaning process...")

print("  🔄 Step 1: Removing records with critical nulls...")
cleaned_df = bronze_df.filter(
    col("customer_id").isNotNull() &
    col("branch_id").isNotNull() &
    col("employee_id").isNotNull() &
    col("treatment_id").isNotNull() &
    col("rating_value").isNotNull() &
    col("timestamp").isNotNull()
)

print("  🔄 Step 2: Filtering invalid rating values...")
cleaned_df = cleaned_df.filter(
    (col("rating_value") >= 1.0) & 
    (col("rating_value") <= 5.0) & 
    ~isnan(col("rating_value"))
)

print("  🔄 Step 3: Cleaning comment field...")
cleaned_df = cleaned_df.withColumn(
    "comment",
    when(
        (col("comment").isNull()) | 
        (trim(col("comment")) == "") |
        (length(trim(col("comment"))) == 0),
        "No comment provided"
    ).otherwise(
        trim(regexp_replace(col("comment"), r'[^\w\s\.\,\!\?\-]', ''))
    )
)

print("  🔄 Step 4: Filtering future timestamps...")
cleaned_df = cleaned_df.filter(
    col("timestamp") <= current_timestamp()
)

print("  🔄 Step 5: Filtering very old timestamps...")
from datetime import datetime, timedelta
two_years_ago = datetime.now() - timedelta(days=730)
cleaned_df = cleaned_df.filter(
    col("timestamp") >= two_years_ago
)

print("  🔄 Step 6: Removing duplicates...")
window_spec = Window.partitionBy(
    "customer_id", "branch_id", "employee_id", "treatment_id", 
    "rating_value", "comment"
).orderBy(col("timestamp").desc())

cleaned_df = cleaned_df.withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

print(f"✅ Data cleaning completed! {cleaned_df.count()} records after cleaning")

# ===== DATA TRANSFORMATION STEP =====
print("🔄 Starting data transformation...")

# Assign row_number as rating_id
print("  🔄 Assigning rating_id...")
window = Window.orderBy(col("timestamp"), col("customer_id"))
silver_df = cleaned_df.withColumn("rating_id", row_number().over(window))

# Extract rating_date from timestamp
silver_df = silver_df.withColumn("rating_date", to_date(col("timestamp")))

# Add ingestion_time (current time)
silver_df = silver_df.withColumn("ingestion_time", current_timestamp())

# Add data quality score
silver_df = silver_df.withColumn(
    "data_quality_score",
    when(col("comment") == "No comment provided", 0.8)
    .otherwise(1.0)
)

# ===== DATA VERIFICATION =====
print("\n📊 Data Quality Summary:")
total = silver_df.count()
cleaned_count = cleaned_df.count()
dropped = total - cleaned_count
print(f"  - Total records in bronze: {total}")
print(f"  - Records after cleaning: {cleaned_count}")
print(f"  - Records written to silver: {cleaned_count}")
print(f"  - Dropped records: {dropped}")
# Null counts per column
key_fields = ["rating_id", "customer_id", "branch_id", "employee_id", "treatment_id", "rating_value", "comment", "rating_date", "timestamp", "ingestion_time", "data_quality_score"]
for c in key_fields:
    nulls = silver_df.filter(col(c).isNull()).count()
    print(f"  - {c}: {nulls} nulls after cleaning")

# ===== WRITE TO SILVER TABLE =====
print("🏗️ Writing to silver_ratings table...")
try:
    spark.sql("DROP TABLE IF EXISTS my_catalog.silver_ratings")
    silver_df.writeTo("my_catalog.silver_ratings") \
        .tableProperty("write.format.default", "parquet") \
        .create()
    print(f"✅ Silver ratings table created and populated with {cleaned_count} records!")
except Exception as e:
    print(f"❌ ERROR during table creation: {str(e)}")
finally:
    spark.stop()
    print("🛑 Spark session stopped") 
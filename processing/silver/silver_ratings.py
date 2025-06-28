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

# Step 1: Remove records with critical nulls
print("  🔄 Step 1: Removing records with critical nulls...")
cleaned_df = bronze_df.filter(
    col("customer_id").isNotNull() &
    col("branch_id").isNotNull() &
    col("employee_id").isNotNull() &
    col("treatment_id").isNotNull() &
    col("rating_value").isNotNull() &
    col("timestamp").isNotNull()
)

# Step 2: Filter out invalid rating values
print("  🔄 Step 2: Filtering invalid rating values...")
cleaned_df = cleaned_df.filter(
    (col("rating_value") >= 1.0) & 
    (col("rating_value") <= 5.0) & 
    ~isnan(col("rating_value"))
)

# Step 3: Clean comment field
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

# Step 4: Filter out future timestamps
print("  🔄 Step 4: Filtering future timestamps...")
cleaned_df = cleaned_df.filter(
    col("timestamp") <= current_timestamp()
)

# Step 5: Filter out very old timestamps (older than 2 years)
print("  🔄 Step 5: Filtering very old timestamps...")
from datetime import datetime, timedelta
two_years_ago = datetime.now() - timedelta(days=730)
cleaned_df = cleaned_df.filter(
    col("timestamp") >= two_years_ago
)

# Step 6: Remove duplicates (keeping the latest record)
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

# Create the silver_ratings table if it doesn't exist
print("🏗️ Creating silver_ratings table...")

# Drop the table if it exists to ensure schema consistency
try:
    spark.sql("DROP TABLE IF EXISTS my_catalog.silver_ratings")
    print("  🔄 Dropped existing silver_ratings table")
except Exception as e:
    print(f"  ⚠️  Warning when dropping table: {e}")

# Create the table with the correct schema
spark.sql("""
    CREATE TABLE my_catalog.silver_ratings (
        rating_id INT,
        customer_id INT,
        branch_id INT,
        employee_id INT,
        treatment_id INT,
        rating_value FLOAT,
        comment STRING,
        rating_date DATE,
        timestamp TIMESTAMP,
        ingestion_time TIMESTAMP,
        data_quality_score FLOAT
    ) USING iceberg
    PARTITIONED BY (rating_date)
""")
print("  ✅ Created silver_ratings table with correct schema")

# Select and reorder columns for silver schema
silver_df = silver_df.select(
    "rating_id", "customer_id", "branch_id", "employee_id", "treatment_id",
    "rating_value", "comment", "rating_date", "timestamp", "ingestion_time",
    "data_quality_score"
)

# Write to silver_ratings (overwrite for demo; use .append() for incremental)
print("💾 Writing to silver_ratings table...")
silver_df.writeTo("my_catalog.silver_ratings").overwritePartitions()

print(f"✅ Silver ratings table created and populated with {silver_df.count()} records!")

# Show some statistics
print("\n📊 Silver Ratings Statistics:")
print("=" * 40)
silver_df.groupBy("rating_value").count().orderBy("rating_value").show()
print(f"Average data quality score: {silver_df.agg({'data_quality_score': 'avg'}).collect()[0]['avg(data_quality_score)']:.2f}")

spark.stop() 
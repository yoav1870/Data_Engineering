from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType

# Spark session configuration
spark = SparkSession.builder \
    .appName("Bronze Instagram Table Creation") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

print("✅ Spark session created successfully")

# Define the schema for the bronze_instagram table
schema = StructType([
    StructField("post_id", IntegerType(), True),
    StructField("color_id", IntegerType(), True),
    StructField("likes", IntegerType(), True),
    StructField("comments", IntegerType(), True),
    StructField("post_date", DateType(), True),
    StructField("ingestion_date", DateType(), True)
])

try:
    # Read the Instagram CSV data from MinIO
    print("📖 Reading instagram.csv from MinIO...")
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .load("s3a://raw-data/instagram.csv")

    print("✅ Successfully read instagram.csv from MinIO")
    df.printSchema()
    df.show(5)

    # Create the bronze_instagram bronze table
    print("🏗️ Creating bronze_instagram table in Bronze layer...")
    df.writeTo("my_catalog.bronze_instagram") \
        .tableProperty("write.format.default", "parquet") \
        .create()

    print("✅ Successfully created and populated 'bronze_instagram' table.")

except Exception as e:
    print(f"❌ ERROR during table creation: {str(e)}")

finally:
    spark.stop()
    print("🛑 Spark session stopped") 
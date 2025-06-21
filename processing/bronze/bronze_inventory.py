from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType

# Spark session configuration
spark = SparkSession.builder \
    .appName("Bronze Inventory Table Creation") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

print("✅ Spark session created successfully")

# Define the schema for the raw_inventory table
# This must match the CSV file structure and the target table definition
schema = StructType([
    StructField("record_id", IntegerType(), True),
    StructField("branch_id", IntegerType(), True),
    StructField("color_id", IntegerType(), True),
    StructField("quantity_used", IntegerType(), True),
    StructField("timestamp", TimestampType(), True)
])

try:
    # Read the inventory CSV data from MinIO
    print("📖 Reading inventory.csv from MinIO...")
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .load("s3a://raw-data/inventory.csv")

    print("✅ Successfully read inventory.csv from MinIO")
    df.printSchema()
    df.show(5)

    # Create the bronze_inventory bronze table
    print("🏗️ Creating bronze_inventory table in Bronze layer...")
    df.writeTo("my_catalog.bronze_inventory") \
        .tableProperty("write.format.default", "parquet") \
        .create()

    print("✅ Successfully created and populated 'bronze_inventory' table.")

except Exception as e:
    print(f"❌ ERROR during table creation: {str(e)}")

finally:
    spark.stop()
    print("🛑 Spark session stopped") 
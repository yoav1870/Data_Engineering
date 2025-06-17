from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Bronze Ratings Ingestion") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

print("✅ Starting Spark job")

try:
    df = spark.read.option("header", True).option("inferSchema", True) \
        .csv("s3a://raw-data/ratings.csv")

    print("✅ Read ratings.csv successfully")
    df.printSchema()
    df.show(5)

    print("✅ Writing to Iceberg table...")
    df.writeTo("my_catalog.bronze_ratings").createOrReplace()

    print("✅ Write complete")

except Exception as e:
    print("❌ ERROR during Spark job:", str(e))

spark.stop()

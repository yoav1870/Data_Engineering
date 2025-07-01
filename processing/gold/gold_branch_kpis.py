from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum as spark_sum

# Create SparkSession
spark = SparkSession.builder \
    .appName("Gold Branch KPIs") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

print("🟦 Loading silver_sessions + employees + branches...")
sessions_df = spark.table("my_catalog.silver_sessions")
employees_df = spark.read.option("header", True).csv("processing/dimensions/employees.csv")
branches_df = spark.read.option("header", True).csv("processing/dimensions/branches.csv")

# KPI aggregation
agg_df = sessions_df.groupBy("branch_id").agg(
    avg("rating_value").alias("avg_rating"),
    count("session_id").alias("total_sessions"),
    spark_sum("payment_amount").alias("total_revenue")
)

# calculate the number of active employees per branch
active_employees = employees_df.filter(col("status") == "active") \
    .groupBy("branch_id").count() \
    .withColumnRenamed("count", "num_active_employees")

# add branch names to the aggregated DataFrame
agg_with_branch = agg_df.join(branches_df.select("branch_id", "branch_name"), on="branch_id", how="left")

# add active employees count
final_df = agg_with_branch.join(active_employees, on="branch_id", how="left") \
    .fillna({"num_active_employees": 0})

# writing the final table --> gold_branch_kpis 
print("Writing gold_branch_kpis table...")
final_df.writeTo("my_catalog.gold_branch_kpis").createOrReplace()

print(" --> Done.")

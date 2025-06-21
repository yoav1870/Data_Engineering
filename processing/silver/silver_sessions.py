#!/usr/bin/env python3
"""
Silver Layer - Sessions Transformation
Transforms bronze ratings into enriched session data with dimension lookups
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp, to_date, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType

class SilverSessionsTransformer:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Silver Sessions Transformer") \
            .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.my_catalog.type", "hadoop") \
            .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "password") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()
    
    def create_silver_sessions_table(self):
        """Create the Silver sessions table structure"""
        print("🏗️  Creating Silver sessions table structure...")
        
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS my_catalog.silver_sessions (
                session_id STRING,
                customer_id INT,
                customer_first_name STRING,
                customer_last_name STRING,
                customer_email STRING,
                customer_city STRING,
                branch_id INT,
                branch_name STRING,
                branch_city STRING,
                employee_id INT,
                employee_first_name STRING,
                employee_last_name STRING,
                employee_role STRING,
                employee_experience_years INT,
                treatment_id INT,
                treatment_name STRING,
                treatment_price DECIMAL(10,2),
                treatment_duration_minutes INT,
                rating_value FLOAT,
                rating_category STRING,
                comment STRING,
                session_date DATE,
                session_year INT,
                session_month INT,
                session_day INT,
                session_season STRING,
                is_weekend BOOLEAN,
                is_holiday BOOLEAN,
                timestamp STRING,
                processed_timestamp TIMESTAMP
            ) USING iceberg
            PARTITIONED BY (session_date)
        """)
        
        print("✅ Silver sessions table structure created")
    
    def transform_bronze_to_silver(self):
        """Transform bronze ratings into silver sessions with dimension lookups"""
        print("🔄 Transforming bronze ratings to silver sessions...")
        
        try:
            # Read bronze ratings data
            bronze_df = self.spark.table("my_catalog.bronze_ratings")
            print(f"📊 Read {bronze_df.count()} records from bronze_ratings")
            
            # Read dimension tables
            customers_df = self.spark.table("my_catalog.dim_customers")
            branches_df = self.spark.table("my_catalog.dim_branches")
            employees_df = self.spark.table("my_catalog.dim_employees")
            treatments_df = self.spark.table("my_catalog.dim_treatments")
            date_df = self.spark.table("my_catalog.dim_date")
            
            print("✅ Loaded all dimension tables")
            
            # Join with dimensions to create enriched sessions
            silver_df = bronze_df \
                .join(
                    customers_df.filter(col("is_current") == True),
                    "customer_id",
                    "left"
                ) \
                .join(
                    branches_df.filter(col("active") == True),
                    "branch_id",
                    "left"
                ) \
                .join(
                    employees_df.filter(col("active") == True),
                    "employee_id",
                    "left"
                ) \
                .join(
                    treatments_df.filter(col("active") == True),
                    "treatment_id",
                    "left"
                ) \
                .join(
                    date_df,
                    to_date(col("timestamp")) == col("date"),
                    "left"
                )
            
            print("✅ Joined bronze data with all dimensions")
            
            # Generate session ID and add business logic
            silver_enriched = silver_df \
                .withColumn("session_id", 
                    col("customer_id").cast("string") + "_" + 
                    col("timestamp").substr(1, 19).replace(":", "").replace("-", "").replace("T", "_")
                ) \
                .withColumn("rating_category",
                    when(col("rating_value") >= 4.5, "Excellent")
                    .when(col("rating_value") >= 4.0, "Very Good")
                    .when(col("rating_value") >= 3.5, "Good")
                    .when(col("rating_value") >= 3.0, "Average")
                    .when(col("rating_value") >= 2.0, "Below Average")
                    .otherwise("Poor")
                ) \
                .withColumn("session_date", to_date(col("timestamp"))) \
                .withColumn("session_year", year(col("timestamp"))) \
                .withColumn("session_month", month(col("timestamp"))) \
                .withColumn("session_day", dayofmonth(col("timestamp"))) \
                .withColumn("processed_timestamp", current_timestamp())
            
            print("✅ Applied business logic and transformations")
            
            # Select final columns for silver table
            final_silver = silver_enriched.select(
                "session_id",
                "customer_id",
                "first_name as customer_first_name",
                "last_name as customer_last_name",
                "email as customer_email",
                "city as customer_city",
                "branch_id",
                "name as branch_name",
                "city as branch_city",
                "employee_id",
                "first_name as employee_first_name",
                "last_name as employee_last_name",
                "role as employee_role",
                "experience_years as employee_experience_years",
                "treatment_id",
                "name as treatment_name",
                "price as treatment_price",
                "duration_minutes as treatment_duration_minutes",
                "rating_value",
                "rating_category",
                "comment",
                "session_date",
                "session_year",
                "session_month",
                "session_day",
                "season as session_season",
                "is_weekend",
                "is_holiday",
                "timestamp",
                "processed_timestamp"
            )
            
            print("✅ Prepared final silver dataset")
            
            # Write to silver table
            final_silver.writeTo("my_catalog.silver_sessions").append()
            
            print(f"✅ Successfully wrote {final_silver.count()} records to silver_sessions")
            
            return final_silver
            
        except Exception as e:
            print(f"❌ Error in transformation: {str(e)}")
            raise
    
    def validate_silver_data(self):
        """Validate the silver data quality"""
        print("🔍 Validating silver data quality...")
        
        try:
            silver_df = self.spark.table("my_catalog.silver_sessions")
            
            # Data quality checks
            total_records = silver_df.count()
            print(f"📊 Total records: {total_records}")
            
            # Check for nulls in key fields
            null_checks = [
                ("customer_id", silver_df.filter(col("customer_id").isNull()).count()),
                ("branch_id", silver_df.filter(col("branch_id").isNull()).count()),
                ("employee_id", silver_df.filter(col("employee_id").isNull()).count()),
                ("treatment_id", silver_df.filter(col("treatment_id").isNull()).count()),
                ("rating_value", silver_df.filter(col("rating_value").isNull()).count())
            ]
            
            print("🔍 Null value checks:")
            for field, null_count in null_checks:
                percentage = (null_count / total_records * 100) if total_records > 0 else 0
                print(f"  {field}: {null_count} nulls ({percentage:.2f}%)")
            
            # Rating distribution
            print("📊 Rating distribution:")
            silver_df.groupBy("rating_category").count().orderBy("count", ascending=False).show()
            
            # Branch performance
            print("🏢 Branch performance (avg rating):")
            silver_df.groupBy("branch_name").agg(
                {"rating_value": "avg", "session_id": "count"}
            ).withColumnRenamed("avg(rating_value)", "avg_rating") \
             .withColumnRenamed("count(session_id)", "session_count") \
             .orderBy("avg_rating", ascending=False).show()
            
            print("✅ Silver data validation completed")
            
        except Exception as e:
            print(f"❌ Error in validation: {str(e)}")
    
    def create_sample_queries(self):
        """Create sample business queries for silver data"""
        print("📋 Creating sample business queries...")
        
        silver_df = self.spark.table("my_catalog.silver_sessions")
        
        print("\n1. Top performing employees by average rating:")
        silver_df.groupBy("employee_first_name", "employee_last_name", "employee_role") \
            .agg({"rating_value": "avg", "session_id": "count"}) \
            .withColumnRenamed("avg(rating_value)", "avg_rating") \
            .withColumnRenamed("count(session_id)", "session_count") \
            .orderBy("avg_rating", ascending=False) \
            .show(10)
        
        print("\n2. Most popular treatments:")
        silver_df.groupBy("treatment_name", "treatment_price") \
            .count() \
            .orderBy("count", ascending=False) \
            .show(10)
        
        print("\n3. Customer satisfaction by season:")
        silver_df.groupBy("session_season") \
            .agg({"rating_value": "avg", "session_id": "count"}) \
            .withColumnRenamed("avg(rating_value)", "avg_rating") \
            .withColumnRenamed("count(session_id)", "session_count") \
            .orderBy("avg_rating", ascending=False) \
            .show()
        
        print("\n4. Weekend vs weekday performance:")
        silver_df.groupBy("is_weekend") \
            .agg({"rating_value": "avg", "session_id": "count"}) \
            .withColumnRenamed("avg(rating_value)", "avg_rating") \
            .withColumnRenamed("count(session_id)", "session_count") \
            .show()
    
    def run_complete_transformation(self):
        """Run the complete silver transformation process"""
        print("🚀 Starting complete silver transformation process...")
        
        try:
            # Create table structure
            self.create_silver_sessions_table()
            
            # Transform data
            silver_data = self.transform_bronze_to_silver()
            
            # Validate data
            self.validate_silver_data()
            
            # Show sample queries
            self.create_sample_queries()
            
            print("✅ Silver transformation completed successfully!")
            
        except Exception as e:
            print(f"❌ Error in complete transformation: {str(e)}")
            raise
        finally:
            self.spark.stop()

if __name__ == "__main__":
    transformer = SilverSessionsTransformer()
    transformer.run_complete_transformation() 
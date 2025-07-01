#!/usr/bin/env python3
"""
Create Dimension Tables for Nail Salon
Creates all dimension tables and loads sample data
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import os

class DimensionTableCreator:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Nail Salon Dimension Tables Creator") \
            .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.my_catalog.type", "hadoop") \
            .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "password") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()
    
    def create_colors_table(self):
        """Create COLORS dimension table"""
        print("🎨 Creating COLORS dimension table...")
        
        # Create table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS my_catalog.dim_colors (
                color_id INT,
                name STRING,
                hex_code STRING,
                category STRING,
                active BOOLEAN,
                created_timestamp TIMESTAMP
            ) USING iceberg
        """)
        
        # Load data from CSV
        try:
            df = self.spark.read.option("header", True).option("inferSchema", True) \
                .csv("s3a://raw-data/colors.csv")
            
            df_with_timestamp = df.withColumn("created_timestamp", current_timestamp())
            
            df_with_timestamp.writeTo("my_catalog.dim_colors").append()
            print(f"✅ COLORS table created and loaded with {df.count()} records")
            
        except Exception as e:
            print(f"⚠️  Could not load colors.csv: {e}")
            print("📝 Creating empty COLORS table structure")
    
    def create_branches_table(self):
        """Create BRANCHES dimension table"""
        print("🏢 Creating BRANCHES dimension table...")
        
        # Create table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS my_catalog.dim_branches (
                branch_id INT,
                name STRING,
                city STRING,
                address STRING,
                active BOOLEAN,
                opening_date DATE,
                created_timestamp TIMESTAMP
            ) USING iceberg
        """)
        
        # Load data from CSV
        try:
            df = self.spark.read.option("header", True).option("inferSchema", True) \
                .csv("s3a://raw-data/branches.csv")
            
            df_with_timestamp = df.withColumn("created_timestamp", current_timestamp())
            
            df_with_timestamp.writeTo("my_catalog.dim_branches").append()
            print(f"✅ BRANCHES table created and loaded with {df.count()} records")
            
        except Exception as e:
            print(f"⚠️  Could not load branches.csv: {e}")
            print("📝 Creating empty BRANCHES table structure")
    
    def create_employees_table(self):
        """Create EMPLOYEES dimension table"""
        print("👩‍💼 Creating EMPLOYEES dimension table...")
        # Drop table if exists to ensure schema update
        self.spark.sql("DROP TABLE IF EXISTS my_catalog.dim_employees")
        # Create table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS my_catalog.dim_employees (
                employee_id INT,
                first_name STRING,
                last_name STRING,
                role STRING,
                experience_years INT,
                active BOOLEAN,
                hire_date DATE,
                branch_id INT,
                created_timestamp TIMESTAMP
            ) USING iceberg
        """)
        
        # Load data from CSV
        try:
            df = self.spark.read.option("header", True).option("inferSchema", True) \
                .csv("s3a://raw-data/employees.csv")
            
            df_with_timestamp = df.withColumn("created_timestamp", current_timestamp())
            
            df_with_timestamp.writeTo("my_catalog.dim_employees").append()
            print(f"✅ EMPLOYEES table created and loaded with {df.count()} records")
            
        except Exception as e:
            print(f"⚠️  Could not load employees.csv: {e}")
            print("📝 Creating empty EMPLOYEES table structure")
    
    def create_customers_table(self):
        """Create CUSTOMERS dimension table (SCD Type 2)"""
        print("👥 Creating CUSTOMERS dimension table (SCD Type 2)...")
        
        # Create table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS my_catalog.dim_customers (
                customer_id INT,
                version_id INT,
                first_name STRING,
                last_name STRING,
                email STRING,
                phone STRING,
                address STRING,
                city STRING,
                start_date DATE,
                end_date DATE,
                is_current BOOLEAN,
                created_timestamp TIMESTAMP
            ) USING iceberg
        """)
        
        # Load data from CSV
        try:
            df = self.spark.read.option("header", True).option("inferSchema", True) \
                .csv("s3a://raw-data/customers.csv")
            
            df_with_timestamp = df.withColumn("created_timestamp", current_timestamp())
            
            df_with_timestamp.writeTo("my_catalog.dim_customers").append()
            print(f"✅ CUSTOMERS table created and loaded with {df.count()} records")
            
        except Exception as e:
            print(f"⚠️  Could not load customers.csv: {e}")
            print("📝 Creating empty CUSTOMERS table structure")
    
    def create_treatments_table(self):
        """Create TREATMENTS dimension table"""
        print("💅 Creating TREATMENTS dimension table...")
        
        # Create table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS my_catalog.dim_treatments (
                treatment_id INT,
                name STRING,
                price DECIMAL(10,2),
                duration_minutes INT,
                active BOOLEAN,
                created_timestamp TIMESTAMP
            ) USING iceberg
        """)
        
        # Load data from CSV
        try:
            df = self.spark.read.option("header", True).option("inferSchema", True) \
                .csv("s3a://raw-data/treatments.csv")
            
            df_with_timestamp = df.withColumn("created_timestamp", current_timestamp())
            
            df_with_timestamp.writeTo("my_catalog.dim_treatments").append()
            print(f"✅ TREATMENTS table created and loaded with {df.count()} records")
            
        except Exception as e:
            print(f"⚠️  Could not load treatments.csv: {e}")
            print("📝 Creating empty TREATMENTS table structure")
    
    def create_date_dimension_table(self):
        """Create DATE_DIM dimension table"""
        print("📅 Creating DATE_DIM dimension table...")
        
        # Create table
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS my_catalog.dim_date (
                date_id STRING,
                date DATE,
                day_of_week STRING,
                day_of_month INT,
                month STRING,
                month_number INT,
                quarter INT,
                year INT,
                season STRING,
                is_weekend BOOLEAN,
                is_holiday BOOLEAN,
                created_timestamp TIMESTAMP
            ) USING iceberg
        """)
        
        # Load data from CSV
        try:
            df = self.spark.read.option("header", True).option("inferSchema", True) \
                .csv("s3a://raw-data/date_dim.csv")
            
            df_with_timestamp = df.withColumn("created_timestamp", current_timestamp())
            
            df_with_timestamp.writeTo("my_catalog.dim_date").append()
            print(f"✅ DATE_DIM table created and loaded with {df.count()} records")
            
        except Exception as e:
            print(f"⚠️  Could not load date_dim.csv: {e}")
            print("📝 Creating empty DATE_DIM table structure")
    
    def create_all_dimension_tables(self):
        """Create all dimension tables"""
        print("🚀 Creating all dimension tables...")
        
        try:
            self.create_colors_table()
            self.create_branches_table()
            self.create_employees_table()
            self.create_customers_table()
            self.create_treatments_table()
            self.create_date_dimension_table()
            
            print("✅ All dimension tables created successfully!")
            
            # Show all tables - Fixed the SHOW TABLES command
            print("\n📊 Available tables in catalog:")
            try:
                # Use a simpler approach to list tables
                tables_df = self.spark.sql("SHOW TABLES")
                tables_df.show(truncate=False)
            except Exception as e:
                print(f"⚠️  Could not show tables: {e}")
                print("📝 Tables created but SHOW TABLES failed")
                print("📋 Manually created tables:")
                print("   - my_catalog.dim_colors")
                print("   - my_catalog.dim_branches") 
                print("   - my_catalog.dim_employees")
                print("   - my_catalog.dim_customers")
                print("   - my_catalog.dim_treatments")
                print("   - my_catalog.dim_date")
            
        except Exception as e:
            print(f"❌ Error creating dimension tables: {str(e)}")
            raise
        finally:
            self.spark.stop()
    
    def verify_dimension_tables(self):
        """Verify that all dimension tables exist and have data"""
        print("🔍 Verifying dimension tables...")
        
        tables = ['dim_colors', 'dim_branches', 'dim_employees', 'dim_customers', 'dim_treatments', 'dim_date']
        
        for table in tables:
            try:
                count = self.spark.sql(f"SELECT COUNT(*) as count FROM my_catalog.{table}").collect()[0]['count']
                print(f"✅ {table}: {count} records")
            except Exception as e:
                print(f"❌ {table}: Error - {e}")

if __name__ == "__main__":
    creator = DimensionTableCreator()
    creator.create_all_dimension_tables() 
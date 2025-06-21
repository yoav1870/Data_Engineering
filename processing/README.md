# Nail Salon Data Processing Layer

This directory contains the batch processing components for the Nail Salon data engineering pipeline.

## Data Flow Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Ratings       │    │   Instagram     │    │   Inventory     │
│   (Streaming)   │    │   (Batch)       │    │   (Batch)       │
│   Kafka         │    │   Daily CSV     │    │   Nightly CSV   │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          ▼                      ▼                      ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ bronze_ratings  │    │ bronze_instagram│    │ bronze_inventory│
│ (Streaming)     │    │ (Batch)         │    │ (Batch)         │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          └──────────┬───────────┴──────────┬───────────┘
                     ▼                      ▼
        ┌─────────────────────┐    ┌─────────────────────┐
        │   Silver Layer      │    │   Gold Layer        │
        │   (Processing)      │    │   (Analytics)       │
        └─────────────────────┘    └─────────────────────┘
                     ▲                      ▲
                     │                      │
        ┌─────────────────────────────────────────┐
        │         Dimension Tables                │
        │  (COLORS, BRANCHES, EMPLOYEES, etc.)    │
        └─────────────────────────────────────────┘
```

## Data Sources

### 1. Ratings (Real-time Streaming) ✅ IMPLEMENTED

- **Source**: Kafka topic `nail_salon_ratings`
- **Consumer**: Spark Structured Streaming
- **Table**: `bronze_ratings`
- **Frequency**: Real-time (as ratings come in)
- **Use Case**: Live customer satisfaction tracking

### 2. Instagram Engagement (Batch) 🔄 TO BE IMPLEMENTED

- **Source**: Daily CSV/Excel export from Instagram API
- **Processor**: Spark Batch Job
- **Table**: `bronze_instagram`
- **Frequency**: Daily
- **Use Case**: Track social media engagement, late data handling (up to 48h delay)

### 3. Inventory Updates (Batch) 🔄 TO BE IMPLEMENTED

- **Source**: Nightly CSV export of stock levels
- **Processor**: Spark Batch Job
- **Table**: `bronze_inventory`
- **Frequency**: Nightly
- **Use Case**: Inventory management, stock level tracking

## Bronze Layer Tables

### bronze_ratings ✅ IMPLEMENTED

```sql
CREATE TABLE bronze_ratings (
    customer_id INT,
    branch_id INT,
    employee_id INT,
    treatment_id INT,
    rating_value FLOAT,
    comment STRING,
    timestamp STRING,
    processed_timestamp TIMESTAMP,
    _kafka_topic STRING,
    _kafka_partition INT,
    _kafka_offset BIGINT
) USING iceberg
PARTITIONED BY (days(timestamp))
```

### bronze_instagram 🔄 TO BE IMPLEMENTED

```sql
CREATE TABLE bronze_instagram (
    post_id STRING,
    color_id INT,
    likes_count INT,
    comments_count INT,
    engagement_date DATE,
    processed_timestamp TIMESTAMP
) USING iceberg
PARTITIONED BY (engagement_date)
```

### bronze_inventory 🔄 TO BE IMPLEMENTED

```sql
CREATE TABLE bronze_inventory (
    color_id INT,
    branch_id INT,
    stock_quantity INT,
    reorder_level INT,
    last_updated TIMESTAMP,
    processed_timestamp TIMESTAMP
) USING iceberg
PARTITIONED BY (last_updated)
```

## Dimension Tables ✅ IMPLEMENTED

### 🎨 **dim_colors** - Nail Polish Colors

- **40+ colors** across 8 categories (Red, Pink, Purple, Blue, Green, Yellow, Orange, Neutral)
- **Realistic HEX codes** for each color
- **Active/inactive tracking** for availability

### 🏢 **dim_branches** - Salon Locations

- **5 branches** across NYC boroughs
- **Location data** for geographic analysis
- **Opening dates** for historical tracking

### 👩‍💼 **dim_employees** - Staff Information

- **10 employees** with various roles and experience levels
- **Performance tracking** support
- **Hire dates** for tenure analysis

### 👥 **dim_customers** - Customer Data (SCD Type 2)

- **20 customers** with 1-3 versions each
- **Historical tracking** of customer information changes
- **Current version flag** for easy querying

### 💅 **dim_treatments** - Service Offerings

- **15 different nail treatments**
- **Pricing and duration** information
- **Service popularity** analysis support

### 📅 **dim_date** - Time Dimension

- **Complete date range** from 2020-2025
- **Seasonal analysis** support
- **Weekend/holiday** tracking

## Current Status

### ✅ Completed

- **Streaming Infrastructure**: Kafka + Spark Streaming setup
- **Bronze Ratings Table**: Structure created for streaming data
- **Real-time Processing**: Ratings flow from Kafka to Bronze layer
- **Dimension Tables**: All 6 dimension tables created with sample data
- **Data Generation**: Automated dimension data generation scripts

### 🔄 Next Steps

1. **Silver Layer**: Transform bronze data with dimension lookups
2. **Instagram Batch Processing**: Implement daily Instagram data ingestion
3. **Inventory Batch Processing**: Implement nightly inventory data ingestion
4. **Gold Layer**: Create business metrics and ML features

## Getting Started

### 1. Start the Processing Infrastructure

```bash
# Start MinIO, bronze setup, and dimension tables
cd processing
docker-compose up -d
```

### 2. Verify Setup

```bash
# Check dimension data generation
docker logs dimension-data-generator

# Check bronze table setup
docker logs spark-bronze-setup

# Check dimension table creation
docker logs spark-dimension-tables

# Access MinIO console at http://localhost:9001
# Check warehouse bucket for all tables
```

### 3. Query the Data

```sql
-- Check if bronze table exists
SHOW TABLES IN my_catalog;

-- Check ratings data (will be populated by streaming)
SELECT COUNT(*) FROM my_catalog.bronze_ratings;

-- Check dimension tables
SELECT COUNT(*) FROM my_catalog.dim_colors;
SELECT COUNT(*) FROM my_catalog.dim_branches;
SELECT COUNT(*) FROM my_catalog.dim_employees;
SELECT COUNT(*) FROM my_catalog.dim_customers;
SELECT COUNT(*) FROM my_catalog.dim_treatments;
SELECT COUNT(*) FROM my_catalog.dim_date;

-- Sample query with dimension joins
SELECT
    r.customer_id,
    c.first_name,
    c.last_name,
    b.name as branch_name,
    t.name as treatment_name,
    r.rating_value,
    d.season
FROM my_catalog.bronze_ratings r
JOIN my_catalog.dim_customers c ON r.customer_id = c.customer_id AND c.is_current = true
JOIN my_catalog.dim_branches b ON r.branch_id = b.branch_id
JOIN my_catalog.dim_treatments t ON r.treatment_id = t.treatment_id
JOIN my_catalog.dim_date d ON CAST(r.timestamp AS DATE) = d.date
LIMIT 10;
```

## Files Structure

```
processing/
├── docker-compose.yml          # Infrastructure orchestration
├── Dockerfile                  # Spark container
├── requirements.txt            # Dependencies
├── README.md                   # This file
├── bronze/
│   └── bronze_ratings.py       # Bronze table setup script
├── dimensions/                 # ✅ IMPLEMENTED
│   ├── README.md              # Dimension documentation
│   ├── dimension_data_generator.py  # Data generation
│   ├── create_dimension_tables.py   # Table creation
│   ├── colors.csv             # Generated color data
│   ├── branches.csv           # Generated branch data
│   ├── employees.csv          # Generated employee data
│   ├── customers.csv          # Generated customer data
│   ├── treatments.csv         # Generated treatment data
│   └── date_dim.csv           # Generated date dimension data
├── silver/                     # (To be implemented)
└── gold/                       # (To be implemented)
```

## 🚀 Quick Start

### Step 1: Generate CSV Files (Host Machine)

Before running Docker, generate the dimension CSV files on your host machine:

**Option A: Using the batch script (Windows)**

```bash
# In the processing directory
generate_csvs.bat
```

**Option B: Manual Python execution**

```bash
# Install required packages
pip install pandas numpy

# Navigate to dimensions directory
cd dimensions

# Run the data generator
python dimension_data_generator.py
```

This will create the following CSV files in `processing/dimensions/`:

- `colors.csv` - Nail polish colors
- `branches.csv` - Salon branch locations
- `employees.csv` - Staff information
- `customers.csv` - Customer data (SCD Type 2)
- `treatments.csv` - Available treatments
- `date_dim.csv` - Date dimension table

### Step 2: Start Docker Services

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker logs dimension-data-copier
docker logs spark-dimension-tables
```

### Step 3: Access MinIO

- **Web Console**: http://localhost:9001
- **Login**: admin / password
- **S3 API**: http://localhost:9000

## 📁 Directory Structure

```
processing/
├── bronze/                 # Bronze layer transformations
│   └── bronze_ratings.py   # Bronze ratings table setup
├── dimensions/             # Dimension data and tables
│   ├── dimension_data_generator.py
│   ├── create_dimension_tables.py
│   └── *.csv              # Generated dimension files
├── silver/                 # Silver layer transformations
│   └── silver_sessions.py  # Silver sessions table
├── gold/                   # Gold layer (business metrics)
├── docker-compose.yml      # Docker services configuration
├── generate_csvs.bat       # Windows script to generate CSVs
└── README.md              # This file
```

## 🔧 Services

1. **dimension-data-copier**: Copies CSV files from host to MinIO
2. **spark-bronze-setup**: Creates bronze_ratings table structure
3. **spark-dimension-tables**: Creates dimension tables in Iceberg
4. **minio**: Object storage for raw data and warehouse
5. **minio-init**: Initializes MinIO buckets

## 📊 Data Flow

1. **CSV Generation** (Host) → `processing/dimensions/*.csv`
2. **Copy to MinIO** (Docker) → `s3a://raw-data/*.csv`
3. **Create Tables** (Spark) → `my_catalog.dim_*` (Iceberg tables)
4. **Bronze Setup** (Spark) → `my_catalog.bronze_ratings` (for streaming data)

## 🐛 Troubleshooting

### CSV Files Not Found

- Ensure you ran `generate_csvs.bat` or the Python script first
- Check that files exist in `processing/dimensions/`

### MinIO Access Issues

- Verify MinIO is running: `docker ps`
- Check MinIO logs: `docker logs minio`
- Try accessing http://localhost:9001

### Spark Job Failures

- Check Spark logs: `docker logs spark-dimension-tables`
- Verify MinIO connectivity
- Ensure CSV files are in MinIO: `docker exec -it minio-init mc ls local/raw-data/`

## 🔄 Next Steps

After successful setup:

1. Start the streaming pipeline (see `../streaming/README.md`)
2. Run Silver layer transformations
3. Query the data for business insights

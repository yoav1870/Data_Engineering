# Nail Salon Dimension Tables

This directory contains all dimension tables for the Nail Salon data warehouse. These tables provide the foundational reference data that all fact tables will use.

## Dimension Tables Overview

### 🎨 **dim_colors** - Nail Polish Colors

**Purpose**: Reference data for nail polish colors used in treatments

```sql
CREATE TABLE dim_colors (
    color_id INT,           -- Primary key
    name STRING,            -- Color name (e.g., "Crimson Red")
    hex_code STRING,        -- HEX color code (e.g., "#FF0000")
    category STRING,        -- Color category (e.g., "Red", "Pink", "Neutral")
    active BOOLEAN,         -- Whether color is currently available
    created_timestamp TIMESTAMP
)
```

**Sample Data**:

- 40+ colors across 8 categories (Red, Pink, Purple, Blue, Green, Yellow, Orange, Neutral)
- Realistic HEX codes for each color
- Supports color preference analysis

### 🏢 **dim_branches** - Salon Locations

**Purpose**: Reference data for nail salon branches

```sql
CREATE TABLE dim_branches (
    branch_id INT,          -- Primary key
    name STRING,            -- Branch name (e.g., "Downtown Salon")
    city STRING,            -- City location
    address STRING,         -- Street address
    active BOOLEAN,         -- Whether branch is operational
    opening_date DATE,      -- When branch opened
    created_timestamp TIMESTAMP
)
```

**Sample Data**:

- 5 branches across NYC boroughs
- Supports location-based analysis and inventory management

### 👩‍💼 **dim_employees** - Staff Information

**Purpose**: Reference data for salon employees

```sql
CREATE TABLE dim_employees (
    employee_id INT,        -- Primary key
    first_name STRING,      -- Employee first name
    last_name STRING,       -- Employee last name
    role STRING,            -- Job role (e.g., "Senior Nail Technician")
    experience_years INT,   -- Years of experience
    active BOOLEAN,         -- Whether employee is currently employed
    hire_date DATE,         -- When employee was hired
    created_timestamp TIMESTAMP
)
```

**Sample Data**:

- 10 employees with various roles and experience levels
- Supports employee performance analysis

### 👥 **dim_customers** - Customer Data (SCD Type 2)

**Purpose**: Customer information with historical tracking

```sql
CREATE TABLE dim_customers (
    customer_id INT,        -- Customer identifier
    version_id INT,         -- SCD Type 2 version number
    first_name STRING,      -- Customer first name
    last_name STRING,       -- Customer last name
    email STRING,           -- Email address
    phone STRING,           -- Phone number
    address STRING,         -- Street address
    city STRING,            -- City
    start_date DATE,        -- When this version became effective
    end_date DATE,          -- When this version ended (NULL for current)
    is_current BOOLEAN,     -- Whether this is the current version
    created_timestamp TIMESTAMP
)
```

**Sample Data**:

- 20 customers with 1-3 versions each (SCD Type 2)
- Tracks customer information changes over time
- Supports customer lifecycle analysis

### 💅 **dim_treatments** - Service Offerings

**Purpose**: Reference data for nail salon services

```sql
CREATE TABLE dim_treatments (
    treatment_id INT,       -- Primary key
    name STRING,            -- Treatment name (e.g., "Gel Manicure")
    price DECIMAL(10,2),    -- Service price
    duration_minutes INT,   -- Estimated duration in minutes
    active BOOLEAN,         -- Whether treatment is currently offered
    created_timestamp TIMESTAMP
)
```

**Sample Data**:

- 15 different nail treatments
- Various price points and durations
- Supports service popularity analysis

### 📅 **dim_date** - Time Dimension

**Purpose**: Comprehensive time reference for all fact tables

```sql
CREATE TABLE dim_date (
    date_id STRING,         -- Date identifier (YYYYMMDD format)
    date DATE,              -- Actual date
    day_of_week STRING,     -- Day name (Monday, Tuesday, etc.)
    day_of_month INT,       -- Day of month (1-31)
    month STRING,           -- Month name (January, February, etc.)
    month_number INT,       -- Month number (1-12)
    quarter INT,            -- Quarter (1-4)
    year INT,               -- Year
    season STRING,          -- Season (Spring, Summer, Fall, Winter)
    is_weekend BOOLEAN,     -- Whether date is weekend
    is_holiday BOOLEAN,     -- Whether date is holiday
    created_timestamp TIMESTAMP
)
```

**Sample Data**:

- Complete date range from 2020-2025
- Seasonal analysis support
- Weekend/holiday tracking

## Data Generation

### Automatic Generation

The dimension data is automatically generated using `dimension_data_generator.py`:

```bash
# Generate all dimension data
python dimensions/dimension_data_generator.py
```

### Manual Generation

You can also generate specific dimensions:

```python
from dimension_data_generator import NailSalonDimensionGenerator

generator = NailSalonDimensionGenerator()

# Generate specific dimensions
colors = generator.generate_colors()
branches = generator.generate_branches()
employees = generator.generate_employees()
customers = generator.generate_customers()
treatments = generator.generate_treatments()
date_dim = generator.generate_date_dimension()
```

## Table Creation

### Automatic Creation

Dimension tables are created automatically using `create_dimension_tables.py`:

```bash
# Create all dimension tables
python dimensions/create_dimension_tables.py
```

### Manual Creation

You can also create specific tables:

```python
from create_dimension_tables import DimensionTableCreator

creator = DimensionTableCreator()

# Create specific tables
creator.create_colors_table()
creator.create_branches_table()
creator.create_employees_table()
creator.create_customers_table()
creator.create_treatments_table()
creator.create_date_dimension_table()
```

## Data Quality Features

### 1. **Referential Integrity**

- All dimension tables have proper primary keys
- Fact tables will reference these dimensions

### 2. **SCD Type 2 Support**

- Customer dimension supports Slowly Changing Dimensions Type 2
- Tracks historical changes in customer information

### 3. **Active/Inactive Tracking**

- Colors, branches, employees, and treatments track active status
- Supports current vs. historical analysis

### 4. **Comprehensive Time Dimension**

- Full date dimension with business-friendly attributes
- Supports seasonal and temporal analysis

## Usage in Fact Tables

These dimension tables will be referenced by the following fact tables:

### **fact_sessions** (from ratings)

```sql
SELECT
    r.customer_id,
    c.first_name,
    c.last_name,
    b.name as branch_name,
    e.first_name as employee_name,
    t.name as treatment_name,
    t.price,
    r.rating_value,
    d.season,
    d.is_weekend
FROM bronze_ratings r
JOIN dim_customers c ON r.customer_id = c.customer_id AND c.is_current = true
JOIN dim_branches b ON r.branch_id = b.branch_id
JOIN dim_employees e ON r.employee_id = e.employee_id
JOIN dim_treatments t ON r.treatment_id = t.treatment_id
JOIN dim_date d ON CAST(r.timestamp AS DATE) = d.date
```

### **fact_instagram** (future)

```sql
SELECT
    i.post_id,
    c.name as color_name,
    c.category as color_category,
    i.likes_count,
    i.comments_count,
    d.season,
    d.month
FROM bronze_instagram i
JOIN dim_colors c ON i.color_id = c.color_id
JOIN dim_date d ON i.engagement_date = d.date
```

### **fact_inventory** (future)

```sql
SELECT
    i.color_id,
    c.name as color_name,
    b.name as branch_name,
    i.stock_quantity,
    i.reorder_level,
    d.month,
    d.quarter
FROM bronze_inventory i
JOIN dim_colors c ON i.color_id = c.color_id
JOIN dim_branches b ON i.branch_id = b.branch_id
JOIN dim_date d ON CAST(i.last_updated AS DATE) = d.date
```

## Files Structure

```
dimensions/
├── README.md                           # This file
├── dimension_data_generator.py         # Data generation script
├── create_dimension_tables.py          # Table creation script
├── colors.csv                          # Generated color data
├── branches.csv                        # Generated branch data
├── employees.csv                       # Generated employee data
├── customers.csv                       # Generated customer data
├── treatments.csv                      # Generated treatment data
└── date_dim.csv                        # Generated date dimension data
```

## Next Steps

1. **Silver Layer**: Transform bronze data with dimension lookups
2. **Gold Layer**: Create business metrics using enriched data
3. **ML Features**: Use dimension data for customer prediction models
4. **Data Quality**: Add validation rules and monitoring

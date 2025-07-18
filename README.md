# Nail Salon Data Engineering Final Project

## Overview

End-to-end data engineering solution using Apache Iceberg, MinIO, Spark, Kafka, and Airflow. The pipeline covers batch and streaming data, with orchestration and data quality checks.

## Prerequisites

- Docker & Docker Compose

## Quick Start

1. **Clone the repository**

   ```bash
   git clone <your-repo-url>
   cd final_project
   ```

2. **Start all components**

   - Processing (MinIO, Spark, Iceberg):
     ```bash
     cd processing
     docker-compose up -d --build
     ```
   - Streaming (Kafka, Producer, Consumer):
     ```bash
     cd ../streaming
     docker-compose up -d --build
     ```
   - Orchestration (Airflow):
     ```bash
     cd ../orchestration
     docker-compose up -d --build
     ```

3. **Access UIs**

   - MinIO: http://localhost:9001 (admin/password)
   - Airflow: http://localhost:8081 (admin/admin)

4. **Trigger pipelines**
   - Use Airflow UI to run ETL jobs and monitor progress.

## Email Notifications

- **To receive email notifications:**
  - Update the email addresses and SMTP password in both the Airflow DAG (ETL) and your local `docker-compose.yml` file.

## Documentation

- See `docs/` for architecture and data model diagrams (Mermaid.js).
- See `docs/checks.txt` for ready-to-use SQL queries for data validation.

## Stopping

```bash
cd orchestration && docker-compose down
cd ../streaming && docker-compose down
cd ../processing && docker-compose down
```

---

## Table Overview

| Layer      | Tables                                                                           |
| ---------- | -------------------------------------------------------------------------------- |
| **Bronze** | bronze_inventory, bronze_instagram, bronze_ratings                               |
| **Silver** | silver_inventory, silver_instagram, silver_ratings                               |
| **Gold**   | gold_branch_kpis, gold_color_engagement, gold_customer_metrics                   |
| **Dim**    | dim_branches, dim_colors, dim_customers, dim_date, dim_employees, dim_treatments |

---

## Table Features

### Bronze Layer

- **bronze_inventory**: Raw inventory usage data loaded from nightly CSVs; tracks product usage per branch and color.
- **bronze_instagram**: Raw Instagram engagement data from daily exports; tracks likes, comments, and post metadata.
- **bronze_ratings**: Raw customer ratings ingested in real-time from Kafka; includes customer, branch, employee, treatment, and rating details.

### Silver Layer

- **silver_inventory**: Cleaned and deduplicated inventory data; includes only valid, recent records with report and ingestion dates.
- **silver_instagram**: Cleaned Instagram data joined with date dimension; includes post, color, engagement, and season info.
- **silver_ratings**: Cleaned customer ratings joined with dimension tables; includes enriched rating, customer, employee, and branch info.

### Gold Layer

- **gold_branch_kpis**: Aggregated branch-level KPIs; includes average rating, total ratings, and number of active employees per branch.
- **gold_color_engagement**: Color-level engagement metrics from Instagram; tracks popularity and trends by color.
- **gold_customer_metrics**: Customer-level metrics; includes engagement, satisfaction, and visit frequency.

### Dimension Tables

- **dim_branches**: Reference data for all salon branches, including location and operational status.
- **dim_colors**: Reference data for nail polish colors, categories, and availability.
- **dim_customers**: Customer master data with SCD Type 2 for historical tracking.
- **dim_date**: Time dimension for all fact tables, supporting seasonal and temporal analysis.
- **dim_employees**: Reference data for employees, roles, and employment status.
- **dim_treatments**: Reference data for all nail salon services, pricing, and duration.

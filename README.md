<h1 align="center">💅 Nail Salon Data Engineering Final Project</h1>

## Built with the tools and technologies
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![Apache Iceberg](https://img.shields.io/badge/Iceberg-4FBDBA?style=for-the-badge)
![MinIO](https://img.shields.io/badge/MinIO-C81F25?style=for-the-badge)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

## Overview
Welcome to Nail Salon ! 
End-to-end data engineering solution using Apache Iceberg, MinIO, Spark, Kafka, and Airflow. The pipeline covers batch and streaming data, with orchestration and data quality checks.

## Prerequisites

- Docker & Docker Compose

## Quick Start

1. **Clone the repository**

   ```bash
   git clone https://github.com/yoav1870/Data_Engineering.git
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

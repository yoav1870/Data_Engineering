from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "start_date": datetime(2023, 1, 1),
    "retries": 2,  # Retry failed tasks up to 2 times
    "retry_delay": timedelta(minutes=10),  # Wait 10 minutes between retries
    "email_on_failure": True, 
    "email": ["myteamemailshenkar@gmail.com"],  
}

with DAG(
    dag_id="full_etl_pipeline",
    default_args=default_args,
    schedule_interval='@daily',  
    catchup=False,
    tags=["silver", "gold", "etl"],
) as dag:

    # -------- Silver Layer Tasks --------
    silver_inventory = BashOperator(
        task_id="silver_inventory",
        bash_command="docker exec spark-submit spark-submit /app/silver/silver_inventory.py"
    )

    silver_instagram = BashOperator(
        task_id="silver_instagram",
        bash_command="docker exec spark-submit spark-submit /app/silver/silver_instagram.py"
    )

    silver_ratings = BashOperator(
        task_id="silver_ratings",
        bash_command="docker exec spark-submit spark-submit /app/silver/silver_ratings.py"
    )

    # -------- Gold Layer Tasks --------
    gold_inventory = BashOperator(
        task_id="gold_inventory",
        bash_command="docker exec spark-submit spark-submit /app/gold/gold_branch_kpis.py"
    )

    gold_instagram = BashOperator(
        task_id="gold_instagram",
        bash_command="docker exec spark-submit spark-submit /app/gold/gold_color_engagement.py"
    )

    gold_ratings = BashOperator(
        task_id="gold_ratings",
        bash_command="docker exec spark-submit spark-submit /app/gold/gold_customer_metrics.py"
    )

    # -------- Dependencies: Silver must finish before Gold --------
    silver_inventory >> gold_inventory
    silver_instagram >> gold_instagram
    silver_ratings >> gold_ratings



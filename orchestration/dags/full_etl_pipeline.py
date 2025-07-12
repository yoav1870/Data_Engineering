from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    dag_id="full_etl_pipeline",
    default_args=default_args,
    schedule_interval=None,  
    catchup=False,
    tags=["bronze", "silver", "gold", "etl"],
) as dag:

    # -------- Inventory Flow --------
    bronze_inventory = BashOperator(
        task_id="bronze_inventory",
        bash_command="docker exec spark-submit spark-submit /app/bronze/bronze_inventory.py"
    )

    silver_inventory = BashOperator(
        task_id="silver_inventory",
        bash_command="docker exec spark-submit spark-submit /app/silver/silver_inventory.py"
    )

    gold_inventory = BashOperator(
        task_id="gold_inventory",
        bash_command="docker exec spark-submit spark-submit /app/gold/gold_branch_kpis.py"
    )

    # -------- Instagram Flow --------
    bronze_instagram = BashOperator(
        task_id="bronze_instagram",
        bash_command="docker exec spark-submit spark-submit /app/bronze/bronze_instagram.py"
    )

    silver_instagram = BashOperator(
        task_id="silver_instagram",
        bash_command="docker exec spark-submit spark-submit /app/silver/silver_instagram.py"
    )

    gold_instagram = BashOperator(
        task_id="gold_instagram",
        bash_command="docker exec spark-submit spark-submit /app/gold/gold_color_engagement.py"
    )

    # -------- Ratings Flow --------
    bronze_ratings = BashOperator(
        task_id="bronze_ratings",
        bash_command="docker exec spark-submit spark-submit /app/bronze/bronze_ratings.py"
    )

    silver_ratings = BashOperator(
        task_id="silver_ratings",
        bash_command="docker exec spark-submit spark-submit /app/silver/silver_ratings.py"
    )

    gold_ratings = BashOperator(
        task_id="gold_ratings",
        bash_command="docker exec spark-submit spark-submit /app/gold/gold_customer_metrics.py"
    )

    # -------- Dependencies --------
    bronze_inventory >> silver_inventory >> gold_inventory
    bronze_instagram >> silver_instagram >> gold_instagram
    bronze_ratings >> silver_ratings >> gold_ratings

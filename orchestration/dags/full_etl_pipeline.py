from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.utils.email import send_email

default_args = {
    "start_date": datetime(2023, 1, 1),
    "retries": 2,  
    "retry_delay": timedelta(minutes=1),  
    "email_on_failure": True, 
    "email_on_retry": True, 
    "email_on_success": True,
    "email": ["myteamemailshenkar@gmail.com"], 
}

def dag_success_callback(context):
    subject = f"DAG {context['dag'].dag_id} Succeeded"
    body = f"DAG {context['dag'].dag_id} completed successfully on {context['execution_date']}"
    to = context['dag'].default_args.get('email', [])
    send_email(to=to, subject=subject, html_content=body)

with DAG(
    dag_id="full_etl_pipeline",
    default_args=default_args,
    schedule_interval='@daily',  
    catchup=False,
    tags=["silver", "gold", "etl"],
    on_success_callback=dag_success_callback,
) as dag:

    # -------- Silver Layer Tasks --------
    silver_inventory = BashOperator(
        task_id="silver_inventory",
        bash_command="docker exec spark-submit spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 /app/silver/silver_inventory.py"
    )

    silver_instagram = BashOperator(
        task_id="silver_instagram",
        bash_command="docker exec spark-submit spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 /app/silver/silver_instagram.py"
    )

    silver_ratings = BashOperator(
        task_id="silver_ratings",
        bash_command="docker exec spark-submit spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 /app/silver/silver_ratings.py"
    )

    # -------- SCD2 dim_customers update task --------
    update_dim_customers_scd2 = BashOperator(
        task_id="update_dim_customers_scd2",
        bash_command="docker exec spark-submit spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 /app/dimensions/update_dim_customers_scd2.py"
    )

    # -------- Gold Layer Tasks --------
    gold_inventory = BashOperator(
        task_id="gold_inventory",
        bash_command="docker exec spark-submit spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 /app/gold/gold_branch_kpis.py"
    )

    gold_instagram = BashOperator(
        task_id="gold_instagram",
        bash_command="docker exec spark-submit spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 /app/gold/gold_color_engagement.py"
    )

    gold_ratings = BashOperator(
        task_id="gold_ratings",
        bash_command="docker exec spark-submit spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 /app/gold/gold_customer_metrics.py"
    )

    # --------  Gold tasks depend on specific Silver tasks --------
    [silver_inventory, silver_instagram] >> gold_instagram  
    [silver_inventory, silver_instagram] >> gold_inventory   
    silver_ratings >> update_dim_customers_scd2 >> gold_ratings





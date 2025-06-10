from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="network_test_spark_show_databases",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["network", "spark"],
) as dag:

    show_databases = BashOperator(
        task_id="show_spark_databases",
        bash_command="/opt/spark/bin/spark-sql --master spark://spark-iceberg:7077 -e 'SHOW DATABASES;'"
    )

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="silver_ratings_spark_submit",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["silver", "spark"],
) as dag:

    silver_ratings = BashOperator(
        task_id="run_silver_ratings_transformation",
        bash_command=(
            "docker exec spark-submit spark-submit "
            "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 "
            "--conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog "
            "--conf spark.sql.catalog.my_catalog.type=hadoop "
            "--conf spark.sql.catalog.my_catalog.warehouse=s3a://warehouse "
            "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
            "--conf spark.hadoop.fs.s3a.access.key=admin "
            "--conf spark.hadoop.fs.s3a.secret.key=password "
            "--conf spark.hadoop.fs.s3a.path.style.access=true "
            "/app/silver/silver_ratings.py"
        ),
    ) 
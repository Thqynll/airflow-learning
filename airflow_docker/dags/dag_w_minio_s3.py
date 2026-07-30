from datetime import datetime, timedelta

from airflow.sdk import DAG

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=1)
    }

with DAG(
    dag_id="dag_w_minio_s3_v2",
    default_args=default_args,
    start_date=datetime(2026, 6, 25),
    schedule="@daily"
) as dag:
    task1 = S3KeySensor(
        task_id="sensor_minio_s3",
        bucket_name="airflow",
        bucket_key="data.csv",
        aws_conn_id="minio_conn",
        mode="poke",
        poke_interval=5,
        timeout=30
    )
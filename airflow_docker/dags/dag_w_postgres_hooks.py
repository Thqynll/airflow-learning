import csv
import logging
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# FIX: Modern standard Airflow 3.0 import path for S3Hook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

def postgres_to_s3(ds, **context):
    # Calculate next day manually from the injected standard ds string
    next_ds = (datetime.strptime(ds, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    
    logging.info(f"Running Query Window: WHERE date >= '{ds}' AND date < '{next_ds}'")
    
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    # Executing using standard parameterized strings
    cursor.execute("select * from orders where date >= %s and date < %s;", (ds, next_ds))
    rows = cursor.fetchall()
    
    # CRITICAL TROUBLESHOOTING LOG: Check this in your task logs!
    logging.info(f"Filtered rows count found in Postgres: {len(rows)}")
    
    file_path = f"/opt/airflow/dags/get_orders_{ds}.txt"
    with NamedTemporaryFile(mode="w", suffix=f"{ds}") as f:
    #with open(file_path, "w", newline="", encoding="utf-8") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])  # write headers
        if len(rows) > 0:
            csv_writer.writerows(rows)  # write data rows if they exist
        f.flush()  # Ensure data is written to the temporary file
        
        cursor.close()
        conn.close()
        logging.info(f"Saved local file temporary snapshot: {file_path}")
        
    # Upload to MinIO/S3
        s3_hook = S3Hook(aws_conn_id="minio_conn")
        s3_hook.load_file(
            filename=f.name,
            key=f"orders/get_orders_{ds}.txt",
            bucket_name="airflow",
            replace=True
        )
        logging.info(f"Orders file %s has been pushed to S3", f.name)

with DAG(
    dag_id="dag_w_postgres_hooks_v4",
    default_args=default_args,
    start_date=datetime(2022, 2, 28),
    schedule="@daily",
    catchup=False,
    max_active_runs=5
) as dag:

    task1 = PythonOperator(
        task_id="postgres_to_s3",
        python_callable=postgres_to_s3,
        op_kwargs={
            'ds': '{{ dag_run.data_interval_start.strftime("%Y-%m-%d") }}'
        }
    )

    task1
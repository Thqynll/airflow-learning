from datetime import datetime, timedelta

from airflow.sdk import DAG

from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dag_w_catchup_n_backfill_v2",
    description="A simple DAG with catchup and backfill enabled",
    start_date=datetime(2026, 6, 26),
    schedule="@daily",
    catchup=False
) as dag:
    
    task1 = BashOperator(
        task_id="task1",
        bash_command="echo 'Task 1 executed successfully!'"
    )
    
    task1
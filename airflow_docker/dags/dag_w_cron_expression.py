from datetime import datetime, timedelta

from airflow.sdk import DAG

from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dag_w_cron_expression",
    description="A simple DAG with cron expression scheduling",
    start_date=datetime(2026, 6, 26),
    schedule="0 14 * * Tue-Thu"
) as dag:
    
    task1 = BashOperator(
        task_id="task1",
        bash_command="echo 'Task 1 executed successfully yehh!'"
    )
    
    task1
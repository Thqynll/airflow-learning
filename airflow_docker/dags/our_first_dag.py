from datetime import datetime, timedelta

from airflow.sdk import DAG

from airflow.providers.standard.operators.bash import BashOperator

# Global pipeline retry behaviors
default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

# Core DAG Declaration Block
with DAG(
    dag_id="our_first_dag",
    default_args=default_args,
    description="My first official Airflow 3.0 pipeline executing bash scripting",
    start_date=datetime(2026, 6, 20), # Safe historical execution start point
    schedule="@daily"
) as dag:

    # Define Task 1
    task_one = BashOperator(
        task_id="first_task",
        bash_command="echo 'Task One executed successfully inside the Debian container context!'"
    )

    # Define Task 2
    task_two = BashOperator(
        task_id="second_task",
        bash_command="echo 'Task Two completed cleanly directly after Task One.'"
    )

    # Set structural pipeline dependency (Bitshift sequencing)
    task_one >> task_two
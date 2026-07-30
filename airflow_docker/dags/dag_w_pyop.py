from datetime import datetime, timedelta

from airflow.sdk import DAG

from airflow.providers.standard.operators.python import PythonOperator

def greet(age, ti):
    f_name = ti.xcom_pull(task_ids="get_name", key="f_name")
    l_name = ti.xcom_pull(task_ids="get_name", key="l_name")
    print(f"Hello everybody. My name is {f_name} {l_name} and I am {age} years old.")

def get_name(ti):
    f_name = ti.xcom_push(key="f_name", value="La")
    l_name = ti.xcom_push(key="l_name", value="Lune")

default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=1)
    }

with DAG(
    dag_id="dag_w_pyop_v2",
    description="A simple DAG with a PythonOperator",
    default_args=default_args,
    start_date=datetime(2026, 6, 23),
    schedule="@daily"    
) as dag:

    task1 = PythonOperator(
        task_id="greet",
        python_callable=greet,
        op_kwargs={"age": 25}
    )
    
    task2 = PythonOperator(
        task_id="get_name",
        python_callable=get_name
    )
    
    task2 >> task1
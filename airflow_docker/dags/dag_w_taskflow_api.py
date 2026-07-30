from datetime import datetime, timedelta

from airflow.sdk import dag, task

default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=1)
    }

@dag(
    dag_id="dag_w_taskflow_api",
    description="A simple DAG with a TaskFlow API",
    default_args=default_args,
    start_date=datetime(2026, 6, 23),
    schedule="@daily"
)

def hello_world():
    
    @task()
    def get_name():
        return "Lune"
    
    @task()
    def get_age():
        return 25
    
    @task()
    def greet(name, age):
        print(f"Hello everybody. My name is {name} and I am {age} years old.")
        
    name = get_name()
    age = get_age()
    greet(name=name, age=age)
    
hello_world()
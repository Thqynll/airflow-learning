from datetime import datetime, timedelta

from airflow.sdk import DAG

from airflow.providers.standard.operators.python import PythonOperator

default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=1)
    }

def get_sklearn():
    import sklearn
    print(f"sklearn version: {sklearn.__version__}")
    
def get_matplotlib():   
    import matplotlib
    print(f"matplotlib version: {matplotlib.__version__}")

with DAG(
    dag_id="dag_w_py_dependency_v2",
    description="A simple DAG in python dependency",
    default_args=default_args,
    start_date=datetime(2026, 6, 25),
    schedule="@daily"    
) as dag:

    task1 = PythonOperator(
        task_id="get_sklearn",
        python_callable=get_sklearn
    )
    
    task2 = PythonOperator(
        task_id="get_matplotlib",
        python_callable=get_matplotlib
    )
    
    task1 >> task2
from datetime import datetime, timedelta

from airflow.sdk import DAG

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=1)
    }

with DAG(
    dag_id="dag_w_postgres_op_v3",
    description="A simple DAG with a PostgresOperator",
    default_args=default_args,
    start_date=datetime(2026, 6, 25),
    schedule="@daily"    
) as dag:

    task1 = SQLExecuteQueryOperator(
        task_id="create_postgres_table",
        conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS dag_runrun (
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            );
            """
    )
    
    task2 = SQLExecuteQueryOperator(
        task_id="insert_into_postgres_table",
        conn_id="postgres_default",
        sql="""
            INSERT INTO dag_runrun (dt, dag_id) VALUES (CURRENT_DATE, '{{ dag.dag_id }}');
            """
    )
        
    task3 = SQLExecuteQueryOperator(
        task_id="delete_from_postgres_table",
        conn_id="postgres_default",
        sql="""
            DELETE FROM dag_runrun WHERE dt = CURRENT_DATE AND dag_id = '{{ dag.dag_id }}';
            """
    )
    
    task1 >> task3 >> task2
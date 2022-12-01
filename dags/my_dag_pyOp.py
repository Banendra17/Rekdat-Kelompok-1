from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'rekdat22',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

def greet():
    print("Hello World!")

with DAG(
    default_args=default_args,
    dag_id='dag_pyOp_v01',
    description='first dag with python op',
    start_date=datetime(2022,11,29),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet
    )

    task1

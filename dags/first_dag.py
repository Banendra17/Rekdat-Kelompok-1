from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from time import sleep
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'email': ['rizky.j.n@mail.ugm.ac.id'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='first_dag',
    start_date=datetime(2022, 11, 30),
    schedule_interval='@hourly',
    default_args=default_args
)


def task1():
    sleep(10)
    return "This is task 1"


def task2():
    sleep(30)
    return "This is task 2"


task1 = PythonOperator(
    task_id='task1',
    python_callable=task1,
    dag=dag
)

task2 = PythonOperator(
    task_id='task2',
    python_callable=task2,
    dag=dag
)

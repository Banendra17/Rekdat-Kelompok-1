from datetime import datetime, timedelta
from spotipy_get import call_playlist

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner' : 'rekdat22',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id='dag_spotipy',
    description='first dag with python op',
    start_date=datetime(2022,11,23),
    schedule_interval='@once'
) as dag:
    task1 = PythonOperator(
        task_id='df_playlist',
        python_callable=call_playlist,
        op_kwargs={'creator':'spotify','id_playlist':'37i9dQZF1DWWhB4HOWKFQc','country':'ID'},
        dag=dag
    )

    task1
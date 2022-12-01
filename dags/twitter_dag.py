import tweepy
import pandas as pd
import csv
from pathlib import Path
from datetime import datetime
from datetime import timedelta
import glob
import psycopg2 as pg

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


api_key = "CPyHoCo8IFvLggqxAdDExhHlI"
api_secret_key = "XadKFgrmsE74Ijd2NPjTso9Eg88QotMjQBwgYH9NhNwBiHBEWS"
access_token = "1589554347272474626-4iZCP5f1goD69T9d6MAubIrFNz5ae0"
access_token_secret = "wgNXWBHwLVkOXypH0rBEfBqSDnykzZSCwZli1SRT2byx3"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['jun@mail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'twitter_trends_dag',
    default_args=default_args,
    description='twitter scrapping DAG',
    schedule_interval='@daily'
)


def get_auth():
    auth = tweepy.OAuthHandler(api_key, api_secret_key)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)
    return api


def get_trends(**kwargs):
    api = get_auth()
    trends = api.get_place_trends(id=1047378)
    print(trends)
    return trends

def parse_data(**context):

    trends = context['task_instance'].xcom_pull(task_ids='scraping_data')
    trends_dict = trends[0]['trends']
    trends_time = trends[0]['as_of']
    print(trends_dict)
    print(trends_time)
    df = pd.DataFrame(trends_dict)
    df['trend_time'] = trends_time
    print(df)
    return df


csv_path = Path("/opt/airflow/data/tweets_trends.csv")


def save_data(**kwargs):
    # Xcoms to get the list
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='parsing_data')
    df = df.drop(['url', 'promoted_content', 'query'], axis=1)

    try:
        print(df)
        df.to_csv(csv_path, index=False, header=True)
        return True
    except OSError as e:
        print(e)
        return False


def addtodb():
    try:
        conn = pg.connect(
            "dbname='airflow' user='airflow' host='jun_postgres_1' password='airflow'"
        )
    except Exception as error:
        print(error)

    path = "/opt/airflow/data/*.csv"
    glob.glob(path)
    for fname in glob.glob(path):
        fname = fname.split('/')
        csvname = fname[-1]
        csvname = csvname.split('.')
        tablename = str(csvname[0])

    # create the table if it does not already exist
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS """ + tablename + """ (
                trendName varchar(50),
                trendCount varchar(50),
                trendTime varchar(150)
            );
        """
                       )
        conn.commit()

    # insert each csv row as a record in our database
    with open('/opt/airflow/data/tweets_trends.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute(
                "INSERT INTO tweets_trends VALUES (%s, %s, %s)",
                row
            )
    conn.commit()


t1 = PythonOperator(
    task_id='scraping_data',
    python_callable=get_trends,
    dag=dag)

t2 = PythonOperator(
    task_id='parsing_data',
    python_callable=parse_data,
    provide_context=True,
    dag=dag)
t3 = PythonOperator(
    task_id='save_data',
    python_callable=save_data,
    provide_context=True,
    dag=dag)
t4 = PythonOperator(
    task_id='add_db',
    python_callable=addtodb,
    dag=dag)

t1 >> t2 >> t3 >> t4

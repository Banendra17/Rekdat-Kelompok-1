import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
import requests
import json
import pandas as pd
import csv
import psycopg2 as pg

args={'owner': 'airflow'}

default_args = {
    'owner': 'airflow',    
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

def getDataToLocal():  
    
    url = "https://data.cityofnewyork.us/resource/4tqt-y424.json"
    response = requests.get(url)

    df = pd.DataFrame(json.loads(response.content))
    df = df.set_index("school_year")

    df.to_csv("/home/hduser/drivers.csv", sep=',' ,escapechar='\\', quoting=csv.QUOTE_ALL, encoding='utf-8' )

def creatableLoad():

    try:
        dbconnect = pg.connect(
            "dbname='dezyre_new' user='postgres' host='localhost' password='root'"
        )
    except Exception as error:
        print(error)
    
    # create the table if it does not already exist
    cursor = dbconnect.cursor()
    cursor.execute("""
         CREATE TABLE IF NOT EXISTS drivers_data (
            school_year varchar(50),
            vendor_name varchar(50),
            type_of_service varchar(50),
            active_employees varchar(50),
            job_type varchar(50)
        );
        
        TRUNCATE TABLE drivers_data;
    """
    )
    dbconnect.commit()
    
    # insert each csv row as a record in our database
    with open('/home/hduser/drivers.csv', 'r') as f:
        next(f)  # skip the first row (header)     
        for row in f:
            cursor.execute("""
                INSERT INTO drivers_data
                VALUES ('{}', '{}', '{}', '{}', '{}')
            """.format(
            row.split(",")[0],
            row.split(",")[1],
            row.split(",")[2],
            row.split(",")[3],
            row.split(",")[4])
            )
    dbconnect.commit()

dag_pandas = DAG(
	dag_id = "using_pandas_demo",
	default_args=default_args ,
	# schedule_interval='0 0 * * *',
	schedule_interval='@once',	
	dagrun_timeout=timedelta(minutes=60),
	description='use case of pandas  in airflow',
	start_date = airflow.utils.dates.days_ago(1))

getDataToLocal = PythonOperator(task_id='getDataToLocal', python_callable=getDataToLocal, dag=dag_pandas)

creatableLoad = PythonOperator(task_id='creatableLoad', python_callable=creatableLoad, dag=dag_pandas)

getDataToLocal>>creatableLoad

if __name__ == '__main__ ':
    dag_pandas.cli()
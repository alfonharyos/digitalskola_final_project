#importing the DAG object
from datetime import timedelta
from airflow import DAG
# import the operators used in our tasks
from airflow.operators.python_operator import PythonOperator
# We then import the days_ago function
from airflow.utils.dates import days_ago

from sqlalchemy import create_engine, Table, Column, Float, MetaData, DATE, String, Numeric, Integer, JSON, DateTime
import datetime
import psycopg2
import json
import csv
import os

# get dag directory path
dag_path = os.getcwd()

host="localhost",
database="final_project",
user="postgres",
password="12345678"
engine = create_engine('postgresql://'+user+':'+password+'@'+host+'/'+database)
create_dwh_dir = f"{dag_path}/dag/dwh_layer/create-dwh-schema.sql"
load_dwh_dir = f"{dag_path}/dag/dwh_layer/create-dwh-schema.sql"


def create_dwh_schema_table(create_dwh_dir, host, database, user, password):
    #connect to potsgresql
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    # open SQL file 
    with open(create_dwh_dir, 'r') as f:
        sql = f.read()
    # run sql
    cur  = conn.cursor()
    cur.execute(sql)  
    conn.commit()
    # Menutup koneksi
    cur.close()
    conn.close()


def load_to_dwh(load_dwh_dir, host, database, user, password):
    #connect to potsgresql
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    # open SQL file 
    with open(load_dwh_dir, 'r') as f:
        sql = f.read()
    # run sql
    cur  = conn.cursor()
    cur.execute(sql)  
    conn.commit()
    # Menutup koneksi
    cur.close()
    conn.close()


# initializing the default arguments that we'll pass to our DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

dwh_layer = DAG(
    'dwh_layer',
    default_args=default_args,
    description='load from raw_layer to dwh_layer',
    schedule_interval=timedelta(days=1),
    catchup=False
)

task_1 = PythonOperator(
    task_id='create_dwh_schema_table',
    python_callable=create_dwh_schema_table,
    dag=dwh_layer,
)

task_2 = PythonOperator(
    task_id='load_to_dwh',
    python_callable=load_to_dwh,
    dag=dwh_layer,
)

task_1 >> task_2
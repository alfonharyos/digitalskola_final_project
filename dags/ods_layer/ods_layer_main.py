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
create_ods_dir = f"{dag_path}/dag/raw_layer/create-ods-schema.sql"


def create_ods_schema_table(create_ods_dir, host, database, user, password):
    #connect to potsgresql
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    # open SQL file 
    with open(create_ods_dir, 'r') as f:
        sql = f.read()
    # run sql
    cur  = conn.cursor()
    cur.execute(sql)  
    conn.commit()
    # Menutup koneksi
    cur.close()
    conn.close()


def load_to_ods(host, database, user, password):
    #connect to potsgresql
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    sql_list = [
            'business', 
            'business-loc', 
            'business-attrib', 
            'business-hours',
            'user',
            'tip',
            'review',
            'checkin',
            'temp',
            'precip'
            ]
    for sql_name in sql_list:
        with open(f"{dag_path}/dag/ods_layer/EL-ods-{sql_name}'.sql", 'r') as f:
            sql = f.read()
        cur  = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
    # close connection
    conn.close()


# initializing the default arguments that we'll pass to our DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

ods_layer = DAG(
    'ods_layer',
    default_args=default_args,
    description='load from raw_layer to ods_layer',
    schedule_interval=timedelta(days=1),
    catchup=False
)

create_ods_schema_table_task = PythonOperator(
    task_id='create_ods_schema_table',
    python_callable=create_ods_schema_table,
    dag=ods_layer,
)

load_to_ods_task = PythonOperator(
    task_id='load_to_ods',
    python_callable=load_to_ods,
    dag=ods_layer,
)

create_ods_schema_table_task >> load_to_ods_task
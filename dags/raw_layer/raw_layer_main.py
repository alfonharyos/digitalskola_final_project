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
create_raw_dir = f"{dag_path}/dag/raw_layer/create-raw-schema.sql"
yelp_academic_dataset_business_dir= f"{dag_path}/data/yelp/yelp_academic_dataset_business.json"
yelp_academic_dataset_checkin_dir= f"{dag_path}/data/yelp/yelp_academic_dataset_checkin.json"
yelp_academic_dataset_review_dir= f"{dag_path}/data/yelp/yelp_academic_dataset_review.json"
yelp_academic_dataset_tip_dir= f"{dag_path}/data/yelp/yelp_academic_dataset_tip.json"
yelp_academic_dataset_user_dir= f"{dag_path}/data/yelp/yelp_academic_dataset_user.json"
precipitation_dir= f"{dag_path}/data/yelp_academic_dataset_user.csv"
temp_dir= f"{dag_path}/data/USW00023169-temperature-degreeF.csv"


def create_raw_schema_table(create_raw_dir, host, database, user, password):
    #connect to potsgresql
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    # open SQL file 
    with open(create_raw_dir, 'r') as f:
        sql_raw = f.read()
    # run sql
    cur  = conn.cursor()
    cur.execute(sql_raw)  
    conn.commit()
    # Menutup koneksi
    cur.close()
    conn.close()


def load_raw_business(engine, yelp_academic_dataset_business_dir):
# define table object
    metadata = MetaData()
    yelp_business = Table('yelp_business', metadata,
        Column('business_id', String),
        Column('name', String),
        Column('address', String),
        Column('city', String),
        Column('state', String),
        Column('postal_code', String),
        Column('latitude', Float),
        Column('longitude', Float),
        Column('stars', Numeric(3,2)),
        Column('review_count', Integer),
        Column('is_open', Integer),
        Column('attributes', JSON),
        Column('categories', String),
        Column('hours', JSON),
        schema='raw_layer'
    )
    # load data from file
    with open(yelp_academic_dataset_business_dir, 'r', encoding='utf-8') as f:
        values_list = [json.loads(line) for line in f]
    # split data into chunks of 10000 rows
    chunk_size = 10000
    chunks = [values_list[i:i+chunk_size] for i in range(0, len(values_list), chunk_size)]
    # insert data in batches
    for chunk in chunks:
        engine.execute(yelp_business.insert().values(chunk))


def load_raw_checkin(engine, yelp_academic_dataset_checkin_dir):
    metadata = MetaData()
    yelp_checkin = Table('yelp_checkin', metadata,
        Column('business_id', String),
        Column('date', String),
        schema='raw_layer'
    )
    # load data from file
    with open(yelp_academic_dataset_checkin_dir, 'r', encoding='utf-8') as f:
        values_list = [json.loads(line) for line in f]
    # split data into chunks of 10000 rows
    chunk_size = 10000
    chunks = [values_list[i:i+chunk_size] for i in range(0, len(values_list), chunk_size)]
    # insert data in batches
    for chunk in chunks:
        engine.execute(yelp_checkin.insert().values(chunk))


def load_raw_review(engine, yelp_academic_dataset_review_dir):
    metadata = MetaData()
    yelp_review = Table('yelp_review', metadata,
        Column('review_id', String),
        Column('user_id', String),
        Column('business_id', String),
        Column('stars', Numeric(3,2)),
        Column('useful', Integer),
        Column('funny', Integer),
        Column('cool', Integer),
        Column('text', String),
        Column('timestamp', DateTime),
        schema='raw_layer'
    )
    # load data from file
    with open(yelp_academic_dataset_review_dir, 'r', encoding='utf-8') as f:
        values_list = [json.loads(line.replace('date', 'timestamp')) for line in f]
    # split data into chunks of 10000 rows
    chunk_size = 10000
    chunks = [values_list[i:i+chunk_size] for i in range(0, len(values_list), chunk_size)]
    # insert data in batches
    for chunk in chunks:
        engine.execute(yelp_review.insert().values(chunk))


def load_raw_tip(engine, yelp_academic_dataset_tip_dir):
# define table object
    metadata = MetaData()
    yelp_tip = Table('yelp_tip', metadata,
        Column('user_id', String),
        Column('business_id', String),
        Column('text', String),
        Column('timestamp', DateTime),
        Column('compliment_count', Integer),
        schema='raw_layer'
    )
    # load data from file
    with open(yelp_academic_dataset_tip_dir, 'r', encoding='utf-8') as f:
        values_list = [json.loads(line.replace('date', 'timestamp')) for line in f]
    # split data into chunks of 10000 rows
    chunk_size = 10000
    chunks = [values_list[i:i+chunk_size] for i in range(0, len(values_list), chunk_size)]
    # insert data in batches
    for chunk in chunks:
        engine.execute(yelp_tip.insert().values(chunk))


def load_raw_user(engine,yelp_academic_dataset_user_dir):
    # define table object
    metadata = MetaData()
    yelp_user = Table('yelp_user', metadata,
        Column('user_id', String),
        Column('name', String),
        Column('review_count', Integer),
        Column('yelping_since', DateTime),
        Column('useful', Integer),
        Column('funny', Integer),
        Column('cool', Integer),
        Column('fans', Integer),
        Column('elite', String),
        Column('friends', String),
        Column('average_stars', Numeric(3,2)),
        Column('compliment_hot', Integer),
        Column('compliment_more', Integer),
        Column('compliment_profile', Integer),
        Column('compliment_cute', Integer),
        Column('compliment_list', Integer),
        Column('compliment_note', Integer),
        Column('compliment_plain', Integer),
        Column('compliment_cool', Integer),
        Column('compliment_funny', Integer),
        Column('compliment_writer', Integer),
        Column('compliment_photos', Integer),
        schema='raw_layer'
    )
    # load data from file
    with open(yelp_academic_dataset_user_dir, 'r', encoding='utf-8') as f:
        values_list = [json.loads(line.replace('date', 'timestamp')) for line in f]
    # split data into chunks of 1000 rows
    chunk_size = 1000
    chunks = [values_list[i:i+chunk_size] for i in range(0, len(values_list), chunk_size)]
    # insert data in batches
    for chunk in chunks:
        engine.execute(yelp_user.insert().values(chunk))


def load_raw_prep(engine, precipitation_dir):
    # define table object
    metadata = MetaData()
    weather_precipitation = Table('weather_precipitation', metadata,
        Column('date', DATE),
        Column('precipitation', Float),
        Column('precipitation_normal', Float),
        schema='raw_layer'
    )
    # load data from file
    with open(precipitation_dir, 'r', encoding='utf-8') as f:
        next(f)  # skip the header row
        values_list = [row for row in csv.reader(f)]
    # convert date column to string format of 'YYYYMMDD'
    for row in values_list:
        row[0] = datetime.strptime(row[0], '%Y%m%d').strftime('%Y-%m-%d')
        row[1] = 0.1 if row[1] == 'T' else row[1] if row[1] else None
    # insert data in batches
    for values in values_list:
        engine.execute(weather_precipitation.insert().values(values))


def load_raw_temp(engine, temp_dir):
# define table object
    metadata = MetaData()
    weather_temperature = Table('weather_temperature', metadata,
        Column('date', DATE),
        Column('min', Float),
        Column('max', Float),
        Column('normal_min', Float),
        Column('normal_max', Float),
        Column('precipitation_normal', Float),
        schema='raw_layer'
    )
    # load data from file
    with open(temp_dir, 'r', encoding='utf-8') as f:
        next(f)  # skip the header row
        values_list = [row for row in csv.reader(f)]
    # convert date column to string format of 'YYYYMMDD'
    for row in values_list:
        row[0] = datetime.strptime(row[0], '%Y%m%d').strftime('%Y-%m-%d')
        row[1] = None if row[1] == '' else row[1]
        row[2] = None if row[2] == '' else row[2]
        row[3] = None if row[3] == '' else row[3]
        row[4] = None if row[4] == '' else row[4]
    # insert data in batches
    for values in values_list:
        engine.execute(weather_temperature.insert().values(values))



# initializing the default arguments that we'll pass to our DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

raw_layer = DAG(
    'raw_layer_yelp',
    default_args=default_args,
    description='Aggregates booking records for data analysis',
    schedule_interval=timedelta(days=1),
    catchup=False
)

create_raw_schema_table_task = PythonOperator(
    task_id='create_raw_schema_table',
    python_callable=create_raw_schema_table,
    dag=raw_layer,
)

load_raw_business_task = PythonOperator(
    task_id='load_raw_business',
    python_callable=load_raw_business,
    dag=raw_layer,
)

load_raw_checkin_task = PythonOperator(
    task_id='load_raw_checkin',
    python_callable=load_raw_checkin,
    dag=raw_layer,
)

load_raw_review_task = PythonOperator(
    task_id='load_raw_review',
    python_callable=load_raw_review,
    dag=raw_layer,
)

load_raw_tip_task = PythonOperator(
    task_id='load_raw_tip',
    python_callable=load_raw_tip,
    dag=raw_layer,
)

load_raw_prep_task = PythonOperator(
    task_id='load_raw_prep',
    python_callable=load_raw_prep,
    dag=raw_layer,
)

load_raw_temp_task = PythonOperator(
    task_id='load_raw_temp',
    python_callable=load_raw_temp,
    dag=raw_layer,
)

create_raw_schema_table_task >> load_raw_business_task\
    >> load_raw_checkin_task >> load_raw_review_task\
        >> load_raw_tip_task >> load_raw_prep_task\
            >> load_raw_temp_task
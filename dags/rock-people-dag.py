from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.version import version
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook

from rock_people import fetch_and_save_people
from rock_campus import fetch_and_save_campuses

import base64
import requests


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('rock_people_dag-2',
         start_date=datetime(2021, 2, 25),
         max_active_runs=1,
         schedule_interval=timedelta(minutes=30),  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         # catchup=False # enable if you don't want historical dag runs to run
         ) as dag:

    t0 = PythonOperator(
        task_id='fetch_and_save_campuses',
        python_callable=fetch_and_save_campuses,  # make sure you don't include the () of the function
    )

    # generate tasks with a loop. task_id must be unique
    t1 = PythonOperator(
        task_id='fetch_and_save_people',
        python_callable=fetch_and_save_people,  # make sure you don't include the () of the function
    )

    t0 >> t1
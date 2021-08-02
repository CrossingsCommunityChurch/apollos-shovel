from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from rock_people import fetch_and_save_people
from rock_campus import fetch_and_save_campuses



# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG('backfill_rock_people_chaseoaks_dag',
         start_date=datetime(2021, 6, 30),
         max_active_runs=1,
         schedule_interval='@once',  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         tags=['chaseoaks', 'backfill', 'people'],
         # catchup=False # enable if you don't want historical dag runs to run
         ) as dag:

    t0 = PythonOperator(
        task_id='fetch_and_save_campuses',
        python_callable=fetch_and_save_campuses,  # make sure you don't include the () of the function
        op_kwargs={'client': 'chaseoaks'}
    )

    # generate tasks with a loop. task_id must be unique
    t1 = PythonOperator(
        task_id='fetch_and_save_people',
        python_callable=fetch_and_save_people,  # make sure you don't include the () of the function
        op_kwargs={'do_backfill': True, 'client': 'chaseoaks'}
    )

    t0 >> t1

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from rock_media import fetch_and_save_media

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
with DAG('rock_media_backfill_dag',
         start_date=datetime(2021, 2, 22),
         max_active_runs=1,
         schedule_interval='@once',
         default_args=default_args,
         # catchup=False # enable if you don't want historical dag runs to run
         ) as dag:

    PythonOperator(
        task_id='fetch_and_save_media',
        python_callable=fetch_and_save_media, # make sure you don't include the () of the function
        op_kwargs={'client': 'core', 'do_backfill': True}
    )
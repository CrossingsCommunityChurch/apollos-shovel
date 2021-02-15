from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.version import version
from datetime import datetime, timedelta

import requests

def fetch_rock_model(execution_date,**kwargs):
    headers = {"Authorization-Token": kwargs['rock_token']}
    print(execution_date)


    # rock_now = datetime.datetime.strptime(ts, '%a %b %d %Y').strftime('%Y-%m-%d%z')

    r = requests.get(
            f"{kwargs['rock_api']}/ContentChannelItems",
            params={
                "$top": 100,
                "$filter": f"ModifiedDateTime gte datetime'{execution_date.strftime('%Y-%m-%dT00:00')}'"
            },
            headers=headers)
    print(f"ModifiedDateTime gte datetime'{execution_date.strftime('%Y-%m-%dT00:00')}'")
    print(r.json())


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('rock_model_dag',
         start_date=datetime(2019, 1, 1),
         max_active_runs=3,
         schedule_interval=timedelta(minutes=30),  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         # catchup=False # enable if you don't want historical dag runs to run
         ) as dag:

    t0 = DummyOperator(
        task_id='start'
    )

    # generate tasks with a loop. task_id must be unique
    t1 = PythonOperator(
        task_id='fetch_rock_model',
        python_callable=fetch_rock_model,  # make sure you don't include the () of the function
        op_kwargs={'rock_model': "People", 'rock_token': "ASZjZWdf3IqrbZX9sedtB4wb", 'rock_api': "https://rock.apollos.app/api"},
        provide_context=True
    )

    t0 >> t1
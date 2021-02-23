from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.version import version
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook

from aes import AESCrypto

import base64
import requests

def apollos_id(type, id):
    return type + ":" + id

def fetch_rock_model(ds, *args, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='apollos_postgres')
    headers = {"Authorization-Token": Variable.get("rock_token")}

    r = requests.get(
            f"{Variable.get('rock_api')}/{kwargs['rock_model']}",
            params={
                "$top": 100,
                "$filter": f"ModifiedDateTime ge datetime'{kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}'"
            },
            headers=headers)

    rock_objects = r.json()

    gender_map = ('UNKNOWN', 'MALE', 'FEMALE')

    for obj in rock_objects:

        dts_insert = """
        INSERT into people ("createdAt", "updatedAt", "originId", "originType", "apollosType", "firstName", "lastName", gender, "birthDate")
        values (%(current_date)s, %(current_date)s, %(id)s, 'rock', 'Person', %(first_name)s, %(last_name)s, %(gender)s, %(birth_date)s)
        ON CONFLICT ("originId", "originType")
        DO UPDATE SET ("firstName", "lastName") = (%(first_name)s, %(last_name)s)
        """

        pg_hook.run(dts_insert, parameters=({
            'id': obj['Id'],
            'first_name': obj['FirstName'],
            'last_name': obj['LastName'],
            'current_date': kwargs['execution_date'],
            'gender': gender_map[obj['Gender']],
            'birth_date': obj['BirthDate']
        }))

        users_without_apollos_id_select = """
        SELECT id from people
        WHERE "originType" = 'rock' and "apollosId" IS NULL
        """

        for new_id in pg_hook.get_records(users_without_apollos_id_select):
            apollos_id_update = """
            UPDATE people
            SET "apollosId" = %s
            WHERE id = %s::uuid
            """

            pg_hook.run(
                apollos_id_update,
                parameters=((apollos_id('Person', new_id[0]), new_id[0]))
            )


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
with DAG('rock_people_dag',
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
        op_kwargs={'rock_model': "People"},
    )

    t0 >> t1
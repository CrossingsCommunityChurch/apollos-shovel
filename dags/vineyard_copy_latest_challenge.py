import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, date, timedelta
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook


def recycle_challenge():
    # check if there's an item that starts today
    headers = {"Authorization-Token": Variable.get("vineyard_rock_token")}
    items = requests.get(
        f"{Variable.get('vineyard_rock_api')}/ContentChannelItems?$filter=ContentChannelId eq 2056 and StartDateTime lt datetime'{(date.today() + timedelta(days = 1)).isoformat()}' and StartDateTime gt datetime'{date.today().isoformat()}'",
        headers=headers,
    ).json()

    # if not
    if not len(items):
        # get oldest item
        item_id = requests.get(
            f"{Variable.get('vineyard_rock_api')}/ContentChannelItems?$filter=ContentChannelId eq 2056&$orderby=StartDateTime",
            headers=headers,
        ).json()[0]["Id"]

        # and patch it with todays date
        requests.patch(
            f"{Variable.get('vineyard_rock_api')}/ContentChannelItems/{item_id}",
            json={"StartDateTime": date.today().isoformat()},
            headers=headers,
        )

        pg_hook = PostgresHook(
            postgres_conn_id="vineyard_apollos_postgres",
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )

        # find the item in the DB
        node_id = pg_hook.get_first(
            f"""
            SELECT id
            FROM content_item
            WHERE origin_id = '{item_id}'
            """
        )[0]

        # delete the completed interactions
        pg_hook.run(
            f"""
            DELETE FROM interaction
            WHERE node_id = '{node_id}' AND action = 'COMPLETE'
            """
        )


with DAG(
    "vineyard_recycle_challenge",
    default_args={"owner": "airflow"},
    description="moves oldest challenge to today",
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    copy = PythonOperator(
        task_id="recycle_challenge",
        python_callable=recycle_challenge,
    )

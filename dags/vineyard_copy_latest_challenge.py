from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import timedelta, datetime


def copy_latest_challenge():
    pg_hook = PostgresHook(
        postgres_conn_id="vineyard_apollos_postgres",
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )

    # find the oldest challenge
    oldest_challenge_id = pg_hook.get_first(
        """
        SELECT id
        FROM content_item
        WHERE content_item_category_id = '1acff1cb-805b-4561-b5f7-ee136b0b39b4'
        ORDER BY publish_at ASC
        """
    )[0]

    # delete the completed interactions
    pg_hook.run(
        f"""
        DELETE FROM interaction
        WHERE node_id = '{oldest_challenge_id}' AND action = 'COMPLETE'
        """
    )

    # update the publish date
    pg_hook.run(
        f"""
        UPDATE content_item
        SET publish_at = NOW()
        WHERE id = '{oldest_challenge_id}'
        """
    )


with DAG(
    "vineyard_copy_latest_challenge",
    default_args={"owner": "airflow"},
    description="Copies the next challenge over to the DB as a new content item so it is not completed by anyone",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    copy = PythonOperator(
        task_id="copy_latest_challenge",
        python_callable=copy_latest_challenge,
    )

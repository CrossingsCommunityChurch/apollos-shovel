from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from rock_content_items import fetch_and_save_content_items
from rock_content_items_connections import fetch_and_save_content_items_connections

# Default settings applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG(
    "rock_content_item_backfill_example_dag",
    start_date=datetime(2021, 2, 22),
    max_active_runs=1,
    schedule_interval="@once",
    default_args=default_args,
    # catchup=False # enable if you don't want historical dag runs to run
) as dag:

    t0 = PythonOperator(
        task_id="fetch_and_save_content_items",
        python_callable=fetch_and_save_content_items,  # make sure you don't include the () of the function
        op_kwargs={"client": None},
    )

    t1 = PythonOperator(
        task_id="fetch_and_save_content_items_connections",
        python_callable=fetch_and_save_content_items_connections,  # make sure you don't include the () of the function
        op_kwargs={"client": None, "do_backfill": True},
    )

    t0 >> t1

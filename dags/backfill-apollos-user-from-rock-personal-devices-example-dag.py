from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from rock_personal_devices import (
    fetch_and_save_personal_devices_to_apollos_user
)


# Default settings applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    "backfill_apollos_user_from_rock_personal_devices_example",
    start_date=datetime(2021, 3, 22),
    max_active_runs=1,
    # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
    schedule_interval="@once",
    default_args=default_args,
    # catchup=False # enable if you don't want historical dag runs to run
) as dag:

    # generate tasks with a loop. task_id must be unique
    PythonOperator(
        task_id="backfill_apollos_user_from_personal_devices",
        python_callable=fetch_and_save_personal_devices_to_apollos_user,
        op_kwargs={
            "do_backfill": True,
            "rock_mobile_device_type_id": 919,
            "client": None,
        },
    )

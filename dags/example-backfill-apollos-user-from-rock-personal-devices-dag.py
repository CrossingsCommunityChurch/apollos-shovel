from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from rock.rock_personal_devices import fetch_and_save_personal_devices_to_apollos_user


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
    "example_backfill_apollos_user_from_rock_personal_devices",
    start_date=datetime(2021, 3, 22),
    max_active_runs=1,
    schedule_interval="@once",  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
    default_args=default_args,
    tags=["example", "backfill", "personal_devices"],
    # catchup=False # enable if you don't want historical dag runs to run
) as dag:

    # generate tasks with a loop. task_id must be unique
    PythonOperator(
        task_id="backfill_apollos_user_from_personal_devices",
        python_callable=fetch_and_save_personal_devices_to_apollos_user,  # make sure you don't include the () of the function
        op_kwargs={
            "do_backfill": True,
            "rock_mobile_device_type_id": 675,
            "client": None,
        },
    )

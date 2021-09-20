from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from rock.rock_prayer_requests import (
    fetch_and_save_prayer_request,
    delete_prayer_requests,
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


def create_prayer_request_dag(church, start_date, schedule_interval, do_backfill):
    tags = [church, "prayer_request"]
    name = f"{church}_rock_prayer_request_dag"
    if do_backfill:
        tags.append("backfill")
        name = f"{church}_backfill_rock_prayer_request_dag"

    dag = DAG(
        name,
        start_date=start_date,
        max_active_runs=1,
        schedule_interval=schedule_interval,
        default_args=default_args,
        tags=tags,
    )

    with dag:
        prayer_requests = PythonOperator(
            task_id="fetch_and_save_prayer_requests",
            # make sure you don't include the () of the function
            python_callable=fetch_and_save_prayer_request,
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        deleted_prayer_requests = PythonOperator(
            task_id="delete_prayer_requests",
            # make sure you don't include the () of the function
            python_callable=delete_prayer_requests,
            op_kwargs={"client": church},
        )

        prayer_requests >> deleted_prayer_requests

    return dag, name

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta

from rock.rock_people import fetch_and_save_people
from rock.rock_campus import fetch_and_save_campuses

# Default settings applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def create_rock_people_dag(church, start_date, schedule_interval, do_backfill):
    tags = [church, "people"]
    name = f"{church}_rock_people_dag"
    if do_backfill:
        tags.append("backfill")
        name = f"{church}_backfill_rock_people_dag"

    dag = DAG(
        name,
        start_date=start_date,
        max_active_runs=1,
        schedule_interval=schedule_interval,
        default_args=default_args,
        tags=tags
        # catchup=False # enable if you don't want historical dag runs to run
    )

    with dag:

        t0 = PythonOperator(
            task_id="fetch_and_save_campuses",
            python_callable=fetch_and_save_campuses,  # make sure you don't include the () of the function
            op_kwargs={"client": church},
        )

        # generate tasks with a loop. task_id must be unique
        t1 = PythonOperator(
            task_id="fetch_and_save_people",
            python_callable=fetch_and_save_people,  # make sure you don't include the () of the function
            op_kwargs={"do_backfill": do_backfill, "client": church},
        )

        t0 >> t1

    return dag

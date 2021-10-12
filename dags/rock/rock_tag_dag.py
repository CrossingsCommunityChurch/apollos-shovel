from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from rock.rock_tags import (
    fetch_and_save_persona_tags,
    attach_persona_tags_to_people,
    attach_persona_tags_to_content,
)
from rock.rock_deleted_tags import remove_deleted_tags

# Default settings applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def create_rock_tag_dag(church, start_date, schedule_interval, do_backfill):
    tags = [church, "tags"]
    name = f"{church}_rock_tag_dag"

    if do_backfill:
        tags.append("backfill")
        name = f"{church}_backfill_rock_tag_dag"

    dag = DAG(
        name,
        start_date=start_date,
        max_active_runs=1,
        schedule_interval=schedule_interval,
        default_args=default_args,
        tags=tags,
        # Let tasks run no more than three times longer than the schedule interval.
        dagrun_timeout=(
            schedule_interval * 3 if type(schedule_interval) is not str else None
        ),
    )

    with dag:
        content_tags = PythonOperator(
            task_id="attach_persona_tags_to_content",
            python_callable=attach_persona_tags_to_content,
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        persona_tags = PythonOperator(
            task_id="attach_persona_tags_to_people",
            python_callable=attach_persona_tags_to_people,
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        tags = PythonOperator(
            task_id="fetch_and_save_persona_tags",
            python_callable=fetch_and_save_persona_tags,  # make sure you don't include the () of the function
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        deleted_tags = PythonOperator(
            task_id="remove_deleted_tags",
            python_callable=remove_deleted_tags,  # make sure you don't include the () of the function
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        tags >> [persona_tags, content_tags]

        tags >> deleted_tags

    return dag, name

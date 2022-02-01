from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta

from contentful_crossroads.kids_club.cr_kids_club_content_items import (
    KidsClubContentItem,
)
from contentful_crossroads.cr_contentful_content_items import fetch_and_save_entries

from contentful_crossroads.cr_contentful_deletion import fetch_and_delete_items
from contentful_crossroads.kids_club.cr_kids_club_categories import (
    create_kids_club_categories,
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


def create_cr_kids_club_content_item_dag(
    start_date, schedule_interval, do_backfill, church="crossroads_kids_club"
):
    tags = [church, "content"]
    name = f"{church}_contentful_content_item_dag"
    if do_backfill:
        tags.append("backfill")
        name = f"{church}_backfill_contentful_content_item_dag"

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

    if do_backfill:
        with dag:
            categories = PythonOperator(
                task_id="create_categories",
                python_callable=create_kids_club_categories,
                op_kwargs={
                    "client": church,
                    "do_backfill": True,
                },
            )
            base_items = PythonOperator(
                task_id="fetch_and_save_entries",
                # make sure you don't include the () of the function
                python_callable=fetch_and_save_entries,
                op_kwargs={
                    "client": church,
                    "do_backfill": True,
                    "contentful_filters": {"content_type": "video"},
                    "klass": KidsClubContentItem,
                },
            )
            deletion = PythonOperator(
                task_id="fetch_and_delete_items",
                # make sure you don't include the () of the function
                python_callable=fetch_and_delete_items,
                op_kwargs={
                    "client": church,
                    "do_backfill": True,
                },
            )

            categories >> base_items >> deletion
    else:
        with dag:

            base_items = PythonOperator(
                task_id="fetch_and_save_entries",
                # make sure you don't include the () of the function
                python_callable=fetch_and_save_entries,
                op_kwargs={
                    "client": church,
                    "do_backfill": False,
                    "klass": KidsClubContentItem,
                },
            )
            deletion = PythonOperator(
                task_id="fetch_and_delete_items",
                # make sure you don't include the () of the function
                python_callable=fetch_and_delete_items,
                op_kwargs={
                    "client": church,
                    "do_backfill": False,
                },
            )

            base_items >> deletion

    return dag, name

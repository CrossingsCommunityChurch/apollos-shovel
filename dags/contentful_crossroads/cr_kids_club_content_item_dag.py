from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta

from contentful_crossroads.cr_contentful_content_items import (
    fetch_and_save_content_items,
)
from contentful_crossroads.cr_contentful_assets import (
    fetch_and_save_assets,
    remove_unused_assets,
)
from contentful_crossroads.cr_kids_club_categories import create_kids_club_categories


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

    with dag:
        assets = PythonOperator(
            task_id="fetch_and_save_assets",
            # make sure you don't include the () of the function
            python_callable=fetch_and_save_assets,
            op_kwargs={
                "client": church,
                "do_backfill": do_backfill,
                "localization": "en-US",
            },
        )

        base_items = PythonOperator(
            task_id="fetch_and_save_content_items",
            # make sure you don't include the () of the function
            python_callable=fetch_and_save_content_items,
            op_kwargs={
                "client": church,
                "do_backfill": do_backfill,
                "contentful_filters": {"type": "Entry", "content_type": "video"},
                "localization": "en-US",
            },
        )

        delete_unused_assets = PythonOperator(
            task_id="remove_unused_assets",
            # make sure you don't include the () of the function
            python_callable=remove_unused_assets,
            op_kwargs={
                "client": church,
                "do_backfill": do_backfill,
                "localization": "en-US",
            },
        )

        categories = PythonOperator(
            task_id="create_categories",
            python_callable=create_kids_club_categories,
            op_kwargs={
                "client": church,
                "do_backfill": do_backfill,
            },
        )

        assets >> base_items >> delete_unused_assets

        categories >> base_items

    return dag, name

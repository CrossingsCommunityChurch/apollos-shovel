from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from rock_content_items import fetch_and_save_content_items
from rock_media import fetch_and_save_media
from rock_content_items_connections import fetch_and_save_content_items_connections, set_parent_id
from rock_cover_image import fetch_and_save_cover_image
from rock_content_item_categories import fetch_and_save_content_item_categories, attach_content_item_categories
from rock_tags import fetch_and_save_persona_tags

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('rock_content_item_backfill_core_dag',
         start_date=datetime(2021, 2, 22),
         max_active_runs=1,
         schedule_interval='@once',
         default_args=default_args,
         # catchup=False # enable if you don't want historical dag runs to run
         ) as dag:

    t0 = PythonOperator(
        task_id='fetch_and_save_content_items',
        python_callable=fetch_and_save_content_items,  # make sure you don't include the () of the function
        op_kwargs={'client': 'core', 'do_backfill': True}
    )

    t1 = PythonOperator(
        task_id='fetch_and_save_content_items_connections',
        python_callable=fetch_and_save_content_items_connections,  # make sure you don't include the () of the function
        op_kwargs={'client': 'core', 'do_backfill': True}
    )

    t2 = PythonOperator(
        task_id='fetch_and_save_media',
        python_callable=fetch_and_save_media,  # make sure you don't include the () of the function
        op_kwargs={'client': 'core', 'do_backfill': True}
    )

    t3 = PythonOperator(
        task_id='fetch_and_save_content_item_categories',
        python_callable=fetch_and_save_content_item_categories,  # make sure you don't include the () of the function
        op_kwargs={'client': 'core', 'do_backfill': True}
    )

    t4 = PythonOperator(
        task_id='attach_content_item_categories',
        python_callable=attach_content_item_categories,  # make sure you don't include the () of the function
        op_kwargs={'client': 'core', 'do_backfill': True}
    )

    t6 = PythonOperator(
        task_id='set_parent_id',
        python_callable=set_parent_id,  # make sure you don't include the () of the function
        op_kwargs={'client': 'core', 'do_backfill': True}
    )

    t5 = PythonOperator(
        task_id='fetch_and_save_cover_image',
        python_callable=fetch_and_save_cover_image,  # make sure you don't include the () of the function
        op_kwargs={'client': 'core', 'do_backfill': True}
    )

    t7 = PythonOperator(
        task_id='fetch_and_save_persona_tags',
        python_callable=fetch_and_save_persona_tags,  # make sure you don't include the () of the function
        op_kwargs={'client': 'core', 'do_backfill': True}
    )

    # Adding and syncing categories depends on having content items
    t0 >> t3 >> t4

    t2 >> t5

    t1 >> t6

    t0 >> [t1, t2]

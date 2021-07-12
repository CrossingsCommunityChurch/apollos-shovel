from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from rock_content_items import fetch_and_save_content_items
from rock_media import fetch_and_save_media
from rock_content_items_connections import fetch_and_save_content_items_connections, set_parent_id
from rock_cover_image import fetch_and_save_cover_image
from rock_content_item_categories import fetch_and_save_content_item_categories, attach_content_item_categories
from rock_tags import fetch_and_save_persona_tags, attach_persona_tags_to_people, attach_persona_tags_to_content
from rock_features import fetch_and_save_features

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

    base_items = PythonOperator(
        task_id='fetch_and_save_content_items',
        python_callable=fetch_and_save_content_items,  # make sure you don't include the () of the function
        op_kwargs={'client': 'core', 'do_backfill': True}
    )

    connections = PythonOperator(
        task_id='fetch_and_save_content_items_connections',
        python_callable=fetch_and_save_content_items_connections,  # make sure you don't include the () of the function
        op_kwargs={'client': 'core', 'do_backfill': True}
    )

    media = PythonOperator(
        task_id='fetch_and_save_media',
        python_callable=fetch_and_save_media,  # make sure you don't include the () of the function
        op_kwargs={'client': 'core', 'do_backfill': True}
    )

    add_categories = PythonOperator(
        task_id='fetch_and_save_content_item_categories',
        python_callable=fetch_and_save_content_item_categories,  # make sure you don't include the () of the function
        op_kwargs={'client': 'core', 'do_backfill': True}
    )

    attach_categories = PythonOperator(
        task_id='attach_content_item_categories',
        python_callable=attach_content_item_categories,  # make sure you don't include the () of the function
        op_kwargs={'client': 'core', 'do_backfill': True}
    )

    set_parent_id = PythonOperator(
        task_id='set_parent_id',
        python_callable=set_parent_id,  # make sure you don't include the () of the function
        op_kwargs={'client': 'core', 'do_backfill': True}
    )

    set_cover_image = PythonOperator(
        task_id='fetch_and_save_cover_image',
        python_callable=fetch_and_save_cover_image,  # make sure you don't include the () of the function
        op_kwargs={'client': 'core', 'do_backfill': True}
    )

    content_tags = PythonOperator(
        task_id='attach_persona_tags_to_content',
        python_callable=attach_persona_tags_to_content,
        op_kwargs={'client': 'core', 'do_backfill': True}
    )

    persona_tags = PythonOperator(
        task_id='attach_persona_tags_to_people',
        python_callable=attach_persona_tags_to_people,
        op_kwargs={'client': 'core', 'do_backfill': True}
    )      

    tags = PythonOperator(
        task_id='fetch_and_save_persona_tags',
        python_callable=fetch_and_save_persona_tags,  # make sure you don't include the () of the function
        op_kwargs={'client': 'core', 'do_backfill': True}
    )

    features = PythonOperator(
        task_id='fetch_and_save_features',
        python_callable=fetch_and_save_features,  # make sure you don't include the () of the function
        op_kwargs={'client': 'core', 'do_backfill': True}
    )

    # Adding and syncing categories depends on having content items
    base_items >> add_categories >> attach_categories

    media >> set_cover_image

    connections >> set_parent_id >> features

    tags >> [persona_tags, content_tags]

    base_items >> [connections, media]

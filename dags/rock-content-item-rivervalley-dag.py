from datetime import datetime, timedelta
from rock_content_item_dag import create_rock_content_item_dag

start_date = datetime(2021, 7, 16)

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


globals()['backfill_rivervalley_content_item_rivervalley'] = create_rock_content_item_dag(
    'rivervalley',
    'backfill_rock_content_item_rivervalley',
    start_date,
    '@once',
    True
)

globals()['rivervalley_rock_content_item'] = create_rock_content_item_dag(
    'rivervalley',
    'rivervalley_rock_content_item',
    start_date,
    timedelta(minutes=30),
    False
)

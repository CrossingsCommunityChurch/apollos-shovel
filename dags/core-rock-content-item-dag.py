from datetime import datetime, timedelta
from rock.rock_content_item_dag import create_rock_content_item_dag

start_date = datetime(2021, 7, 16)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

globals()["backfill_rock_content_item_core"] = create_rock_content_item_dag(
    "core", "backfill_rock_content_item_core", start_date, "@once", True
)

globals()["core_rock_content_item"] = create_rock_content_item_dag(
    "core", "core_rock_content_item", start_date, timedelta(minutes=30), False
)

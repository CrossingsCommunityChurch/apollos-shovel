from datetime import datetime, timedelta
from rock.rock_tag_dag import create_rock_tag_dag

start_date = datetime(2021, 8, 4)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


globals()["core_backfill_rock_tag_dag"] = create_rock_tag_dag(
    "core", start_date, "@once", True
)

globals()["core_rock_tag_dag"] = create_rock_tag_dag(
    "core", start_date, timedelta(minutes=60), False
)

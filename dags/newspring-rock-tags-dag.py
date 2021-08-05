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

globals()["newspring_backfill_rock_tag_dag"] = create_rock_tag_dag(
    "newspring", start_date, "@once", True
)

globals()["newspring_rock_tag_dag"] = create_rock_tag_dag(
    "newspring", start_date, timedelta(minutes=60), False
)

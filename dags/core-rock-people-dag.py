from airflow import DAG
from datetime import datetime, timedelta

from rock.rock_people_dag import create_rock_people_dag

# Default settings applied to all tasks

start_date = datetime(2021, 6, 21)

globals()["core_backfill_rock_people_dag"] = create_rock_people_dag(
    "core", start_date, "@once", True
)

globals()["core_rock_people_dag"] = create_rock_people_dag(
    "core", start_date, timedelta(minutes=30), False
)

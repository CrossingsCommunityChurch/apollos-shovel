from airflow import DAG
from datetime import datetime, timedelta

from rock.rock_people_dag import create_rock_people_dag

start_date = datetime(2021, 4, 29)

globals()["newspring_backfill_rock_people_dag"] = create_rock_people_dag(
    "newspring", start_date, "@once", True
)

globals()["newspring_rock_people_dag"] = create_rock_people_dag(
    "newspring", start_date, timedelta(minutes=30), False
)

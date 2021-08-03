from airflow import DAG
from datetime import datetime, timedelta

from rock.rock_people_dag import create_rock_people_dag

# Default settings applied to all tasks

start_date = datetime(2021, 6, 28)
client_name = "example"

globals()[f"{client_name}_backfill_rock_people_dag"] = create_rock_people_dag(
    client_name, start_date, "@once", True
)

globals()[f"{client_name}_rock_people_dag"] = create_rock_people_dag(
    client_name, start_date, timedelta(minutes=30), False
)

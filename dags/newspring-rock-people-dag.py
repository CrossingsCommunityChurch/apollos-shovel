from airflow import DAG  # noqa: F401

from datetime import datetime, timedelta

from rock.rock_people_dag import create_rock_people_dag

start_date = datetime(2021, 4, 29)


backfill_dag, backfill_name = create_rock_people_dag(
    "newspring", start_date, "@once", True
)

globals()[backfill_name] = backfill_dag

dag, name = create_rock_people_dag(
    "newspring", start_date, timedelta(minutes=30), False
)

globals()[name] = dag

from airflow import DAG  # noqa: F401

from datetime import datetime, timedelta

from rock.rock_people_dag import create_rock_people_dag

start_date = datetime(2021, 11, 18)

backfill_dag, backfill_name = create_rock_people_dag(
    "eleven22", start_date, "@once", True, camelcased_tables=False
)

globals()[backfill_name] = backfill_dag

dag, name = create_rock_people_dag(
    "eleven22", start_date, timedelta(minutes=30), False, camelcased_tables=False
)

globals()[name] = dag

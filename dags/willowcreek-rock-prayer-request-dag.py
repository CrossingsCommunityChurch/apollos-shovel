from airflow import DAG  # noqa: F401

from datetime import datetime, timedelta

from rock.rock_prayer_request_dag import create_prayer_request_dag

start_date = datetime(2022, 1, 19)

dag, name = create_prayer_request_dag("willowcreek", start_date, "@once", True)

globals()[name] = dag

dag, name = create_prayer_request_dag(
    "willowcreek", start_date, timedelta(minutes=30), False
)

globals()[name] = dag

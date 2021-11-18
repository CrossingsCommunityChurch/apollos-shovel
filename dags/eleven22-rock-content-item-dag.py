from airflow import DAG  # noqa: F401

from datetime import datetime, timedelta
from rock.rock_content_item_dag import create_rock_content_item_dag

start_date = datetime(2021, 11, 18)


backfill_dag, backfill_name = create_rock_content_item_dag(
    "eleven22", start_date, "@once", True
)
globals()[backfill_name] = backfill_dag

dag, dag_name = create_rock_content_item_dag(
    "eleven22", start_date, timedelta(minutes=30), False
)

globals()[dag_name] = dag

from airflow import DAG  # noqa: F401

from datetime import datetime, timedelta
from rock.rock_tag_dag import create_rock_tag_dag

start_date = datetime(2022, 1, 19)

backfill_dag, backfill_name = create_rock_tag_dag(
    "willowcreek", start_date, "@once", True
)

globals()[backfill_name] = backfill_dag

dag, name = create_rock_tag_dag("willowcreek", start_date, timedelta(minutes=60), False)
globals()[name] = dag

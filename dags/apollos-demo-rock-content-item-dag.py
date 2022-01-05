from airflow import DAG  # noqa: F401

from datetime import datetime, timedelta
from rock.rock_content_item_dag import create_rock_content_item_dag
from algolia_multitenant.algolia_content_dag import create_algolia_dag

start_date = datetime(2021, 10, 7)


backfill_dag, backfill_name = create_rock_content_item_dag(
    "apollos_demo", start_date, "@once", True
)
globals()[backfill_name] = backfill_dag

dag, dag_name = create_rock_content_item_dag(
    "apollos_demo", start_date, timedelta(minutes=30), False
)

globals()[dag_name] = dag

algolia_dag_name, algolia_dag = create_algolia_dag(
    "apollos_demo", start_date, timedelta(hours=12)
)

globals()[algolia_dag_name] = algolia_dag

from airflow import DAG  # noqa: F401

from datetime import datetime, timedelta
from rock.rock_content_item_dag import create_rock_content_item_dag
from algolia_multitenant.algolia_content_dag import create_algolia_dag
from vimeo.rock_vimeo import create_rock_vimeo_dag

start_date = datetime(2021, 11, 18)


backfill_dag, backfill_name = create_rock_content_item_dag(
    "eleven22", start_date, "@once", True
)
globals()[backfill_name] = backfill_dag

dag, dag_name = create_rock_content_item_dag(
    "eleven22", start_date, timedelta(minutes=30), False
)
globals()[dag_name] = dag

algolia_dag_name, algolia_dag = create_algolia_dag(
    "eleven22", start_date, timedelta(hours=12)
)
globals()[algolia_dag_name] = algolia_dag

vimeo_backfill_dag, vimeo_backfill_name = create_rock_vimeo_dag(
    "eleven22", start_date, "@once", True
)
globals()[vimeo_backfill_name] = vimeo_backfill_dag

vimeo_dag, vimeo_name = create_rock_vimeo_dag(
    "eleven22", start_date, timedelta(minutes=30), False
)
globals()[vimeo_name] = vimeo_dag

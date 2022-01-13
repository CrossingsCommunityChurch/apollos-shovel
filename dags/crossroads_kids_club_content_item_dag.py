from airflow import DAG  # noqa: F401

from datetime import datetime, timedelta

from contentful_crossroads.cr_kids_club_content_item_dag import (
    create_cr_kids_club_content_item_dag,
)

start_date = datetime(2021, 11, 18)

backfill_dag, backfill_name = create_cr_kids_club_content_item_dag(
    start_date, "@once", True
)

globals()[backfill_name] = backfill_dag

dag, name = create_cr_kids_club_content_item_dag(
    start_date, timedelta(minutes=30), False
)

globals()[name] = dag

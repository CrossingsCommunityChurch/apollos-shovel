from datetime import datetime, timedelta

from rock.rock_people_dag import create_rock_people_dag

# Default settings applied to all tasks

start_date = datetime(2021, 6, 30)

backfill_dag, backfill_name = create_rock_people_dag(
    "chaseoaks", start_date, "@once", True
)

globals()[backfill_name] = backfill_dag

dag, name = create_rock_people_dag(
    "chaseoaks", start_date, timedelta(minutes=30), False
)

globals()[name] = dag

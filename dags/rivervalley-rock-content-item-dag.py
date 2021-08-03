from datetime import datetime, timedelta
from rock.rock_content_item_dag import create_rock_content_item_dag

start_date = datetime(2021, 7, 16)

globals()["rivervalley_backfill_rock_content_item"] = create_rock_content_item_dag(
    "rivervalley",
    "rivervalley_backfill_rock_content_item",
    start_date,
    "@once",
    True,
)

globals()["rivervalley_rock_content_item"] = create_rock_content_item_dag(
    "rivervalley",
    "rivervalley_rock_content_item",
    start_date,
    timedelta(minutes=30),
    False,
)

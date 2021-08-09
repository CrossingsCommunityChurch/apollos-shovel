from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from rock.rock_content_items import fetch_and_save_content_items
from rock.rock_media import fetch_and_save_media
from rock.rock_content_items_connections import (
    fetch_and_save_content_items_connections,
    set_content_item_parent_id,
)
from rock.rock_cover_image import fetch_and_save_cover_image
from rock.rock_content_item_categories import (
    fetch_and_save_content_item_categories,
    attach_content_item_categories,
)
from rock.rock_tags import (
    fetch_and_save_persona_tags,
    attach_persona_tags_to_people,
    attach_persona_tags_to_content,
)
from rock.rock_features import fetch_and_save_features
from rock.rock_deleted_tags import remove_deleted_tags
from rock.rock_deleted_content_items_dag import remove_deleted_content_items
from misc.wista import set_wistia_urls
import json

start_date = datetime(2021, 8, 9)

# Default settings applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Currently we do not support Audio in s3
def create_content_item_dag(dag_name, start_date, schedule_interval, do_backfill):
    def parse_asset_url(value, media_type):
        if media_type == "IMAGE":
            try:
                parsed_value = json.loads(value)
                if "path" in parsed_value:
                    return parsed_value["path"]
                if "Key" in parsed_value and "AssetStorageProviderId" in parsed_value:
                    return f"https://images.crossings.church/fit-in/700x700/{parsed_value['Key']}"
            except Exception as err:
                print(err)
                print(value)
                return ""
        return value

    def is_media_image(attribute, contentItem):
        attributeKey = attribute["Key"]
        attributeValue = contentItem["AttributeValues"][attributeKey]["Value"]
        return attribute["FieldTypeId"] == 10 or attribute["FieldTypeId"] == 131 or (
            "image" in attributeKey.lower()
            and isinstance(attributeValue, str)
            and attributeValue.startswith("http")
        )
    dag = DAG(
        dag_name,
        start_date=start_date,
        max_active_runs=1,
        schedule_interval=schedule_interval,
        default_args=default_args,
    )

    with dag:
        base_items = PythonOperator(
            task_id="fetch_and_save_content_items",
            python_callable=fetch_and_save_content_items,  # make sure you don't include the () of the function
            op_kwargs={"client": "crossings", "do_backfill": do_backfill},
        )

        connections = PythonOperator(
            task_id="fetch_and_save_content_items_connections",
            python_callable=fetch_and_save_content_items_connections,  # make sure you don't include the () of the function
            op_kwargs={"client": "crossings", "do_backfill": do_backfill},
        )

        media = PythonOperator(
            task_id="fetch_and_save_media",
            python_callable=fetch_and_save_media,  # make sure you don't include the () of the function
            op_kwargs={
                "client": "crossings",
                "do_backfill": do_backfill,
                "parse_asset_url": parse_asset_url,
                "is_image": is_media_image,
            },
        )

        add_categories = PythonOperator(
            task_id="fetch_and_save_content_item_categories",
            python_callable=fetch_and_save_content_item_categories,  # make sure you don't include the () of the function
            op_kwargs={"client": "crossings", "do_backfill": do_backfill},
        )

        attach_categories = PythonOperator(
            task_id="attach_content_item_categories",
            python_callable=attach_content_item_categories,  # make sure you don't include the () of the function
            op_kwargs={"client": "crossings", "do_backfill": do_backfill},
        )

        set_parent_id = PythonOperator(
            task_id="set_parent_id",
            python_callable=set_content_item_parent_id,  # make sure you don't include the () of the function
            op_kwargs={"client": "crossings", "do_backfill": do_backfill},
        )

        set_cover_image = PythonOperator(
            task_id="fetch_and_save_cover_image",
            python_callable=fetch_and_save_cover_image,  # make sure you don't include the () of the function
            op_kwargs={
                "client": "crossings",
                "do_backfill": do_backfill,
                "is_image": is_media_image,
            },
        )

        features = PythonOperator(
            task_id="fetch_and_save_features",
            python_callable=fetch_and_save_features,  # make sure you don't include the () of the function
            op_kwargs={"client": "crossings", "do_backfill": do_backfill},
        )

        deleted_content_items = PythonOperator(
            task_id="remove_deleted_content_items",
            python_callable=remove_deleted_content_items,  # make sure you don't include the () of the function
            op_kwargs={"client": "crossings", "do_backfill": do_backfill},
        )

        # Adding and syncing categories depends on having content items
        base_items >> add_categories >> attach_categories

        media >> set_cover_image

        connections >> set_parent_id >> features

        base_items >> [connections, media, deleted_content_items]

    return dag


globals()["crossings_rock_content_item_backfill_dag"] = create_content_item_dag(
    "crossings_rock_content_item_backfill_dag", start_date, "@once", True
)

globals()["crossings_rock_content_item_dag"] = create_content_item_dag(
    "crossings_rock_content_item_dag", start_date, timedelta(minutes=30), False
)

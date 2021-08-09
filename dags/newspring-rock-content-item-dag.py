from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from rock.rock_content_items import fetch_and_save_content_items
from rock.rock_media import fetch_and_save_media, Media
from rock.rock_content_items_connections import (
    fetch_and_save_content_items_connections,
    set_content_item_parent_id,
)
from rock.rock_cover_image import fetch_and_save_cover_image, CoverImage
from rock.rock_content_item_categories import (
    fetch_and_save_content_item_categories,
    attach_content_item_categories,
)
from rock.rock_features import fetch_and_save_features
from rock.rock_deleted_content_items_dag import remove_deleted_content_items
from misc.wista import set_wistia_urls
import json

start_date = datetime(2021, 7, 22)

# Default settings applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

ASSET_MAP = {
    "6": "ns.assets",
    "7": "ns.downloads",
    "8": "ns.images",
    "9": "ns.videos",
}


def is_media_image(content_item, attribute):
    attribute_key = attribute["Key"]
    attribute_value = content_item["AttributeValues"][attribute_key]["Value"]
    # S3
    if attribute["FieldTypeId"] == 133 and bool(attribute_value):
        try:
            parsed_value = json.loads(attribute_value)
            return (
                "audio" not in parsed_value["Key"].split("/")
                and "video" not in parsed_value["Key"].split("/")
                and ".mp3" not in parsed_value["Key"]
            )

        except Exception as err:
            return True

    return False


class NewspringMedia(Media):
    def parse_asset_url(self, value, media_type):
        if media_type == "IMAGE" or media_type == "AUDIO" and bool(value):
            try:
                parsed_value = json.loads(value)
                if "path" in parsed_value:
                    return parsed_value["path"]
                if "Key" in parsed_value and "AssetStorageProviderId" in parsed_value:
                    return f"https://s3.amazonaws.com/{ASSET_MAP[parsed_value['AssetStorageProviderId']]}/{parsed_value['Key']}"
            except Exception as err:
                print(err)
                print(value)
                return ""
        return value

    def is_video(self, _, attribute):
        return attribute["FieldTypeId"] == 125

    def is_image(self, content_item, attribute):
        return is_media_image(content_item, attribute)


class NewspringCoverImage(CoverImage):
    def is_image(self, content_item, attribute):
        return is_media_image(content_item, attribute)


def create_content_item_dag(dag_name, start_date, schedule_interval, do_backfill):
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
            op_kwargs={"client": "newspring", "do_backfill": do_backfill},
        )

        connections = PythonOperator(
            task_id="fetch_and_save_content_items_connections",
            python_callable=fetch_and_save_content_items_connections,  # make sure you don't include the () of the function
            op_kwargs={"client": "newspring", "do_backfill": do_backfill},
        )

        media = PythonOperator(
            task_id="fetch_and_save_media",
            python_callable=fetch_and_save_media,  # make sure you don't include the () of the function
            op_kwargs={
                "client": "newspring",
                "do_backfill": do_backfill,
                "klass": NewspringMedia,
            },
        )

        wistia = PythonOperator(
            task_id="set_wistia_urls",
            python_callable=set_wistia_urls,  # make sure you don't include the () of the function
            op_kwargs={
                "client": "newspring",
            },
        )

        add_categories = PythonOperator(
            task_id="fetch_and_save_content_item_categories",
            python_callable=fetch_and_save_content_item_categories,  # make sure you don't include the () of the function
            op_kwargs={"client": "newspring", "do_backfill": do_backfill},
        )

        attach_categories = PythonOperator(
            task_id="attach_content_item_categories",
            python_callable=attach_content_item_categories,  # make sure you don't include the () of the function
            op_kwargs={"client": "newspring", "do_backfill": do_backfill},
        )

        set_parent_id = PythonOperator(
            task_id="set_parent_id",
            python_callable=set_content_item_parent_id,  # make sure you don't include the () of the function
            op_kwargs={"client": "newspring", "do_backfill": do_backfill},
        )

        set_cover_image = PythonOperator(
            task_id="fetch_and_save_cover_image",
            python_callable=fetch_and_save_cover_image,  # make sure you don't include the () of the function
            op_kwargs={
                "client": "newspring",
                "do_backfill": do_backfill,
                "klass": NewspringCoverImage,
            },
        )

        features = PythonOperator(
            task_id="fetch_and_save_features",
            python_callable=fetch_and_save_features,  # make sure you don't include the () of the function
            op_kwargs={"client": "newspring", "do_backfill": do_backfill},
        )

        deleted_content_items = PythonOperator(
            task_id="remove_deleted_content_items",
            python_callable=remove_deleted_content_items,  # make sure you don't include the () of the function
            op_kwargs={"client": "newspring", "do_backfill": do_backfill},
        )

        # Adding and syncing categories depends on having content items
        base_items >> add_categories >> attach_categories

        media >> [set_cover_image, wistia]

        connections >> set_parent_id >> features

        base_items >> [connections, media, deleted_content_items]

    return dag


globals()["newspring_rock_content_item_backfill_dag"] = create_content_item_dag(
    "newspring_rock_content_item_backfill_dag", start_date, "@once", True
)

globals()["newspring_rock_content_item_dag"] = create_content_item_dag(
    "newspring_rock_content_item_dag", start_date, timedelta(minutes=30), False
)

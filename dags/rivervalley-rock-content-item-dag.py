from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from rock.rock_content_items import fetch_and_save_content_items, ContentItem
from rock.rock_media import fetch_and_save_media, fetch_and_save_channel_image, Media
from rock.rock_content_items_connections import (
    fetch_and_save_content_items_connections,
    set_content_item_parent_id,
)
from rock.rock_cover_image import fetch_and_save_cover_image
from rock.rock_content_item_categories import (
    fetch_and_save_content_item_categories,
    attach_content_item_categories,
)
from rock.rock_features import fetch_and_save_features
from rock.rock_deleted_content_items_dag import remove_deleted_content_items
from misc.hopestream import set_hopestream_urls
from river_valley_rock_feature_backfill import create_daily_rock_feature_backfill


start_date = datetime(2021, 8, 12)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def is_media_video(content_item, attribute):
    attribute_key = attribute["Key"]
    attribute_value = content_item["AttributeValues"][attribute_key]["Value"]
    return (
        [79, 80].count(attribute["FieldTypeId"]) == 1
        or (
            "video" in attribute_key.lower()
            and isinstance(attribute_value, str)
            and attribute_value.startswith("http")
        )
        or "hopestream" in attribute_key.lower()
    )


def is_media_audio(content_item, attribute):
    attribute_key = attribute["Key"]
    attribute_value = content_item["AttributeValues"][attribute_key]["Value"]
    return (
        [77, 78].count(attribute["FieldTypeId"]) == 1
        or (
            "audio" in attribute_key.lower()
            and isinstance(attribute_value, str)
            and attribute_value.startswith("http")
        )
        or "hopestream" in attribute_key.lower()
    )


class RivervalleyContentItem(ContentItem):
    def has_audio_or_video(self, item, attribute):
        print(is_media_audio(item, attribute) or is_media_video(item, attribute))
        return is_media_audio(item, attribute) or is_media_video(item, attribute)


class RivervalleyMedia(Media):
    def is_audio(self, content_item, attribute):
        return is_media_audio(content_item, attribute)

    def is_video(self, content_item, attribute):
        return is_media_video(content_item, attribute)


def create_rock_content_item_dag(church, start_date, schedule_interval, do_backfill):
    tags = [church, "content"]
    name = f"{church}_rock_content_item_dag"
    if do_backfill:
        tags.append("backfill")
        name = f"{church}_backfill_rock_content_item_dag"

    dag = DAG(
        name,
        start_date=start_date,
        max_active_runs=1,
        schedule_interval=schedule_interval,
        default_args=default_args,
        tags=tags,
    )

    with dag:
        base_items = PythonOperator(
            task_id="fetch_and_save_content_items",
            python_callable=fetch_and_save_content_items,  # make sure you don't include the () of the function
            op_kwargs={
                "client": church,
                "do_backfill": do_backfill,
                "klass": RivervalleyContentItem,
            },
        )

        connections = PythonOperator(
            task_id="fetch_and_save_content_items_connections",
            python_callable=fetch_and_save_content_items_connections,  # make sure you don't include the () of the function
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        media = PythonOperator(
            task_id="fetch_and_save_media",
            python_callable=fetch_and_save_media,  # make sure you don't include the () of the function
            op_kwargs={
                "client": church,
                "do_backfill": do_backfill,
                "klass": RivervalleyMedia,
            },
        )

        hopestream = PythonOperator(
            task_id="set_hopestream_urls",
            python_callable=set_hopestream_urls,  # make sure you don't include the () of the function
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        add_categories = PythonOperator(
            task_id="fetch_and_save_content_item_categories",
            python_callable=fetch_and_save_content_item_categories,  # make sure you don't include the () of the function
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        channel_image = PythonOperator(
            task_id="fetch_and_save_channel_image",
            python_callable=fetch_and_save_channel_image,  # make sure you don't include the () of the function
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        attach_categories = PythonOperator(
            task_id="attach_content_item_categories",
            python_callable=attach_content_item_categories,  # make sure you don't include the () of the function
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        set_parent_id = PythonOperator(
            task_id="set_parent_id",
            python_callable=set_content_item_parent_id,  # make sure you don't include the () of the function
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        set_cover_image = PythonOperator(
            task_id="fetch_and_save_cover_image",
            python_callable=fetch_and_save_cover_image,  # make sure you don't include the () of the function
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        features = PythonOperator(
            task_id="fetch_and_save_features",
            python_callable=fetch_and_save_features,  # make sure you don't include the () of the function
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        deleted_content_items = PythonOperator(
            task_id="remove_deleted_content_items",
            python_callable=remove_deleted_content_items,  # make sure you don't include the () of the function
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        # Adding and syncing categories depends on having content items
        base_items >> add_categories >> attach_categories >> channel_image

        add_categories >> attach_categories >> media

        media >> [hopestream, set_cover_image]

        connections >> set_parent_id >> features

        deleted_content_items

        base_items >> [connections, deleted_content_items, add_categories]

        channel_image >> set_cover_image

    return dag, name


backfill_dag, backfill_name = create_rock_content_item_dag(
    "rivervalley", datetime(2021, 1, 1), "@once", True
)
globals()[backfill_name] = backfill_dag

dag, dag_name = create_rock_content_item_dag(
    "rivervalley", start_date, timedelta(minutes=5), False
)

globals()[dag_name] = dag

feature_backfill_dag, feature_backfill_dag_name = create_daily_rock_feature_backfill(
    "rivervalley", start_date, "@daily", True
)

globals()[feature_backfill_dag_name] = feature_backfill_dag

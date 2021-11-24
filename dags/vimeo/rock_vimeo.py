from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
import requests
import pytz
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from rock.utilities import (
    find_supported_fields,
)


def get_delta_offset(kwargs, type):
    local_zone = pytz.timezone(
        Variable.get(kwargs["client"] + "_rock_tz", default_var="EST")
    )
    execution_date_string = (
        kwargs["execution_date"].astimezone(local_zone).strftime("%Y-%m-%dT%H:%M:%S")
    )
    return f"ModifiedDateTime ge datetime'{execution_date_string}' or ModifiedDateTime eq null or PersistedLastRefreshDateTime ge datetime'{execution_date_string}'"


class Vimeo:
    def __init__(self, kwargs):
        self.kwargs = kwargs
        self.rock_headers = {
            "Authorization-Token": Variable.get(kwargs["client"] + "_rock_token")
        }

        self.vimeo_headers = {
            "Authorization": f'Bearer {Variable.get(kwargs["client"] + "_vimeo_token")}'
        }

        self.pg_connection = kwargs["client"] + "_apollos_postgres"
        self.pg_hook = PostgresHook(
            postgres_conn_id=self.pg_connection,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )

        self.item_entity_id = requests.get(
            f"{Variable.get(kwargs['client'] + '_rock_api')}/EntityTypes",
            params={"$filter": "Name eq 'Rock.Model.ContentChannelItem'"},
            headers=self.rock_headers,
        ).json()[0]["Id"]

    def fetch_video_from_vimeo(self, attribute_value):
        video_details = {}
        try:
            video_details = requests.get(
                f"https://api.vimeo.com/videos/{attribute_value['Value']}",
                headers=self.vimeo_headers,
            ).json()

            return (
                self.identify_hls_url(video_details),
                attribute_value["EntityId"],
                attribute_value["Id"],
            )
        except BaseException as err:
            print(video_details)
            print(f"Unexpected {err}, {type(err)}, Attributes {attribute_value}")
            return None

    def identify_hls_url(self, video):
        file = next(
            (file for file in video["files"] if file["quality"] == "hls"),
            None,
        )
        return file["link"] if file else None

    def get_vimeo_urls(self, attribute_values):
        return list(
            filter(
                lambda url: bool(url),
                map(self.fetch_video_from_vimeo, attribute_values),
            )
        )

    def get_content_item(self, content_item_id):
        images = self.pg_hook.get_first(
            "select id from content_item as c where c.origin_id = %s and c.origin_type = 'rock' ;",
            (f"{content_item_id}",),
        )

        return images[0] if images else None

    def run_create_media_from_vimeo(self):

        # Attribute values that belong to fields that are dataviews (and are content items)
        vimeo_attribute_values = requests.get(
            f"{Variable.get(self.kwargs['client'] + '_rock_api')}/AttributeValues",
            params={
                "$filter": f"Attribute/EntityTypeId eq {self.item_entity_id} and Attribute/Key eq 'VimeoId'",
                "$select": "Value, EntityId, Id",
            },
            headers=self.rock_headers,
        ).json()

        videos_links = self.get_vimeo_urls(vimeo_attribute_values)

        media = [
            {
                "apollos_type": "Media",
                "created_at": self.kwargs["execution_date"],
                "updated_at": self.kwargs["execution_date"],
                "node_id": self.get_content_item(entity_id),
                "node_type": "ContentItem",
                "type": "VIDEO",
                "url": url,
                "origin_id": f"{entity_id}/{attribute_id}",
                "origin_type": "rock",
            }
            for url, entity_id, attribute_id in videos_links
        ]

        content_to_insert, columns, constraint = find_supported_fields(
            pg_hook=self.pg_hook,
            table_name="media",
            insert_data=list(media),
        )

        self.pg_hook.insert_rows(
            "media",
            content_to_insert,
            columns,
            0,
            True,
            replace_index=constraint,
        )

        add_apollos_ids = """
        UPDATE media
        SET apollos_id = apollos_type || ':' || id::varchar
        WHERE origin_type = 'rock' and apollos_id IS NULL
        """

        self.pg_hook.run(add_apollos_ids)


def fetch_and_save_media_vimeo_ids(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    Klass = Vimeo if "klass" not in kwargs else kwargs["klass"]  # noqa: N806
    vimeo_task = Klass(kwargs)

    vimeo_task.run_create_media_from_vimeo()


# Default settings applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def create_rock_vimeo_dag(church, start_date, schedule_interval, do_backfill):
    tags = [church, "vimeo"]
    name = f"{church}_rock_vimeo_dag"

    if do_backfill:
        tags.append("backfill")
        name = f"{church}_backfill_rock_vimeo_dag"

    dag = DAG(
        name,
        start_date=start_date,
        max_active_runs=1,
        schedule_interval=schedule_interval,
        default_args=default_args,
        tags=tags,
    )

    with dag:
        PythonOperator(
            task_id="fetch_and_save_media_vimeo_ids",
            python_callable=fetch_and_save_media_vimeo_ids,
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

    return dag, name

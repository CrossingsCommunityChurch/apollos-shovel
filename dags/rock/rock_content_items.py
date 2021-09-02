from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from html_sanitizer import Sanitizer
import nltk
from utilities import safeget, get_delta_offset, rock_timestamp_to_utc
from rock.rock_media import is_media_video, is_media_audio

import requests

nltk.download("punkt")


class ContentItem:
    summary_sanitizer = Sanitizer(
        {
            "tags": {"h1", "h2", "h3", "h4", "h5", "h6"},
            "empty": {},
            "separate": {},
            "attributes": {},
        }
    )

    html_allowed_tags = {
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "blockquote",
        "p",
        "a",
        "ul",
        "ol",
        "li",
        "b",
        "i",
        "strong",
        "em",
        "br",
        "caption",
        "img",
        "div",
    }

    html_sanitizer = Sanitizer(
        {
            "tags": html_allowed_tags,
            "empty": {},
            "seperate": {},
            "attributes": {
                **{
                    "a": {"href", "target", "rel"},
                    "img": {"src"},
                },
                **dict.fromkeys(html_allowed_tags, {"class", "style"}),
            },
        }
    )

    def __init__(self, kwargs):
        self.kwargs = kwargs
        self.config = Variable.get(
            kwargs["client"] + "_rock_config", deserialize_json=True
        )
        self.headers = {
            "Authorization-Token": Variable.get(kwargs["client"] + "_rock_token")
        }
        self.pg_connection = kwargs["client"] + "_apollos_postgres"
        self.pg_hook = PostgresHook(
            postgres_conn_id=self.pg_connection,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )

    # "created_at","updated_at", "origin_id", "origin_type", "apollos_type", "summary", "htmlContent", "title", "publish_at", "active"
    def map_content_to_columns(self, obj):
        return (
            self.kwargs["execution_date"],
            self.kwargs["execution_date"],
            obj["Id"],
            "rock",
            self.get_typename(obj, self.config),
            self.create_summary(obj),
            self.create_html_content(obj),
            obj["Title"],
            self.get_start_date(obj),
            self.get_status(obj),
        )

    def get_start_date(self, item):
        if not item["StartDateTime"]:
            return None
        return rock_timestamp_to_utc(item, self.kwargs)

    def create_summary(self, item):
        summary_value = safeget(item, "AttributeValues", "Summary", "Value")
        if summary_value and summary_value != "":
            return summary_value

        if not item["Content"]:
            return ""

        cleaned = self.summary_sanitizer.sanitize(item["Content"])
        sentences = nltk.sent_tokenize(cleaned)

        return sentences[0] if len(sentences) > 0 else ""

    def create_html_content(self, item):
        if not item["Content"]:
            return ""

        return self.html_sanitizer.sanitize(item["Content"])

    def has_audio_or_video(self, item, attribute):
        return is_media_audio(item, attribute) or is_media_video(item, attribute)

    def get_typename(self, item, config):
        mappings = safeget(config, "CONTENT_MAPPINGS")

        if mappings:
            types = mappings.keys()

            match_by_type_id = next(
                (
                    t
                    for t in types
                    if safeget(item, "ContentChannelTypeId")
                    in (safeget(mappings[t], "ContentChannelTypeId") or [])
                ),
                None,
            )

            if match_by_type_id:
                return match_by_type_id

            match_by_channel_id = next(
                (
                    t
                    for t in types
                    if safeget(item, "ContentChannelId")
                    in (safeget(mappings[t], "ContentChannelId") or [])
                ),
                None,
            )

            if match_by_channel_id:
                return match_by_channel_id

        is_media_item = (
            len(
                list(
                    filter(
                        lambda a: self.has_audio_or_video(item, a),
                        item["Attributes"].values(),
                    )
                )
            )
            > 0
        )
        print(is_media_item)
        if is_media_item:
            return "MediaContentItem"

        return "UniversalContentItem"

    def get_status(self, contentItem):
        if (
            contentItem["Status"] == 2
            or not contentItem["ContentChannel"]["RequiresApproval"]
        ):
            return True
        else:
            return False

    def run_fetch_and_save_content_items(self):

        fetched_all = False
        skip = 0
        top = 10000

        while not fetched_all:
            # Fetch people records from Rock.

            params = {
                "$top": top,
                "$skip": skip,
                "$expand": "ContentChannel",
                # "$select": "Id,Content",
                "loadAttributes": "expanded",
                # "attributeKeys": "Summary",
                "$orderby": "ModifiedDateTime desc",
            }

            if not self.kwargs["do_backfill"]:
                params["$filter"] = get_delta_offset(self.kwargs)

            print(params)

            rock_objects = requests.get(
                f"{Variable.get(self.kwargs['client'] + '_rock_api')}/ContentChannelItems",
                params=params,
                headers=self.headers,
            ).json()

            if not isinstance(rock_objects, list):
                print(rock_objects)
                print("oh uh, we might have made a bad request")
                print("top: {top}")
                print("skip: {skip}")
                skip += top
                continue

            skip += top
            fetched_all = len(rock_objects) < top

            content_to_insert = list(map(self.map_content_to_columns, rock_objects))
            columns = (
                "created_at",
                "updated_at",
                "origin_id",
                "origin_type",
                "apollos_type",
                "summary",
                "html_content",
                "title",
                "publish_at",
                "active",
            )

            self.pg_hook.insert_rows(
                '"content_item"',
                content_to_insert,
                columns,
                0,
                True,
                replace_index=('"origin_id"', '"origin_type"'),
            )

            add_apollos_ids = """
            UPDATE content_item
            SET apollos_id = apollos_type || ':' || id::varchar
            WHERE origin_type = 'rock' and apollos_id IS NULL
            """

            self.pg_hook.run(add_apollos_ids)


def fetch_and_save_content_items(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    Klass = ContentItem if "klass" not in kwargs else kwargs["klass"]

    content_item_task = Klass(kwargs)

    content_item_task.run_fetch_and_save_content_items()

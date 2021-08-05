from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from html_sanitizer import Sanitizer
import nltk
from utilities import safeget
from rock.rock_media import is_media_video, is_media_audio
from datetime import datetime

import requests

nltk.download("punkt")

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


def create_summary(item):
    summary_value = safeget(item, "AttributeValues", "Attribute", "Value")
    if summary_value and summary_value is not "":
        return summary_value

    if not item["Content"]:
        return ""

    cleaned = summary_sanitizer.sanitize(item["Content"])
    sentences = nltk.sent_tokenize(cleaned)

    return sentences[0] if len(sentences) > 0 else ""


def create_html_content(item):
    if not item["Content"]:
        return ""

    return html_sanitizer.sanitize(item["Content"])


def get_typename(item, config):
    mappings = safeget(config, "CONTENT_MAPPINGS")

    if not mappings:
        return "UniversalContentItem"

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

    def has_audio_or_video(attribute):
        return is_media_audio(attribute, item) or is_media_video(attribute, item)

    is_media_item = (
        len(list(filter(has_audio_or_video, item["Attributes"].values()))) > 0
    )
    if is_media_item:
        return "MediaContentItem"

    return "UniversalContentItem"


def get_status(contentItem):
    # The split strips the seconds off, which can cause issues when parsing.
    startDateTime = (
        datetime.fromisoformat(contentItem["StartDateTime"].split(".")[0])
        if contentItem["StartDateTime"]
        else None
    )
    expireDateTime = (
        datetime.fromisoformat(contentItem["ExpireDateTime"].split(".")[0])
        if contentItem["ExpireDateTime"]
        else None
    )

    if (
        (startDateTime == None or startDateTime < datetime.now())
        and (expireDateTime == None or expireDateTime > datetime.now())
    ) and (
        contentItem["Status"] == 2
        or contentItem["ContentChannel"]["RequiresApproval"] == False
    ):
        return True
    else:
        return False


def fetch_and_save_content_items(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    headers = {"Authorization-Token": Variable.get(kwargs["client"] + "_rock_token")}

    fetched_all = False
    skip = 0
    top = 10000

    pg_connection = kwargs["client"] + "_apollos_postgres"
    pg_hook = PostgresHook(
        postgres_conn_id=pg_connection,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )

    while fetched_all == False:
        # Fetch people records from Rock.

        params = {
            "$top": top,
            "$skip": skip,
            "$expand": "ContentChannel",
            # "$select": "Id,Content",
            "loadAttributes": "expanded",
            "attributeKeys": "Summary",
            "$orderby": "ModifiedDateTime desc",
        }

        if not kwargs["do_backfill"]:
            params[
                "$filter"
            ] = f"ModifiedDateTime ge datetime'{kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}' or ModifiedDateTime eq null"

        print(params)

        r = requests.get(
            f"{Variable.get(kwargs['client'] + '_rock_api')}/ContentChannelItems",
            params=params,
            headers=headers,
        )
        rock_objects = r.json()

        if not isinstance(rock_objects, list):
            print(rock_objects)
            print("oh uh, we might have made a bad request")
            print("top: {top}")
            print("skip: {skip}")
            skip += top
            continue

        skip += top
        fetched_all = len(rock_objects) < top

        config = Variable.get(kwargs["client"] + "_rock_config", deserialize_json=True)

        # "created_at","updated_at", "origin_id", "origin_type", "apollos_type", "summary", "htmlContent", "title", "publish_at", "active"
        def update_content(obj):
            return (
                kwargs["execution_date"],
                kwargs["execution_date"],
                obj["Id"],
                "rock",
                get_typename(obj, config),
                create_summary(obj),
                create_html_content(obj),
                obj["Title"],
                obj["StartDateTime"],
                get_status(obj),
            )

        def fix_casing(col):
            return '"{}"'.format(col)

        content_to_insert = list(map(update_content, rock_objects))
        columns = list(
            map(
                fix_casing,
                (
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
                ),
            )
        )

        pg_hook.insert_rows(
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

        pg_hook.run(add_apollos_ids)

from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from utilities import safeget
from PIL import ImageFile
import urllib
import json

import requests


def getsizes(uri):
    # get file size *and* image size (None if not known)
    try:
        file = urllib.request.urlopen(uri.replace(" ", ""))
        size = file.headers.get("content-length")
        if size:
            size = int(size)
        p = ImageFile.Parser()
        while 1:
            data = file.read(1024)
            if not data:
                break
            p.feed(data)
            if p.image:
                return p.image.size
                break
        file.close()
        return size
    except Exception as err:
        print("Failed to fetch image:")
        print(err)
        print(uri)
        return None


def is_media_image(attribute, contentItem):
    attributeKey = attribute["Key"]
    attributeValue = contentItem["AttributeValues"][attributeKey]["Value"]
    return attribute["FieldTypeId"] == 10 or (
        "image" in attributeKey.lower()
        and isinstance(attributeValue, str)
        and attributeValue.startswith("http")
    )


def is_media_video(attribute, contentItem):
    attributeKey = attribute["Key"]
    attributeValue = contentItem["AttributeValues"][attributeKey]["Value"]
    return (
        [79, 80].count(attribute["FieldTypeId"]) == 1
        or "video" in attributeKey.lower()
        and isinstance(attributeValue, str)
        and attributeValue.startswith("http")
    )


def is_media_audio(attribute, contentItem):
    attributeKey = attribute["Key"]
    attributeValue = contentItem["AttributeValues"][attributeKey]["Value"]
    return (
        [77, 78].count(attribute["FieldTypeId"]) == 1
        or "audio" in attributeKey.lower()
        and isinstance(attributeValue, str)
        and attributeValue.startswith("http")
    )


def fetch_and_save_media(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    headers = {"Authorization-Token": Variable.get(kwargs["client"] + "_rock_token")}

    pg_connection = kwargs["client"] + "_apollos_postgres"
    pg_hook = PostgresHook(
        postgres_conn_id=pg_connection,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )

    def default_parse_asset_url(value, media_type):
        if media_type == "IMAGE":
            rock_host = (Variable.get(kwargs["client"] + "_rock_api")).split("/api")[0]
            return rock_host + "/GetImage.ashx?guid=" + value if len(value) > 0 else ""
        else:
            return value

    # It's pretty common for churches to need custom functions to parse image urls.
    # For example, when using S3.
    parse_asset_url = safeget(kwargs, "parse_asset_url") or default_parse_asset_url
    is_image = safeget(kwargs, "is_image") or is_media_image
    is_video = safeget(kwargs, "is_video") or is_media_video
    is_audio = safeget(kwargs, "is_audio") or is_media_audio

    def get_content_item_id(rockOriginId):
        try:
            return pg_hook.get_first(
                f"SELECT id FROM content_item WHERE origin_id::Integer = {rockOriginId}"
            )[0]
        except:
            print("Item not found")
            print(rockOriginId)
            return None

    def mapContentItems(content_item):

        nodeId = get_content_item_id(content_item["Id"])
        if not nodeId:
            return None

        def filter_media_attributes(attribute):
            return (
                is_image(attribute, content_item)
                or is_video(attribute, content_item)
                or is_audio(attribute, content_item)
            )

        def get_media_type(attribute):
            if is_image(attribute, content_item):
                return "IMAGE"
            elif is_video(attribute, content_item):
                return "VIDEO"
            elif is_audio(attribute, content_item):
                return "AUDIO"
            else:
                return None

        def get_media_value(attribute):
            media_type = get_media_type(attribute)
            attribute_key = attribute["Key"]
            attribute_value = content_item["AttributeValues"][attribute_key]["Value"]
            asset_url = parse_asset_url(attribute_value, media_type)
            return asset_url

        def map_attributes(attribute):
            attribute_value_id = str(content_item["Id"]) + "/" + str(attribute["Id"])
            media_type = get_media_type(attribute)
            media_value = get_media_value(attribute)
            metadata = {}

            if media_type is None:
                return None

            if media_type == "IMAGE" and media_value:
                image_dimensions = getsizes(media_value)
                if image_dimensions:
                    try:
                        metadata["width"] = image_dimensions[0]
                        metadata["height"] = image_dimensions[1]
                    except:
                        print("Error getting media sizes")
                        print(image_dimensions)
                        print(attribute)
                        print(media_value)

            if media_value:
                return (
                    "Media",
                    kwargs["execution_date"],
                    kwargs["execution_date"],
                    nodeId,
                    "ContentItem",
                    media_type,
                    media_value,
                    attribute_value_id,
                    "rock",
                    json.dumps(metadata),
                )

            return None

        filteredAttributes = filter(
            filter_media_attributes, content_item["Attributes"].values()
        )
        mappedAttributes = map(map_attributes, filteredAttributes)

        return list(filter(lambda media: bool(media), mappedAttributes))

    fetched_all = False
    skip = 0
    top = 100

    while fetched_all == False:
        # Fetch people records from Rock.

        params = {
            "$top": top,
            "$skip": skip,
            # "$select": "Id,Content",
            "loadAttributes": "expanded",
            "$orderby": "ModifiedDateTime desc",
        }

        if not kwargs["do_backfill"]:
            params[
                "$filter"
            ] = f"ModifiedDateTime ge datetime'{kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}' or ModifiedDateTime eq null"

        rock_objects = requests.get(
            f"{Variable.get(kwargs['client'] + '_rock_api')}/ContentChannelItems",
            params=params,
            headers=headers,
        ).json()

        if not isinstance(rock_objects, list):
            print(rock_objects)
            print("oh uh, we might have made a bad request")
            print(f"top: {top}")
            print(f"skip: {skip}")
            skip += top
            continue

        def fix_casing(col):
            return '"{}"'.format(col)

        mediaAttributeLists = filter(
            lambda atr_list: bool(atr_list), list(map(mapContentItems, rock_objects))
        )
        mediaAttributes = [
            mediaAttribute
            for sublist in mediaAttributeLists
            for mediaAttribute in sublist
        ]
        columns = list(
            map(
                fix_casing,
                (
                    "apollos_type",
                    "created_at",
                    "updated_at",
                    "node_id",
                    "node_type",
                    "type",
                    "url",
                    "origin_id",
                    "origin_type",
                    "metadata",
                ),
            )
        )

        print("Media Items Added: ")
        print(len(list(mediaAttributes)))

        pg_hook.insert_rows(
            '"media"',
            list(mediaAttributes),
            columns,
            0,
            True,
            replace_index=('"origin_id"', '"origin_type"'),
        )

        add_apollos_ids = """
        UPDATE media
        SET apollos_id = apollos_type || ':' || id::varchar
        WHERE origin_type = 'rock' and apollos_id IS NULL
        """

        pg_hook.run(add_apollos_ids)

        skip += top
        fetched_all = len(rock_objects) < top

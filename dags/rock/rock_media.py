from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from PIL import ImageFile
from rock.utilities import (
    safeget,
    get_delta_offset,
    get_delta_offset_with_content_attributes,
)
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


# These are defined outside the class since we import them within other files.
def is_media_image(content_item, attribute):
    attribute_key = attribute["Key"]
    attribute_value = content_item["AttributeValues"][attribute_key]["Value"]
    return attribute["FieldTypeId"] == 10 or (
        "image" in attribute_key.lower()
        and isinstance(attribute_value, str)
        and attribute_value.startswith("http")
    )


def is_media_video(content_item, attribute):
    attribute_key = attribute["Key"]
    attribute_value = content_item["AttributeValues"][attribute_key]["Value"]
    return (
        [79, 80].count(attribute["FieldTypeId"]) == 1
        or "video" in attribute_key.lower()
        and isinstance(attribute_value, str)
        and attribute_value.startswith("http")
    )


def is_media_audio(content_item, attribute):
    attribute_key = attribute["Key"]
    attribute_value = content_item["AttributeValues"][attribute_key]["Value"]
    return (
        [77, 78].count(attribute["FieldTypeId"]) == 1
        or "audio" in attribute_key.lower()
        and isinstance(attribute_value, str)
        and attribute_value.startswith("http")
    )


class Media:
    def __init__(self, kwargs):
        self.kwargs = kwargs
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

    def parse_asset_url(self, value, media_type):
        if value and media_type == "IMAGE":
            rock_host = (Variable.get(self.kwargs["client"] + "_rock_api")).split(
                "/api"
            )[0]
            return rock_host + "/GetImage.ashx?guid=" + value if len(value) > 0 else ""
        else:
            return value

    def filter_media_attributes(self, content_item, attribute):
        return (
            self.is_image(content_item, attribute)
            or self.is_video(content_item, attribute)
            or self.is_audio(content_item, attribute)
        )

    def is_image(self, content_item, attribute):
        return is_media_image(content_item, attribute)

    def is_audio(self, content_item, attribute):
        return is_media_audio(content_item, attribute)

    def is_video(self, content_item, attribute):
        return is_media_video(content_item, attribute)

    def get_media_type(self, content_item, attribute):
        if self.is_image(content_item, attribute):
            return "IMAGE"
        elif self.is_video(content_item, attribute):
            return "VIDEO"
        elif self.is_audio(content_item, attribute):
            return "AUDIO"
        else:
            return None

    def get_media_value(self, content_item, attribute):
        media_type = self.get_media_type(content_item, attribute)
        attribute_key = attribute["Key"]
        attribute_value = content_item["AttributeValues"][attribute_key]["Value"]
        asset_url = self.parse_asset_url(attribute_value, media_type)
        return asset_url

    def map_attributes(self, content_item, attribute):
        node_id = content_item["node_id"]
        if not node_id:
            return None

        attribute_value_id = str(content_item["Id"]) + "/" + str(attribute["Id"])
        media_type = self.get_media_type(content_item, attribute)
        media_value = self.get_media_value(content_item, attribute)
        metadata = {}
        if media_type is None:
            return None

        if media_type == "IMAGE" and media_value:
            image_dimensions = getsizes(media_value)
            if image_dimensions:
                try:
                    metadata["width"] = image_dimensions[0]
                    metadata["height"] = image_dimensions[1]
                except:  # noqa E722
                    print("Error getting media sizes")
                    print(image_dimensions)
                    print(attribute)
                    print(media_value)

        if media_value:
            return (
                "Media",
                self.kwargs["execution_date"],
                self.kwargs["execution_date"],
                node_id,
                "ContentItem",
                media_type,
                media_value,
                attribute_value_id,
                "rock",
                json.dumps(metadata),
            )

        return None

    def get_channel_image_attribute_value(self, channel):
        attributes = safeget(channel, "Attributes")
        images = []
        for attribute in attributes.values():
            if is_media_image(channel, attribute):
                images.append(attribute)

        if len(images) > 0:
            image_key = images[0]["Key"]
            return channel["AttributeValues"][image_key]

    def reduce_media_from_content_item(self, content_item):
        filtered_attributes = filter(
            lambda a: self.filter_media_attributes(content_item, a),
            content_item["Attributes"].values(),
        )
        mapped_attributes = map(
            lambda a: self.map_attributes(content_item, a), filtered_attributes
        )

        return list(filter(lambda media: bool(media), mapped_attributes))

    def add_postgres_data_to_rock_media(self, media):
        if len(media) == 0:
            return []

        origin_ids = ", ".join(map(lambda r: f"'{str(r['Id'])}'", media))
        postgres_records = self.pg_hook.get_records(
            f"""
            SELECT content_item.id, content_item.origin_id
            FROM content_item
            WHERE content_item.origin_id in ({origin_ids})
            """
        )
        postgres_records_by_origin_id = {f[1]: f[0] for f in postgres_records}
        media_with_postgres_data = [
            {
                **media_item,
                "node_id": postgres_records_by_origin_id.get(str(media_item["Id"])),
            }
            for media_item in media
        ]
        return media_with_postgres_data

    def reduce_media_from_content_channel(self, channel):
        image_attribute_value = self.get_channel_image_attribute_value(channel)
        if image_attribute_value:
            image_url = self.parse_asset_url(
                safeget(image_attribute_value, "Value"), "IMAGE"
            )

            if not image_url:
                return None

            channel_id = channel["Id"]
            node_id = self.pg_hook.get_first(
                "SELECT id FROM content_item_category WHERE origin_id = %s",
                (f"{channel_id}",),
            )[0]
            attribute_id = safeget(image_attribute_value, "AttributeId")
            attribute_value_id = f"{channel['Id']}/{attribute_id}"

            metadata = {}
            image_dimensions = getsizes(image_url)
            if image_dimensions:
                try:
                    metadata["width"] = image_dimensions[0]
                    metadata["height"] = image_dimensions[1]
                except:  # noqa E722
                    print("Error getting media sizes")
                    print(image_dimensions)
                    print(image_attribute_value)
                    print(image_url)

            return (
                "Media",
                self.kwargs["execution_date"],
                self.kwargs["execution_date"],
                node_id,
                "ContentItemCategory",
                "IMAGE",
                image_url,
                attribute_value_id,
                "rock",
                json.dumps(metadata),
            )
        else:
            return None

    def run_fetch_and_save_channel_image(self):
        params = {
            "loadAttributes": "expanded",
            "$orderby": "ModifiedDateTime desc",
        }

        if not self.kwargs["do_backfill"]:
            params["$filter"] = get_delta_offset(self.kwargs)

        channels = requests.get(
            f"{Variable.get(self.kwargs['client'] + '_rock_api')}/ContentChannels",
            params=params,
            headers=self.headers,
        ).json()

        channels_with_images = map(
            self.reduce_media_from_content_channel,
            channels,
        )

        channels_with_image_values = filter(
            lambda channel: channel, channels_with_images
        )

        columns = (
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
        )

        self.pg_hook.insert_rows(
            "media",
            list(channels_with_image_values),
            columns,
            0,
            True,
            replace_index=("origin_id", "origin_type"),
        )

        add_apollos_ids = """
            UPDATE media
            SET apollos_id = apollos_type || ':' || id::varchar
            WHERE origin_type = 'rock' and apollos_id IS NULL
            """

        self.pg_hook.run(add_apollos_ids)

    def run_fetch_and_save_media(self):
        fetched_all = False
        skip = 0
        top = 100

        retry_count = 0

        while not fetched_all:
            # Fetch people records from Rock.

            params = {
                "$top": top,
                "$skip": skip,
                # "$select": "Id,Content",
                "loadAttributes": "expanded",
                "$orderby": "ModifiedDateTime desc",
            }

            if not self.kwargs["do_backfill"]:
                params["$filter"] = get_delta_offset_with_content_attributes(
                    self.kwargs
                )

            rock_objects = requests.get(
                f"{Variable.get(self.kwargs['client'] + '_rock_api')}/ContentChannelItems",
                params=params,
                headers=self.headers,
            ).json()

            if not isinstance(rock_objects, list):
                print(rock_objects)
                print("oh uh, we might have made a bad request")
                print(f"top: {top}")
                print(f"skip: {skip}")
                print(f"params: {params}")

                if retry_count >= 3:
                    raise Exception(f"Rock Error: {rock_objects}")

                skip += top
                continue

            media_with_postgres_id = self.add_postgres_data_to_rock_media(rock_objects)
            media_attribute_lists = filter(
                lambda atr_list: bool(atr_list),
                list(map(self.reduce_media_from_content_item, media_with_postgres_id)),
            )
            media_attributes = [
                media_attribute
                for sublist in media_attribute_lists
                for media_attribute in sublist
            ]
            columns = (
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
            )

            print("Media Items Added: ")

            self.pg_hook.insert_rows(
                '"media"',
                list(media_attributes),
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

            self.pg_hook.run(add_apollos_ids)

            skip += top
            fetched_all = len(rock_objects) < top


def fetch_and_save_media(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")
    Klass = Media if "klass" not in kwargs else kwargs["klass"]

    media_task = Klass(kwargs)

    media_task.run_fetch_and_save_media()
    # It's pretty common for churches to need custom functions to parse image urls.
    # For example, when using S3.


def fetch_and_save_channel_image(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    Klass = Media if "klass" not in kwargs else kwargs["klass"]

    media_task = Klass(kwargs)

    media_task.run_fetch_and_save_channel_image()

from airflow.hooks.postgres_hook import PostgresHook
import markdown
import json
import re
from contentful_crossroads.cr_contentful_client import ContentfulClient

from rock.utilities import (
    find_supported_fields,
)


accepted_distribution_channels = ["www.crossroads.net"]
kids_club_category_id = "24jij4G5LUqBaM0c9jBsuW"


class ContentItem:
    def __init__(self, kwargs):
        self.kwargs = kwargs
        self.pg_connection = kwargs["client"] + "_apollos_postgres"
        self.pg_hook = PostgresHook(
            postgres_conn_id=self.pg_connection,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )

    def update_assets_with_ids(self, entries):
        for entry in entries:
            contentful_entry_id = entry.sys["id"]
            pg_entry_id = self.pg_hook.get_first(
                f"SELECT id FROM content_item WHERE origin_id = '{contentful_entry_id}'"
            )[0]
            try:
                fields = self.get_entry_fields(entry)
                asset_id = vars(fields["image"])["sys"]["id"]

                # update the asset in the media table to link it to the content_item
                add_node_ids = f"""
                UPDATE media
                SET node_id = '{pg_entry_id}', node_type='ContentItem'
                WHERE origin_id = '{asset_id}'
                """
                self.pg_hook.run(add_node_ids)

                # update the content_item with the media id
                pg_asset_id = pg_entry_id = self.pg_hook.get_first(
                    f"SELECT id FROM media WHERE origin_id = '{asset_id}'"
                )[0]
                add_asset_ids = f"""
                UPDATE content_item
                SET cover_image_id = '{pg_asset_id}'
                WHERE origin_id = '{contentful_entry_id}'
                """
                self.pg_hook.run(add_asset_ids)
            except AttributeError:
                print("CANT GET AN ASSET. THATS BAD.")

    def get_kids_club_items_only(self, items):
        def should_save_item(entry):
            distribution_channel_check = self.check_distribution_channels(entry)
            is_kids_club = self.check_is_kids_club(entry)
            return distribution_channel_check and is_kids_club

        return list(filter(should_save_item, items))

    def check_distribution_channels(self, entry):
        try:
            fields = self.get_entry_fields(entry)
            distribution_channels = list(
                map(lambda item: item["site"], fields["distribution_channels"])
            )
            return any(
                item in distribution_channels for item in accepted_distribution_channels
            )
        except KeyError:
            return False

    def check_is_kids_club(self, entry):
        fields = self.get_entry_fields(entry)
        category = None
        try:
            category = fields["category"]
            category_id = vars(category)["sys"]["id"]
            return category_id == kids_club_category_id
        except KeyError:
            return False

    def get_entry_fields(self, entry):
        return entry._fields[self.kwargs["localization"]]

    def get_title(self, entry):
        fields = self.get_entry_fields(entry)
        title = fields["title"]
        split_title = title.split("(")
        return split_title[0].strip()

    def map_content_to_columns(self, entry):
        fields = self.get_entry_fields(entry)
        sys = entry.sys
        return {
            "created_at": self.kwargs["execution_date"],
            "updated_at": self.kwargs["execution_date"],
            "origin_id": sys["id"],
            "origin_type": "contentful",
            "apollos_type": "MediaContentItem",
            "summary": None,
            "html_content": markdown.markdown(fields["description"]),
            "title": self.get_title(entry),
            "publish_at": entry.published_at,
            "active": bool(entry.published_at),
            "expire_at": None,
        }

    def map_media_to_columns(self, entry):
        origin_id = entry.sys["id"]
        pg_entry_id = self.pg_hook.get_first(
            f"SELECT id FROM content_item WHERE origin_id = '{origin_id}'"
        )[0]
        # Getting the video url is a big ol' TBD.
        # We don't have streaming links for the video files yet.
        # In the meantime, we'll use a placeholder, the same for all videos.
        # fields = self.get_entry_fields(entry)
        # video_url = fields['file']['url']
        video_url = "https://player.vimeo.com/external/522397154.m3u8?s=288eb6e0d740e0b2344b7d003b59f1b4245dfde3"
        fields = self.get_entry_fields(entry)
        metadata = {}
        metadata["name"] = fields["title"]
        return {
            "apollos_type": "Media",
            "created_at": self.kwargs["execution_date"],
            "updated_at": self.kwargs["execution_date"],
            "node_id": pg_entry_id,
            "node_type": "ContentItem",
            "type": "VIDEO",
            "url": video_url,
            "origin_id": origin_id,
            "origin_type": "contentful",
            "metadata": json.dumps(metadata),
        }

    def save_video_url_as_media(self, entries):
        insert_data = list(map(self.map_media_to_columns, entries))

        content_to_insert, columns, constraint = find_supported_fields(
            pg_hook=self.pg_hook,
            table_name="media",
            insert_data=insert_data,
        )

        self.pg_hook.insert_rows(
            "media",
            content_to_insert,
            columns,
            0,
            True,
            replace_index=constraint,
        )

    def get_category_id(self, entry):
        # This is NOT the right way to do this. Since the older/younger categories
        #   don't exist in contentful, the only way to identify what category a video
        #   belongs in is by parsing the title. Better than nothing.
        fields = self.get_entry_fields(entry)
        title = fields["title"]
        younger_match = re.search(r"younger", title, re.IGNORECASE)
        pg_entry_id = None
        if younger_match:
            pg_entry_id = self.pg_hook.get_first(
                "SELECT id FROM content_item_category WHERE origin_id = 'younger-kids'"
            )[0]
        else:
            pg_entry_id = self.pg_hook.get_first(
                "SELECT id FROM content_item_category WHERE origin_id = 'older-kids'"
            )[0]
        return pg_entry_id

    def save_categories(self, entries):
        for entry in entries:
            origin_id = entry.sys["id"]
            category_id = self.get_category_id(entry)
            print("CATEGORY ID IS: ", category_id, origin_id)
            if category_id:
                add_content_item_category = f"""
                UPDATE content_item
                SET content_item_category_id = '{category_id}'
                WHERE origin_id = '{origin_id}'
                """
                self.pg_hook.run(add_content_item_category)

    def delete_items_not_synced(self, synced_ids):
        if self.kwargs["do_backfill"]:
            synced_ids_string = "', '".join(synced_ids)
            delete_not_synced_content_items = f"""
            DELETE FROM content_item
            WHERE origin_id NOT IN ('{synced_ids_string}')
            """
            self.pg_hook.run(delete_not_synced_content_items)

    def run_fetch_and_save_content_items(self):
        fetched_all = False
        skip = 0
        limit = 100
        next_sync_token = False
        all_contentful_ids = []

        while not fetched_all:
            if next_sync_token:
                sync = ContentfulClient.sync({"sync_token": next_sync_token})
            else:
                opts = {
                    "initial": self.kwargs["do_backfill"],
                    "limit": limit,
                    **self.kwargs["contentful_filters"],
                }
                sync = ContentfulClient.sync(opts)

            items_to_save = self.get_kids_club_items_only(sync.items)
            items_to_save_ids = list(map(lambda item: item.sys["id"], items_to_save))
            all_contentful_ids += items_to_save_ids

            insert_data = list(map(self.map_content_to_columns, items_to_save))

            content_to_insert, columns, constraint = find_supported_fields(
                pg_hook=self.pg_hook,
                table_name="content_item",
                insert_data=insert_data,
            )

            self.pg_hook.insert_rows(
                "content_item",
                content_to_insert,
                columns,
                0,
                True,
                replace_index=constraint,
            )

            add_apollos_ids = """
            UPDATE content_item
            SET apollos_id = apollos_type || ':' || id::varchar
            WHERE origin_type = 'contentful' and apollos_id IS NULL
            """

            self.pg_hook.run(add_apollos_ids)

            next_sync_token = sync.next_sync_token
            skip += limit
            self.update_assets_with_ids(items_to_save)
            self.save_video_url_as_media(items_to_save)
            self.save_categories(items_to_save)
            fetched_all = len(sync.items) < limit

        self.delete_items_not_synced(all_contentful_ids)


def fetch_and_save_content_items(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    Klass = ContentItem if "klass" not in kwargs else kwargs["klass"]  # noqa N806

    content_item_task = Klass(kwargs)

    content_item_task.run_fetch_and_save_content_items()

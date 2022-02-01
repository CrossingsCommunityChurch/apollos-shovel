from airflow.hooks.postgres_hook import PostgresHook
import markdown
import json
from contentful_crossroads.cr_contentful_client import ContentfulClient
from contentful_crossroads.cr_redis import RedisClient
from rock.utilities import (
    find_supported_fields,
)


class ContentItem:
    def __init__(self, kwargs):
        self.kwargs = kwargs
        self.pg_connection = kwargs["client"] + "_apollos_postgres"
        self.redis = RedisClient(client=kwargs["client"])
        self.pg_hook = PostgresHook(
            postgres_conn_id=self.pg_connection,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )

    def get_asset_url(self, asset):
        try:
            return asset.file["url"]
        except AttributeError:
            print("Can't get file url for asset:", asset.id)
            print(asset.file)
        return None

    def get_image_metadata(self, asset):
        metadata = {}
        try:
            metadata["width"] = asset.file["details"]["image"]["width"]
            metadata["height"] = asset.file["details"]["image"]["height"]
        except (KeyError, AttributeError):  # noqa E722
            print("Error getting media sizes:", asset.id, asset.file)
        return metadata

    def map_asset_to_columns(self, asset):
        return {
            "apollos_type": "Media",
            "created_at": self.kwargs["execution_date"],
            "updated_at": self.kwargs["execution_date"],
            "node_id": None,
            "node_type": None,
            "type": "IMAGE",
            "url": self.get_asset_url(asset),
            "origin_id": asset.id,
            "origin_type": "contentful",
            "metadata": json.dumps(self.get_image_metadata(asset)),
        }

    def save_asset_to_postgres(self, asset):
        insert_data = list(map(self.map_asset_to_columns, [asset]))
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

    def save_assets(self, entries):
        for entry in entries:
            pg_entry_id = self.pg_hook.get_first(
                f"SELECT id FROM content_item WHERE origin_id = '{entry.id}'"
            )[0]
            asset = entry.image.resolve(ContentfulClient)
            self.save_asset_to_postgres(asset)
            # update the asset in the media table to link it to the content_item
            add_node_ids = f"""
            UPDATE media
            SET node_id = '{pg_entry_id}', node_type='ContentItem'
            WHERE origin_id = '{asset.id}'
            """
            self.pg_hook.run(add_node_ids)
            # update the content_item with the media id
            pg_asset_id = pg_entry_id = self.pg_hook.get_first(
                f"SELECT id FROM media WHERE origin_id = '{asset.id}'"
            )[0]
            add_asset_ids = f"""
            UPDATE content_item
            SET cover_image_id = '{pg_asset_id}'
            WHERE origin_id = '{entry.id}'
            """
            self.pg_hook.run(add_asset_ids)

    def get_title(self, entry):
        return entry.title

    def get_video_url(self, entry):
        try:
            return entry.file["url"]
        except AttributeError:
            pass

    def map_content_to_columns(self, entry):
        return {
            "created_at": self.kwargs["execution_date"],
            "updated_at": self.kwargs["execution_date"],
            "origin_id": entry.id,
            "origin_type": "contentful",
            "apollos_type": "MediaContentItem",
            "summary": None,
            "html_content": markdown.markdown(entry.description),
            "title": self.get_title(entry),
            "publish_at": entry.published_at,
            "active": bool(entry.published_at),
            "expire_at": None,
        }

    def map_media_to_columns(self, entry):
        origin_id = entry.id
        pg_entry_id = self.pg_hook.get_first(
            f"SELECT id FROM content_item WHERE origin_id = '{origin_id}'"
        )[0]
        metadata = {}
        metadata["name"] = entry.title
        return {
            "apollos_type": "Media",
            "created_at": self.kwargs["execution_date"],
            "updated_at": self.kwargs["execution_date"],
            "node_id": pg_entry_id,
            "node_type": "ContentItem",
            "type": "VIDEO",
            "url": self.get_video_url(entry),
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
        pg_entry_id = None
        try:
            pg_entry_id = self.pg_hook.get_first(
                f"SELECT id FROM content_item_category WHERE origin_id = {entry.category.id}"
            )[0]
        except (TypeError, AttributeError):
            pass
        return pg_entry_id

    def save_categories(self, entries):
        for entry in entries:
            category_id = self.get_category_id(entry)
            if category_id:
                add_content_item_category = f"""
                UPDATE content_item
                SET content_item_category_id = '{category_id}'
                WHERE origin_id = '{entry.id}'
                """
                self.pg_hook.run(add_content_item_category)

    def delete_items_not_synced(self, synced_ids):
        if self.kwargs["do_backfill"]:
            synced_ids_string = "', '".join(synced_ids)
            delete_not_synced_entries = f"""
            DELETE FROM content_item
            WHERE origin_id NOT IN ('{synced_ids_string}')
            """
            self.pg_hook.run(delete_not_synced_entries)

    def save_items_to_postgres(self, items):
        print("Count of items to save to database:", len(items))
        insert_data = list(map(self.map_content_to_columns, items))
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

    # def save_assets_to_postgres(self, assets):
    #     asset_class = Asset(self.kwargs)
    #     asset_class.save_assets_to_postgres(assets)

    def remove_deleted_entries(self, entries):
        deleted_origin_ids = list(map(lambda entry: entry.id, entries))
        deleted_ids_string = "', '".join(deleted_origin_ids)
        delete_entries = f"""
        DELETE FROM content_item
        WHERE origin_id IN ('{deleted_ids_string}')
        """
        self.pg_hook.run(delete_entries)

    def save_sync_token(self, next_sync_token):
        self.redis.save_sync_token("entry", next_sync_token)

    def get_sync_token(self):
        return self.redis.get_sync_token("entry")

    def add_apollos_ids(self):
        add_entry_apollos_ids = """
        UPDATE content_item
        SET apollos_id = apollos_type || ':' || id::varchar
        WHERE origin_type = 'contentful' and apollos_id IS NULL
        """
        self.pg_hook.run(add_entry_apollos_ids)

        add_asset_apollos_ids = """
        UPDATE media
        SET apollos_id = apollos_type || ':' || id::varchar
        WHERE origin_type = 'contentful' and apollos_id IS NULL
        """

        self.pg_hook.run(add_asset_apollos_ids)

    def run_sync(self):
        sync = ContentfulClient.sync({"sync_token": self.get_sync_token()})
        new_entries = list(sync.items)
        print("New Entries: ", len(new_entries))

        entries_to_save = self.get_items_to_save(new_entries)
        self.save_items_to_postgres(entries_to_save)
        self.save_assets(entries_to_save)
        self.save_video_url_as_media(entries_to_save)
        self.save_categories(entries_to_save)
        self.save_sync_token(sync.next_sync_token)
        self.add_apollos_ids()

    def run_backfill_entries(self):
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
                    "initial": True,
                    "limit": limit,
                    "type": "Entry",
                    **self.kwargs["contentful_filters"],
                }
                sync = ContentfulClient.sync(opts)

            items_to_save = self.get_items_to_save(sync.items)
            items_to_save_ids = list(map(lambda item: item.id, items_to_save))
            all_contentful_ids += items_to_save_ids
            print(
                f"\n\n--------------------\n\tGetting Entries! {skip + len(sync.items)}\n--------------------\n\n"
            )
            self.save_items_to_postgres(items_to_save)
            next_sync_token = sync.next_sync_token
            skip += limit
            self.save_assets(items_to_save)
            self.save_video_url_as_media(items_to_save)
            self.save_categories(items_to_save)
            self.save_sync_token(next_sync_token)
            fetched_all = len(sync.items) < limit
        self.add_apollos_ids()
        self.delete_items_not_synced(all_contentful_ids)

    def run_fetch_and_save_entries(self):
        sync_token = self.get_sync_token()
        do_backfill = self.kwargs["do_backfill"]
        if do_backfill or (not do_backfill and not sync_token):
            print("Running Contentful full sync for Entries...")
            self.run_backfill_entries()
        else:
            print("Running Contentful delta sync for Entries...")
            self.run_sync()


def fetch_and_save_entries(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")
    Klass = ContentItem if "klass" not in kwargs else kwargs["klass"]  # noqa N806
    content_item_task = Klass(kwargs)
    content_item_task.run_fetch_and_save_entries()

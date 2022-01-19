import json
from airflow.hooks.postgres_hook import PostgresHook

from contentful_crossroads.cr_contentful_client import ContentfulClient
from rock.utilities import find_supported_fields


# Only used for backfilling assets. During a delta sync, assets are included.
class Asset:
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
        self.assets = {}
        self.localization = kwargs["localization"]

    def get_asset_url(self, asset):
        return asset._fields[self.localization]["file"]["url"]

    def get_image_metadata(self, asset):
        metadata = {}
        try:
            metadata["width"] = asset._fields[self.localization]["file"]["details"][
                "image"
            ]["width"]
            metadata["height"] = asset._fields[self.localization]["file"]["details"][
                "image"
            ]["height"]
        except:  # noqa E722
            print("Error getting media sizes")
            print(asset.sys["id"])
        return metadata

    def get_count(self):
        return len(self.assets)

    def map_content_to_columns(self, asset):
        contentful_asset_id = asset.sys["id"]

        return {
            "apollos_type": "Media",
            "created_at": self.kwargs["execution_date"],
            "updated_at": self.kwargs["execution_date"],
            "node_id": None,
            "node_type": None,
            "type": "IMAGE",
            "url": self.get_asset_url(asset),
            "origin_id": contentful_asset_id,
            "origin_type": "contentful",
            "metadata": json.dumps(self.get_image_metadata(asset)),
        }

    def remove_unused_assets(self):
        delete_unused_assets = """
            DELETE FROM media
            WHERE node_id IS NULL
            """
        self.pg_hook.run(delete_unused_assets)

    def save_assets_to_postgres(self, assets):
        insert_data = list(map(self.map_content_to_columns, assets))
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

        add_apollos_ids = """
        UPDATE media
        SET apollos_id = apollos_type || ':' || id::varchar
        WHERE origin_type = 'contentful' and apollos_id IS NULL
        """

        self.pg_hook.run(add_apollos_ids)

    def run_fetch_and_save_assets(self):
        fetched_all = False
        skip = 0
        limit = 100
        next_sync_token = False

        while not fetched_all:
            print("Syncing Assets!", skip, next_sync_token)
            if next_sync_token:
                sync = ContentfulClient.sync({"sync_token": next_sync_token})
            else:
                opts = {
                    "initial": self.kwargs["do_backfill"],
                    "limit": limit,
                    "type": "Asset",
                }
                sync = ContentfulClient.sync(opts)

            next_sync_token = sync.next_sync_token
            skip += limit

            fetched_all = len(sync.items) < limit


def fetch_and_save_assets(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    Klass = Asset if "klass" not in kwargs else kwargs["klass"]  # noqa N806

    asset_task = Klass(kwargs)

    asset_task.run_fetch_and_save_assets()


def remove_unused_assets(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    Klass = Asset if "klass" not in kwargs else kwargs["klass"]  # noqa N806

    asset_task = Klass(kwargs)

    asset_task.remove_unused_assets()

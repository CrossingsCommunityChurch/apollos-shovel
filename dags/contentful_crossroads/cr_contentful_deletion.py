from airflow.hooks.postgres_hook import PostgresHook
from contentful_crossroads.cr_contentful_client import ContentfulClient
from contentful_crossroads.cr_redis import RedisClient
from pprint import pprint


accepted_distribution_channels = ["www.crossroads.net"]
kids_club_category_id = "24jij4G5LUqBaM0c9jBsuW"


class DeletionSubscription:
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

    def remove_deleted_entries(self, entries):
        deleted_origin_ids = list(map(lambda entry: entry.id, entries))
        deleted_ids_string = "', '".join(deleted_origin_ids)
        delete_entries = f"""
        DELETE FROM content_item
        WHERE origin_id IN ('{deleted_ids_string}')
        """
        self.pg_hook.run(delete_entries)

    def get_sync_token(self):
        return self.redis.get_sync_token("deletion")

    def save_sync_token(self, next_sync_token):
        self.redis.save_sync_token("deletion", next_sync_token)

    def run_sync(self):
        sync = ContentfulClient.sync({"sync_token": self.get_sync_token()})
        self.remove_deleted_entries(sync.items)
        self.save_sync_token(sync.next_sync_token)

    def run_backfill_items(self):
        # for a backfill, only
        fetched_all = False
        skip = 0
        limit = 100
        next_sync_token = False

        while not fetched_all:
            if next_sync_token:
                sync = ContentfulClient.sync({"sync_token": next_sync_token})
            else:
                opts = {
                    "initial": True,
                    "limit": limit,
                    "type": "Deletion",
                }
                sync = ContentfulClient.sync(opts)
            pprint(sync.items)
            print(
                f"\n\n--------------------\n\tGetting Deleted Items! {skip + len(sync.items)}\n--------------------\n\n"
            )
            self.remove_deleted_entries(sync.items)
            self.save_sync_token(sync.next_sync_token)
            next_sync_token = sync.next_sync_token
            skip += limit
            fetched_all = len(sync.items) < limit

    def run_fetch_and_delete_items(self):
        sync_token = self.get_sync_token()
        do_backfill = self.kwargs["do_backfill"]
        if do_backfill or (not do_backfill and not sync_token):
            print("Running Contentful full sync for Deletion...")
            self.run_backfill_items()
        else:
            print("Running Contentful delta sync for Deletion...")
            self.run_sync()


def fetch_and_delete_items(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")
    Klass = (  # noqa N806
        DeletionSubscription if "klass" not in kwargs else kwargs["klass"]
    )
    deletion_task = Klass(kwargs)
    deletion_task.run_fetch_and_delete_items()

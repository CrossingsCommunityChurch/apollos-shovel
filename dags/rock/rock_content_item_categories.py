from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from utilities import safeget

import requests


class ContentItemCategory:
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

    def map_content_channel_to_category(self, obj):
        return (
            self.kwargs["execution_date"],
            self.kwargs["execution_date"],
            obj["Id"],
            "rock",
            "ContentChannel",
            obj["Name"],
        )

    def set_content_item_category_query(self, obj):
        return """
            UPDATE content_item
            SET content_item_category_id = (SELECT id FROM content_item_category WHERE origin_id = '{}')
            WHERE origin_id = '{}';
            """.format(
            str(safeget(obj, "ContentChannel", "Id")), str(obj["Id"])
        )

    def run_attach_content_item_categories(self):
        fetched_all = False

        skip = 0
        top = 10000

        while fetched_all == False:
            # Fetch people records from Rock.

            params = {
                "$top": top,
                "$skip": skip,
                "$expand": "ContentChannel",
                "$select": "Id,ContentChannel/Id",
                "$orderby": "ModifiedDateTime desc",
            }

            if not self.kwargs["do_backfill"]:
                params[
                    "$filter"
                ] = f"ModifiedDateTime ge datetime'{self.kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}' or ModifiedDateTime eq null"

            print(params)

            r = requests.get(
                f"{Variable.get(self.kwargs['client'] + '_rock_api')}/ContentChannelItems",
                params=params,
                headers=self.headers,
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

            self.pg_hook.run(
                list(map(self.set_content_item_category_query, rock_objects))
            )

    def run_fetch_and_save_content_item_categories(self):
        fetched_all = False
        skip = 0
        top = 10000

        while fetched_all == False:
            # Fetch people records from Rock.

            params = {
                "$top": top,
                "$skip": skip,
                # "$expand": "Photo",
                "$select": "Id,Name",
                "$orderby": "ModifiedDateTime desc",
            }

            if not self.kwargs["do_backfill"]:
                params[
                    "$filter"
                ] = f"ModifiedDateTime ge datetime'{self.kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}' or ModifiedDateTime eq null"

            print(params)

            r = requests.get(
                f"{Variable.get(self.kwargs['client'] + '_rock_api')}/ContentChannels",
                params=params,
                headers=self.headers,
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

            # "create_aAt","updated_at", "origin_id", "origin_type", "apollos_type", "title"

            content_to_insert = list(
                map(self.map_content_channel_to_category, rock_objects)
            )

            columns = (
                "created_at",
                "updated_at",
                "origin_id",
                "origin_type",
                "apollos_type",
                "title",
            )

            self.pg_hook.insert_rows(
                '"content_item_category"',
                content_to_insert,
                columns,
                0,
                True,
                replace_index=('"origin_id"', '"origin_type"'),
            )

            add_apollos_ids = """
            UPDATE content_item_category
            SET apollos_id = apollos_type || id::varchar
            WHERE origin_type = 'rock' and apollos_id IS NULL
            """

            self.pg_hook.run(add_apollos_ids)


def fetch_and_save_content_item_categories(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    Klass = ContentItemCategory if "klass" not in kwargs else kwargs["klass"]

    category_task = Klass(kwargs)

    category_task.run_fetch_and_save_content_item_categories()


def attach_content_item_categories(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")
    Klass = ContentItemCategory if "klass" not in kwargs else kwargs["klass"]

    category_task = Klass(kwargs)

    category_task.run_attach_content_item_categories()

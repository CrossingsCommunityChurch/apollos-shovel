from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from utilities import get_delta_offset
import requests


class ContentItemConnection:
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

    def get_child_and_parent(self, child_id, parent_id):
        child, parent = self.pg_hook.get_records(
            """
            SELECT id, origin_id
            FROM content_item
            JOIN unnest('{{{child_id},{parent_id}}}'::text[]) WITH ORDINALITY t(origin_id, ord) USING (origin_id)
            WHERE origin_id = '{child_id}' or origin_id = '{parent_id}'
            ORDER BY t.ord
        """.format(
                child_id=child_id, parent_id=parent_id
            )
        )
        return (child[0], parent[0])

    def map_rock_connection_to_postgres_connection(self, obj):
        child_id, parent_id = self.get_child_and_parent(
            obj["ChildContentChannelItemId"], obj["ContentChannelItemId"]
        )
        return (
            self.kwargs["execution_date"],
            self.kwargs["execution_date"],
            obj["Id"],
            "rock",
            "ContentItemsConnection",
            child_id,
            parent_id,
            obj["Order"],
        )

    def run_fetch_and_save_content_items_connections(self):
        fetched_all = False
        skip = 0
        top = 10000

        while not fetched_all:
            # Fetch people records from Rock.

            params = {
                "$top": top,
                "$skip": skip,
                # "$expand": "Photo",
                "$select": "Id,ChildContentChannelItemId,ContentChannelItemId,Order",
                "$orderby": "ModifiedDateTime desc",
            }

            if not self.kwargs["do_backfill"]:
                params["$filter"] = get_delta_offset(self.kwargs)

            print(params)

            r = requests.get(
                f"{Variable.get(self.kwargs['client'] + '_rock_api')}/ContentChannelItemAssociations",
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

            # "created_at","updated_at", "origin_id", "origin_type", "apollos_type", "child_id", "parent_id", "order"

            content_to_insert = list(
                map(self.map_rock_connection_to_postgres_connection, rock_objects)
            )
            columns = (
                "created_at",
                "updated_at",
                "origin_id",
                "origin_type",
                "apollos_type",
                "child_id",
                "parent_id",
                '"order"',
            )

            self.pg_hook.insert_rows(
                '"content_item_connection"',
                content_to_insert,
                columns,
                0,
                True,
                replace_index=("origin_id", "origin_type"),
            )

            add_apollos_ids = """
            UPDATE content_item_category
            SET apollos_id = apollos_type || id::varchar
            WHERE origin_type = 'rock' and apollos_id IS NULL
            """

            self.pg_hook.run(add_apollos_ids)

    def run_set_content_item_parent_id(self):
        rock_config = Variable.get(
            self.kwargs["client"] + "_rock_config", deserialize_json=True
        )
        series_parent_category_ids = ", ".join(
            map(lambda id: f"'{id}'", rock_config["SERIES_CATEGORY_ORIGIN_IDS"])
        )

        add_apollos_parents = f"""
        WITH rows_to_update AS
          (SELECT content_item_connection.parent_id,
                  content_item_connection.child_id AS id
           FROM
             (SELECT c.id,
              count(cc.child_id) AS parents_count
              FROM content_item c
              LEFT JOIN content_item_connection cc ON c.id = cc.child_id
              LEFT JOIN content_item p ON p.id = cc.parent_id
              LEFT JOIN content_item_category p_cat ON p.content_item_category_id = p_cat.id
              WHERE p_cat.origin_id IN ({series_parent_category_ids})
              GROUP BY c.id) AS items_and_parents
           INNER JOIN content_item_connection ON child_id = items_and_parents.id
           WHERE parents_count = 1)
        UPDATE content_item
        SET parent_id = rows_to_update.parent_id
        FROM rows_to_update
        WHERE content_item.id = rows_to_update.id;
        """

        self.pg_hook.run(add_apollos_parents)


def fetch_and_save_content_items_connections(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")
    Klass = ContentItemConnection if "klass" not in kwargs else kwargs["klass"]

    content_item_connection_task = Klass(kwargs)

    content_item_connection_task.run_fetch_and_save_content_items_connections()


def set_content_item_parent_id(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")
    Klass = ContentItemConnection if "klass" not in kwargs else kwargs["klass"]

    content_item_connection_task = Klass(kwargs)

    content_item_connection_task.run_set_content_item_parent_id()

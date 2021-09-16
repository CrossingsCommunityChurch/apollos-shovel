from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
import requests


def remove_deleted_content_items(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    headers = {"Authorization-Token": Variable.get(kwargs["client"] + "_rock_token")}
    content_items = []

    pg_connection = kwargs["client"] + "_apollos_postgres"
    pg_hook = PostgresHook(
        postgres_conn_id=pg_connection,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )

    postgres_content_items = pg_hook.get_records(
        "SELECT id, origin_id FROM content_item"
    )

    params = {
        "$select": "Id",
        "loadAttributes": "expanded",
    }

    r = requests.get(
        f"{Variable.get(kwargs['client'] + '_rock_api')}/ContentChannelItems",
        params=params,
        headers=headers,
    )

    content_items = list(map(lambda contentItem: contentItem["Id"], r.json()))

    deleted_content_items = list(
        map(
            lambda contentItem: contentItem[0],
            filter(
                lambda contentItem: int(contentItem[1]) not in content_items,
                postgres_content_items,
            ),
        )
    )

    if len(deleted_content_items) > 0:
        pg_hook.run(
            """
            DELETE FROM content_item
            WHERE id = ANY(%s::uuid[])
        """,
            True,
            (deleted_content_items,),
        )

        pg_hook.run(
            """
            DELETE FROM "media"
            WHERE "node_id" = ANY(%s::uuid[])
        """,
            True,
            (deleted_content_items,),
        )
        print("Content Items Deleted: " + str(len(deleted_content_items)))
    else:
        print("No Content Items Deleted")

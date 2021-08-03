from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
import requests


def remove_deleted_content_items(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    headers = {"Authorization-Token": Variable.get(kwargs["client"] + "_rock_token")}
    contentItems = []

    pg_connection = kwargs["client"] + "_apollos_postgres"
    pg_hook = PostgresHook(
        postgres_conn_id=pg_connection,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )

    postgresContentItems = pg_hook.get_records("SELECT id, origin_id FROM content_item")

    params = {
        "$select": "Id",
        "loadAttributes": "expanded",
    }

    r = requests.get(
        f"{Variable.get(kwargs['client'] + '_rock_api')}/ContentChannelItems",
        params=params,
        headers=headers,
    )

    contentItems = list(map(lambda contentItem: contentItem["Id"], r.json()))

    deletedContentItems = list(
        map(
            lambda contentItem: contentItem[0],
            filter(
                lambda contentItem: int(contentItem[1]) not in contentItems,
                postgresContentItems,
            ),
        )
    )

    if len(deletedContentItems) > 0:
        pg_hook.run(
            """
            DELETE FROM content_item
            WHERE id = ANY(%s::uuid[])
        """,
            True,
            (deletedContentItems,),
        )

        pg_hook.run(
            """
            DELETE FROM "media"
            WHERE "node_id" = ANY(%s::uuid[])
        """,
            True,
            (deletedContentItems,),
        )
        print("Content Items Deleted: " + str(len(deletedContentItems)))
    else:
        print("No Content Items Deleted")

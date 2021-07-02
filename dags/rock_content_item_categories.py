from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from utilities import safeget

import requests


def fetch_and_save_content_item_categories(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    headers = {"Authorization-Token": Variable.get(kwargs["client"] + "_rock_token")}

    fetched_all = False
    skip = 0
    top = 10000

    pg_connection = kwargs["client"] + "_apollos_postgres"
    pg_hook = PostgresHook(
        postgres_conn_id=pg_connection,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )

    while fetched_all == False:
        # Fetch people records from Rock.

        params = {
            "$top": top,
            "$skip": skip,
            # "$expand": "Photo",
            "$select": "Id,Name",
            "$orderby": "ModifiedDateTime desc",
        }

        if not kwargs["do_backfill"]:
            params[
                "$filter"
            ] = f"ModifiedDateTime ge datetime'{kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}' or ModifiedDateTime eq null"

        print(params)

        r = requests.get(
            f"{Variable.get(kwargs['client'] + '_rock_api')}/ContentChannels",
            params=params,
            headers=headers,
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

        # "createdAt","updatedAt", "originId", "originType", "apollosType", "title"
        def update_content(obj):
            return (
                kwargs["execution_date"],
                kwargs["execution_date"],
                obj["Id"],
                "rock",
                "ContentChannel",
                obj["Name"],
            )

        def fix_casing(col):
            return '"{}"'.format(col)

        content_to_insert = list(map(update_content, rock_objects))
        columns = list(
            map(
                fix_casing,
                (
                    "createdAt",
                    "updatedAt",
                    "originId",
                    "originType",
                    "apollosType",
                    "title",
                ),
            )
        )

        pg_hook.insert_rows(
            '"contentItemCategories"',
            content_to_insert,
            columns,
            0,
            True,
            replace_index=('"originId"', '"originType"'),
        )

        add_apollos_ids = """
        UPDATE "contentItemCategories"
        SET "apollosId" = "apollosType" || id::varchar
        WHERE "originType" = 'rock' and "apollosId" IS NULL
        """

        pg_hook.run(add_apollos_ids)


def attach_content_item_categories(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    headers = {"Authorization-Token": Variable.get(kwargs["client"] + "_rock_token")}

    fetched_all = False
    skip = 0
    top = 10000

    pg_connection = kwargs["client"] + "_apollos_postgres"
    pg_hook = PostgresHook(
        postgres_conn_id=pg_connection,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )

    while fetched_all == False:
        # Fetch people records from Rock.

        params = {
            "$top": top,
            "$skip": skip,
            "$expand": "ContentChannel",
            "$select": "Id,ContentChannel/Id",
            "$orderby": "ModifiedDateTime desc",
        }

        if not kwargs["do_backfill"]:
            params[
                "$filter"
            ] = f"ModifiedDateTime ge datetime'{kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}' or ModifiedDateTime eq null"

        print(params)

        r = requests.get(
            f"{Variable.get(kwargs['client'] + '_rock_api')}/ContentChannelItems",
            params=params,
            headers=headers,
        )
        rock_objects = r.json()

        print(rock_objects)

        if not isinstance(rock_objects, list):
            print(rock_objects)
            print("oh uh, we might have made a bad request")
            print("top: {top}")
            print("skip: {skip}")
            skip += top
            continue

        skip += top
        fetched_all = len(rock_objects) < top

        def update_content_queries(obj):
            return """
            UPDATE "contentItems"
            SET "contentItemCategoryId" = (SELECT id FROM "contentItemCategories" WHERE "originId" = '{}')
            WHERE "originId" = '{}';
            """.format(
                str(safeget(obj, "ContentChannel", "Id")), str(obj["Id"])
            )

        pg_hook.run(list(map(update_content_queries, rock_objects)))

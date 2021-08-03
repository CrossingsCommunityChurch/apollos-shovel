from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from utilities import safeget

import requests

def fetch_and_save_content_item_categories(ds, *args, **kwargs):
    if 'client' not in kwargs or kwargs['client'] is None:
        raise Exception("You must configure a client for this operator")

    headers = {"Authorization-Token": Variable.get(kwargs['client'] + "_rock_token")}

    fetched_all = False
    skip = 0
    top = 10000

    pg_connection = kwargs['client'] + '_apollos_postgres'
    pg_hook = PostgresHook(postgres_conn_id=pg_connection,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
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

        if not kwargs['do_backfill']:
            params['$filter'] = f"ModifiedDateTime ge datetime'{kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}' or ModifiedDateTime eq null"

        print(params)

        r = requests.get(
                f"{Variable.get(kwargs['client'] + '_rock_api')}/ContentChannels",
                params=params,
                headers=headers)
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
        def update_content(obj):
            return (
                kwargs['execution_date'],
                kwargs['execution_date'],
                obj['Id'],
                'rock',
                'ContentChannel',
                obj['Name']
            )

        def fix_casing(col):
            return "\"{}\"".format(col)

        content_to_insert = list(map(update_content, rock_objects))
        columns = list(map(fix_casing, ("created_at","updated_at", "origin_id", "origin_type", "apollos_type", "title")))


        pg_hook.insert_rows(
            '"content_item_category"',
            content_to_insert,
            columns,
            0,
            True,
            replace_index = ('"origin_id"', '"origin_type"')
        )


        add_apollos_ids = """
        UPDATE content_item_category
        SET apollos_id = apollos_type || id::varchar
        WHERE origin_type = 'rock' and apollos_id IS NULL
        """

        pg_hook.run(add_apollos_ids)

def attach_content_item_categories(ds, *args, **kwargs):
    if 'client' not in kwargs or kwargs['client'] is None:
        raise Exception("You must configure a client for this operator")

    headers = {"Authorization-Token": Variable.get(kwargs['client'] + "_rock_token")}

    fetched_all = False
    skip = 0
    top = 10000

    pg_connection = kwargs['client'] + '_apollos_postgres'
    pg_hook = PostgresHook(postgres_conn_id=pg_connection,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
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

        if not kwargs['do_backfill']:
            params['$filter'] = f"ModifiedDateTime ge datetime'{kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}' or ModifiedDateTime eq null"

        print(params)

        r = requests.get(
                f"{Variable.get(kwargs['client'] + '_rock_api')}/ContentChannelItems",
                params=params,
                headers=headers)
        rock_objects = r.json()

        print(rock_objects);

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
            UPDATE content_item
            SET content_item_category_id = (SELECT id FROM content_item_category WHERE origin_id = '{}')
            WHERE origin_id = '{}';
            """.format(str(safeget(obj, 'ContentChannel', 'Id')), str(obj['Id']))

        pg_hook.run(list(map(update_content_queries, rock_objects)))

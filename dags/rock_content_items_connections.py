from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
from apollos_type import apollos_id

from html_sanitizer import Sanitizer
import nltk

import requests

nltk.download('punkt')

def safeget(dct, *keys):
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            return None
        except TypeError:
            return None
    return dct


def fetch_and_save_content_items_connections(ds, *args, **kwargs):
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

    def get_postgres_id(id):
        obj = pg_hook.get_first("""
            SELECT id
            FROM "contentItems"
            WHERE "originId" = '{}'
        """.format(id))
        return obj[0]

    while fetched_all == False:
        # Fetch people records from Rock.

        params = {
            "$top": top,
            "$skip": skip,
            # "$expand": "Photo",
            "$select": "Id,ChildContentChannelItemId,ContentChannelItemId",
            "loadAttributes": "simple",
            "attributeKeys": "Summary",
            "$orderby": "ModifiedDateTime desc",
        }

        if not kwargs['do_backfill']:
            params['$filter'] = f"ModifiedDateTime ge datetime'{kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}' or ModifiedDateTime eq null"

        print(params)

        r = requests.get(
                f"{Variable.get(kwargs['client'] + '_rock_api')}/ContentChannelItemAssociations",
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

        # "createdAt","updatedAt", "originId", "originType", "apollosType", "childId", "parentId"
        def update_content(obj):
            return (
                kwargs['execution_date'],
                kwargs['execution_date'],
                obj['Id'],
                'rock',
                'ContentItemsConnection',
                get_postgres_id(obj['ChildContentChannelItemId']),
                get_postgres_id(obj['ContentChannelItemId']),
            )

        def fix_casing(col):
            return "\"{}\"".format(col)

        content_to_insert = list(map(update_content, rock_objects))
        columns = list(map(fix_casing, ("createdAt","updatedAt", "originId", "originType", "apollosType", "childId", "parentId")))


        pg_hook.insert_rows(
            '"contentItemsConnections"',
            content_to_insert,
            columns,
            0,
            True,
            replace_index = ('"originId"', '"originType"')
        )


        add_apollos_ids = """
        UPDATE "contentItemsConnections"
        SET "apollosId" = "apollosType" || id::varchar
        WHERE "originType" = 'rock' and "apollosId" IS NULL
        """

        pg_hook.run(add_apollos_ids)


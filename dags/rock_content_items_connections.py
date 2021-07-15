from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from utilities import safeget

import requests


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
            "$select": "Id,ChildContentChannelItemId,ContentChannelItemId,Order",
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
                obj['Order']
            )

        def fix_casing(col):
            return "\"{}\"".format(col)

        content_to_insert = list(map(update_content, rock_objects))
        columns = list(map(fix_casing, ("createdAt","updatedAt", "originId", "originType", "apollosType", "childId", "parentId", "order")))


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

def set_parent_id(ds, *args, **kwargs):
    if 'client' not in kwargs or kwargs['client'] is None:
        raise Exception("You must configure a client for this operator")

    headers = {"Authorization-Token": Variable.get(kwargs['client'] + "_rock_token")}

    pg_connection = kwargs['client'] + '_apollos_postgres'
    pg_hook = PostgresHook(postgres_conn_id=pg_connection,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )


    add_apollos_parents = """
    WITH rows_to_update AS
      (SELECT "contentItemsConnections"."parentId",
              "contentItemsConnections"."childId" AS id
       FROM
         (SELECT c.id,
                 count(cc."childId") AS parents_count
          FROM "contentItems" c
          LEFT JOIN "contentItemsConnections" cc ON c.id = cc."childId"
          WHERE c."parentId" IS NULL
          GROUP BY c.id
          ORDER BY c.id) AS items_and_parents
       INNER JOIN "contentItemsConnections" ON "childId" = items_and_parents.id
       WHERE parents_count = 1)
    UPDATE "contentItems"
    SET "parentId" = rows_to_update."parentId"
    FROM rows_to_update
    WHERE "contentItems".id = rows_to_update.id;
    """

    pg_hook.run(add_apollos_parents)

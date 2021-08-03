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

    def get_child_and_parent(child_id, parent_id):
        child, parent = pg_hook.get_records("""
            SELECT id, origin_id
            FROM content_item
            JOIN unnest('{{{child_id},{parent_id}}}'::text[]) WITH ORDINALITY t(origin_id, ord) USING (origin_id)
            WHERE origin_id = '{child_id}' or origin_id = '{parent_id}'
            ORDER BY t.ord
        """.format(child_id=child_id, parent_id=parent_id))
        return (child[0], parent[0])

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

        # "created_at","updated_at", "origin_id", "origin_type", "apollos_type", "child_id", "parent_id", "order"
        def update_content(obj):
            child_id, parent_id = get_child_and_parent(obj['ChildContentChannelItemId'], obj['ContentChannelItemId'])
            return (
                kwargs['execution_date'],
                kwargs['execution_date'],
                obj['Id'],
                'rock',
                'ContentItemsConnection',
                child_id,
                parent_id,
                obj['Order']
            )

        def fix_casing(col):
            return "\"{}\"".format(col)

        content_to_insert = list(map(update_content, rock_objects))
        columns = list(map(fix_casing, ("created_at","updated_at", "origin_id", "origin_type", "apollos_type", "child_id", "parent_id", "order")))


        pg_hook.insert_rows(
            '"content_item_connection"',
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

def set_content_item_parent_id(ds, *args, **kwargs):
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

    rock_config = Variable.get(kwargs['client'] + "_rock_config", deserialize_json=True)
    series_parent_category_ids = ", ".join(map(lambda id: f"'{id}'", rock_config['SERIES_CATEGORY_ORIGIN_IDS']))

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

    pg_hook.run(add_apollos_parents)

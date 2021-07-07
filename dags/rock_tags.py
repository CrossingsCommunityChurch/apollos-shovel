from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from utilities import safeget
from psycopg2.extras import Json

import requests

def fetch_and_save_persona_tags(ds, *args, **kwargs):
    if 'client' not in kwargs or kwargs['client'] is None:
        raise Exception("You must configure a client for this operator")

    headers = {"Authorization-Token": Variable.get(kwargs['client'] + "_rock_token")}

    fetched_all = False
    skip = 0
    top = 10000

    pg_connection = kwargs['client'] + '_apollos_postgres'
    pg_hook = PostgresHook(
        postgres_conn_id=pg_connection,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )

    person_entity_id = requests.get(
        f"{Variable.get(kwargs['client'] + '_rock_api')}/EntityTypes",
        params={"$filter": "Name eq 'Rock.Model.Person'"},
        headers=headers
    ).json()[0]['Id']

    while not fetched_all:

        params = {
            "$top": top,
            "$skip": skip,
            "$filter": f"EntityTypeId eq {person_entity_id} and CategoryId eq {186}",
            "$select": "Id,Name,Guid",
            "$orderby": "ModifiedDateTime desc",
        }

        if not kwargs['do_backfill']:
            params['$filter'] = f"ModifiedDateTime ge datetime'{kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}' or ModifiedDateTime eq null or PersistedLastRefreshDateTime ge datetime'{kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}'"

        print(params)

        r = requests.get(
                f"{Variable.get(kwargs['client'] + '_rock_api')}/DataViews",
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

        # "createdAt", "updatedAt", "originId", "originType", "apollosType", "name", "data", "Persona"
        def update_tags(obj):
            return (
                kwargs['execution_date'],
                kwargs['execution_date'],
                obj['Id'],
                'rock',
                'Tag',
                obj['Name'],
                Json({ "guid": obj["Guid"] }),
                "Persona"
            )

        def fix_casing(col):
            return "\"{}\"".format(col)

        tags_to_insert = list(map(update_tags, rock_objects))
        columns = list(map(fix_casing, ("createdAt","updatedAt", "originId", "originType", "apollosType", "name", "data")))

        pg_hook.insert_rows(
            'tags',
            tags_to_insert,
            columns,
            0,
            True,
            replace_index = ('"originId"', '"originType"')
        )

        add_apollos_ids = """
        UPDATE "tags"
        SET "apollosId" = "apollosType" || ':' || id::varchar
        WHERE "originType" = 'rock' and "apollosId" IS NULL
        """

        pg_hook.run(add_apollos_ids)

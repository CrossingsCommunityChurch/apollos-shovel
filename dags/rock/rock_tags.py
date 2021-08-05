from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from utilities import safeget
from psycopg2.extras import Json

from functools import reduce

import requests


def fetch_and_save_persona_tags(ds, *args, **kwargs):
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

    person_entity_id = requests.get(
        f"{Variable.get(kwargs['client'] + '_rock_api')}/EntityTypes",
        params={"$filter": "Name eq 'Rock.Model.Person'"},
        headers=headers,
    ).json()[0]["Id"]

    rock_config = Variable.get(kwargs["client"] + "_rock_config", deserialize_json=True)

    while not fetched_all:

        params = {
            "$top": top,
            "$skip": skip,
            "$filter": f"EntityTypeId eq {person_entity_id} and CategoryId eq {rock_config['PERSONA_CATEGORY_ID']}",
            "$select": "Id,Name,Guid",
            "$orderby": "ModifiedDateTime desc",
        }

        if not kwargs["do_backfill"]:
            params[
                "$filter"
            ] += f" and (ModifiedDateTime ge datetime'{kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}' or ModifiedDateTime eq null or PersistedLastRefreshDateTime ge datetime'{kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}')"

        print(params)

        r = requests.get(
            f"{Variable.get(kwargs['client'] + '_rock_api')}/DataViews",
            params=params,
            headers=headers,
        )
        rock_objects = r.json()

        if not isinstance(rock_objects, list):
            print(rock_objects)
            print("oh uh, we might have made a bad request")
            print(f"top: {top}")
            print(f"skip: {skip}")
            skip += top
            continue

        skip += top
        fetched_all = len(rock_objects) < top

        # "created_at","updated_at", "origin_id", "origin_type", "apollos_type", "name", "data", "type"
        def update_tags(obj):
            return (
                kwargs["execution_date"],
                kwargs["execution_date"],
                obj["Id"],
                "rock",
                "Tag",
                obj["Name"],
                Json({"guid": obj["Guid"]}),
                "Persona",
            )

        def fix_casing(col):
            return '"{}"'.format(col)

        tags_to_insert = list(map(update_tags, rock_objects))
        columns = list(
            map(
                fix_casing,
                (
                    "created_at",
                    "updated_at",
                    "origin_id",
                    "origin_type",
                    "apollos_type",
                    "name",
                    "data",
                    "type",
                ),
            )
        )

        pg_hook.insert_rows(
            "tag",
            tags_to_insert,
            columns,
            0,
            True,
            replace_index=('"origin_id"', '"origin_type"'),
        )

        add_apollos_ids = """
        UPDATE tag
        SET apollos_id = apollos_type || ':' || id::varchar
        WHERE origin_type = 'rock' and apollos_id IS NULL
        """

        pg_hook.run(add_apollos_ids)


def attach_persona_tags_to_people(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    headers = {"Authorization-Token": Variable.get(kwargs["client"] + "_rock_token")}

    pg_connection = kwargs["client"] + "_apollos_postgres"
    pg_hook = PostgresHook(
        postgres_conn_id=pg_connection,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )

    person_entity_id = requests.get(
        f"{Variable.get(kwargs['client'] + '_rock_api')}/EntityTypes",
        params={"$filter": "Name eq 'Rock.Model.Person'"},
        headers=headers,
    ).json()[0]["Id"]

    rock_config = Variable.get(kwargs["client"] + "_rock_config", deserialize_json=True)

    persona_params = {
        "$filter": f"EntityTypeId eq {person_entity_id} and CategoryId eq {rock_config['PERSONA_CATEGORY_ID']}",
        "$select": "Id,Name,Guid",
        "$orderby": "ModifiedDateTime desc",
    }

    if not kwargs["do_backfill"]:
        persona_params[
            "$filter"
        ] += f" and (ModifiedDateTime ge datetime'{kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}' or ModifiedDateTime eq null or PersistedLastRefreshDateTime ge datetime'{kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}')"

    personas = requests.get(
        f"{Variable.get(kwargs['client'] + '_rock_api')}/DataViews",
        params=persona_params,
        headers=headers,
    ).json()

    print("Personas:")
    print(personas)

    for persona in personas:
        fetched_all = False
        skip = 0
        top = 10000

        while not fetched_all:
            params = {
                "$top": top,
                "$skip": skip,
                "$select": "Id",
                "$orderby": "ModifiedDateTime desc",
            }

            r = requests.get(
                f"{Variable.get(kwargs['client'] + '_rock_api')}/People/DataView/{persona['Id']}",
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

            def generate_insert_sql(tag_id, person_id):
                return f"""
                    INSERT INTO people_tag (tag_id, person_id, created_at, updated_at)
                    SELECT t.id,
                           p.id,
                           NOW(),
                           NOW()
                    FROM people p,
                         tag t
                    WHERE t.origin_id = '{tag_id}'
                      AND p.origin_id = '{person_id}'
                    ON CONFLICT (tag_id, person_id) DO NOTHING
                """

            sql_to_run = map(
                lambda p: generate_insert_sql(persona["Id"], p["Id"]), rock_objects
            )

            pg_hook.run(sql_to_run)

            skip += top
            fetched_all = len(rock_objects) < top


def attach_persona_tags_to_content(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    headers = {"Authorization-Token": Variable.get(kwargs["client"] + "_rock_token")}

    pg_connection = kwargs["client"] + "_apollos_postgres"
    pg_hook = PostgresHook(
        postgres_conn_id=pg_connection,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )

    # Entity of Content Item
    item_entity_id = requests.get(
        f"{Variable.get(kwargs['client'] + '_rock_api')}/EntityTypes",
        params={"$filter": "Name eq 'Rock.Model.ContentChannelItem'"},
        headers=headers,
    ).json()[0]["Id"]

    # Fields that represent dataview (a single dataview), and dataviews.
    dataview_fields = requests.get(
        f"{Variable.get(kwargs['client'] + '_rock_api')}/FieldTypes",
        params={
            "$filter": "Class eq 'Rock.Field.Types.DataViewsFieldType' or Class eq 'Rock.Field.Types.DataViewFieldType'"
        },
        headers=headers,
    ).json()

    dataview_id, dataviews_id = map(lambda f: f["Id"], dataview_fields)

    # Attribute values that belong to fields that are dataviews (and are content items)
    dataview_attribute_values = requests.get(
        f"{Variable.get(kwargs['client'] + '_rock_api')}/AttributeValues",
        params={
            "$filter": f"Attribute/EntityTypeId eq {item_entity_id} and (Attribute/FieldTypeId eq {dataview_id} or Attribute/FieldTypeId eq {dataviews_id})",
            "$select": "Value, EntityId",
        },
        headers=headers,
    ).json()

    def split_to_multiple_attributes(attribute):
        attributes = attribute["Value"].split(",")
        return list(
            map(lambda a: {"Value": a, "EntityId": attribute["EntityId"]}, attributes)
        )

    split_attribute_values = list(
        map(split_to_multiple_attributes, dataview_attribute_values)
    )
    attribute_values = reduce(lambda x, y: x + y, split_attribute_values, [])

    def generate_insert_sql(obj):
        return f"""
            INSERT INTO content_tag (tag_id, content_item_id, created_at, updated_at)
            SELECT t.id,
                   i.id,
                   NOW(),
                   NOW()
            FROM tag t,
                 content_item i
            WHERE t.data ->> 'guid' = '{obj["Value"]}'
              AND i.origin_id = '{obj["EntityId"]}'
            ON CONFLICT (tag_id, content_item_id) DO NOTHING
        """

    sql_to_run = map(generate_insert_sql, attribute_values)

    pg_hook.run(sql_to_run)
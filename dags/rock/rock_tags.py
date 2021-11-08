from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import Json
from rock.utilities import find_supported_fields
from functools import reduce
import requests
import pytz


def get_delta_offset(kwargs):
    local_zone = pytz.timezone(
        Variable.get(kwargs["client"] + "_rock_tz", default_var="EST")
    )
    execution_date_string = (
        kwargs["execution_date"].astimezone(local_zone).strftime("%Y-%m-%dT%H:%M:%S")
    )
    return f"ModifiedDateTime ge datetime'{execution_date_string}' or ModifiedDateTime eq null or PersistedLastRefreshDateTime ge datetime'{execution_date_string}'"


class Tag:
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

        self.person_entity_id = requests.get(
            f"{Variable.get(kwargs['client'] + '_rock_api')}/EntityTypes",
            params={"$filter": "Name eq 'Rock.Model.Person'"},
            headers=self.headers,
        ).json()[0]["Id"]

        self.content_item_entity_id = requests.get(
            f"{Variable.get(kwargs['client'] + '_rock_api')}/EntityTypes",
            params={"$filter": "Name eq 'Rock.Model.ContentChannelItem'"},
            headers=self.headers,
        ).json()[0]["Id"]

    def map_dataview_to_tag(self, obj):

        return {
            "created_at": self.kwargs["execution_date"],
            "updated_at": self.kwargs["execution_date"],
            "origin_id": obj["Id"],
            "origin_type": "rock",
            "apollos_type": "Tag",
            "name": obj["Name"],
            "data": Json({"guid": obj["Guid"]}),
            "type": self.get_tag_type(obj),
        }

    def map_rock_tag_to_tag(self, obj):
        return {
            "created_at": self.kwargs["execution_date"],
            "updated_at": self.kwargs["execution_date"],
            "origin_id": obj["Id"],
            "origin_type": "rock",
            "apollos_type": "Tag",
            "name": obj["Name"],
            "data": None,
            "type": "ContentItem",
        }

    def get_tag_type(self, obj):
        if obj["EntityTypeId"] == self.person_entity_id:
            return "Persona"
        elif obj["EntityTypeId"] == self.content_item_entity_id:
            return "ContentItem"
        return None

    def run_fetch_and_save_persona_tags(self):
        fetched_all = False
        skip = 0
        top = 10000

        rock_config = Variable.get(
            self.kwargs["client"] + "_rock_config", deserialize_json=True
        )

        while not fetched_all:

            params = {
                "$top": top,
                "$skip": skip,
                "$filter": f"(EntityTypeId eq {self.person_entity_id} and CategoryId eq {rock_config['PERSONA_CATEGORY_ID']}) or EntityTypeId eq {self.content_item_entity_id}",
                "$select": "Id,Name,Guid,EntityTypeId",
                "$orderby": "ModifiedDateTime desc",
            }

            if not self.kwargs["do_backfill"]:
                params["$filter"] += f" and ({get_delta_offset(self.kwargs)})"

            print(params)

            r = requests.get(
                f"{Variable.get(self.kwargs['client'] + '_rock_api')}/DataViews",
                params=params,
                headers=self.headers,
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

            tags_to_insert = list(map(self.map_dataview_to_tag, rock_objects))

            data_to_insert, columns, constraints = find_supported_fields(
                pg_hook=self.pg_hook,
                table_name="tag",
                insert_data=tags_to_insert,
            )

            self.pg_hook.insert_rows(
                "tag",
                data_to_insert,
                columns,
                0,
                True,
                replace_index=constraints,
            )

            add_apollos_ids = """
            UPDATE tag
            SET apollos_id = apollos_type || ':' || id::varchar
            WHERE origin_type = 'rock' and apollos_id IS NULL
            """

            self.pg_hook.run(add_apollos_ids)

    def run_fetch_and_save_rock_tags(self):
        fetched_all = False
        skip = 0
        top = 10000

        while not fetched_all:

            params = {
                "$top": top,
                "$skip": skip,
                "$filter": f"EntityTypeId eq {self.content_item_entity_id}",
            }

            if not self.kwargs["do_backfill"]:
                params["$filter"] += f" and ({get_delta_offset(self.kwargs)})"
            r = requests.get(
                f"{Variable.get(self.kwargs['client'] + '_rock_api')}/Tags",
                params=params,
                headers=self.headers,
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

            tags_to_insert = list(map(self.map_rock_tag_to_tag, rock_objects))

            data_to_insert, columns, constraints = find_supported_fields(
                pg_hook=self.pg_hook,
                table_name="tag",
                insert_data=tags_to_insert,
            )

            self.pg_hook.insert_rows(
                "tag",
                data_to_insert,
                columns,
                0,
                True,
                replace_index=constraints,
            )

            add_apollos_ids = """
            UPDATE tag
            SET apollos_id = apollos_type || ':' || id::varchar
            WHERE origin_type = 'rock' and apollos_id IS NULL
            """

            self.pg_hook.run(add_apollos_ids)

    def map_tagged_item(self, obj):
        # Get postgres Id from origin ID
        postgres_tag_id = self.pg_hook.get_first(
            """
                SELECT id from tag WHERE tag.origin_id = %s
            """,
            (str(obj["TagId"]),),
        )[0]

        # Get rock content item id from rock guid
        params = {"$filter": f"Guid eq guid'{obj['EntityGuid']}'", "$select": "Id"}

        rock_content_item_id = requests.get(
            f"{Variable.get(self.kwargs['client'] + '_rock_api')}/ContentChannelItems",
            params=params,
            headers=self.headers,
        ).json()[0]["Id"]

        # Get postgred content item id from rock content item id
        postgres_content_item_id = self.pg_hook.get_first(
            """
                SELECT id from content_item WHERE content_item.origin_id = %s
            """,
            (str(rock_content_item_id),),
        )[0]

        return {
            "created_at": self.kwargs["execution_date"],
            "updated_at": self.kwargs["execution_date"],
            "tag_id": postgres_tag_id,
            "content_item_id": postgres_content_item_id,
        }

    def run_fetch_and_save_tagged_items(self):
        fetched_all = False
        skip = 0
        top = 10000

        while not fetched_all:

            params = {
                "$top": top,
                "$skip": skip,
                "$filter": f"EntityTypeId eq {self.content_item_entity_id}",
                "$select": "TagId,EntityTypeId,EntityGuid",
            }

            if not self.kwargs["do_backfill"]:
                params["$filter"] += f" and ({get_delta_offset(self.kwargs)})"
            r = requests.get(
                f"{Variable.get(self.kwargs['client'] + '_rock_api')}/TaggedItems",
                params=params,
                headers=self.headers,
            )
            rock_objects = r.json()

            if not isinstance(rock_objects, list):
                print(rock_objects)
                print("oh uh, we might have made a bad request")
                print(f"top: {top}")
                print(f"skip: {skip}")
                skip += top
                continue

            tags_to_insert = list(map(self.map_tagged_item, rock_objects))

            data_to_insert, columns, constraints = find_supported_fields(
                pg_hook=self.pg_hook,
                table_name="content_tag",
                insert_data=tags_to_insert,
            )

            self.pg_hook.insert_rows(
                "content_tag",
                data_to_insert,
                columns,
                0,
                True,
                replace_index=("tag_id", "content_item_id"),
            )

            skip += top
            fetched_all = len(rock_objects) < top

    def generate_person_tag_import_sql(self, tag_id, person_id):
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

    def run_attach_persona_tags_to_people(self):
        rock_config = Variable.get(
            self.kwargs["client"] + "_rock_config", deserialize_json=True
        )

        persona_params = {
            "$filter": f"EntityTypeId eq {self.person_entity_id} and CategoryId eq {rock_config['PERSONA_CATEGORY_ID']}",
            "$select": "Id,Name,Guid,EntityTypeId",
            "$orderby": "ModifiedDateTime desc",
        }

        if not self.kwargs["do_backfill"]:
            persona_params["$filter"] += f" and ({get_delta_offset(self.kwargs)})"

        personas = requests.get(
            f"{Variable.get(self.kwargs['client'] + '_rock_api')}/DataViews",
            params=persona_params,
            headers=self.headers,
        ).json()

        print("Personas:")

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
                    f"{Variable.get(self.kwargs['client'] + '_rock_api')}/People/DataView/{persona['Id']}",
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

                sql_to_run = map(
                    lambda p: self.generate_person_tag_import_sql(
                        persona["Id"], p["Id"]
                    ),
                    rock_objects,
                )

                self.pg_hook.run(sql_to_run)

                skip += top
                fetched_all = len(rock_objects) < top

    def get_dataview_field_type_ids(self):
        # Fields that represent dataview (a single dataview), and dataviews.
        dataview_fields = requests.get(
            f"{Variable.get(self.kwargs['client'] + '_rock_api')}/FieldTypes",
            params={
                "$filter": "Class eq 'Rock.Field.Types.DataViewsFieldType' or Class eq 'Rock.Field.Types.DataViewFieldType'"
            },
            headers=self.headers,
        ).json()

        dataview_id, dataviews_id = map(lambda f: f["Id"], dataview_fields)
        return dataview_id, dataviews_id

    def split_attribute_value(self, attribute):
        attributes = attribute["Value"].split(",")
        return list(
            map(
                lambda a: {"Value": a, "EntityId": attribute["EntityId"]},
                attributes,
            )
        )

    def generate_content_tag_insert_sql(self, obj):
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

    def run_attach_persona_tags_to_content(self):
        # Entity of Content Item
        item_entity_id = requests.get(
            f"{Variable.get(self.kwargs['client'] + '_rock_api')}/EntityTypes",
            params={"$filter": "Name eq 'Rock.Model.ContentChannelItem'"},
            headers=self.headers,
        ).json()[0]["Id"]

        dataview_id, dataviews_id = self.get_dataview_field_type_ids()

        # Attribute values that belong to fields that are dataviews (and are content items)
        dataview_attribute_values = requests.get(
            f"{Variable.get(self.kwargs['client'] + '_rock_api')}/AttributeValues",
            params={
                "$filter": f"Attribute/EntityTypeId eq {item_entity_id} and (Attribute/FieldTypeId eq {dataview_id} or Attribute/FieldTypeId eq {dataviews_id})",
                "$select": "Value, EntityId",
            },
            headers=self.headers,
        ).json()

        split_attribute_values = list(
            map(self.split_attribute_value, dataview_attribute_values)
        )
        attribute_values = reduce(lambda x, y: x + y, split_attribute_values, [])

        sql_to_run = map(self.generate_content_tag_insert_sql, attribute_values)

        self.pg_hook.run(sql_to_run)


def fetch_and_save_persona_tags(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    Klass = Tag if "klass" not in kwargs else kwargs["klass"]  # noqa: N806
    tag_task = Klass(kwargs)

    tag_task.run_fetch_and_save_persona_tags()


def fetch_and_save_rock_tags(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    Klass = Tag if "klass" not in kwargs else kwargs["klass"]  # noqa: N806
    tag_task = Klass(kwargs)

    tag_task.run_fetch_and_save_rock_tags()


def fetch_and_save_tagged_items(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    Klass = Tag if "klass" not in kwargs else kwargs["klass"]  # noqa: N806
    tag_task = Klass(kwargs)

    tag_task.run_fetch_and_save_tagged_items()


def attach_persona_tags_to_people(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    Klass = Tag if "klass" not in kwargs else kwargs["klass"]  # noqa: N806
    tag_task = Klass(kwargs)

    tag_task.run_attach_persona_tags_to_people()


def attach_persona_tags_to_content(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    Klass = Tag if "klass" not in kwargs else kwargs["klass"]  # noqa: N806
    tag_task = Klass(kwargs)

    tag_task.run_attach_persona_tags_to_content()

from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from rock.utilities import (
    safeget_no_case,
    safeget,
    get_delta_offset_with_content_attributes,
    find_supported_fields,
)
from urllib.parse import unquote
from psycopg2.extras import Json
from functools import reduce
import json

import requests


class Feature:
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

        self.rock_content_cache = {}
        self.all_postgres_features = []

    def fetch_all_postgres_features(self):
        allpostgres_features = self.pg_hook.get_records(
            """
                SELECT content_item.origin_id, array_to_json(array_agg(fo)) AS features
                FROM   content_item
                INNER JOIN (SELECT feature.*,
                                content_item.origin_id AS origin_id
                        FROM   content_item
                                INNER JOIN feature
                                        ON feature.parent_id = content_item.id
                        WHERE  feature IS NOT NULL
                ) AS fo
                    ON fo.parent_id = content_item.id
                GROUP  BY ( content_item.origin_id );
            """
        )

        def sort_features(features):
            features.sort(key=lambda feature: feature["priority"])
            return features

        self.all_postgres_features = {
            f"{id}": sort_features(features) for id, features in allpostgres_features
        }

    def get_rock_content_item(self, origin_id):
        if origin_id in self.rock_content_cache:
            return self.rock_content_cache[origin_id]

        params = {
            "loadAttributes": "expanded",
            "attributeKeys": "childrenHaveComments",
        }

        r = requests.get(
            f"{Variable.get(self.kwargs['client'] + '_rock_api')}/ContentChannelItems/{origin_id}",
            params=params,
            headers=self.headers,
        )

        self.rock_content_cache[origin_id] = r.json()
        return r.json()

    def add_postgres_data_to_rock_content(self, rock_content):

        if len(rock_content) == 0:
            return []

        all_origin_ids = ", ".join(map(lambda r: f"'{str(r['Id'])}'", rock_content))
        postgres_content = self.pg_hook.get_records(
            f"""
            SELECT content_item.id, parent_item.origin_id, content_item.origin_id
            FROM content_item
            LEFT OUTER JOIN content_item AS parent_item ON parent_item.id = content_item.parent_id
            WHERE content_item.origin_id in ({all_origin_ids})
            """
        )
        postgres_data = {
            row[2]: {"node_id": row[0], "parent_origin_id": row[1]}
            for row in postgres_content
        }

        content_with_postgres_data = [
            {
                "Id": item["Id"],
                "AttributeValues": item["AttributeValues"],
                **postgres_data.get(str(item["Id"])),
            }
            for item in rock_content
            if str(item["Id"]) in postgres_data.keys()
        ]
        return content_with_postgres_data

    # Rock transports { foo: 'bar', baz: 'bat' } as 'foo^bar|baz^bat`
    def parse_key_value_attribute(self, value):
        if value == "":
            return []

        entries = value.split("|")

        def string_to_dict(s):
            k, v = s.split("^")
            return {"key": k, "value": unquote(v)}

        return list(map(string_to_dict, entries))

    # map feature priority from index
    def map_feature_priority(self, feature):
        index = feature[0]
        obj = feature[1]
        return {**obj, "priority": index}

    def check_postgres_features(self, rock_content_item_features, content_item_id):
        added_features = []
        updated_features = []
        deleted_features = []

        pg_content_item_features = safeget(
            self.all_postgres_features, str(content_item_id)
        )

        if pg_content_item_features:
            pg_feature_count = len(pg_content_item_features)
            rock_feature_count = len(rock_content_item_features)

            # Get deleted features
            if pg_feature_count > rock_feature_count:
                deleted_slice = slice(rock_feature_count, pg_feature_count)
                for feature in pg_content_item_features[deleted_slice]:
                    deleted_features.append(feature["id"])

            # Get added features
            if pg_feature_count < rock_feature_count:
                added_slice = slice(pg_feature_count, rock_feature_count)
                for feature in rock_content_item_features[added_slice]:
                    added_features.append(feature)

            # Get updated features
            for index, rock_feature in enumerate(rock_content_item_features):
                postgres_feature = safeget(pg_content_item_features, index)
                if postgres_feature and (
                    safeget(rock_feature, "type") != safeget(postgres_feature, "type")
                    or safeget(rock_feature, "data")
                    != safeget(postgres_feature, "data")
                    or safeget(rock_feature, "priority")
                    != safeget(postgres_feature, "priority")
                ):
                    updated_features.append(
                        {**rock_feature, "postgres_id": postgres_feature["id"]}
                    )
        else:
            for feature in rock_content_item_features:
                added_features.append(feature)

        return {
            "added_features": added_features,
            "updated_features": updated_features,
            "deleted_features": deleted_features,
        }

    def get_attribute_matrix_field_type_id(self):
        if hasattr(self, "attribute_matrix_id"):
            return self.attribute_matrix_id
        # We'll use this when parsing features.
        self.attribute_matrix_id = requests.get(
            f"{Variable.get(self.kwargs['client'] + '_rock_api')}/FieldTypes",
            params={
                "$filter": "Class eq 'Rock.Field.Types.MatrixFieldType'",
                "$select": "Id",
            },
            headers=self.headers,
        ).json()[0]["Id"]
        return self.attribute_matrix_id

    def get_features(self, content):
        features = []
        # Add features from a key value list
        # This is now the only way to add scripture and text features
        # We previously supported a textFeature and

        location_feature = safeget_no_case(
            content, "AttributeValues", "Location", "Value"
        )

        if location_feature:
            params = {"$filter": f"Guid eq guid'{location_feature}'"}

            location_obj = requests.get(
                f"{Variable.get(self.kwargs['client'] + '_rock_api')}/Locations",
                params=params,
                headers=self.headers,
            ).json()

            if isinstance(location_obj, list):
                location_obj = location_obj[0]

                features.append(
                    {
                        "type": "Location",
                        "data": {
                            "name": location_obj["Name"],
                            "street": location_obj["Street1"],
                            "city": location_obj["City"],
                            "state": location_obj["State"],
                            "zip": location_obj["PostalCode"],
                            "lat": location_obj["Latitude"],
                            "long": location_obj["Longitude"],
                        },
                        "parent_id": content["node_id"],
                    }
                )

        event_date_feature = safeget_no_case(
            content, "AttributeValues", "EventDate", "Value"
        )

        if event_date_feature:
            features.append(
                {
                    "type": "EventDate",
                    "data": {"date": event_date_feature},
                    "parent_id": content["node_id"],
                }
            )

        key_value_features = self.parse_key_value_attribute(
            safeget_no_case(content, "AttributeValues", "Features", "Value") or ""
        )

        scriptures = safeget(content, "AttributeValues", "Scriptures", "Value")

        if scriptures:
            scripture_field_type = safeget(
                content, "Attributes", "Scriptures", "FieldTypeId"
            )

            # If the attribute is a matrix.
            if scripture_field_type == self.get_attribute_matrix_field_type_id():
                # Get the formated vaule field (JSON string of the matrix)
                formatted_value = safeget(
                    content, "AttributeValues", "Scriptures", "ValueFormatted"
                )
                # If there is indeed a matrix
                if formatted_value:
                    try:
                        # Parse the formatted value
                        parsed_matrix = json.loads(formatted_value)
                        if "Attributes" in parsed_matrix:
                            # And then for each row, create a feature.
                            for ref in parsed_matrix["Attributes"]:
                                key_value_features.append(
                                    {
                                        "key": "scripture",
                                        "value": f"{ref['Book']} {ref['Reference']}",
                                    }
                                )
                    except ValueError:
                        print("Error parsing")
                        print(formatted_value)
                        print(content)

            else:
                for scripture in scriptures.split(","):
                    key_value_features.append({"key": "scripture", "value": scripture})

        for kv in key_value_features:
            feature_type = kv["key"]

            if feature_type == "scripture":
                features.append(
                    {
                        "type": "Scripture",
                        "data": {"reference": kv["value"]},
                        "parent_id": content["node_id"],
                    }
                )
            if feature_type == "text":
                features.append(
                    {
                        "type": "Text",
                        "data": {"text": kv["value"]},
                        "parent_id": content["node_id"],
                    }
                )

        button_link_feature = safeget_no_case(
            content, "AttributeValues", "ButtonText", "Value"
        )
        if button_link_feature:
            features.append(
                {
                    "type": "Button",
                    "data": {
                        "title": safeget_no_case(
                            content, "AttributeValues", "ButtonText", "Value"
                        ),
                        "url": safeget_no_case(
                            content, "AttributeValues", "ButtonLink", "Value"
                        ),
                        "action": "OPEN_AUTHENTICATED_URL",
                    },
                    "parent_id": content["node_id"],
                }
            )

        complete_button_feature = (
            content["AttributeValues"].get("CompleteButtonText", {}).get("Value")
        )
        if complete_button_feature:
            features.append(
                {
                    "type": "Button",
                    "data": {
                        "title": content["AttributeValues"]["CompleteButtonText"][
                            "Value"
                        ],
                        "action": "COMPLETE_NODE",
                    },
                    "parent_id": content["node_id"],
                }
            )

        comment_feature = (
            safeget_no_case(content, "AttributeValues", "comments", "value") or "False"
        )
        if comment_feature == "True":
            features.append(
                {
                    "type": "CommentList",
                    "data": {
                        "prompt": safeget_no_case(
                            content, "AttributeValues", "initialPrompt", "value"
                        )
                    },
                    "parent_id": content["node_id"],
                }
            )
            features.append(
                {
                    "type": "AddComment",
                    "data": {
                        "prompt": safeget_no_case(
                            content, "AttributeValues", "initialPrompt", "value"
                        )
                    },
                    "parent_id": content["node_id"],
                }
            )
        elif content["parent_origin_id"]:
            parent_item = self.get_rock_content_item(content["parent_origin_id"])
            parent_comment_feature = (
                safeget_no_case(
                    parent_item, "AttributeValues", "childrenHaveComments", "value"
                )
                or "False"
            )
            if parent_comment_feature == "True":
                features.append(
                    {
                        "type": "CommentList",
                        "data": {
                            "prompt": safeget_no_case(
                                content, "AttributeValues", "initalPrompt", "value"
                            )
                        },
                        "parent_id": content["node_id"],
                    }
                )
                features.append(
                    {
                        "type": "AddComment",
                        "data": {
                            "prompt": safeget_no_case(
                                content, "AttributeValues", "initialPrompt", "value"
                            )
                        },
                        "parent_id": content["node_id"],
                    }
                )

        features_with_priority = list(
            map(self.map_feature_priority, enumerate(features))
        )

        return self.check_postgres_features(features_with_priority, content["Id"])

    def map_feature_to_object(self, obj):

        return {
            "updated_at": self.kwargs["execution_date"],
            "created_at": self.kwargs["execution_date"],
            "apollos_type": obj["type"] + "Feature",
            "data": Json(obj["data"]),
            "type": obj["type"],
            "parent_id": obj["parent_id"],
            "parent_type": "ContentItem",
            "priority": obj["priority"],
        }

    def get_action_features(self, feature_action_arrays, features):
        for action, action_features in features.items():
            if len(action_features) > 0:
                for feature in action_features:
                    feature_action_arrays[action].append(feature)
        return feature_action_arrays

    def delete_features(self, deleted_features):
        print(f"Deleted Features: {len(deleted_features)}")
        if len(deleted_features) > 0:
            self.pg_hook.run(
                """
                    DELETE FROM feature
                    WHERE feature.id = ANY(%s::uuid[])
                """,
                True,
                (deleted_features,),
            )

    def update_features(self, updated_features):
        print(f"Updated Features: {len(updated_features)}")
        if len(updated_features) > 0:
            for feature in updated_features:
                self.pg_hook.run(
                    """
                    UPDATE feature
                    SET type = %(type)s,
                        data = %(data)s,
                        apollos_type = %(apollos_type)s,
                        apollos_id = %(apollos_id)s,
                        priority = %(priority)s
                    WHERE feature.id = %(postgres_id)s;
                """,
                    True,
                    {
                        "type": feature["type"],
                        "data": Json(feature["data"]),
                        "apollos_type": f"{feature['type']}Feature",
                        "apollos_id": f"{feature['type']}Feature:{feature['postgres_id']}",
                        "priority": feature["priority"],
                        "postgres_id": feature["postgres_id"],
                    },
                )

    def insert_features(self, added_features):
        print(f"Added Features: {len(added_features)}")
        if len(added_features) > 0:
            # "created_at", "updated_at", "apollos_type", "data", "type", "parent_id", "parent_type"
            insert_features = list(map(self.map_feature_to_object, added_features))

            data_to_insert, columns, _ = find_supported_fields(
                pg_hook=self.pg_hook,
                table_name="feature",
                insert_data=insert_features,
            )

            self.pg_hook.insert_rows(
                '"feature"',
                data_to_insert,
                columns,
                0,
                True,
                replace_index=('"parent_id"', '"type"', '"data"', '"priority"'),
            )

    def run_fetch_and_save_features(self):
        fetched_all = False
        skip = 0
        top = 10000
        retry_count = 0

        self.fetch_all_postgres_features()

        while not fetched_all:
            # Fetch content item records from Rock.
            params = {
                "$top": top,
                "$skip": skip,
                "loadAttributes": "expanded",
                "$orderby": "ModifiedDateTime desc",
                "attributeKeys": "features, comments, initialPrompt, buttontext, buttonlink, completeButtonText, scriptures, location, eventdate",
            }

            if not self.kwargs["do_backfill"]:
                params["$filter"] = get_delta_offset_with_content_attributes(
                    self.kwargs
                )

            rock_objects = requests.get(
                f"{Variable.get(self.kwargs['client'] + '_rock_api')}/ContentChannelItems",
                params=params,
                headers=self.headers,
            ).json()

            if not isinstance(rock_objects, list):
                print(rock_objects)
                print("oh uh, we might have made a bad request")
                print(f"top: {top}")
                print(f"skip: {skip}")
                print(f"params: {params}")

                if retry_count >= 3:
                    raise Exception(f"Rock Error: {rock_objects}")

                retry_count += 1
                skip += top
                continue

            content_with_postgres_data = self.add_postgres_data_to_rock_content(
                rock_objects
            )

            features = list(map(self.get_features, content_with_postgres_data))

            action_features = reduce(
                self.get_action_features,
                features,
                {"added_features": [], "updated_features": [], "deleted_features": []},
            )

            self.insert_features(action_features["added_features"])
            self.update_features(action_features["updated_features"])
            self.delete_features(action_features["deleted_features"])

            skip += top
            fetched_all = len(rock_objects) < top

        add_apollos_ids = """
            UPDATE feature
            SET apollos_id = apollos_type || ':' || id::varchar
            WHERE apollos_id IS NULL
            """

        self.pg_hook.run(add_apollos_ids)


def fetch_and_save_features(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    Klass = Feature if "klass" not in kwargs else kwargs["klass"]  # noqa N806

    feature_task = Klass(kwargs)

    feature_task.run_fetch_and_save_features()

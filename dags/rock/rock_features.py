from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from utilities import safeget_no_case, safeget, get_delta_offset_with_content_attributes
from urllib.parse import unquote
from psycopg2.extras import Json

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

    def add_postgres_data_to_rock_features(self, features):
        if len(features) == 0:
            return []

        origin_ids = ", ".join(map(lambda r: f"'{str(r['Id'])}'", features))
        postgres_records = self.pg_hook.get_records(
            f"""
            SELECT content_item.id, parent_item.origin_id, content_item.origin_id
            FROM content_item
            LEFT OUTER JOIN content_item AS parent_item ON parent_item.id = content_item.parent_id
            WHERE content_item.origin_id in ({origin_ids})
            """
        )
        postgres_records_by_origin_id = {
            f[2]: {"node_id": f[0], "parent_origin_id": f[1]} for f in postgres_records
        }
        rock_with_postgres_features = [
            {**feature, **postgres_records_by_origin_id.get(str(feature["Id"]))}
            for feature in features
        ]
        return rock_with_postgres_features

    # Rock transports { foo: 'bar', baz: 'bat' } as 'foo^bar|baz^bat`
    def parse_key_value_attribute(self, value):
        if value == "":
            return []

        entries = value.split("|")

        def string_to_dict(s):
            k, v = s.split("^")
            return {"key": k, "value": unquote(v)}

        return map(string_to_dict, entries)

    # map feature priority from index
    def map_feature_priority(self, feature):
        index = feature[0]
        obj = feature[1]
        return {**obj, "priority": index}

    # def get_
    def get_features(self, content):
        features = []

        # Add features from a key value list
        # This is now the only way to add scripture and text features
        # We previously supported a textFeature and

        key_value_features = self.parse_key_value_attribute(
            safeget_no_case(content, "AttributeValues", "Features", "Value") or ""
        )

        scriptures = safeget(content, "AttributeValues", "Scriptures", "Value")

        if scriptures:
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

        comment_feature = (
            safeget_no_case(content, "AttributeValues", "comments", "value") or "False"
        )
        if comment_feature == "True":
            features.append(
                {
                    "type": "AddComment",
                    "data": {
                        "initialPrompt": "Write Something...",
                        "addPrompt": "What stands out to you?",
                    },
                    "parent_id": content["node_id"],
                }
            )
            features.append(
                {
                    "type": "CommentList",
                    "data": {
                        "initialPrompt": "Write Something...",
                        "addPrompt": "What stands out to you?",
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
                        "type": "AddComment",
                        "data": {
                            "initialPrompt": "Write Something...",
                            "addPrompt": "What stands out to you?",
                        },
                        "parent_id": content["node_id"],
                    }
                )
                features.append(
                    {
                        "type": "CommentList",
                        "data": {
                            "initialPrompt": "Write Something...",
                            "addPrompt": "What stands out to you?",
                        },
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

        features_with_priority = list(
            map(self.map_feature_priority, enumerate(features))
        )

        return features_with_priority

    def map_feature_to_columns(self, obj):

        return (
            self.kwargs["execution_date"],
            self.kwargs["execution_date"],
            obj["type"] + "Feature",
            Json(obj["data"]),
            obj["type"],
            obj["parent_id"],
            "ContentItem",
            obj["priority"],
        )

    def run_fetch_and_save_features(self):
        fetched_all = False
        skip = 0
        top = 10000

        retry_count = 0

        while not fetched_all:
            # Fetch people records from Rock.

            params = {
                "$top": top,
                "$skip": skip,
                # "$select": "Id,Content",
                "loadAttributes": "expanded",
                "$orderby": "ModifiedDateTime desc",
                "attributeKeys": "features, comments, buttontext, buttonlink, Scriptures",
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

            features_with_postgres_data = self.add_postgres_data_to_rock_features(
                rock_objects
            )
            features_by_item = list(map(self.get_features, features_with_postgres_data))
            flat_features_list = [item for items in features_by_item for item in items]

            # "created_at", "updated_at", "apollos_type", "data", "type", "parent_id", "parent_type"

            insert_features = list(map(self.map_feature_to_columns, flat_features_list))

            columns = (
                "created_at",
                "updated_at",
                "apollos_type",
                "data",
                "type",
                "parent_id",
                "parent_type",
                "priority",
            )

            self.pg_hook.insert_rows(
                '"feature"',
                list(insert_features),
                columns,
                0,
                True,
                replace_index=('"parent_id"', '"type"', '"data"'),
            )

            add_apollos_ids = """
            UPDATE feature
            SET apollos_id = apollos_type || ':' || id::varchar
            WHERE apollos_id IS NULL
            """

            self.pg_hook.run(add_apollos_ids)

            skip += top
            fetched_all = len(rock_objects) < top


def fetch_and_save_features(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    Klass = Feature if "klass" not in kwargs else kwargs["klass"]

    feature_task = Klass(kwargs)

    feature_task.run_fetch_and_save_features()

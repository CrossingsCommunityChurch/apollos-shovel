from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from utilities import safeget_no_case, safeget
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

    def get_rock_content_item(self, origin_id):
        params = {
            "loadAttributes": "expanded",
            "$orderby": "ModifiedDateTime desc",
            "attributeKeys": "childrenHaveComments",
        }

        r = requests.get(
            f"{Variable.get(self.kwargs['client'] + '_rock_api')}/ContentChannelItems/{origin_id}",
            params=params,
            headers=self.headers,
        )
        return r.json()

    # Rock transports { foo: 'bar', baz: 'bat' } as 'foo^bar|baz^bat`
    def parse_key_value_attribute(self, value):
        if value == "":
            return []

        entries = value.split("|")

        def string_to_dict(s):
            k, v = s.split("^")
            return {"key": k, "value": unquote(v)}

        return map(string_to_dict, entries)

    # def get_

    def get_features(self, content):
        features = []

        postgres_id, parent_rock_id = self.pg_hook.get_first(
            f'select content_item.id, "parentItem".origin_id from content_item left outer join content_item as "parentItem" on "parentItem".id = content_item.parent_id where content_item.origin_id = \'{content["Id"]}\''
        )
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
                        "parent_id": postgres_id,
                    }
                )
            if feature_type == "text":
                features.append(
                    {
                        "type": "Text",
                        "data": {"text": kv["value"]},
                        "parent_id": postgres_id,
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
                    "parent_id": postgres_id,
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
                    "parent_id": postgres_id,
                }
            )
            features.append(
                {
                    "type": "CommentList",
                    "data": {
                        "initialPrompt": "Write Something...",
                        "addPrompt": "What stands out to you?",
                    },
                    "parent_id": postgres_id,
                }
            )
        elif parent_rock_id:
            parent_item = self.get_rock_content_item(parent_rock_id)
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
                        "parent_id": postgres_id,
                    }
                )
                features.append(
                    {
                        "type": "CommentList",
                        "data": {
                            "initialPrompt": "Write Something...",
                            "addPrompt": "What stands out to you?",
                        },
                        "parent_id": postgres_id,
                    }
                )

        return features

    def map_feature_to_columns(self, obj):
        return (
            self.kwargs["execution_date"],
            self.kwargs["execution_date"],
            obj["type"] + "Feature",
            Json(obj["data"]),
            obj["type"],
            obj["parent_id"],
            "ContentItem",
        )

    def run_fetch_and_save_features(self):
        fetched_all = False
        skip = 0
        top = 10000

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
                params[
                    "$filter"
                ] = f"ModifiedDateTime ge datetime'{self.kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}' or ModifiedDateTime eq null"

            rock_objects = requests.get(
                f"{Variable.get(self.kwargs['client'] + '_rock_api')}/ContentChannelItems",
                params=params,
                headers=self.headers,
            ).json()

            features_by_item = list(map(self.get_features, rock_objects))
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

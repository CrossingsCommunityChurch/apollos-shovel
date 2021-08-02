from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from utilities import safeget_no_case
from urllib.parse import unquote
from psycopg2.extras import Json

import requests

def fetch_and_save_features(ds, *args, **kwargs):
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


    def get_rock_content_item(originId):
        params = {
            "loadAttributes": "expanded",
            "$orderby": "ModifiedDateTime desc",
            "attributeKeys": "childrenHaveComments",
        }

        r = requests.get(
                f"{Variable.get(kwargs['client'] + '_rock_api')}/ContentChannelItems/{originId}",
                params=params,
                headers=headers)
        return r.json()

    # Rock transports { foo: 'bar', baz: 'bat' } as 'foo^bar|baz^bat`
    def parse_key_value_attribute(value):
        if value == '':
            return []

        entries = value.split('|')

        def string_to_dict(s):
            k, v = s.split('^')
            return { "key": k, "value": unquote(v) }

        return map(string_to_dict, entries)

    # def get_

    def get_features(content):
        features = []

        postgres_id, parent_rock_id = pg_hook.get_first(f'select content_item.id, "parentItem".origin_id from content_item left outer join content_item as "parentItem" on "parentItem".id = content_item.parent_id where content_item.origin_id = \'{content["Id"]}\'')
        # Add features from a key value list
        # This is now the only way to add scripture and text features
        # We previously supported a textFeature and
        key_value_features = parse_key_value_attribute(
            safeget_no_case(content, 'AttributeValues', 'Features', 'Value') or ''
        )
        for kv in key_value_features:
            feature_type = kv["key"]

            if feature_type == "scripture":
                features.append({
                    "type": "Scripture",
                    "data": {
                        "reference": kv["value"]
                    },
                    "parent_id": postgres_id
                })
            if feature_type == "text":
                features.append({
                    "type": "Text",
                    "data": {
                        "text": kv["value"]
                    },
                    "parent_id": postgres_id
                })

        button_link_feature = safeget_no_case(content, 'AttributeValues', 'ButtonText', 'Value')
        if (button_link_feature):
            features.append({
                "type": "Button",
                "data": {
                    "title": safeget_no_case(content, 'AttributeValues', 'ButtonText', 'Value'),
                    "url": safeget_no_case(content, 'AttributeValues', 'ButtonLink', 'Value'),
                    "action": "OPEN_AUTHENTICATED_URL",
                },
                "parent_id": postgres_id
            })

        comment_feature = safeget_no_case(content, 'AttributeValues', 'comments', 'value') or 'False'
        if comment_feature == 'True':
            features.append({
                "type": "AddComment",
                "data": {
                    "initialPrompt": "Write Something...",
                    "addPrompt": "What stands out to you?"
                },
                "parent_id": postgres_id
            })
            features.append({
                "type": "CommentList",
                "data": {
                    "initialPrompt": "Write Something...",
                    "addPrompt": "What stands out to you?"
                },
                "parent_id": postgres_id
            })
        elif parent_rock_id:
            parent_item = get_rock_content_item(parent_rock_id)
            parent_comment_feature = safeget_no_case(parent_item, 'AttributeValues', 'childrenHaveComments', 'value') or 'False'
            if parent_comment_feature == 'True':
                features.append({
                    "type": "AddComment",
                    "data": {
                        "initialPrompt": "Write Something...",
                        "addPrompt": "What stands out to you?"
                    },
                    "parent_id": postgres_id
                })
                features.append({
                    "type": "CommentList",
                    "data": {
                        "initialPrompt": "Write Something...",
                        "addPrompt": "What stands out to you?"
                    },
                    "parent_id": postgres_id
                })

        return features

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
            "attributeKeys": "features, comments, buttontext, buttonlink",
        }

        r = requests.get(
                f"{Variable.get(kwargs['client'] + '_rock_api')}/ContentChannelItems",
                params=params,
                headers=headers)
        rock_objects = r.json()

        def fix_casing(col):
            return "\"{}\"".format(col)

        features_by_item = list(map(get_features, rock_objects))
        print(features_by_item)
        flat_features_list = [item for items in features_by_item for item in items]

        # "created_at", "updated_at", "apollos_type", "data", "type", "parent_id", "parent_type"
        def feature_attributes(obj):
            return (
                kwargs['execution_date'],
                kwargs['execution_date'],
                obj["type"] + "Feature",
                Json(obj['data']),
                obj["type"],
                obj['parent_id'],
                "ContentItem"
            )

        insert_features = list(map(feature_attributes, flat_features_list))

        columns = list(map(fix_casing, ("created_at", "updated_at", "apollos_type", "data", "type", "parent_id", "parent_type")))

        pg_hook.insert_rows(
            '"feature"',
            list(insert_features),
            columns,
            0,
            True,
            replace_index=('"parent_id"', '"type"', '"data"')
        )

        add_apollos_ids = """
        UPDATE feature
        SET apollos_id = apollos_type || ':' || id::varchar
        WHERE apollos_id IS NULL
        """

        pg_hook.run(add_apollos_ids)

        skip += top
        fetched_all = len(r.json()) < top

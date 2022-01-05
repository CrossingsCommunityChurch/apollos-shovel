from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import requests
from datetime import timedelta, datetime
from rock.rock_content_items import fetch_and_save_content_items
from rock.rock_media import fetch_and_save_media
from rock.rock_content_items_connections import (
    fetch_and_save_content_items_connections,
    set_content_item_parent_id,
    delete_content_item_connections,
)
from rock.rock_cover_image import fetch_and_save_cover_image
from rock.rock_content_item_categories import (
    fetch_and_save_content_item_categories,
    attach_content_item_categories,
)
from rock.rock_features import fetch_and_save_features, Feature
from rock.rock_deleted_content_items_dag import remove_deleted_content_items
from rock.utilities import (
    safeget_no_case,
    safeget,
    get_delta_offset_with_content_attributes,
)
from functools import reduce
import json

start_date = datetime(2021, 9, 8)

# Default settings applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


class VineyardFeature(Feature):
    def get_features(self, content):
        features = []
        # Add features from a key value list
        # This is now the only way to add scripture and text features
        # We previously supported a textFeature and

        organization_feature_name = safeget_no_case(
            content, "AttributeValues", "organizationName", "Value"
        )

        if organization_feature_name:
            organization_feature_logo_id = safeget_no_case(
                content, "AttributeValues", "organizationLogo", "Value"
            )

            rock_host = (Variable.get(self.kwargs["client"] + "_rock_api")).split(
                "/api"
            )[0]
            organization_feature_logo_url = (
                rock_host + "/GetImage.ashx?guid=" + organization_feature_logo_id
                if len(organization_feature_logo_id) > 0
                else ""
            )

            features.append(
                {
                    "type": "Organization",
                    "data": {
                        "name": organization_feature_name,
                        "logo": organization_feature_logo_url,
                    },
                    "parent_id": content["node_id"],
                }
            )

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
                "attributeKeys": "features, comments, initialPrompt, buttontext, buttonlink, completeButtonText, scriptures, location, eventdate, organizationName, organizationLogo",
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


def create_rock_content_item_dag(church, start_date, schedule_interval, do_backfill):
    tags = [church, "content"]
    name = f"{church}_rock_content_item_dag"
    if do_backfill:
        tags.append("backfill")
        name = f"{church}_backfill_rock_content_item_dag"

    dag = DAG(
        name,
        start_date=start_date,
        max_active_runs=1,
        schedule_interval=schedule_interval,
        default_args=default_args,
        tags=tags,
        # Let tasks run no more than three times longer than the schedule interval.
        dagrun_timeout=(
            schedule_interval * 3 if type(schedule_interval) is not str else None
        ),
    )

    with dag:
        base_items = PythonOperator(
            task_id="fetch_and_save_content_items",
            python_callable=fetch_and_save_content_items,  # make sure you don't include the () of the function
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        connections = PythonOperator(
            task_id="fetch_and_save_content_items_connections",
            python_callable=fetch_and_save_content_items_connections,  # make sure you don't include the () of the function
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        delete_connections = PythonOperator(
            task_id="delete_content_item_connections",
            python_callable=delete_content_item_connections,  # make sure you don't include the () of the function
            op_kwargs={"client": church},
        )

        media = PythonOperator(
            task_id="fetch_and_save_media",
            python_callable=fetch_and_save_media,  # make sure you don't include the () of the function
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        add_categories = PythonOperator(
            task_id="fetch_and_save_content_item_categories",
            python_callable=fetch_and_save_content_item_categories,  # make sure you don't include the () of the function
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        attach_categories = PythonOperator(
            task_id="attach_content_item_categories",
            python_callable=attach_content_item_categories,  # make sure you don't include the () of the function
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        set_parent_id = PythonOperator(
            task_id="set_parent_id",
            python_callable=set_content_item_parent_id,  # make sure you don't include the () of the function
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        set_cover_image = PythonOperator(
            task_id="fetch_and_save_cover_image",
            python_callable=fetch_and_save_cover_image,  # make sure you don't include the () of the function
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        features = PythonOperator(
            task_id="fetch_and_save_features",
            python_callable=fetch_and_save_features,  # make sure you don't include the () of the function
            op_kwargs={
                "client": church,
                "do_backfill": do_backfill,
                "klass": VineyardFeature,
            },
        )

        deleted_content_items = PythonOperator(
            task_id="remove_deleted_content_items",
            python_callable=remove_deleted_content_items,  # make sure you don't include the () of the function
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        # Adding and syncing categories depends on having content items
        base_items >> add_categories >> attach_categories

        media >> set_cover_image

        connections >> [delete_connections, set_parent_id] >> features

        deleted_content_items

        base_items >> [connections, media, deleted_content_items]

    return dag, name


backfill_dag, backfill_name = create_rock_content_item_dag(
    "vineyard", start_date, "@once", True
)
globals()[backfill_name] = backfill_dag

dag, dag_name = create_rock_content_item_dag(
    "vineyard", start_date, timedelta(minutes=30), False
)

globals()[dag_name] = dag

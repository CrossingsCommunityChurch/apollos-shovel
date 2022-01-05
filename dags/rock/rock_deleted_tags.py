from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
import requests


def remove_deleted_tags(ds, *args, **kwargs):
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

    postgres_tags = pg_hook.get_records("SELECT id, origin_id FROM tag")

    person_entity_id = requests.get(
        f"{Variable.get(kwargs['client'] + '_rock_api')}/EntityTypes",
        params={"$filter": "Name eq 'Rock.Model.Person'"},
        headers=headers,
    ).json()[0]["Id"]

    content_item_entity_id = requests.get(
        f"{Variable.get(kwargs['client'] + '_rock_api')}/EntityTypes",
        params={"$filter": "Name eq 'Rock.Model.ContentChannelItem'"},
        headers=headers,
    ).json()[0]["Id"]

    rock_config = Variable.get(kwargs["client"] + "_rock_config", deserialize_json=True)

    params = {
        "$filter": f"(EntityTypeId eq {person_entity_id} and CategoryId eq {rock_config['PERSONA_CATEGORY_ID']}) or EntityTypeId eq {content_item_entity_id}",
        "$select": "Id",
        "$orderby": "ModifiedDateTime desc",
    }

    data_view_tags = list(
        map(
            lambda tag: tag["Id"],
            requests.get(
                f"{Variable.get(kwargs['client'] + '_rock_api')}/DataViews",
                params=params,
                headers=headers,
            ).json(),
        )
    )

    params = {
        "$select": "Id",
        "$filter": f"EntityTypeId eq {content_item_entity_id}",
    }

    rock_tags = list(
        map(
            lambda tag: tag["Id"],
            requests.get(
                f"{Variable.get(kwargs['client'] + '_rock_api')}/Tags",
                params=params,
                headers=headers,
            ).json(),
        )
    )

    all_tags = data_view_tags + rock_tags

    print("ALL TAGS")
    print(all_tags)

    deleted_tags = list(
        map(
            lambda tag: tag[0],
            filter(lambda tag: int(tag[1]) not in all_tags, postgres_tags),
        )
    )

    if len(deleted_tags) > 0:
        pg_hook.run(
            """
            DELETE FROM tag
            WHERE id = ANY(%s::uuid[])
        """,
            True,
            (deleted_tags,),
        )

        print("Tags Deleted: " + str(len(deleted_tags)))
    else:
        print("No Content Tags Deleted")

from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
import requests


def remove_deleted_tags(ds, *args, **kwargs):
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
    
    postgresTags = pg_hook.get_records('SELECT id, "originId" FROM "tags"')

    person_entity_id = requests.get(
        f"{Variable.get(kwargs['client'] + '_rock_api')}/EntityTypes",
        params={"$filter": "Name eq 'Rock.Model.Person'"},
        headers=headers
    ).json()[0]['Id']

    params = {
        "$filter": f"EntityTypeId eq {person_entity_id} and CategoryId eq {186}",
        "$select": "Id",
        "$orderby": "ModifiedDateTime desc",
    }

    r = requests.get(
            f"{Variable.get(kwargs['client'] + '_rock_api')}/DataViews",
            params=params,
            headers=headers)
            
    rockTags = list(map(lambda tag: tag['Id'], r.json()));
    
    deletedTags = list(
        map(
            lambda tag: tag[0],
            filter(
                lambda tag: int(tag[1]) not in rockTags,
                postgresTags
            )
        )
    );

    if(len(deletedTags) > 0):
        pg_hook.run("""
            DELETE FROM "tags"
            WHERE id = ANY(%s::uuid[])
        """, True, (deletedTags,))

        print('Tags Deleted: ' + str(len(deletedTags)))
    else:
        print('No Content Tags Deleted')

    

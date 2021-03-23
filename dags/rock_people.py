from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
from apollos_type import apollos_id

import requests

def fetch_and_save_people(ds, *args, **kwargs):
    headers = {"Authorization-Token": Variable.get("rock_token")}

    fetched_all = False
    skip = 0
    top = 10000

    pg_hook = PostgresHook(postgres_conn_id='apollos_postgres',
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )

    # Rock stores gender as an integer
    gender_map = ('UNKNOWN', 'MALE', 'FEMALE')

    # Fetch the available campuses.
    campuses = pg_hook.get_records("""
        SELECT "originId", id
        FROM campuses
        WHERE "originType" = 'rock'
    """
    )

    campus_map = dict(campuses)
    campus_map['None'] = None

    while fetched_all == False:
        # Fetch people records from Rock.

        params = {
            "$top": top,
            "$skip": skip,
            "$expand": "Photo",
            "$select": "Id,NickName,LastName,Gender,BirthDate,PrimaryCampusId,Email,Photo/Path",
            "$orderby": "ModifiedDateTime desc",
        }

        if not kwargs['do_backfill']:
            params['$filter'] = f"ModifiedDateTime ge datetime'{kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}' or ModifiedDateTime eq null"

        print(params)

        r = requests.get(
                f"{Variable.get('rock_api')}/People",
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

        def update_people(obj):
            return (
                kwargs['execution_date'],
                kwargs['execution_date'],
                obj['Id'],
                'rock',
                'Person',
                obj['NickName'],
                obj['LastName'],
                gender_map[obj['Gender']],
                obj['BirthDate'],
                campus_map[str(obj["PrimaryCampusId"])],
                obj['Email'],
                get(obj, 'Photo', {}).get('Path') # may need to be a different attribute, depending on the church.
            )

        def fix_casing(col):
            return "\"{}\"".format(col)

        people_to_insert = list(map(update_people, rock_objects))
        columns = list(map(fix_casing, ("createdAt", "updatedAt", "originId", "originType", "apollosType", "firstName", "lastName", "gender", "birthDate", "campusId", "email", "profileImageUrl")))


        pg_hook.insert_rows(
            'people',
            people_to_insert,
            columns,
            0,
            True,
            replace_index = ('"originId"', '"originType"')
        )


        add_apollos_ids = """
        UPDATE people
        SET "apollosId" = 'Person:' || id::varchar
        WHERE "originType" = 'rock' and "apollosId" IS NULL
        """

        pg_hook.run(add_apollos_ids)


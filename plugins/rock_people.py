from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
from apollos_type import apollos_id

import requests
from pypika import Query, Table

def fetch_and_save_people(ds, *args, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='apollos_postgres')
    headers = {"Authorization-Token": Variable.get("rock_token")}

    fetched_all = False
    skip = 0
    top = 100

    people = Table('people')

    while fetched_all == False:
        # Fetch people records from Rock.
        r = requests.get(
                f"{Variable.get('rock_api')}/People",
                params={
                    "$top": top,
                    "$skip": skip,
                    "$orderby": "ModifiedDateTime desc",
                    "$filter": f"ModifiedDateTime ge datetime'{kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}' or ModifiedDateTime eq null"

                },
                headers=headers)
        rock_objects = r.json()

        if not isinstance(rock_objects, list):
            print(rock_objects)
            print("oh uh, we might have made a bad request")


        skip += top
        fetched_all = len(rock_objects) < top

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

        def update_people(obj):
            return (
                kwargs['execution_date'],
                kwargs['execution_date'],
                obj['Id'],
                'rock',
                'Person',
                obj['FirstName'],
                obj['LastName'],
                gender_map[obj['Gender']],
                obj['BirthDate'],
                campus_map[str(obj["PrimaryCampusId"])],
                obj['Email']
            )

        def fix_casing(col):
            return "\"{}\"".format(col)

        print("Rock objects here")
        print(rock_objects)
        print("Rock objects here")
        people_to_insert = list(map(update_people, rock_objects))
        columns = list(map(fix_casing, ("createdAt", "updatedAt", "originId", "originType", "apollosType", "firstName", "lastName", "gender", "birthDate", "campusId", "email")))


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


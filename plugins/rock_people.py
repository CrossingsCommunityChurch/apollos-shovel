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
                    "$orderby": "ModifiedDateTime asc",
                    "$filter": f"ModifiedDateTime ge datetime'{kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}' or ModifiedDateTime eq null"

                },
                headers=headers)
        rock_objects = r.json()
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

        # Turn the campuses into a dictonary, key is rock id, value is our internal id
        campus_map = dict(campuses)
        campus_map['None'] = None;

        def update_people(obj):
            return "('{current_date}', '{current_date}', '{id}', 'rock', 'Person', '{first_name}', '{last_name}', '{gender}', '{birth_date}', {campus_id}, '{email}')".format(**{
                'id': obj['Id'],
                'first_name': obj['FirstName'],
                'last_name': obj['LastName'],
                'current_date': kwargs['execution_date'],
                'gender': gender_map[obj['Gender']],
                'birth_date': obj['BirthDate'],
                'email': obj['Email'],
                'campus_id': campus_map[str(obj["PrimaryCampusId"])]
            })

        all_people = " , ".join(map(update_people, rock_objects))
        print(all_people)


        insert_all = """
        INSERT into people ("createdAt", "updatedAt", "originId", "originType", "apollosType", "firstName", "lastName", gender, "birthDate", "campusId", "email")
        VALUES {}
        ON CONFLICT ("originId", "originType")
        DO UPDATE SET ("updatedAt", "firstName", "lastName", gender, "birthDate", "campusId", "email") = (EXCLUDED."updatedAat", EXCLUDED."firstName", EXCLUDED."lastName", EXCLUDED."gender", EXCLUDED."birthDate", EXCLUDED."campusId", EXCLUDED."email")
        """.format(all_people)

        pg_hook.run(insert_all, parameters=({
            'current_date': kwargs['execution_date'],
            'people_updates': all_people
        }))


        add_apollos_ids = """
        UPDATE people
        SET "apollosId" = 'Person:' || id::varchar
        WHERE "originType" = 'rock' and "apollosId" IS NULL
        """

        pg_hook.run(add_apollos_ids)
        # for obj in rock_objects:

        #     # Insert or update each user.
        #     dts_insert = """
        #     INSERT into people ("createdAt", "updatedAt", "originId", "originType", "apollosType", "firstName", "lastName", gender, "birthDate", "campusId", "email")
        #     values (%(current_date)s, %(current_date)s, %(id)s, 'rock', 'Person', %(first_name)s, %(last_name)s, %(gender)s, %(birth_date)s, %(campus_id)s, %(email)s)
        #     ON CONFLICT ("originId", "originType")
        #     DO UPDATE SET ("updatedAt", "firstName", "lastName", gender, "birthDate", "campusId", "email") = (%(current_date)s, %(first_name)s, %(last_name)s, %(gender)s, %(birth_date)s, %(campus_id)s, %(email)s)
        #     """

        #     pg_hook.run(dts_insert, parameters=({
        #         'id': obj['Id'],
        #         'first_name': obj['FirstName'],
        #         'last_name': obj['LastName'],
        #         'current_date': kwargs['execution_date'],
        #         'gender': gender_map[obj['Gender']],
        #         'birth_date': obj['BirthDate'],
        #         'email': obj['Email'],
        #         'campus_id': campus_map[str(obj["PrimaryCampusId"])]
        #     }))

            # Adds apollos ids to all of our new users

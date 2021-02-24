from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
from apollos_type import apollos_id

import requests

def fetch_and_save_people(ds, *args, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='apollos_postgres')
    headers = {"Authorization-Token": Variable.get("rock_token")}

    fetched_all = False
    skip = 0
    top = 100

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


        for obj in rock_objects:

            # Insert or update each user.
            dts_insert = """
            INSERT into people ("createdAt", "updatedAt", "originId", "originType", "apollosType", "firstName", "lastName", gender, "birthDate", "campusId", "email")
            values (%(current_date)s, %(current_date)s, %(id)s, 'rock', 'Person', %(first_name)s, %(last_name)s, %(gender)s, %(birth_date)s, %(campus_id)s, %(email)s)
            ON CONFLICT ("originId", "originType")
            DO UPDATE SET ("updatedAt", "firstName", "lastName", gender, "birthDate", "campusId", "email") = (%(current_date)s, %(first_name)s, %(last_name)s, %(gender)s, %(birth_date)s, %(campus_id)s, %(email)s)
            """

            pg_hook.run(dts_insert, parameters=({
                'id': obj['Id'],
                'first_name': obj['FirstName'],
                'last_name': obj['LastName'],
                'current_date': kwargs['execution_date'],
                'gender': gender_map[obj['Gender']],
                'birth_date': obj['BirthDate'],
                'email': obj['Email'],
                'campus_id': campus_map[str(obj["PrimaryCampusId"])]
            }))

            # For all our "new" users (users we have pulled in who don't yet have an apollos id)
            users_without_apollos_id_select = """
            SELECT id from people
            WHERE "originType" = 'rock' and "apollosId" IS NULL
            """

            for new_id in pg_hook.get_records(users_without_apollos_id_select):
                apollos_id_update = """
                UPDATE people
                SET "apollosId" = %s
                WHERE id = %s::uuid
                """

                # Set their ID using the apollos_id method.
                pg_hook.run(
                    apollos_id_update,
                    parameters=((apollos_id('Person', new_id[0]), new_id[0]))
                )
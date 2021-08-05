from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
from utilities import safeget

import requests


def clean_string(string):
    if isinstance(string, str):
        return string.replace("\x00", "")
    return string


def fetch_and_save_people(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    headers = {"Authorization-Token": Variable.get(kwargs["client"] + "_rock_token")}

    fetched_all = False
    skip = 0
    top = 10000

    pg_connection = kwargs["client"] + "_apollos_postgres"
    pg_hook = PostgresHook(
        postgres_conn_id=pg_connection,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )

    # Rock stores gender as an integer
    gender_map = ("UNKNOWN", "MALE", "FEMALE")

    # Fetch the available campuses.
    campuses = pg_hook.get_records(
        """
        SELECT origin_id, id
        FROM campus
        WHERE origin_type = 'rock'
    """
    )

    campus_map = dict(campuses)
    campus_map["None"] = None

    while fetched_all == False:
        # Fetch people records from Rock.

        params = {
            "$top": top,
            "$skip": skip,
            "$expand": "Photo",
            "$select": "Id,NickName,LastName,Gender,BirthDate,PrimaryCampusId,Email,Photo/Path",
            "$orderby": "ModifiedDateTime desc",
        }

        if not kwargs["do_backfill"]:
            params[
                "$filter"
            ] = f"ModifiedDateTime ge datetime'{kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}' or ModifiedDateTime eq null"

        print(params)

        r = requests.get(
            f"{Variable.get(kwargs['client'] + '_rock_api')}/People",
            params=params,
            headers=headers,
        )
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

        def photo_url(path):
            if path is None:
                return None
            elif path.startswith("~"):
                rock_host = (Variable.get(kwargs["client"] + "_rock_api")).split(
                    "/api"
                )[0]
                return path.replace("~", rock_host)
            else:
                return path

        # "createdAt", "updatedAt", "originId", "originType", "apollosType", "firstName", "lastName", "gender", "birthDate", "campusId", "email", "profileImageUrl"
        def update_people(obj):
            return (
                kwargs["execution_date"],
                kwargs["execution_date"],
                obj["Id"],
                "rock",
                "Person",
                clean_string(obj["NickName"]),
                clean_string(obj["LastName"]),
                gender_map[obj["Gender"]],
                obj["BirthDate"],
                campus_map[str(obj["PrimaryCampusId"])],
                clean_string(obj["Email"]),
                photo_url(safeget(obj, "Photo", "Path")),
            )

        def fix_casing(col):
            return '"{}"'.format(col)

        people_to_insert = list(map(update_people, rock_objects))
        columns = list(
            map(
                fix_casing,
                (
                    "created_at",
                    "updated_at",
                    "origin_id",
                    "origin_type",
                    "apollos_type",
                    "first_name",
                    "last_name",
                    "gender",
                    "birth_date",
                    "campus_id",
                    "email",
                    "profile_image_url",
                ),
            )
        )

        pg_hook.insert_rows(
            "people",
            people_to_insert,
            columns,
            0,
            True,
            replace_index=('"origin_id"', '"origin_type"'),
        )

        add_apollos_ids = """
        UPDATE people
        SET apollos_id = 'Person:' || id::varchar
        WHERE origin_type = 'rock' and apollos_id IS NULL
        """

        pg_hook.run(add_apollos_ids)

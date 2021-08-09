from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
from utilities import safeget

import requests


class People:
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
        # Rock stores gender as an integer
        self.gender_map = ("UNKNOWN", "MALE", "FEMALE")
        self.campus_map = self.build_campus_map()

    def build_campus_map(self):
        # Fetch the available campuses.
        campuses = self.pg_hook.get_records(
            """
            SELECT origin_id, id
            FROM campus
            WHERE origin_type = 'rock'
        """
        )

        campus_map = dict(campuses)
        campus_map["None"] = None
        return campus_map

    def clean_string(self, string):
        if isinstance(string, str):
            return string.replace("\x00", "")
        return string

    def map_people_to_columns(self, obj):
        return (
            self.kwargs["execution_date"],
            self.kwargs["execution_date"],
            obj["Id"],
            "rock",
            "Person",
            self.clean_string(obj["NickName"]),
            self.clean_string(obj["LastName"]),
            self.gender_map[obj["Gender"]],
            obj["BirthDate"],
            self.campus_map[str(obj["PrimaryCampusId"])],
            self.clean_string(obj["Email"]),
            self.photo_url(safeget(obj, "Photo", "Path")),
        )

    def photo_url(self, path):
        if path is None:
            return None
        elif path.startswith("~"):
            rock_host = (Variable.get(self.kwargs["client"] + "_rock_api")).split(
                "/api"
            )[0]
            return path.replace("~", rock_host)
        else:
            return path

    def run_fetch_and_save_people(self):
        fetched_all = False
        skip = 0
        top = 10000

        while fetched_all == False:
            # Fetch people records from Rock.

            params = {
                "$top": top,
                "$skip": skip,
                "$expand": "Photo",
                "$select": "Id,NickName,LastName,Gender,BirthDate,PrimaryCampusId,Email,Photo/Path",
                "$orderby": "ModifiedDateTime desc",
            }

            if not self.kwargs["do_backfill"]:
                params[
                    "$filter"
                ] = f"ModifiedDateTime ge datetime'{self.kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}' or ModifiedDateTime eq null"

            print(params)

            r = requests.get(
                f"{Variable.get(self.kwargs['client'] + '_rock_api')}/People",
                params=params,
                headers=self.headers,
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

            people_to_insert = list(map(self.map_people_to_columns, rock_objects))
            columns = (
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
            )

            self.pg_hook.insert_rows(
                "people",
                people_to_insert,
                columns,
                0,
                True,
                replace_index=("origin_id", '"origin_type"'),
            )

            add_apollos_ids = """
            UPDATE people
            SET apollos_id = 'Person:' || id::varchar
            WHERE origin_type = 'rock' and apollos_id IS NULL
            """

            self.pg_hook.run(add_apollos_ids)


def fetch_and_save_people(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")
    Klass = People if "klass" not in kwargs else kwargs["klass"]
    people_task = Klass(kwargs)

    people_task.run_fetch_and_save_people()

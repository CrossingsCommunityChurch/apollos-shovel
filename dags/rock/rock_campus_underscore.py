from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
from apollos_type import apollos_id
from utilities import safeget
import requests


class Campus:
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

    def run_fetch_and_save_campuses(self):
        rock_objects = requests.get(
            f"{Variable.get(self.kwargs['client'] + '_rock_api')}/Campuses",
            params={
                "$top": 100,
                # "$filter": f"ModifiedDateTime ge datetime'{kwargs['execution_date'].strftime('%Y-%m-%dT00:00')}' or ModifiedDateTime eq null",
                "$expand": "CampusTypeValue, Location",
            },
            headers=self.headers,
        ).json()

        for obj in rock_objects:
            dts_insert = """
            INSERT into campus ("created_at", "updated_at", "origin_id", "origin_type", "apollos_type", "name", "street1", "street2", "city", "state", "postal_code", "latitude", "longitude", "digital")
            values (%(current_date)s, %(current_date)s, %(id)s, 'rock', 'Campus', %(name)s, %(street1)s, %(street2)s, %(city)s, %(state)s, %(postalCode)s, %(latitude)s, %(longitude)s, %(digital)s)
            ON CONFLICT (origin_id, origin_type)
            DO UPDATE SET ("updated_at", "name", "street1", "street2", "city", "state", "postal_code", "latitude", "longitude", "digital") = (%(current_date)s, %(name)s, %(street1)s, %(street2)s, %(city)s, %(state)s, %(postalCode)s, %(latitude)s, %(longitude)s, %(digital)s)
            """

            self.pg_hook.run(
                dts_insert,
                parameters=(
                    {
                        "current_date": self.kwargs["execution_date"],
                        "id": obj["Id"],
                        "name": obj["Name"],
                        "street1": safeget(obj, "Location", "Street1"),
                        "street2": safeget(obj, "Location", "Street2"),
                        "city": safeget(obj, "Location", "City"),
                        "state": safeget(obj, "Location", "State"),
                        "postalCode": safeget(obj, "Location", "PostalCode"),
                        "latitude": safeget(obj, "Location", "Latitude"),
                        "longitude": safeget(obj, "Location", "Longitude"),
                        "digital": safeget(obj, "CampusTypeValue", "Value") == "Online",
                    }
                ),
            )

            users_without_apollos_id_select = """
            SELECT id from campus
            WHERE origin_type = 'rock' and apollos_id IS NULL
            """

            for new_id in self.pg_hook.get_records(users_without_apollos_id_select):
                apollos_id_update = """
                UPDATE campus
                SET apollos_id = %s
                WHERE id = %s::uuid
                """

                self.pg_hook.run(
                    apollos_id_update,
                    parameters=((apollos_id("Campus", new_id[0]), new_id[0])),
                )


def fetch_and_save_campuses(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    Klass = Campus if "klass" not in kwargs else kwargs["klass"]

    campus_task = Klass(kwargs)

    campus_task.run_fetch_and_save_campuses()

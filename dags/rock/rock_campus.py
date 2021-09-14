from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from apollos_type import apollos_id
from rock.utilities import safeget
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
            INSERT into campuses ("createdAt", "updatedAt", "originId", "originType", "apollosType", "name", "street1", "street2", "city", "state", "postalCode", "latitude", "longitude", "digital")
            values (%(current_date)s, %(current_date)s, %(id)s, 'rock', 'Campus', %(name)s, %(street1)s, %(street2)s, %(city)s, %(state)s, %(postalCode)s, %(latitude)s, %(longitude)s, %(digital)s)
            ON CONFLICT ("originId", "originType")
            DO UPDATE SET ("updatedAt", "name", "street1", "street2", "city", "state", "postalCode", "latitude", "longitude", "digital") = (%(current_date)s, %(name)s, %(street1)s, %(street2)s, %(city)s, %(state)s, %(postalCode)s, %(latitude)s, %(longitude)s, %(digital)s)
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
            SELECT id from campuses
            WHERE "originType" = 'rock' and "apollosId" IS NULL
            """

            for new_id in self.pg_hook.get_records(users_without_apollos_id_select):
                apollos_id_update = """
                UPDATE campuses
                SET "apollosId" = %s
                WHERE id = %s::uuid
                """

                self.pg_hook.run(
                    apollos_id_update,
                    parameters=((apollos_id("Campus", new_id[0]), new_id[0])),
                )


def fetch_and_save_campuses(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    Klass = Campus if "klass" not in kwargs else kwargs["klass"]  # noqa N806

    campus_task = Klass(kwargs)

    campus_task.run_fetch_and_save_campuses()

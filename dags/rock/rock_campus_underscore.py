from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from rock.utilities import safeget, find_supported_fields
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

        data_to_insert, columns = find_supported_fields(
            pg_hook=self.pg_hook,
            table_name="campus",
            insert_data=[
                {
                    "current_date": self.kwargs["execution_date"],
                    "origin_id": obj["Id"],
                    "origin_type": "rock",
                    "apollos_type": "Campus",
                    "name": obj["Name"],
                    "street1": safeget(obj, "Location", "Street1"),
                    "street2": safeget(obj, "Location", "Street2"),
                    "city": safeget(obj, "Location", "City"),
                    "state": safeget(obj, "Location", "State"),
                    "postal_code": safeget(obj, "Location", "PostalCode"),
                    "latitude": safeget(obj, "Location", "Latitude"),
                    "longitude": safeget(obj, "Location", "Longitude"),
                    "digital": safeget(obj, "CampusTypeValue", "Value") == "Online",
                    "active": obj["IsActive"],
                    "created_at": self.kwargs["execution_date"],
                    "updated_at": self.kwargs["execution_date"],
                }
                for obj in rock_objects
            ],
        )

        self.pg_hook.insert_rows(
            "campus",
            data_to_insert,
            columns,
            0,
            True,
            replace_index=('"origin_id"', '"origin_type"'),
        )

        add_apollos_ids = """
        UPDATE campus
        SET apollos_id = apollos_type || ':' || id::varchar
        WHERE origin_type = 'rock' and apollos_id IS NULL
        """

        self.pg_hook.run(add_apollos_ids)


def fetch_and_save_campuses(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    Klass = Campus if "klass" not in kwargs else kwargs["klass"]  # noqa N806

    campus_task = Klass(kwargs)

    campus_task.run_fetch_and_save_campuses()

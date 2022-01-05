from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
import requests
from rock.rock_media import getsizes
from rock.utilities import find_supported_fields
import json


class CampusMedia:
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

    def parse_asset_url(self, guid):
        rock_host = (Variable.get(self.kwargs["client"] + "_rock_api")).split("/api")[0]
        return f"{rock_host}/GetImage.ashx?guid={guid}" if guid else None

    def add_node_id_to_rock_campus(self, campuses):
        if len(campuses) == 0:
            return []

        origin_ids = ", ".join(map(lambda r: f"'{str(r['Id'])}'", campuses))
        postgres_records = self.pg_hook.get_records(
            f"""
            SELECT campus.id, campus.origin_id
            FROM campus
            WHERE campus.origin_id in ({origin_ids})
            """
        )
        postgres_records_by_origin_id = {f[1]: f[0] for f in postgres_records}
        campuses_with_postgres_data = [
            {
                **campus,
                "node_id": postgres_records_by_origin_id.get(str(campus["Id"])),
            }
            for campus in campuses
        ]
        return campuses_with_postgres_data

    def get_image_metadata(self, url):
        image_dimensions = getsizes(url)
        return (
            json.dumps({"width": image_dimensions[0], "height": image_dimensions[1]})
            if image_dimensions and len(image_dimensions) == 2
            else None
        )

    def run_fetch_and_save_campus_media(self):
        rock_objects = requests.get(
            f"{Variable.get(self.kwargs['client'] + '_rock_api')}/Campuses",
            params={
                "$top": 100,
                "$expand": "CampusTypeValue, Location, Location/Image",
            },
            headers=self.headers,
        ).json()

        rock_campuses_with_node_id = self.add_node_id_to_rock_campus(rock_objects)

        media = [
            {
                "apollos_type": "IMAGE",
                "created_at": self.kwargs["execution_date"],
                "updated_at": self.kwargs["execution_date"],
                "node_id": campus["node_id"],
                "node_type": "Campus",
                "type": "IMAGE",
                "url": self.parse_asset_url(campus["Location"]["Image"].get("Guid")),
                "origin_id": campus["Location"]["Image"].get("Id"),
                "origin_type": "rock",
                "metadata": self.get_image_metadata(
                    self.parse_asset_url(campus["Location"]["Image"].get("Guid"))
                ),
            }
            for campus in rock_campuses_with_node_id
            if campus.get("Location", {"Image": None}).get("Image")
        ]

        data_to_insert, columns, constraints = find_supported_fields(
            pg_hook=self.pg_hook, table_name="media", insert_data=media
        )

        self.pg_hook.insert_rows(
            "media",
            data_to_insert,
            columns,
            0,
            True,
            replace_index=constraints,
        )

        for image in media:

            self.pg_hook.run(
                f"""
                UPDATE campus
                SET image_id = media.id
                FROM media
                WHERE campus.id = '{image['node_id']}'::uuid AND media.origin_id = '{image['origin_id']}'
                """
            )


def fetch_and_save_campus_media(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    Klass = CampusMedia if "klass" not in kwargs else kwargs["klass"]  # noqa N806

    campus_task = Klass(kwargs)

    campus_task.run_fetch_and_save_campus_media()

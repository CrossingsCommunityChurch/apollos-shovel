from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
import requests
from rock.rock_media import getsizes
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

    def parse_asset_url(self, value):
        if value:
            rock_host = (Variable.get(self.kwargs["client"] + "_rock_api")).split(
                "/api"
            )[0]
            return rock_host + "/GetImage.ashx?guid=" + value
        else:
            return value

    def add_postgres_data_to_rock_media(self, campuses):
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

    def map_campuses_to_images(self, campuses):
        campuses_with_images = []
        for campus in campuses:
            if (
                "Location" in campus
                and "Image" in campus["Location"]
                and campus["Location"]["Image"]
            ):
                url = self.parse_asset_url(campus["Location"]["Image"]["Guid"])
                metadata = {}
                image_dimensions = getsizes(url)
                if image_dimensions:
                    try:
                        metadata["width"] = image_dimensions[0]
                        metadata["height"] = image_dimensions[1]
                    except:  # noqa E722
                        print("Error getting media sizes")
                        print(image_dimensions)
                        print(url)

                campuses_with_images.append(
                    {
                        "campus_id": campus["node_id"],
                        "url": url,
                        "origin_id": campus["Location"]["Image"]["Id"],
                        "metadata": json.dumps(metadata),
                    }
                )

        return map(
            lambda c: [
                "IMAGE",
                self.kwargs["execution_date"],
                self.kwargs["execution_date"],
                c["campus_id"],
                "Campus",
                "IMAGE",
                c["url"],
                c["origin_id"],
                "rock",
                c["metadata"],
            ],
            campuses_with_images,
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

        media_with_postgres_id = self.add_postgres_data_to_rock_media(rock_objects)

        campuses_with_images = self.map_campuses_to_images(media_with_postgres_id)

        columns = (
            "apollos_type",
            "created_at",
            "updated_at",
            "node_id",
            "node_type",
            "type",
            "url",
            "origin_id",
            "origin_type",
            "metadata",
        )

        insert_list = list(campuses_with_images)

        self.pg_hook.insert_rows(
            "media",
            insert_list,
            columns,
            0,
            True,
            replace_index=("origin_id", "origin_type"),
        )

        for campus_image in insert_list:
            apollos_id_update = """
                UPDATE campus
                SET image_id = media.id
                FROM media
                WHERE campus.id = %s::uuid AND media.origin_id = %s
                """

            self.pg_hook.run(
                apollos_id_update,
                parameters=(str(campus_image[3]), str(campus_image[7])),
            )


def fetch_and_save_campus_media(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    Klass = CampusMedia if "klass" not in kwargs else kwargs["klass"]  # noqa N806

    campus_task = Klass(kwargs)

    campus_task.run_fetch_and_save_campus_media()

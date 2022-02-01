from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from rock.utilities import safeget, get_delta_offset, find_supported_fields
import requests


class PrayerRequest:
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

    # "created_at", "updated_at", "text", "person_id", "origin_id", "origin_type", "apollos_type"
    def map_prayer_requests(self, prayer_request):
        rock_id = prayer_request["RequestedByPersonAlias"]["PersonId"]
        postgres_person = self.pg_hook.get_first(
            f"SELECT id FROM people WHERE people.origin_id = '{rock_id}'"
        )
        # skip prayers that are from people not in the DB
        if not postgres_person:
            return None
        return {
            "created_at": safeget(prayer_request, "CreatedDateTime"),
            "updated_at": safeget(prayer_request, "ModifiedDateTime"),
            "text": safeget(prayer_request, "Text"),
            "person_id": postgres_person[0],
            "origin_id": str(safeget(prayer_request, "Id")),
            "origin_type": "rock",
            "apollos_type": "PrayerRequest",
            "approved": safeget(prayer_request, "IsApproved"),
            "flag_count": safeget(prayer_request, "FlagCount") or 0,
        }

    def run_fetch_and_save_prayer_requests(self):
        fetched_all = False
        skip = 0
        top = 10000

        while not fetched_all:
            # Fetch prayer requests from Rock

            params = {
                "$top": top,
                "$skip": skip,
                "$expand": "RequestedByPersonAlias",
                "$filter": "RequestedByPersonAlias ne null and CreatedByPersonAliasId ne null"
            }

            if not self.kwargs["do_backfill"]:
                params["$filter"] = get_delta_offset(self.kwargs)

            rock_objects = requests.get(
                f"{Variable.get(self.kwargs['client'] + '_rock_api')}/PrayerRequests",
                params=params,
                headers=self.headers,
            ).json()

            if not isinstance(rock_objects, list):
                print(rock_objects)
                print("oh uh, we might have made a bad request")
                print(f"top: {top}")
                print(f"skip: {skip}")
                skip += top
                continue

            skip += top
            fetched_all = len(rock_objects) < top

            # skip prayers with no requestor or that are private
            rock_prayers = [
                prayer
                for prayer in rock_objects
                if prayer["RequestedByPersonAlias"].get("PersonId")
                and prayer["IsPublic"]
            ]
            prayer_requests = [
                self.map_prayer_requests(prayer) for prayer in rock_prayers
            ]
            # filter out skipped prayers
            prayer_requests = [prayer for prayer in prayer_requests if prayer]

            data_to_insert, columns, constraints = find_supported_fields(
                pg_hook=self.pg_hook,
                table_name="prayer_request",
                insert_data=prayer_requests,
            )

            self.pg_hook.insert_rows(
                "prayer_request",
                data_to_insert,
                columns,
                0,
                True,
                replace_index=constraints,
            )

            add_apollos_ids = """
            UPDATE prayer_request
            SET apollos_id = apollos_type || ':' || id::varchar
            WHERE origin_type = 'rock' and apollos_id IS NULL
            """

            self.pg_hook.run(add_apollos_ids)

    def run_delete_prayer_requests(self):
        fetched_all = False
        skip = 0
        top = 10000
        rock_prayer_requests = []
        postgres_prayer_requests = self.pg_hook.get_records(
            "SELECT origin_id, id FROM prayer_request WHERE origin_id is not null"
        )

        while not fetched_all:
            # Fetch prayer requests from Rock
            params = {
                "$top": top,
                "$skip": skip,
            }

            rock_objects = requests.get(
                f"{Variable.get(self.kwargs['client'] + '_rock_api')}/PrayerRequests",
                params=params,
                headers=self.headers,
            ).json()

            if not isinstance(rock_objects, list):
                print(rock_objects)
                print("oh uh, we might have made a bad request")
                print(f"top: {top}")
                print(f"skip: {skip}")
                skip += top
                continue

            prayer_request_ids = list(map(lambda x: x["Id"], rock_objects))
            for prayer_request_id in prayer_request_ids:
                rock_prayer_requests.append(str(prayer_request_id))

            skip += top
            fetched_all = len(rock_objects) < top

        deleted_prayer_requests = list(
            filter(
                lambda prayer_request: prayer_request[0] not in rock_prayer_requests,
                postgres_prayer_requests,
            )
        )

        deleted_prayer_request_ids = list(
            map(lambda prayer_request: prayer_request[1], deleted_prayer_requests)
        )

        if len(deleted_prayer_request_ids) > 0:
            self.pg_hook.run(
                """
                    DELETE FROM prayer_request
                    WHERE id = ANY(%s::uuid[])
                """,
                True,
                (deleted_prayer_request_ids,),
            )


def fetch_and_save_prayer_request(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    prayer_request_task = (
        PrayerRequest(kwargs) if "klass" not in kwargs else kwargs["klass"](kwargs)
    )

    prayer_request_task.run_fetch_and_save_prayer_requests()


def delete_prayer_requests(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    prayer_request_task = (
        PrayerRequest(kwargs) if "klass" not in kwargs else kwargs["klass"](kwargs)
    )

    prayer_request_task.run_delete_prayer_requests()

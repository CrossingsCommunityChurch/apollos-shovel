from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

import requests


def fetch_and_save_personal_devices_to_apollos_user(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")
    headers = {"Authorization-Token": Variable.get(kwargs["client"] + "_rock_token")}

    fetched_all = False
    skip = 0
    top = 1000

    pg_connection = kwargs["client"] + "_apollos_postgres"
    pg_hook = PostgresHook(
        postgres_conn_id=pg_connection,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )

    while not fetched_all:
        # Fetch people records from Rock.

        params = {
            "$top": top,
            "$skip": skip,
            "$expand": "PersonAlias",
            "$orderby": "Id asc",
            "$filter": f"IsActive eq true and PersonalDeviceTypeValueId eq {kwargs['rock_mobile_device_type_id']}",
        }

        print(params)

        r = requests.get(
            f"{Variable.get(kwargs['client'] + '_rock_api')}/PersonalDevices",
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

        def update_statement(obj):
            return f"'{obj['PersonAlias']['PersonId']}'"

        devices_with_people = filter(
            lambda p: "PersonId" in p["PersonAlias"], rock_objects
        )
        people_update_statements = list(map(update_statement, devices_with_people))
        update_statements_joined = ",".join(people_update_statements)

        dts_insert = f"""
        UPDATE people
        SET \"apollosUser\" = True
        WHERE \"originType\" = 'rock' AND \"originId\" IN({update_statements_joined})
        """

        print(dts_insert)

        pg_hook.run(dts_insert)

from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
import requests


def find_asset_url(hopestream_media):
    for asset in hopestream_media:
        if 'streamUrl' in asset:
            return asset['streamUrl'] 

def set_hopestream_urls(ds, *args, **kwargs):
    if 'client' not in kwargs or kwargs['client'] is None:
        raise Exception("You must configure a client for this operator")

    pg_connection = kwargs['client'] + '_apollos_postgres'
    pg_hook = PostgresHook(postgres_conn_id=pg_connection,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )

    media_without_url = pg_hook.get_records("""
      SELECT id, url FROM media WHERE type = 'VIDEO' AND url NOT LIKE 'http%';
    """)

    for id, media_val in media_without_url:
        hopestream_media = requests.get(
            f"https://api.hopestream.com/guest/1/media/{media_val}",
            headers={'Authorization': f"Bearer {Variable.get(kwargs['client'] + '_hopestream_token')}"}
        )

        if not hopestream_media or "media" not in hopestream_media.json():
            continue

        asset_url = find_asset_url(hopestream_media.json()["media"])

        if asset_url:
            pg_hook.run(f"""
                UPDATE media SET url = '{asset_url}' WHERE id = '{id}';
            """)



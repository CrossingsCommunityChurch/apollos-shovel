from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
import requests


def set_wistia_urls(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    pg_connection = kwargs["client"] + "_apollos_postgres"
    pg_hook = PostgresHook(
        postgres_conn_id=pg_connection,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )

    media_without_url = pg_hook.get_records(
        """
      SELECT id, url FROM media WHERE type = 'VIDEO' AND url NOT LIKE 'http%';
    """
    )

    for id, media_val in media_without_url:
        wista_media = requests.get(
            f"https://api.wistia.com/v1/medias/{media_val}.json?access_token={Variable.get(kwargs['client'] + '_wistia_token')}",
        ).json()

        if not wista_media or "assets" not in wista_media:
            continue

        asset_url = ""
        print(wista_media)
        for asset in wista_media["assets"]:
            if asset["type"] == "HdMp4VideoFile" and asset["height"] == 720:
                asset_url = asset["url"].replace(".bin", ".m3u8")
            if asset["type"] == "IphoneVideoFile":
                asset_url = asset["url"].replace(".bin", "./file.mp4")

        asset_url = asset_url.replace("http://embed", "https://embed-ssl")

        pg_hook.run(
            f"""
            UPDATE media SET url = '{asset_url}' WHERE id = '{id}';
        """
        )

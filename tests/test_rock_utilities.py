from airflow.hooks.postgres_hook import PostgresHook
from .test_utils import create_test_database, db_connect

from dags.rock.utilities import find_supported_fields

create_test_database()

print("loaded")


def test_find_support_fields_constraints(monkeypatch):
    pg_hook = PostgresHook()
    monkeypatch.setattr(
        pg_hook,
        "get_conn",
        db_connect,
    )

    _, _, constraints = find_supported_fields(pg_hook, [], "content_item")

    assert constraints == ("origin_id", "origin_type")

    conn = db_connect()
    with conn:
        with conn.cursor() as curs:
            curs.execute(
                """
                ALTER TABLE "public"."content_item" ADD COLUMN "church_slug" text DEFAULT CURRENT_USER;
                DROP INDEX public."content_items_origin_id_origin_type";
                CREATE UNIQUE INDEX "content_items_church_slug_origin_id_origin_type" ON "public"."content_item"("church_slug","origin_id","origin_type");
                """
            )

    conn.close()

    _, _, constraints = find_supported_fields(pg_hook, [], "content_item")
    assert constraints == ("church_slug", "origin_id", "origin_type")

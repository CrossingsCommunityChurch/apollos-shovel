from airflow.models import Variable
import datetime
from .test_utils import create_test_database, db_connect
from dags.rock.rock_content_items import ContentItem
from dags.rock.rock_content_items_connections import ContentItemConnection
import vcr

create_test_database()


def test_run_fetch_and_save_content_item_connections(monkeypatch):
    def mock_get(config, deserialize_json=True, default_var=None):
        if default_var:
            return default_var
        if "_rock_api" in config:
            return "https://rock.apollos.app/api"
        if "_rock_token" in config:
            return "ASZjZWdf3IqrbZX9sedtB4wb"
        if "_rock_config":
            return {
                "CONTENT_MAPPINGS": {
                    "ContentSeriesContentItem": {"ContentChannelTypeId": [6]},
                    "DevotionalContentItem": {"ContentChannelId": [7]},
                    "WeekendContentItem": {"ContentChannelId": [5]},
                },
                "PERSONA_CATEGORY_ID": 186,
                "SERIES_CATEGORY_ORIGIN_IDS": [4, 33],
            }

    monkeypatch.setattr(
        Variable,
        "get",
        mock_get,
    )

    content_item = ContentItem(
        {
            "client": "test",
            "execution_date": datetime.datetime(
                2005, 7, 14, 12, 30, tzinfo=datetime.timezone.utc
            ),
            "do_backfill": True,
        }
    )

    content_item_connection = ContentItemConnection(
        {
            "client": "test",
            "execution_date": datetime.datetime(
                2005, 7, 14, 12, 30, tzinfo=datetime.timezone.utc
            ),
            "do_backfill": True,
        }
    )

    monkeypatch.setattr(
        content_item.pg_hook,
        "get_conn",
        db_connect,
    )

    monkeypatch.setattr(
        content_item_connection.pg_hook,
        "get_conn",
        db_connect,
    )

    with vcr.use_cassette(
        "tests/cassettes/content_item_connections/content_items.yaml"
    ):
        content_item.run_fetch_and_save_content_items()
    with vcr.use_cassette(
        "tests/cassettes/content_item_connections/initial_content_item_connections.yaml"
    ):
        content_item_connection.run_fetch_and_save_content_items_connections()

    conn = db_connect()
    with conn:
        with conn.cursor() as curs:
            # Check for initial parent content item
            curs.execute("SELECT id FROM content_item")

            parent_item_id = curs.fetchone()[0]

            # Check that initial content item connections are correct
            curs.execute(
                """
                SELECT parent_id, origin_id FROM content_item_connection;
                """
            )
            initial_content_item_connections = curs.fetchall()

            assert len(initial_content_item_connections) == 3

            expected = [
                (parent_item_id, "20"),
                (parent_item_id, "18"),
                (parent_item_id, "19"),
            ]

            i = 0
            for connection in initial_content_item_connections:
                assert connection == expected[i]
                i += 1

            # Delete content item connection
            with vcr.use_cassette(
                "tests/cassettes/content_item_connections/delete_content_item_connection.yaml"
            ):
                content_item_connection.run_delete_content_item_connections()

            curs.execute("SELECT parent_id, origin_id FROM content_item_connection;")
            content_item_connections_with_deletion = curs.fetchall()

            assert len(content_item_connections_with_deletion) == 2

            expected = [
                (parent_item_id, "20"),
                (parent_item_id, "18"),
            ]

            i = 0
            for connection in content_item_connections_with_deletion:
                assert connection == expected[i]
                i += 1

    conn.close()

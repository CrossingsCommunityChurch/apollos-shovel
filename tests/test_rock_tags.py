from airflow.models import Variable
import datetime
from .test_utils import create_test_database, db_connect
from dags.rock.rock_tags import Tag
from dags.rock.rock_content_items import ContentItem
import vcr

create_test_database()


def test_run_fetch_and_save_rock_tags(monkeypatch):
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

    tag = Tag(
        {
            "client": "test",
            "execution_date": datetime.datetime(
                2005, 7, 14, 12, 30, tzinfo=datetime.timezone.utc
            ),
            "do_backfill": True,
        }
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

    monkeypatch.setattr(
        tag.pg_hook,
        "get_conn",
        db_connect,
    )

    monkeypatch.setattr(
        content_item.pg_hook,
        "get_conn",
        db_connect,
    )

    with vcr.use_cassette("tests/cassettes/tags/content_items.yaml"):
        content_item.run_fetch_and_save_content_items()
    with vcr.use_cassette("tests/cassettes/tags/initial_tags.yml"):
        tag.run_fetch_and_save_rock_tags()
    with vcr.use_cassette("tests/cassettes/tags/tagged_items.yml"):
        tag.run_fetch_and_save_tagged_items()

    conn = db_connect()
    with conn:
        with conn.cursor() as curs:
            # Check that there are two tags
            curs.execute(
                """
                    SELECT id, name FROM tag
                """
            )
            initial_tags = curs.fetchall()
            assert len(initial_tags) == 2

            # Check that there are two content tags
            curs.execute(
                """
                    SELECT tag_id FROM content_tag
                """
            )
            content_tags = curs.fetchall()
            assert len(content_tags) == 2

    conn.close()

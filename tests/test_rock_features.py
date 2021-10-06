from airflow.models import Variable
import datetime
from .test_utils import create_test_database, db_connect
from dags.rock.rock_features import Feature
from dags.rock.rock_content_items import ContentItem
import vcr

create_test_database()


def test_run_fetch_and_save_features(monkeypatch):
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

    feature = Feature(
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
        feature.pg_hook,
        "get_conn",
        db_connect,
    )

    with vcr.use_cassette("tests/cassettes/features/content_items.yaml"):
        content_item.run_fetch_and_save_content_items()
    with vcr.use_cassette(
        "tests/cassettes/features/initial_features.yaml", record_mode="new_episodes"
    ):
        feature.run_fetch_and_save_features()

    conn = db_connect()
    with conn:
        with conn.cursor() as curs:
            # Check that initial features are correct
            curs.execute(
                """
                SELECT parent_type, type, data, priority FROM feature ORDER BY feature.priority ASC;
                """
            )
            initial_features = curs.fetchall()
            print(initial_features)
            assert len(initial_features) == 5
            expected = [
                ("ContentItem", "Text", {"text": "Some great text"}, 0),
                ("ContentItem", "Scripture", {"reference": "gen 3, gen 5"}, 1),
                ("ContentItem", "Scripture", {"reference": "gen 4"}, 2),
                ("ContentItem", "Text", {"text": "here here"}, 3),
                ("ContentItem", "Scripture", {"reference": "1 John 3:16-18"}, 4),
            ]

            i = 0
            for ftr in initial_features:
                assert ftr == expected[i]
                i += 1

            # Add feature in the middle
            with vcr.use_cassette(
                "tests/cassettes/features/added_feature.yaml",
                record_mode="new_episodes",
            ):
                feature.run_fetch_and_save_features()

            curs.execute(
                """
                SELECT parent_type, type, data, priority FROM feature ORDER BY feature.priority ASC;
                """
            )
            added_features = curs.fetchall()
            expected = [
                ("ContentItem", "Text", {"text": "Some great text"}, 0),
                ("ContentItem", "Scripture", {"reference": "gen 3, gen 5"}, 1),
                ("ContentItem", "Text", {"text": "added text feature"}, 2),
                ("ContentItem", "Scripture", {"reference": "gen 4"}, 3),
                ("ContentItem", "Text", {"text": "here here"}, 4),
                ("ContentItem", "Scripture", {"reference": "1 John 3:16-18"}, 5),
            ]

            i = 0
            for ftr in added_features:
                assert ftr == expected[i]
                i += 1

            # Delete feature
            with vcr.use_cassette(
                "tests/cassettes/features/delete_feature.yaml",
                record_mode="new_episodes",
            ):
                feature.run_fetch_and_save_features()

            curs.execute(
                """
                SELECT parent_type, type, data, priority FROM feature ORDER BY feature.priority ASC;
                """
            )
            deleted_features = curs.fetchall()
            expected = [
                ("ContentItem", "Text", {"text": "Some great text"}, 0),
                ("ContentItem", "Text", {"text": "added text feature"}, 1),
                ("ContentItem", "Scripture", {"reference": "gen 4"}, 2),
                ("ContentItem", "Text", {"text": "here here"}, 3),
                ("ContentItem", "Scripture", {"reference": "1 John 3:16-18"}, 4),
            ]
            i = 0
            for ftr in deleted_features:
                assert ftr == expected[i]
                i += 1

    conn.close()


def test_matrix_value_scriptures(monkeypatch):
    def mock_get(config, deserialize_json=True, default_var=None):
        if default_var:
            return default_var
        if "_rock_api" in config:
            return "https://rock.apollos.app/api"
        if "_rock_token" in config:
            return "ASZjZWdf3IqrbZX9sedtB4wb"

    monkeypatch.setattr(
        Variable,
        "get",
        mock_get,
    )

    feature = Feature(
        {
            "client": "test",
            "execution_date": datetime.datetime(
                2005, 7, 14, 12, 30, tzinfo=datetime.timezone.utc
            ),
            "do_backfill": True,
        }
    )

    feature.attribute_matrix_id = 10

    content = {
        "Id": "123",
        "parent_origin_id": None,
        "node_id": "some-guid-value",
        "AttributeValues": {
            "Scriptures": {
                "Value": "12321-123213-123123",
                "ValueFormatted": '{\n    "Attributes": [{\n            \n            "Book": "Psalm",\n            \n            "Reference": "22"\n    },{\n            \n            "Book": "Psalm",\n            \n            "Reference": "23"\n    },{\n            \n            "Book": "Psalm",\n            \n            "Reference": "24"\n    },{\n            \n            "Book": "Acts",\n            \n            "Reference": "17:1-15"\n    }]\n}',
            }
        },
        "Attributes": {"Scriptures": {"FieldTypeId": 10}},
    }

    result = feature.get_features(content)

    expected = [
        {
            "type": "Scripture",
            "data": {"reference": "Psalm 22"},
            "parent_id": "some-guid-value",
            "priority": 0,
        },
        {
            "type": "Scripture",
            "data": {"reference": "Psalm 23"},
            "parent_id": "some-guid-value",
            "priority": 1,
        },
        {
            "type": "Scripture",
            "data": {"reference": "Psalm 24"},
            "parent_id": "some-guid-value",
            "priority": 2,
        },
        {
            "type": "Scripture",
            "data": {"reference": "Acts 17:1-15"},
            "parent_id": "some-guid-value",
            "priority": 3,
        },
    ]

    i = 0
    for ftr in result["added_features"]:
        assert ftr == expected[i]
        i += 1

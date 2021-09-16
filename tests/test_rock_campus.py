from airflow.models import Variable
import datetime
import pytest
from .test_utils import create_test_database, db_connect

from dags.rock.rock_campus_underscore import Campus

create_test_database()


@pytest.mark.vcr()
def test_run_fetch_and_save_campuses(monkeypatch):
    def mock_get(config):
        if "_rock_api" in config:
            return "https://rock.apollos.app/api"
        if "_rock_token" in config:
            return "ASZjZWdf3IqrbZX9sedtB4wb"

    monkeypatch.setattr(
        Variable,
        "get",
        mock_get,
    )

    campus = Campus(
        {
            "client": "test",
            "execution_date": datetime.datetime(
                2005, 7, 14, 12, 30, tzinfo=datetime.timezone.utc
            ),
        }
    )

    monkeypatch.setattr(
        campus.pg_hook,
        "get_conn",
        db_connect,
    )

    campus.run_fetch_and_save_campuses()

    conn = db_connect()
    with conn:
        with conn.cursor() as curs:
            curs.execute(
                """
                SELECT
                    name,
                    street1,
                    street2,
                    city,
                    state,
                    postal_code,
                    latitude,
                    longitude,
                    image_url,
                    digital,
                    apollos_type,
                    origin_id,
                    origin_type,
                    created_at,
                    updated_at,
                    active,
                    image_id
                from campus
                order by origin_id asc;
                """
            )
            campuses = curs.fetchall()
            print(campuses)
            assert len(campuses) == 4
            expected = [
                (
                    "Main Campus",
                    "120 E 8th St",
                    "",
                    "Cincinnati",
                    "OH",
                    "45202-2118",
                    39.10501,
                    -84.51138,
                    None,
                    False,
                    "Campus",
                    "1",
                    "rock",
                    datetime.datetime(
                        2005,
                        7,
                        14,
                        12,
                        30,
                        tzinfo=datetime.timezone.utc,
                    ),
                    datetime.datetime(
                        2005,
                        7,
                        14,
                        12,
                        30,
                        tzinfo=datetime.timezone.utc,
                    ),
                    True,
                    None,
                ),
                (
                    "Cincinnati",
                    "120 E 8th St",
                    "",
                    "Cincinnati",
                    "OH",
                    "45202-2118",
                    39.10501,
                    -84.51138,
                    None,
                    False,
                    "Campus",
                    "2",
                    "rock",
                    datetime.datetime(
                        2005,
                        7,
                        14,
                        12,
                        30,
                        tzinfo=datetime.timezone.utc,
                    ),
                    datetime.datetime(
                        2005,
                        7,
                        14,
                        12,
                        30,
                        tzinfo=datetime.timezone.utc,
                    ),
                    True,
                    None,
                ),
                (
                    "Portland",
                    "22 SW 3rd Ave",
                    None,
                    "Portland",
                    "OR",
                    "97204-2713",
                    45.52252,
                    -122.67325,
                    None,
                    False,
                    "Campus",
                    "3",
                    "rock",
                    datetime.datetime(
                        2005,
                        7,
                        14,
                        12,
                        30,
                        tzinfo=datetime.timezone.utc,
                    ),
                    datetime.datetime(
                        2005,
                        7,
                        14,
                        12,
                        30,
                        tzinfo=datetime.timezone.utc,
                    ),
                    True,
                    None,
                ),
                (
                    "Remote Campus",
                    "120 E 8th St",
                    "",
                    "Cincinnati",
                    "OH",
                    "45202-2118",
                    39.10501,
                    -84.51138,
                    None,
                    True,
                    "Campus",
                    "4",
                    "rock",
                    datetime.datetime(
                        2005,
                        7,
                        14,
                        12,
                        30,
                        tzinfo=datetime.timezone.utc,
                    ),
                    datetime.datetime(
                        2005,
                        7,
                        14,
                        12,
                        30,
                        tzinfo=datetime.timezone.utc,
                    ),
                    True,
                    None,
                ),
            ]
            i = 0
            for campus in campuses:
                assert campus == expected[i]
                i += 1

    conn.close()

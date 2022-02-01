from airflow.hooks.postgres_hook import PostgresHook

from rock.utilities import find_supported_fields


# Only used for backfilling.
class KidsClubCategories:
    def __init__(self, kwargs):
        self.kwargs = kwargs
        self.pg_connection = kwargs["client"] + "_apollos_postgres"
        self.pg_hook = PostgresHook(
            postgres_conn_id=self.pg_connection,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )
        self.categories = [
            {"id": "older-kids", "title": "For Older Kids"},
            {"id": "younger-kids", "title": "For Younger Kids"},
        ]

    def map_category_to_columns(self, category):
        return {
            "created_at": self.kwargs["execution_date"],
            "updated_at": self.kwargs["execution_date"],
            "origin_id": category["id"],
            "origin_type": "contentful",
            "apollos_type": "ContentChannel",
            "title": category["title"],
        }

    def run_fetch_and_save_categories(self):
        # There's no fetching involved yet.
        # These categories don't exist in Contentful yet, so we'll manually create them.

        insert_data = list(map(self.map_category_to_columns, self.categories))
        content_to_insert, columns, constraint = find_supported_fields(
            pg_hook=self.pg_hook,
            table_name="content_item_category",
            insert_data=insert_data,
        )

        self.pg_hook.insert_rows(
            "content_item_category",
            content_to_insert,
            columns,
            0,
            True,
            replace_index=constraint,
        )

        add_apollos_ids = """
        UPDATE content_item_category
        SET apollos_id = apollos_type || ':' || id::varchar
        WHERE origin_type = 'contentful' and apollos_id IS NULL
        """

        self.pg_hook.run(add_apollos_ids)


def create_kids_club_categories(ds, *args, **kwargs):
    if "client" not in kwargs or kwargs["client"] is None:
        raise Exception("You must configure a client for this operator")

    klass = (
        KidsClubCategories if "klass" not in kwargs else kwargs["klass"]
    )  # noqa N806

    categories_task = klass(kwargs)

    categories_task.run_fetch_and_save_categories()

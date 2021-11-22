from airflow import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from algoliasearch.search_client import SearchClient


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def algolia(ds, *args, **kwargs):

    # pull content items from DB
    pg_connection = kwargs["client"] + "_apollos_postgres"
    pg_hook = PostgresHook(
        postgres_conn_id=pg_connection,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )

    items = []
    categories = []
    category_names = list(
        map(
            lambda r: f"'{r}'",
            Variable.get(
                kwargs["client"] + "_indexed_category_names",
                deserialize_json=True,
                default_var=[],
            ),
        )
    )

    active_filter = "(publish_at < NOW() or publish_at is null) and (expire_at > NOW() or expire_at is null)"

    if len(category_names) > 0:
        categories = pg_hook.get_records(
            f"select id from content_item_category where title in ({','.join(category_names)})"
        )
        category_list = [f"'{category[0]}'" for category in categories]

        items = pg_hook.get_records(
            f"select * from content_item where content_item_category_id in ({','.join(category_list)}) and {active_filter}"
        )
    else:
        items = pg_hook.get_records(f"select * from content_item where {active_filter}")

    media = pg_hook.get_records("select id, url from media")
    urls = {item[0]: item[1] for item in media}

    church_slug = pg_hook.get_records("select current_user")[0][0]

    client = SearchClient.create("Z0GWPR8XBE", Variable.get("multitenant_api_key"))
    index = client.init_index(f"ContentItem_{church_slug}")
    index.clear_objects()
    batch = [
        {
            "id": f"{item[9]}:{item[0]}",
            "title": item[1],
            "summary": item[2],
            "__typename": item[9],
            "coverImage": {"sources": [{"uri": urls.get(item[13], "")}]},
        }
        for item in items
    ]
    index.save_objects(batch, {"autoGenerateObjectIDIfNotExist": True})


def create_algolia_dag(church, start_date, schedule_interval):
    tags = [church, "algolia"]
    name = f"{church}_algolia_dag"

    dag = DAG(
        name,
        start_date=start_date,
        max_active_runs=1,
        schedule_interval=schedule_interval,
        default_args=default_args,
        tags=tags,
        catchup=False,
    )

    with dag:
        algolia_index = PythonOperator(
            task_id="index_items_with_algolia",
            python_callable=algolia,
            op_kwargs={"client": church},
        )

        algolia_index

    return dag, name

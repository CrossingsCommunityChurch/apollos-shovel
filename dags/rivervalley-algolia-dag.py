from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from algoliasearch.search_client import SearchClient


start_date = datetime(2021, 8, 31)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def algolia():

    # pull content items from DB
    pg_hook = PostgresHook(
        postgres_conn_id="rivervalley_apollos_postgres",
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    categories = pg_hook.get_records(
        "select id from content_item_category where title in ('Sermons', 'Sermon Series')"
    )
    categoryList = [f"'{category[0]}'" for category in categories]
    items = pg_hook.get_records(
        f"select * from content_item where content_item_category_id in ({','.join(categoryList)})"
    )
    media = pg_hook.get_records("select id, url from media")
    urls = {item[0]: item[1] for item in media}

    client = SearchClient.create("J5MIK3FKRK", Variable.get("rivervalley_algolia_key"))
    index = client.init_index("prod_ContentItem")
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
    )

    with dag:
        algolia_index = PythonOperator(
            task_id="index_items_with_algolia",
            python_callable=algolia,  # make sure you don't include the () of the function
        )

    return dag, name


dag, dag_name = create_algolia_dag("rivervalley", start_date, timedelta(minutes=30))

globals()[dag_name] = dag
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from rock.rock_features import fetch_and_save_features

# Default settings applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def create_daily_rock_feature_backfill(
    church, start_date, schedule_interval, do_backfill
):
    tags = [church, "content"]
    name = f"{church}_rock_features_backfill"
    dag = DAG(
        name,
        start_date=start_date,
        max_active_runs=1,
        schedule_interval=schedule_interval,
        default_args=default_args,
        tags=tags,
        # Let tasks run no more than three times longer than the schedule interval.
        dagrun_timeout=(
            schedule_interval * 3 if type(schedule_interval) is not str else None
        ),
    )

    with dag:

        features = PythonOperator(
            task_id="fetch_and_save_features",
            python_callable=fetch_and_save_features,  # make sure you don't include the () of the function
            op_kwargs={"client": church, "do_backfill": do_backfill},
        )

        features

    return dag, name

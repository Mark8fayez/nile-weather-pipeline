"""
Stage 3: run the Stage 2 pipeline automatically every morning with Airflow.

Every day at 06:00 Cairo time, Airflow loads YESTERDAY's weather (the last full day)
for every city in analytics.dim_city, by running the same command we ran by hand:
    python -m pipeline.extract_load --start <day> --end <day>
"""

from datetime import timedelta
from zoneinfo import ZoneInfo

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

CAIRO = ZoneInfo("Africa/Cairo")


def day_before(dag_run) -> str:
    """The day a run is responsible for: the day before the run's date, in Cairo time.

    Each run always loads ITS OWN day, so re-running an old run reloads that same day.
    A manual trigger can come without a logical_date, so we fall back to run_after.
    """
    run_time = dag_run.logical_date or dag_run.run_after
    return (run_time.astimezone(CAIRO).date() - timedelta(days=1)).isoformat()


with DAG(
    dag_id="nile_weather_daily",
    description="Load yesterday's weather for every Nile Basket city into PostgreSQL",
    schedule="0 6 * * *",  # cron: minute 0, hour 6, every day
    start_date=pendulum.datetime(2026, 11, 1, tz="Africa/Cairo"),
    catchup=True,  # don't automatically run every missed day since start_date
    default_args={
        "retries": 2,  # the API is down for a minute? try again
        "retry_delay": timedelta(minutes=5),
    },
    user_defined_macros={"day_before": day_before},  # lets the template below call it
    tags=["nile-basket", "weather"],
) as dag:
    extract_load = BashOperator(
        task_id="extract_load",
        bash_command=(
            "python -m pipeline.extract_load "
            "--start {{ day_before(dag_run) }} --end {{ day_before(dag_run) }}"
        ),
        cwd="/opt/airflow/project",  # the project folder, mounted into the container
    )

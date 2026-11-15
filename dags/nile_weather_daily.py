"""
Run the pipeline automatically every morning with Airflow.

Every day at 06:00 Cairo time:
  1. extract_load   : load YESTERDAY's weather (the last full day) for every city
  2. quality_checks : check the last 7 days (gaps, freshness, valid ranges, ...)

Both tasks run the same commands we can run by hand:
    python -m pipeline.extract_load   --start <day> --end <day>
    python -m pipeline.quality_checks --start <day - 6> --end <day>
"""

from datetime import timedelta
from zoneinfo import ZoneInfo

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

CAIRO = ZoneInfo("Africa/Cairo")


def day_before(dag_run, days: int = 1) -> str:
    """A day relative to the run's date, in Cairo time (default: the day before).

    Each run always works on ITS OWN days, so re-running an old run reloads those same days.
    A manual trigger can come without a logical_date, so we fall back to run_after.
    """
    run_time = dag_run.logical_date or dag_run.run_after
    return (run_time.astimezone(CAIRO).date() - timedelta(days=days)).isoformat()


with DAG(
    dag_id="nile_weather_daily",
    description="Load yesterday's weather for every Nile Basket city, then check data quality",
    schedule="0 6 * * *",  # cron: minute 0, hour 6, every day
    start_date=pendulum.datetime(2026, 11, 1, tz="Africa/Cairo"),
    catchup=True,  # missed days (e.g. the laptop was off) are run automatically
    max_active_runs=1,  # run missed days one at a time, in order, so checks see complete data
    default_args={
        "retries": 2,  # the API is down for a minute? try again
        "retry_delay": timedelta(minutes=5),
    },
    user_defined_macros={"day_before": day_before},  # lets the templates below call it
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

    quality_checks = BashOperator(
        task_id="quality_checks",
        bash_command=(
            "python -m pipeline.quality_checks "
            "--start {{ day_before(dag_run, 7) }} --end {{ day_before(dag_run) }}"
        ),
        cwd="/opt/airflow/project",
        retries=0,  # a failed check means bad data, not a temporary glitch: retrying won't fix it
    )

    extract_load >> quality_checks  # checks only run after a successful load

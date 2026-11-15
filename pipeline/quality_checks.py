"""
Stage 4: data quality checks on analytics.fact_weather_daily.

Runs after every load (in Airflow: extract_load >> quality_checks) over a window of days.
Each check is one SQL query that returns the PROBLEM rows: no rows = the check passed.

  severity "error"   -> the run fails (exit code 1, red in Airflow)
  severity "warning" -> logged and stored, but the run still succeeds
                        (for known, accepted issues like two cities sharing a grid point)

Every result is also saved in analytics.dq_check_results, to track quality over time.

Run from the project folder:
  python -m pipeline.quality_checks                                      # last 7 days
  python -m pipeline.quality_checks --start 2026-11-01 --end 2026-11-11
"""

import argparse
import logging
from dataclasses import dataclass
from datetime import date, timedelta

from pipeline.extract_load import connect

log = logging.getLogger("quality_checks")


@dataclass
class Check:
    name: str
    severity: str        # "error" or "warning"
    description: str
    sql: str             # returns one text column describing each problem row


CHECKS = [
    Check(
        "completeness", "error",
        "every city has a row for every day in the window (no gaps)",
        """
        SELECT c.city || ' is missing ' || d::date
        FROM analytics.dim_city c
        -- start at the window start, or at the first day we have data if that's later
        CROSS JOIN generate_series(
            GREATEST(%(start)s::date, (SELECT min(weather_date) FROM analytics.fact_weather_daily)),
            %(end)s::date, interval '1 day') AS d
        LEFT JOIN analytics.fact_weather_daily f
               ON f.city = c.city AND f.weather_date = d::date
        WHERE f.city IS NULL
        ORDER BY 1
        """,
    ),
    Check(
        "freshness", "error",
        "the latest day in the table is at least the end of the window",
        """
        SELECT 'latest day is ' || coalesce(max(weather_date)::text, 'none')
               || ', expected ' || %(end)s::date
        FROM analytics.fact_weather_daily
        HAVING coalesce(max(weather_date), DATE '1900-01-01') < %(end)s::date
        """,
    ),
    Check(
        "valid_ranges", "error",
        "values are physically plausible for Egypt and min <= max",
        """
        SELECT city || ' ' || weather_date || ': ' || problem
        FROM (
            SELECT city, weather_date,
                   CASE
                     WHEN temp_max_c NOT BETWEEN -5 AND 55   THEN 'temp_max_c out of range (' || temp_max_c || ')'
                     WHEN temp_min_c NOT BETWEEN -10 AND 50  THEN 'temp_min_c out of range (' || temp_min_c || ')'
                     WHEN temp_min_c > temp_max_c            THEN 'temp_min_c is above temp_max_c'
                     WHEN precipitation_mm NOT BETWEEN 0 AND 500 THEN 'precipitation_mm out of range (' || precipitation_mm || ')'
                     WHEN wind_max_kmh NOT BETWEEN 0 AND 200     THEN 'wind_max_kmh out of range (' || wind_max_kmh || ')'
                   END AS problem
            FROM analytics.fact_weather_daily
            WHERE weather_date BETWEEN %(start)s AND %(end)s
        ) x
        WHERE problem IS NOT NULL
        ORDER BY 1
        """,
    ),
    Check(
        "missing_values", "warning",
        "no measure is NULL (the API had no value for that day)",
        """
        SELECT city || ' ' || weather_date || ' has NULL measures'
        FROM analytics.fact_weather_daily
        WHERE weather_date BETWEEN %(start)s AND %(end)s
          AND (temp_max_c IS NULL OR temp_min_c IS NULL
               OR precipitation_mm IS NULL OR wind_max_kmh IS NULL)
        ORDER BY 1
        """,
    ),
    Check(
        "sudden_jumps", "warning",
        "max temperature doesn't change by more than 15 C from one day to the next",
        """
        SELECT city || ' ' || weather_date || ': max went from ' || prev_max || ' to ' || temp_max_c
        FROM (
            SELECT city, weather_date, temp_max_c,
                   LAG(temp_max_c) OVER (PARTITION BY city ORDER BY weather_date) AS prev_max
            FROM analytics.fact_weather_daily
            WHERE weather_date BETWEEN %(start)s::date - 1 AND %(end)s
        ) x
        WHERE weather_date >= %(start)s AND abs(temp_max_c - prev_max) > 15
        ORDER BY 1
        """,
    ),
    Check(
        "shared_grid_point", "warning",
        "each city gets its own weather grid point from the API",
        """
        WITH latest AS (
            SELECT DISTINCT ON (city) city,
                   payload->>'latitude' AS lat, payload->>'longitude' AS lon
            FROM raw.weather_api_responses
            ORDER BY city, fetched_at DESC
        )
        SELECT string_agg(city, ' and ' ORDER BY city) || ' share grid point ' || lat || ', ' || lon
        FROM latest
        GROUP BY lat, lon
        HAVING count(*) > 1
        """,
    ),
]


@dataclass
class Result:
    check: Check
    problems: list[str]

    @property
    def passed(self) -> bool:
        return not self.problems


def run_checks(conn, start: date, end: date) -> list[Result]:
    params = {"start": start, "end": end}
    results = []
    with conn.cursor() as cur:
        for check in CHECKS:
            cur.execute(check.sql, params)
            results.append(Result(check, [row[0] for row in cur.fetchall()]))
    return results


def save_results(conn, start: date, end: date, results: list[Result]) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO analytics.dq_check_results "
                "(window_start, window_end, check_name, severity, passed, failing_rows, details) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [(start, end, r.check.name, r.check.severity, r.passed, len(r.problems),
                  "; ".join(r.problems[:20]) or None) for r in results],
            )


def failed_errors(results: list[Result]) -> list[Result]:
    """Only failed checks with severity 'error' make the run fail."""
    return [r for r in results if not r.passed and r.check.severity == "error"]


def report(results: list[Result]) -> None:
    for r in results:
        status = "PASS" if r.passed else ("FAIL" if r.check.severity == "error" else "WARN")
        log.info("%-4s %-18s %s", status, r.check.name, r.check.description)
        for problem in r.problems[:10]:
            log.info("       - %s", problem)
        if len(r.problems) > 10:
            log.info("       ... and %d more", len(r.problems) - 10)


def parse_args() -> tuple[date, date]:
    yesterday = date.today() - timedelta(days=1)
    p = argparse.ArgumentParser(description="Run data quality checks on the weather tables")
    p.add_argument("--end", type=date.fromisoformat, default=yesterday)
    p.add_argument("--start", type=date.fromisoformat, default=None,
                   help="default: 6 days before --end (a 7-day window)")
    args = p.parse_args()
    start = args.start or args.end - timedelta(days=6)
    if start > args.end:
        p.error("--start must be on or before --end")
    return start, args.end


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    start, end = parse_args()
    with connect() as conn:
        log.info("checking %s to %s", start, end)
        results = run_checks(conn, start, end)
        save_results(conn, start, end, results)
    report(results)

    errors = failed_errors(results)
    warnings = [r for r in results if not r.passed and r.check.severity == "warning"]
    log.info("done: %d passed, %d warnings, %d errors",
             sum(r.passed for r in results), len(warnings), len(errors))
    if errors:
        raise SystemExit(1)  # red in Airflow


if __name__ == "__main__":
    main()

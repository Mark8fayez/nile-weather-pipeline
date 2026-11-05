"""
Stage 2: Extract daily weather from the Open-Meteo API and load it into PostgreSQL.

For every city in analytics.dim_city and every requested day:
  1. Extract    : ask the API for that city and that day
  2. Load raw   : keep the full JSON response in raw.weather_api_responses
  3. Transform  : pick the 4 daily values out of the JSON
  4. Upsert     : write them to analytics.fact_weather_daily (one row per city per day)

Run from the project folder:
  python -m pipeline.extract_load                                  # yesterday
  python -m pipeline.extract_load --start 2026-10-28 --end 2026-11-03   # backfill
"""

import argparse
import logging
import os
import time
from datetime import date, timedelta

import psycopg
import requests
from dotenv import load_dotenv
from psycopg.types.json import Jsonb

API_URL = "https://api.open-meteo.com/v1/forecast"

# API variable name -> our column name in analytics.fact_weather_daily
DAILY_VARIABLES = {
    "temperature_2m_max": "temp_max_c",
    "temperature_2m_min": "temp_min_c",
    "precipitation_sum": "precipitation_mm",
    "wind_speed_10m_max": "wind_max_kmh",
}

MAX_PAST_DAYS = 92  # the forecast API only keeps about 3 months of past days

log = logging.getLogger("extract_load")


# ---------- database ----------

def connect() -> psycopg.Connection:
    """Open a connection using the settings in .env (no passwords in the code)."""
    load_dotenv()
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5433"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
        autocommit=True,  # we open our own transaction for each city-day below
    )


def get_cities(conn) -> list[tuple]:
    """The cities to fetch come from the database, not from the code."""
    with conn.cursor() as cur:
        cur.execute("SELECT city, latitude, longitude FROM analytics.dim_city ORDER BY city")
        return cur.fetchall()


UPSERT_SQL = """
INSERT INTO analytics.fact_weather_daily
    (city, weather_date, temp_max_c, temp_min_c, precipitation_mm, wind_max_kmh)
VALUES
    (%(city)s, %(weather_date)s, %(temp_max_c)s, %(temp_min_c)s, %(precipitation_mm)s, %(wind_max_kmh)s)
ON CONFLICT (city, weather_date) DO UPDATE SET
    temp_max_c       = EXCLUDED.temp_max_c,
    temp_min_c       = EXCLUDED.temp_min_c,
    precipitation_mm = EXCLUDED.precipitation_mm,
    wind_max_kmh     = EXCLUDED.wind_max_kmh,
    loaded_at        = now()
"""


def load(conn, city: str, day: date, url: str, payload: dict, row: dict) -> None:
    """Save the raw JSON and the clean row together: both succeed or neither does."""
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO raw.weather_api_responses (city, weather_date, source_url, payload) "
                "VALUES (%s, %s, %s, %s)",
                (city, day, url, Jsonb(payload)),
            )
            cur.execute(UPSERT_SQL, {"city": city, "weather_date": day, **row})


# ---------- API ----------

def fetch_weather(lat, lon, day: date, retries: int = 3) -> tuple[str, dict]:
    """Call the API for one place and one day. Returns (url, json)."""
    params = {
        "latitude": float(lat),
        "longitude": float(lon),
        "daily": ",".join(DAILY_VARIABLES),
        "timezone": "Africa/Cairo",
        "start_date": day.isoformat(),
        "end_date": day.isoformat(),
    }
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(API_URL, params=params, timeout=30)
            if resp.status_code < 500:
                resp.raise_for_status()  # 4xx = our request is wrong, retrying won't help
                return resp.url, resp.json()
            problem = f"server error {resp.status_code}"
        except (requests.ConnectionError, requests.Timeout) as err:
            problem = type(err).__name__
        if attempt == retries:
            raise RuntimeError(f"API failed after {retries} attempts: {problem}")
        wait = 2 ** attempt
        log.warning("attempt %d failed (%s), retrying in %ds", attempt, problem, wait)
        time.sleep(wait)


# ---------- transform ----------

def parse_daily(payload: dict, day: date) -> dict:
    """Pick our 4 values for `day` out of the API response."""
    daily = payload["daily"]
    if daily["time"] != [day.isoformat()]:
        raise ValueError(f"expected data for {day}, got {daily['time']}")
    return {column: daily[api_name][0] for api_name, column in DAILY_VARIABLES.items()}


# ---------- run ----------

def days_between(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def parse_args() -> tuple[date, date]:
    yesterday = date.today() - timedelta(days=1)
    p = argparse.ArgumentParser(description="Load daily weather into PostgreSQL")
    p.add_argument("--start", type=date.fromisoformat, default=yesterday)
    p.add_argument("--end", type=date.fromisoformat, default=yesterday)
    args = p.parse_args()
    if args.start > args.end:
        p.error("--start must be on or before --end")
    if args.end > yesterday:
        p.error("--end can be yesterday at the latest (today is not finished yet)")
    if args.start < date.today() - timedelta(days=MAX_PAST_DAYS):
        p.error(f"--start can be at most {MAX_PAST_DAYS} days ago")
    return args.start, args.end


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    start, end = parse_args()
    days = days_between(start, end)
    loaded = failed = 0

    with connect() as conn:
        cities = get_cities(conn)
        log.info("loading %d cities x %d days (%s to %s)", len(cities), len(days), start, end)
        for city, lat, lon in cities:
            for day in days:
                try:
                    url, payload = fetch_weather(lat, lon, day)
                    row = parse_daily(payload, day)
                    load(conn, city, day, url, payload, row)
                    loaded += 1
                    log.info("%-10s %s  max %s C, min %s C, rain %s mm",
                             city, day, row["temp_max_c"], row["temp_min_c"], row["precipitation_mm"])
                except Exception as err:  # one bad city-day must not stop the others
                    failed += 1
                    log.error("%-10s %s  FAILED: %s", city, day, err)

    log.info("done: %d loaded, %d failed", loaded, failed)
    if failed:
        raise SystemExit(1)  # a non-zero exit code tells Airflow (Stage 3) the run failed


if __name__ == "__main__":
    main()

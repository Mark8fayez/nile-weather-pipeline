from datetime import date

import pytest

from pipeline.extract_load import days_between, parse_daily, retry_wait

# A real-shaped Open-Meteo response for one city and three days
SAMPLE = {
    "latitude": 30.0,
    "longitude": 31.25,
    "timezone": "Africa/Cairo",
    "daily_units": {"time": "iso8601", "temperature_2m_max": "°C"},
    "daily": {
        "time": ["2026-11-01", "2026-11-02", "2026-11-03"],
        "temperature_2m_max": [29.3, 31.7, 27.4],
        "temperature_2m_min": [16.0, 18.8, 17.9],
        "precipitation_sum": [0.0, 0.0, 1.2],
        "wind_speed_10m_max": [12.1, 9.8, 14.2],
    },
}
DAYS = [date(2026, 11, 1), date(2026, 11, 2), date(2026, 11, 3)]


def test_parse_daily_turns_parallel_arrays_into_one_row_per_day():
    rows = parse_daily(SAMPLE, DAYS)
    assert len(rows) == 3
    assert rows[1] == {"weather_date": date(2026, 11, 2), "temp_max_c": 31.7, "temp_min_c": 18.8,
                       "precipitation_mm": 0.0, "wind_max_kmh": 9.8}


def test_parse_daily_rejects_the_wrong_days():
    with pytest.raises(ValueError):
        parse_daily(SAMPLE, [date(2026, 11, 2), date(2026, 11, 3), date(2026, 11, 4)])


def test_parse_daily_rejects_a_missing_day():
    with pytest.raises(ValueError):
        parse_daily(SAMPLE, DAYS[:2])


def test_parse_daily_keeps_missing_values_as_none():
    payload = {"daily": {**SAMPLE["daily"], "precipitation_sum": [0.0, None, 1.2]}}
    assert parse_daily(payload, DAYS)[1]["precipitation_mm"] is None


def test_days_between_includes_both_ends():
    assert days_between(date(2026, 11, 1), date(2026, 11, 3)) == DAYS


class FakeResponse:
    def __init__(self, headers):
        self.headers = headers


@pytest.mark.parametrize("headers, attempt, expected", [
    ({}, 1, 2),                        # no header: exponential backoff 2, 4, 8 ...
    ({}, 3, 8),
    ({"Retry-After": "30"}, 1, 30),    # the API told us how long to wait (HTTP 429)
    ({"Retry-After": "3600"}, 1, 60),  # but never wait more than a minute
    ({"Retry-After": "soon"}, 2, 4),   # not a number: fall back to backoff
])
def test_retry_wait(headers, attempt, expected):
    assert retry_wait(FakeResponse(headers), attempt) == expected


def test_retry_wait_without_a_response():  # connection error or timeout: no response at all
    assert retry_wait(None, 2) == 4

from datetime import date

import pytest

from pipeline.extract_load import days_between, parse_daily

# A real-shaped Open-Meteo response for one city and one day
SAMPLE = {
    "latitude": 30.0,
    "longitude": 31.25,
    "timezone": "Africa/Cairo",
    "daily_units": {"time": "iso8601", "temperature_2m_max": "°C"},
    "daily": {
        "time": ["2026-11-03"],
        "temperature_2m_max": [27.4],
        "temperature_2m_min": [17.9],
        "precipitation_sum": [0.0],
        "wind_speed_10m_max": [14.2],
    },
}


def test_parse_daily_maps_api_names_to_our_columns():
    row = parse_daily(SAMPLE, date(2026, 11, 3))
    assert row == {"temp_max_c": 27.4, "temp_min_c": 17.9, "precipitation_mm": 0.0, "wind_max_kmh": 14.2}


def test_parse_daily_rejects_the_wrong_day():
    with pytest.raises(ValueError):
        parse_daily(SAMPLE, date(2026, 11, 4))


def test_parse_daily_keeps_missing_values_as_none():
    payload = {"daily": {**SAMPLE["daily"], "precipitation_sum": [None]}}
    assert parse_daily(payload, date(2026, 11, 3))["precipitation_mm"] is None


def test_days_between_includes_both_ends():
    assert days_between(date(2026, 11, 1), date(2026, 11, 3)) == [
        date(2026, 11, 1), date(2026, 11, 2), date(2026, 11, 3)]

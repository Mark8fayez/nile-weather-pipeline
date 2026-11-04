-- Two schemas = two layers of the pipeline:
--   raw       : the API response exactly as received (never edited)
--   analytics : clean, typed tables that dashboards and analysts query
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Raw layer: one row per API call, the full JSON kept as evidence
CREATE TABLE IF NOT EXISTS raw.weather_api_responses (
    response_id   BIGSERIAL   PRIMARY KEY,
    city          TEXT        NOT NULL,
    weather_date  DATE        NOT NULL,
    source_url    TEXT        NOT NULL,
    payload       JSONB       NOT NULL,
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Dimension table: the cities Nile Basket delivers to
CREATE TABLE IF NOT EXISTS analytics.dim_city (
    city       TEXT         PRIMARY KEY,
    latitude   NUMERIC(8,4) NOT NULL,
    longitude  NUMERIC(8,4) NOT NULL
);

INSERT INTO analytics.dim_city (city, latitude, longitude) VALUES
    ('Cairo',      30.0444, 31.2357),
    ('Giza',       30.0131, 31.2089),
    ('Alexandria', 31.2001, 29.9187),
    ('Mansoura',   31.0409, 31.3785),
    ('Tanta',      30.7865, 31.0004)
ON CONFLICT (city) DO NOTHING;

-- Fact table: one row per city per day.
-- The primary key (city, weather_date) makes duplicates impossible.
CREATE TABLE IF NOT EXISTS analytics.fact_weather_daily (
    city              TEXT         NOT NULL REFERENCES analytics.dim_city (city),
    weather_date      DATE         NOT NULL,
    temp_max_c        NUMERIC(5,2),
    temp_min_c        NUMERIC(5,2),
    precipitation_mm  NUMERIC(6,2),
    wind_max_kmh      NUMERIC(6,2),
    loaded_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (city, weather_date)
);
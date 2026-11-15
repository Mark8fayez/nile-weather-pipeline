-- A reporting view for analysts and dashboards: the fact table joined to its dimension,
-- plus a few derived columns, so nobody has to rewrite these formulas in every query.

CREATE VIEW analytics.v_daily_weather AS
SELECT
    f.city,
    f.weather_date,
    EXTRACT(ISODOW FROM f.weather_date)::int IN (5, 6) AS is_weekend,  -- Friday, Saturday in Egypt
    f.temp_max_c,
    f.temp_min_c,
    f.temp_max_c - f.temp_min_c                         AS temp_range_c,
    f.precipitation_mm,
    f.precipitation_mm >= 1.0                           AS is_rainy_day,  -- 1 mm = common "wet day" threshold
    f.temp_max_c >= 40                                  AS is_hot_day,
    f.wind_max_kmh,
    c.latitude,
    c.longitude
FROM analytics.fact_weather_daily f
JOIN analytics.dim_city c USING (city);

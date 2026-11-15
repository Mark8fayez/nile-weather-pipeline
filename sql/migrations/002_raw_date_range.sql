-- One API call now covers a whole date range for a city (instead of one call per day),
-- so a raw response needs a start and an end date instead of a single weather_date.
-- Existing rows covered exactly one day, so end_date = start_date for them.

ALTER TABLE raw.weather_api_responses RENAME COLUMN weather_date TO start_date;
ALTER TABLE raw.weather_api_responses ADD COLUMN end_date DATE;
UPDATE raw.weather_api_responses SET end_date = start_date;
ALTER TABLE raw.weather_api_responses ALTER COLUMN end_date SET NOT NULL;
ALTER TABLE raw.weather_api_responses
    ADD CONSTRAINT raw_date_range_valid CHECK (start_date <= end_date);

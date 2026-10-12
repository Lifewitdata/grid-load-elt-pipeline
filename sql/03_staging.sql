-- 02_staging.sql: the first transform - text to typed, with reasons kept.
-- Nothing here silently discards a row: parse failures become flags that the
-- data-quality suite counts and can drill into.

CREATE OR REPLACE VIEW stg.observation AS
SELECT
    r.run_id,
    r.source_row,
    r.unit,
    -- timestamp: strict ISO 'YYYY-MM-DDTHH:MM:SSZ' -> naive UTC timestamp
    CASE WHEN r.ts_text ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'
         THEN to_timestamp(r.ts_text, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AT TIME ZONE 'UTC'
    END AS ts_utc,
    CASE WHEN r.value_text ~ '^-?[0-9]+(\.[0-9]+)?$'
         THEN r.value_text::numeric
    END AS value,
    split_part(r.unit, '_', 1) AS country,
    CASE
        WHEN r.unit LIKE '%\_load\_actual\_entsoe\_transparency' THEN 'load_actual'
        WHEN r.unit LIKE '%\_load\_forecast\_entsoe\_transparency' THEN 'load_forecast'
        WHEN r.unit LIKE '%\_wind\_generation\_actual' THEN 'wind_generation'
        WHEN r.unit LIKE '%\_solar\_generation\_actual' THEN 'solar_generation'
    END AS metric,
    (r.ts_text !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$')
        AS ts_unparseable,
    (r.value_text !~ '^-?[0-9]+(\.[0-9]+)?$') AS value_nonnumeric
FROM raw.observation r;

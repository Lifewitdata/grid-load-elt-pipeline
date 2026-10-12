-- 03_core.sql: typed, deduplicated time-series core, partitioned by year.
-- Storage decision that matters for this workload (README: measured): range
-- partitioning on the timestamp lets a query that filters a date window skip
-- whole year files/pages instead of scanning and discarding.

CREATE TABLE IF NOT EXISTS core.observation (
    ts_utc   timestamp NOT NULL,          -- UTC, hourly
    unit     text NOT NULL,
    country  text NOT NULL,
    metric   text NOT NULL,
    value    numeric NOT NULL,            -- MW as published
    source_row bigint NOT NULL,
    run_id   bigint NOT NULL,
    PRIMARY KEY (ts_utc, unit)
) PARTITION BY RANGE (ts_utc);

DO $$
DECLARE y int;
BEGIN
    FOR y IN 2015..2021 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS core.observation_%s PARTITION OF core.observation
             FOR VALUES FROM (%L) TO (%L)', y, make_date(y,1,1), make_date(y+1,1,1));
    END LOOP;
END $$;

CREATE INDEX IF NOT EXISTS core_obs_metric_idx ON core.observation (metric, ts_utc);
CREATE INDEX IF NOT EXISTS core_obs_country_idx ON core.observation (country, metric, ts_utc);

-- Full reload semantics: rebuilt from landing on every run, so the core is a
-- deterministic function of the pinned file.
TRUNCATE core.observation;

INSERT INTO core.observation (ts_utc, unit, country, metric, value, source_row, run_id)
SELECT DISTINCT ON (ts_utc, unit) ts_utc, unit, country, metric, value, source_row, run_id
FROM stg.observation
WHERE ts_utc IS NOT NULL
  AND value IS NOT NULL
  AND extract(year FROM ts_utc) >= 2000      -- no 1970-epoch sentinel survived this file
ORDER BY ts_utc, unit, source_row;

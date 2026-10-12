-- 04_analytics.sql: modelling for the questions a grid analyst actually asks.
-- All tables are rebuilt from core.observation in one transaction; the
-- dashboard reads ONLY these + the views, never raw scans.
--
-- Cleansing policy (documented, measured, never silent): the file publishes a
-- handful of physically impossible cells (a 2,170,811 MW spike for SK, a
-- 158,000 MW France hour, solar output after local sunset). Those stay in
-- core.observation exactly as published and are flagged by the dq suite; the
-- marts below apply one plausibility band, 0 <= value <= 150,000 MW, so a
-- corrupt cell cannot reach a peak-demand headline. The dq run reconciles
-- core minus marts against the flagged rows to the row.

CREATE TABLE IF NOT EXISTS analytics.series_coverage (
    country            text,
    metric             text,
    unit               text PRIMARY KEY,
    hours_loaded       int NOT NULL,
    hours_expected     int NOT NULL,
    coverage_pct       numeric NOT NULL,
    first_ts           timestamp,
    last_ts            timestamp,
    longest_gap_hours  int
);
TRUNCATE analytics.series_coverage;
INSERT INTO analytics.series_coverage
SELECT country, metric, unit, count(*), 50400,
       round(100.0 * count(*) / 50400, 2),
       min(ts_utc), max(ts_utc),
       max(gap)
FROM (
    SELECT c.*,
           (extract(epoch FROM lead(ts_utc) OVER (PARTITION BY unit ORDER BY ts_utc))
            - extract(epoch FROM ts_utc)) / 3600 - 1 AS gap
    FROM core.observation c
) t
GROUP BY country, metric, unit;

CREATE TABLE IF NOT EXISTS analytics.load_daily (
    country    text NOT NULL,
    day        date NOT NULL,
    hours      int NOT NULL,
    avg_mw     numeric NOT NULL,
    max_mw     numeric NOT NULL,          -- peak demand hour
    min_mw     numeric NOT NULL,          -- valley
    energy_mwh numeric NOT NULL,          -- sum of hourly MW * 1h
    PRIMARY KEY (country, day)
);
TRUNCATE analytics.load_daily;
INSERT INTO analytics.load_daily
SELECT country, ts_utc::date, count(*), round(avg(value),1), max(value), min(value),
       round(sum(value),1)
FROM core.observation
WHERE metric = 'load_actual' AND value BETWEEN 0 AND 150000
GROUP BY 1, 2;

CREATE TABLE IF NOT EXISTS analytics.renewable_daily (
    country      text NOT NULL,
    day          date NOT NULL,
    load_hours   int NOT NULL,
    wind_mwh     numeric,
    solar_mwh    numeric,
    renew_mwh    numeric,
    share_pct    numeric,                 -- (wind+solar) / load, day with all three series
    PRIMARY KEY (country, day)
);
TRUNCATE analytics.renewable_daily;
INSERT INTO analytics.renewable_daily
WITH d AS (
    SELECT country, ts_utc::date AS day,
           sum(value) FILTER (WHERE metric='load_actual')  AS load_mwh,
           count(*)   FILTER (WHERE metric='load_actual')  AS load_hours,
           sum(value) FILTER (WHERE metric='wind_generation')  AS wind_mwh,
           sum(value) FILTER (WHERE metric='solar_generation') AS solar_mwh,
           count(*) FILTER (WHERE metric='wind_generation')  AS wind_hours,
           count(*) FILTER (WHERE metric='solar_generation') AS solar_hours
    FROM core.observation
    WHERE value BETWEEN 0 AND 150000
    GROUP BY 1, 2
)
SELECT country, day, load_hours, round(wind_mwh,1), round(solar_mwh,1),
       round(coalesce(wind_mwh,0)+coalesce(solar_mwh,0),1),
       CASE WHEN load_hours = 24 AND wind_hours = 24 AND solar_hours = 24 AND load_mwh > 0
            THEN round(100.0*(coalesce(wind_mwh,0)+coalesce(solar_mwh,0))/load_mwh, 2)
       END
FROM d
WHERE load_hours > 0;

-- Demand shape: average MW by UTC hour over the whole history.
CREATE TABLE IF NOT EXISTS analytics.hourly_profile (
    country     text NOT NULL,
    hour_utc    int NOT NULL,
    avg_mw      numeric NOT NULL,
    sample_days int NOT NULL,
    PRIMARY KEY (country, hour_utc)
);
TRUNCATE analytics.hourly_profile;
INSERT INTO analytics.hourly_profile
SELECT country, extract(hour FROM ts_utc)::int, round(avg(value),1),
       count(DISTINCT ts_utc::date)
FROM core.observation
WHERE metric = 'load_actual' AND value BETWEEN 0 AND 150000
GROUP BY 1, 2;

-- Forecast quality: how far the published day-ahead forecast sat from actual,
-- by country-month. This is the "bridge simulation output vs reality" metric.
CREATE TABLE IF NOT EXISTS analytics.forecast_month (
    country        text NOT NULL,
    month          date NOT NULL,         -- first of month
    hours          int NOT NULL,          -- hours where both series exist
    mape_pct       numeric NOT NULL,
    mean_bias_pct  numeric NOT NULL,      -- signed: forecast - actual
    PRIMARY KEY (country, month)
);
TRUNCATE analytics.forecast_month;
INSERT INTO analytics.forecast_month
SELECT a.country, date_trunc('month', a.ts_utc)::date, count(*),
       round(avg(abs(f.value - a.value) / a.value) * 100, 3),
       round(avg((f.value - a.value) / a.value) * 100, 3)
FROM core.observation a
JOIN core.observation f ON f.ts_utc = a.ts_utc AND f.unit = replace(a.unit, 'load_actual', 'load_forecast')
WHERE a.metric = 'load_actual' AND a.value > 0 AND a.value <= 150000
  AND f.value BETWEEN 0 AND 150000
GROUP BY 1, 2;

-- BI-facing views (what Power BI/Tableau would point at in this schema).
CREATE OR REPLACE VIEW analytics.v_country_summary AS
SELECT country,
       count(*) FILTER (WHERE hours = 24) AS complete_days,
       max(max_mw) AS peak_mw,
       round(avg(avg_mw), 1) AS avg_daily_mw,
       round(sum(energy_mwh)/1000000, 2) AS twh_history
FROM analytics.load_daily GROUP BY country;

-- Peak day per country. Rewritten from a correlated subquery (which rescans
-- load_daily once per country -> O(n^2)) to a single window pass: same answer,
-- measured on this table from ~29 s to well under 1 s.
CREATE OR REPLACE VIEW analytics.v_peak_days AS
SELECT country, day, peak_mw, energy_mwh FROM (
    SELECT country, day, max_mw AS peak_mw, energy_mwh,
           row_number() OVER (PARTITION BY country ORDER BY max_mw DESC) AS rn
    FROM analytics.load_daily
) t WHERE rn = 1;

CREATE OR REPLACE VIEW analytics.v_renewable_share_year AS
SELECT country, extract(year FROM day)::int AS year,
       round(avg(share_pct), 2) AS avg_daily_share_pct,
       max(share_pct) AS max_daily_share_pct,
       count(share_pct) AS scored_days
FROM analytics.renewable_daily GROUP BY 1, 2;

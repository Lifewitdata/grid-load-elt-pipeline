-- 01_schema.sql: schemas, run manifest, all-text landing table.
-- ELT contract: Python lands the published bytes verbatim (as text) and records
-- how many non-empty cells it saw; every cast, filter and model lives in SQL.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS dq;

CREATE TABLE IF NOT EXISTS meta.load_run (
    run_id         bigserial PRIMARY KEY,
    started_at     timestamptz NOT NULL DEFAULT now(),
    finished_at    timestamptz,
    mode           text NOT NULL,
    file_sha256    text NOT NULL,
    file_bytes     bigint NOT NULL,
    cells_expected bigint,          -- non-empty cells counted while streaming the CSV
    rows_landed    bigint,          -- rows actually COPYed into raw.observation
    header_cols    int,
    series_selected int,
    rows_rejected  bigint NOT NULL DEFAULT 0
);

-- Landing: exactly what the file says, nothing inferred. source_row is the
-- 1-based data-row number in the CSV so any finding can be traced to a byte.
CREATE TABLE IF NOT EXISTS raw.observation (
    ts_text    text NOT NULL,
    unit       text NOT NULL,
    value_text text NOT NULL,
    source_row bigint NOT NULL,
    run_id     bigint NOT NULL REFERENCES meta.load_run(run_id)
);

CREATE TABLE IF NOT EXISTS dq.results (
    run_id     bigint,
    checked_at timestamptz NOT NULL DEFAULT now(),
    check_id   text NOT NULL,
    family     text NOT NULL,
    severity   text NOT NULL check (severity in ('info','warn','fail')),
    status     text NOT NULL check (status in ('pass','warn','fail','error')),
    observed   text,
    expected   text,
    drilldown_sql text,
    message    text
);

CREATE TABLE IF NOT EXISTS dq.alerts (
    raised_at  timestamptz NOT NULL DEFAULT now(),
    run_id     bigint,
    check_id   text,
    severity   text,
    message    text
);

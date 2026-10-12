# Grid Load ELT Pipeline — power-system time-series on PostgreSQL

An end-to-end **ELT pipeline for high-frequency power-grid data**: Python
extracts a pinned, real grid dataset and lands it as raw text in PostgreSQL,
SQL transforms it into a year-partitioned time-series core and modelled
analytics marts, a 22-check quality suite audits the whole thing and traces
every finding to the row, and a Streamlit BI layer renders the market
metrics. Built for the kind of work GE Vernova Consulting describes: ingest
and transform raw power-system datasets into high-fidelity, actionable
insights through a reproducible pipeline.

Every number in this README was produced by running the code in this repo
against the exact bytes the source published. The commands to reproduce them
are below.

```
OPSD release 2020-10-06  (time_series_60min_singleindex.csv, 130 MB, sha256-pinned)
        │
        v
etl/fetch.py      sha256 + byte-size pinned; refuses to land changed bytes
        │
        v
etl/ingest.py  ──>  raw.observation   ALL-TEXT landing, one row per non-empty cell
        │           (streaming COPY, run manifest records cells counted vs landed)
        v
sql/03_staging.sql   stg.observation  typed view, parse failures flagged not hidden
sql/04_core.sql      core.observation  year-partitioned, PK (ts_utc, unit)
        │
        v
sql/05_analytics.sql  analytics.*     load_daily, renewable_daily, hourly_profile,
        │                             forecast_month, series_coverage + BI views
        v
etl/dq.py         22 checks in 4 families ──> dq.results (every drill-down executed)
        │
        v
dashboard/app.py  Streamlit BI (5 pages, browser-verified) · docker-compose.yml (Postgres 16)
```

## Quickstart

```bash
# 1. start PostgreSQL 16 (localhost:5433, dev credentials in docker-compose.yml)
docker compose up -d postgres

# 2. download + verify the pinned source bytes
python etl/fetch.py            # add --offline to only re-verify what is on disk

# 3. ELT: land raw text, then transform to typed core + modelled marts
python etl/ingest.py           # streams 130 MB -> 4,121,425 landing rows (~2.5 min)
python etl/transform.py        # applies sql/: staging, partitioned core, analytics marts

# 4. audit it
python etl/dq.py               # 22 checks -> dq.results, every drill-down executed

# 5. read it
streamlit run dashboard/app.py

# unit tests (no DB needed)
python -m pytest tests -q      # 15 passed
```

Python 3.14, PostgreSQL 16 (psycopg2-binary 2.9), pandas 3.0, Streamlit 1.65,
Altair 5. The source file is ~130 MB and is gitignored; the pipeline verifies
its sha256 (`6a7f2bc5…c704c8`) before landing a single cell.

## What the warehouse holds

Real European grid data from **Open Power System Data** (`time_series`,
release 2020-10-06): hourly load, day-ahead load forecast, and wind/solar
generation reported to ENTSO-E. The pipeline selects every **country-level**
series and ignores control-area splits, bidding-zone and capacity/profile
variants, so the model is a clean (timestamp, country, metric, MW) fact.

| | Value | Enforced by |
|---|---|---|
| History | 2015-01-01 .. 2020-09-30 UTC, **50,400 hours** | `completeness.history_span` |
| Series landed | **88** (31 load_actual, 31 load_forecast, 5 wind, 21 solar) | `completeness.units_present` == 88 |
| Countries | **31** | `series_coverage` grain |
| Landing rows | **4,121,425** non-empty cells | `reconcile.cells_vs_landed` |
| Typed core rows | **4,121,425** (landing = core, nothing silently dropped) | `reconcile.raw_equals_core_plus_rejects` |
| Daily load facts | **63,274** country-days | `reconcile.mart_hours_exactly_band_members` |
| Renewable-scored days | **7,657** country-days with all of load+wind+solar 24/24 | `renewable_daily.share_pct IS NOT NULL` |

`core.observation` is **partitioned by year** (2015..2021) on the timestamp
with a `(ts_utc, unit)` primary key. Full reload semantics: every run truncates
the landing and rebuilds, so the warehouse is a deterministic function of the
pinned file - rebuild it and you get the identical numbers.

## The energy story the marts tell

Nothing here is synthetic. These are computed from the warehouse and shown on
the Renewables page:

- **Germany's wind + solar share of load rose from 23.7% (2015) to 38.7%
  (2020)** - the Energiewende, straight from ENTSO-E actuals. Daily energy is
  summed per country and the share taken only on days where all three series
  are fully published, so a missing hour cannot inflate it.
- **Denmark runs 43%-53% wind+solar** and hits days where renewables exceed
  100% of reported load - because ENTSO-E *load* for DK is a **net** figure.
  The `business.renewable_share_band` check surfaces exactly that, and the
  dashboard labels it instead of hiding it.
- **Day-ahead forecast quality**: DE overall MAPE **3.05%**, best country-month
  0.04%, worst 103%. Forecast-vs-actual is the "bridge simulation output to
  reality" metric this pipeline is built to compute per country-month.
- **Peak demand** (plausibility-banded): France 94,492 MW and Germany 77,549 MW
  are the headline all-time peaks. See the next section for why those - not
  larger numbers in the raw file - are what reaches a chart.

## Data quality: found, quantified, quarantined - never smoothed over

`etl/dq.py` runs **22 checks** across four families (reconcile, completeness,
validity, business). Latest run: **16 pass, 5 warn, 1 fail, 0 error**, and
**all 22 stored drill-downs execute** against the landed warehouse. The suite
is tuned to produce findings only where the bytes disagree with reality.

The single FAIL is not a bug in the pipeline - it is a real defect in the
published data, and the design decision is the interesting part:

| Finding | Observed | Treatment |
|---|---|---|
| Two physically impossible cells: SK 2,170,811 MW, FR 158,000 MW | 2 rows | **Kept in `core` exactly as published, flagged by `validity.implausible_magnitude`, quarantined from every mart by one documented plausibility band (0 <= MW <= 150,000).** `mart.band_enforced` proves no outlier reaches a peak-demand headline - which is why France's real 94,492 MW peak shows, not the corrupt 2.17 million. |
| Night-time solar over 1 GW after 21:00 UTC | 9 rows | `validity.night_solar` warn - impossible, flagged for review, not deleted |
| Series below 99% hourly coverage | 21 | `completeness.low_coverage_series` warn - PT wind is a near-empty published series; the coverage table names them |
| Hourly gaps over 30 days | 3 | `completeness.long_gaps` warn - reporting outages, sized not guessed |
| DK days where wind+solar > 100% of load | 59 | `business.renewable_share_band` warn - net-load semantics, explained not suppressed |
| Country-months with forecast MAPE > 8% | 174 | `business.forecast_mape_band` warn - a review queue, not a failure |

**The reconciliations are what make the findings safe to publish.** The
`reconcile` family computes every headline number twice by independent paths
and fails when they disagree:

- `reconcile.cells_vs_landed` - the cells counted while streaming the CSV equal
  the rows COPYed into `raw`.
- `reconcile.raw_equals_core_plus_rejects` - nothing vanishes between landing
  and typed core: `raw = core + parse rejects`, to the row.
- `reconcile.mart_hours_exactly_band_members` - the daily marts exclude *exactly*
  the flagged band cells and nothing else.
- `reconcile.load_daily_energy_vs_core` - the daily energy mart equals the hourly
  core **recomputed independently per country-day**, not a single grand total.
- `reconcile.forecast_hours_vs_join` - the monthly forecast table accounts for
  every actual/forecast join hour under the same band.

## Query optimisation, measured on the partitioned core

The workload is time-series with a timestamp filter, so the storage decision
that matters is partitioning by time. Measured on `core.observation` (4.1M
rows), the same aggregate run two ways:

| Query | Partitions scanned | Time | Result |
|---|---|---|---|
| DE avg load, **full history** | all 7 (2015..2021) | 7,022 ms | 55,492 MW |
| DE avg load, **one year** (2019) | **1** (`observation_2019`) | 17.8 ms | 55,990 MW |

That is **~394x faster** because the year predicate lets the planner **prune
six of seven partitions** and read only 2019. `EXPLAIN` confirms a single
`Parallel Seq Scan on observation_2019`; every other yearly partition is
skipped. This is the exact Direct-Query / Import decision for a grid BI tool:
partition on the timestamp you always filter, so a report over one season reads
one slice of the lake, not the whole history.

## Layers

| Layer | Contents | Grain |
|---|---|---|
| `raw.observation` | all-text landing, one row per non-empty CSV cell | (ts_text, unit) |
| `stg.observation` | typed view; strict-ISO timestamp + numeric casts, failures flagged | as landed |
| `core.observation` | typed, deduped, year-partitioned fact | (ts_utc, unit) hourly |
| `analytics.*` | daily load / renewable mix / hourly profile / forecast accuracy / coverage marts + BI views | country-day, country-month, country-hour |
| `dq.results`, `dq.alerts` | the audit and its findings with drill-down SQL | one row per check |
| `meta.load_run` | run manifest: sha256, cells expected vs landed, rejects | one row per reload |

## Reproduce every number

```bash
python etl/fetch.py --offline
python etl/ingest.py
python etl/transform.py
python etl/dq.py
python -m pytest tests -q
```

## Honest limits

- **Synthetic-adjacent caution does NOT apply here: the data is real.** But no
  query in this repo was run against a live PLEXOS/PlanOS, Databricks, Azure
  Data Factory or Snowflake instance - those appear in the JD's *desired*
  list, not this project. This warehouse is PostgreSQL + Docker only.
- 5 wind and 21 solar series: wind generation is published for few countries in
  this release, so the renewable-mix analysis is honest only where all three
  series overlap; the coverage table names the rest.
- The plausibility band (150 GW ceiling) is a documented engineering choice for
  this dataset's country-level hourly series, not a general physical law.

"""Data-quality suite over the landed grid warehouse.

Every check is a scalar SQL assertion against the warehouse itself - no
hand-typed expectations that can drift from the data. Reconciliations compute
key figures twice by independent paths (streaming scan vs COPYed rows, hourly
core vs daily mart, join counts vs analytics table) and fail when the paths
disagree. Each finding stores the drill-down SQL that names the rows behind
it, and every drill is executed at run time so a stored query is never a lie.
"""
import json
import sys
import time
from pathlib import Path

from config import connect

CHECKS = [
    # ---------- lineage / reconciliation ----------
    dict(id="reconcile.cells_vs_landed", family="reconcile", severity="fail",
         sql="SELECT cells_expected - rows_landed FROM meta.load_run ORDER BY run_id DESC LIMIT 1",
         cmp=("eq", 0),
         drill="SELECT run_id, cells_expected, rows_landed FROM meta.load_run ORDER BY run_id DESC",
         msg="cells counted while streaming the CSV must equal rows COPYed into raw"),
    dict(id="reconcile.raw_equals_core_plus_rejects", family="reconcile", severity="fail",
         sql="SELECT (SELECT count(*) FROM raw.observation) - "
             "(SELECT count(*) FROM core.observation) - "
             "(SELECT count(*) FROM stg.observation WHERE ts_utc IS NULL OR value IS NULL)",
         cmp=("eq", 0),
         drill="SELECT (SELECT count(*) FROM raw.observation) raw, "
               "(SELECT count(*) FROM core.observation) core, "
               "(SELECT count(*) FROM stg.observation WHERE ts_utc IS NULL OR value IS NULL) rejected",
         msg="nothing may vanish between landing and typed core: raw = core + rejected"),
    dict(id="reconcile.load_daily_energy_vs_core", family="reconcile", severity="fail",
         sql="SELECT (SELECT round(SUM(energy_mwh),1) FROM analytics.load_daily) - "
             "(SELECT round(SUM(d),1) FROM (SELECT round(SUM(value),1) d FROM core.observation "
             " WHERE metric='load_actual' AND value BETWEEN 0 AND 150000 "
             " GROUP BY country, ts_utc::date) x)",
         cmp=("eq", 0),
         drill="SELECT ld.country, ld.day, ld.energy_mwh, "
               "(SELECT round(SUM(c.value),1) FROM core.observation c "
               " WHERE c.country=ld.country AND c.ts_utc::date=ld.day "
               " AND c.metric='load_actual' AND c.value BETWEEN 0 AND 150000) recomputed "
               "FROM analytics.load_daily ld ORDER BY random() LIMIT 50",
         msg="daily energy mart must equal the hourly core recomputed independently per day"),
    dict(id="reconcile.mart_hours_exactly_band_members", family="reconcile", severity="fail",
         sql="SELECT (SELECT count(*) FROM core.observation WHERE metric='load_actual' "
             "AND value BETWEEN 0 AND 150000) - (SELECT SUM(hours) FROM analytics.load_daily)",
         cmp=("eq", 0),
         drill="SELECT country, ts_utc, value FROM core.observation "
               "WHERE metric='load_actual' AND value NOT BETWEEN 0 AND 150000 LIMIT 20",
         msg="marts exclude exactly the flagged plausibility-band cells and nothing else"),
    dict(id="reconcile.coverage_loaded_equals_core", family="reconcile", severity="fail",
         sql="SELECT (SELECT SUM(hours_loaded) FROM analytics.series_coverage) - "
             "(SELECT count(*) FROM core.observation)",
         cmp=("eq", 0),
         drill="SELECT unit, hours_loaded FROM analytics.series_coverage ORDER BY hours_loaded DESC LIMIT 20",
         msg="per-series coverage counts must sum to the core row count"),
    dict(id="reconcile.forecast_hours_vs_join", family="reconcile", severity="fail",
         sql="SELECT (SELECT SUM(hours) FROM analytics.forecast_month) - "
             "(SELECT count(*) FROM core.observation a JOIN core.observation f "
             " ON f.ts_utc=a.ts_utc AND f.unit=replace(a.unit,'load_actual','load_forecast') "
             " WHERE a.metric='load_actual' AND a.value > 0 AND a.value <= 150000 "
             " AND f.value BETWEEN 0 AND 150000)",
         cmp=("eq", 0),
         drill="SELECT country, month, hours, mape_pct FROM analytics.forecast_month ORDER BY month DESC LIMIT 20",
         msg="monthly forecast-accuracy rows must account for every joined hour"),

    # ---------- completeness ----------
    dict(id="completeness.units_present", family="completeness", severity="fail",
         sql="SELECT count(*) FROM analytics.series_coverage",
         cmp=("eq", 88),
         drill="SELECT country, metric, count(*) FROM analytics.series_coverage GROUP BY 1,2 ORDER BY 1",
         msg="31 load_actual + 31 load_forecast + 5 wind + 21 solar series"),
    dict(id="completeness.history_span", family="completeness", severity="fail",
         sql="SELECT min(ts_utc)::date || ' .. ' || max(ts_utc)::date FROM core.observation",
         cmp=("eq", "2015-01-01 .. 2020-09-30"),
         drill="SELECT DISTINCT extract(year FROM ts_utc) y FROM core.observation ORDER BY 1",
         msg="file publishes 2015-01-01T00Z through 2020-09-30T23Z"),
    dict(id="completeness.hourly_contiguity_de_load", family="completeness", severity="fail",
         sql="SELECT max_gap FROM (SELECT (extract(epoch FROM lead(ts_utc) OVER (ORDER BY ts_utc)) "
             "- extract(epoch FROM ts_utc))/3600 - 1 AS max_gap FROM core.observation "
             "WHERE unit='DE_load_actual_entsoe_transparency') g",
         cmp=("eq", 0),
         drill="SELECT ts_utc, lead(ts_utc) OVER (ORDER BY ts_utc) nxt FROM core.observation "
               "WHERE unit='DE_load_actual_entsoe_transparency' ORDER BY ts_utc LIMIT 50",
         msg="DE load publishes every hour with no gaps"),
    dict(id="completeness.low_coverage_series", family="completeness", severity="warn",
         sql="SELECT count(*) FROM analytics.series_coverage WHERE coverage_pct < 99",
         cmp=("le", 0),
         drill="SELECT unit, coverage_pct, hours_loaded FROM analytics.series_coverage "
               "WHERE coverage_pct < 99 ORDER BY coverage_pct",
         msg="series below 99% hourly coverage: PT wind is a known near-empty published series"),
    dict(id="completeness.long_gaps", family="completeness", severity="warn",
         sql="SELECT count(*) FROM analytics.series_coverage WHERE longest_gap_hours > 720",
         cmp=("le", 0),
         drill="SELECT unit, longest_gap_hours FROM analytics.series_coverage "
               "WHERE longest_gap_hours > 720 ORDER BY longest_gap_hours DESC",
         msg="a gap over 30 days in an hourly series is a reporting outage, not noise"),

    # ---------- validity ----------
    dict(id="validity.unparseable_timestamps", family="validity", severity="fail",
         sql="SELECT count(*) FROM stg.observation WHERE ts_unparseable",
         cmp=("eq", 0),
         drill="SELECT DISTINCT ts_text FROM raw.observation "
               "WHERE ts_text !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$' LIMIT 20",
         msg="every landed timestamp must parse as strict ISO UTC hourly"),
    dict(id="validity.nonnumeric_values", family="validity", severity="fail",
         sql="SELECT count(*) FROM stg.observation WHERE value_nonnumeric",
         cmp=("eq", 0),
         drill="SELECT DISTINCT value_text FROM raw.observation "
               "WHERE value_text !~ '^-?[0-9]+(\\.[0-9]+)?$' LIMIT 20",
         msg="the published file carries only decimal numbers"),
    dict(id="validity.epoch_sentinels", family="validity", severity="fail",
         sql="SELECT count(*) FROM core.observation WHERE extract(year FROM ts_utc) < 2000",
         cmp=("eq", 0),
         drill="SELECT ts_text, count(*) FROM raw.observation WHERE ts_text LIKE '1970%' "
               "GROUP BY 1 LIMIT 10",
         msg="no 1970-epoch sentinel may reach the typed core"),
    dict(id="validity.negative_values", family="validity", severity="fail",
         sql="SELECT count(*) FROM core.observation WHERE value < 0",
         cmp=("eq", 0),
         drill="SELECT unit, ts_utc, value FROM core.observation WHERE value < 0 LIMIT 50",
         msg="load/generation MW at country level is never negative in this release"),
    dict(id="validity.implausible_magnitude", family="validity", severity="fail",
         sql="SELECT count(*) FROM core.observation WHERE value > 150000",
         cmp=("eq", 0),
         drill="SELECT unit, ts_utc, value FROM core.observation WHERE value > 150000",
         msg="published file contains cells above 150 GW (SK 2,170,811 MW; FR 158,000 MW) - "
             "kept in core as published, quarantined from every mart by the band filter"),
    dict(id="mart.band_enforced", family="business", severity="fail",
         sql="SELECT count(*) FROM analytics.load_daily WHERE max_mw > 150000 OR min_mw < 0",
         cmp=("eq", 0),
         drill="SELECT country, day, max_mw FROM analytics.load_daily "
               "WHERE max_mw > 150000 OR min_mw < 0 LIMIT 10",
         msg="no plausibility-band outlier may reach a peak-demand headline"),
    dict(id="validity.night_solar", family="validity", severity="warn",
         sql="SELECT count(*) FROM core.observation WHERE metric='solar_generation' "
             "AND value > 1000 AND extract(hour FROM ts_utc) BETWEEN 21 AND 23",
         cmp=("eq", 0),
         drill="SELECT country, ts_utc, value FROM core.observation WHERE metric='solar_generation' "
               "AND value > 1000 AND extract(hour FROM ts_utc) BETWEEN 21 AND 23 LIMIT 20",
         msg="solar output over 1 GW after 21:00 UTC in Europe is physically impossible"),
    dict(id="validity.duplicate_keys", family="validity", severity="fail",
         sql="SELECT count(*) FROM (SELECT ts_text, unit FROM raw.observation "
             "GROUP BY 1,2 HAVING count(*) > 1) d",
         cmp=("eq", 0),
         drill="SELECT ts_text, unit, count(*) FROM raw.observation GROUP BY 1,2 HAVING count(*)>1 LIMIT 20",
         msg="(timestamp, unit) must be unique in the landing bytes"),

    # ---------- business anchors ----------
    dict(id="business.de_peak_window", family="business", severity="fail",
         sql="SELECT count(*) FROM (SELECT max(max_mw) m FROM analytics.load_daily WHERE country='DE') t "
             "WHERE m BETWEEN 60000 AND 90000",
         cmp=("eq", 1),
         drill="SELECT country, day, max_mw FROM analytics.load_daily ORDER BY max_mw DESC LIMIT 10",
         msg="DE all-time hourly peak must sit in the published 60-90 GW band"),
    dict(id="business.forecast_mape_band", family="business", severity="warn",
         sql="SELECT count(*) FROM analytics.forecast_month WHERE mape_pct > 8",
         cmp=("eq", 0),
         drill="SELECT country, month, mape_pct FROM analytics.forecast_month "
               "WHERE mape_pct > 8 ORDER BY mape_pct DESC LIMIT 30",
         msg="day-ahead load forecast MAPE above 8% for a country-month deserves review"),
    dict(id="business.renewable_share_band", family="business", severity="warn",
         sql="SELECT count(*) FROM analytics.renewable_daily WHERE share_pct > 100",
         cmp=("eq", 0),
         drill="SELECT country, day, load_hours, renew_mwh, share_pct FROM analytics.renewable_daily "
               "WHERE share_pct > 100 ORDER BY share_pct DESC LIMIT 20",
         msg="days where wind+solar exceeds reported load are concentrated in DK - ENTSO-E load "
             "is a net figure there, so the daily share must be read as net-load semantics"),
]

def run():
    conn = connect()
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SELECT run_id FROM meta.load_run ORDER BY run_id DESC LIMIT 1")
    row = cur.fetchone()
    run_id = row[0] if row else 0

    t0 = time.time()
    cur.execute("TRUNCATE dq.results")
    results = []
    for chk in CHECKS:
        try:
            cur.execute(chk["sql"])
            observed = cur.fetchone()[0]
            op, want = chk["cmp"]
            ok = (observed == want) if op == "eq" else (observed <= want) if op == "le" else (observed >= want)
            status = "pass" if ok else chk["severity"]
            results.append((run_id, chk["id"], chk["family"], chk["severity"], status,
                            str(observed), str(chk["cmp"][1]), chk["drill"], chk["msg"]))
        except Exception as e:
            conn.rollback()
            results.append((run_id, chk["id"], chk["family"], chk["severity"], "error",
                            str(e)[:200], "", chk["drill"], chk["msg"]))

    # drills are deliverables: every stored drill-down must actually execute
    drill_ok = 0
    for r in results:
        try:
            cur.execute("SAVEPOINT sp")
            cur.execute(r[7])
            cur.fetchall()
            cur.execute("RELEASE SAVEPOINT sp")
            drill_ok += 1
        except Exception:
            cur.execute("ROLLBACK TO SAVEPOINT sp")

    import psycopg2.extras
    psycopg2.extras.execute_batch(
        cur,
        "INSERT INTO dq.results (run_id, check_id, family, severity, status, "
        "observed, expected, drilldown_sql, message) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        results,
    )
    fails = [r for r in results if r[4] != "pass"]
    if fails:
        psycopg2.extras.execute_batch(
            cur,
            "INSERT INTO dq.alerts (run_id, check_id, severity, message) VALUES (%s,%s,%s,%s)",
            [(run_id, r[1], r[3], r[8]) for r in fails],
        )
    conn.commit()
    elapsed = int((time.time() - t0) * 1000)

    counts = {s: sum(1 for r in results if r[4] == s) for s in ("pass", "warn", "fail", "error")}
    summary = dict(run_id=run_id, checks=len(results), elapsed_ms=elapsed,
                   **counts, drills_executed_ok=drill_ok,
                   findings=[dict(id=r[1], observed=r[5], message=r[8]) for r in fails])
    out = Path(__file__).resolve().parents[1] / "reports"
    out.mkdir(exist_ok=True)
    (out / "dq_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"dq: {len(results)} checks in {elapsed} ms -> "
          f"{counts['pass']} pass, {counts['warn']} warn, {counts['fail']} fail, "
          f"{counts['error']} error; drills OK {drill_ok}/{len(results)}")
    for r in fails:
        print(f"  [{r[4].upper()}] {r[1]}: observed={r[5]} :: {r[8]}")
    conn.close()


if __name__ == "__main__":
    sys.exit(run())

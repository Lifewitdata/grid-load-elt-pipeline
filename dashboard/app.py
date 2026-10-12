"""Streamlit BI layer over the PostgreSQL grid warehouse.

Every page is a query against analytics/core - the same read path a Power BI
or Tableau model would use. No chart is drawn from an unverified number: the
marts it reads are the ones the dq suite reconciles to the landing bytes.
"""
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import altair as alt
import pandas as pd
import psycopg2
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "etl"))
from config import connect  # noqa: E402

st.set_page_config(page_title="Grid Load ELT - Analytics", layout="wide")

METRICS = {
    "load_actual": "Load (actual, MW)",
    "load_forecast": "Load (day-ahead forecast, MW)",
    "wind_generation": "Wind generation (MW)",
    "solar_generation": "Solar generation (MW)",
}
FLAG_COLORS = {"pass": "#2e7d32", "warn": "#f9a825", "fail": "#c62828", "error": "#6a1b9a"}


@st.cache_data(ttl=600)
def q(sql: str, params=None) -> pd.DataFrame:
    conn = connect()
    try:
        return pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()


def KPI(label, value, help_text=""):
    st.metric(label, value, help=help_text)


def line_chart(df, x, y, color, title, ylab):
    return alt.Chart(df).mark_line().encode(
        x=alt.X(x, title="Date (UTC)"),
        y=alt.Y(y, title=ylab),
        color=alt.Color(color, legend=alt.Legend(title=None)),
        tooltip=[x, y, color],
    ).properties(title=title, height=320)


# ---------------------------------------------------------------- Overview
def page_overview():
    st.title("Grid load warehouse - overview")
    k = q("SELECT count(*) AS landing, "
          "(SELECT count(*) FROM core.observation) AS typed, "
          "(SELECT count(*) FROM analytics.load_daily) AS day_facts, "
          "(SELECT count(*) FROM analytics.series_coverage) AS series, "
          "(SELECT min(ts_utc)::date::text || ' .. ' || max(ts_utc)::date::text FROM core.observation) AS span "
          "FROM raw.observation")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Hourly observations", f"{int(k.typed[0]):,}")
    c2.metric("Daily load facts", f"{int(k.day_facts[0]):,}")
    c3.metric("Series x countries", f"{int(k.series[0])}")
    c4.metric("Coverage", k.span[0])

    st.subheader("Peak demand by country (all history)")
    pk = q("""SELECT p.country, p.peak_mw, p.day AS peak_day,
                     s.avg_daily_mw
              FROM analytics.v_peak_days p
              JOIN analytics.v_country_summary s USING (country)
              ORDER BY p.peak_mw DESC NULLS LAST LIMIT 12""")
    st.bar_chart(pk.set_index("country")["peak_mw"])

    st.subheader("Last 60 days, country-average load")
    last = q("SELECT max(day) d FROM analytics.load_daily")["d"][0]
    d0 = last - timedelta(days=60)
    recent = q("SELECT day, country, avg_mw AS value FROM analytics.load_daily "
               "WHERE day >= %s AND hours >= 20 ORDER BY day", (d0,))
    st.line_chart(recent.pivot(index="day", columns="country", values="value").ffill())

    st.subheader("Demand shape (average MW by UTC hour, whole history)")
    prof = q("SELECT country, hour_utc, avg_mw FROM analytics.hourly_profile "
             "WHERE country IN ('DE','FR','ES','DK','NL') ORDER BY country, hour_utc")
    st.altair_chart(alt.Chart(prof).mark_line().encode(
        x=alt.X("hour_utc:O", title="Hour of day (UTC)"),
        y=alt.Y("avg_mw:Q", title="Average MW"),
        color="country:N",
        tooltip=["country", "hour_utc", "avg_mw"],
    ).properties(height=300, title="Hourly demand profile"), use_container_width=True)


# ---------------------------------------------------------------- Explorer
def page_explorer():
    st.title("Time explorer")
    units = q("SELECT unit, country, metric FROM analytics.series_coverage ORDER BY unit")
    col1, col2 = st.columns([2, 1])
    with col2:
        start = st.date_input("From", value=date(2019, 1, 1))
        end = st.date_input("To", value=date(2020, 9, 30))
    with col1:
        sel = st.multiselect("Series", units.unit.tolist(),
                             default=["DE_load_actual_entsoe_transparency",
                                     "DE_wind_generation_actual",
                                     "DE_solar_generation_actual"])
    if not sel or end <= start:
        st.info("Pick at least one series and a valid range.")
        return
    df = q("""SELECT ts_utc, unit, value FROM core.observation
              WHERE unit = ANY(%s) AND ts_utc >= %s AND ts_utc < %s + interval '1 day'
                AND value BETWEEN 0 AND 150000 ORDER BY ts_utc""",
           (sel, datetime.combine(start, datetime.min.time()),
            datetime.combine(end, datetime.min.time())))
    if df.empty:
        st.warning("No rows in this window for the selected series.")
        return
    st.caption(f"{len(df):,} hourly rows rendered from PostgreSQL")
    st.altair_chart(alt.Chart(df).mark_line().encode(
        x=alt.X("ts_utc:T", title="Timestamp (UTC)"),
        y=alt.Y("value:Q", title="MW"),
        color=alt.Color("unit:N", legend=alt.Legend(title="Series")),
        tooltip=["ts_utc", "unit", "value"],
    ).interactive().properties(height=420), use_container_width=True)

    st.subheader("Zoom: daily energy by selected country-days")
    day = df.copy()
    day["country"] = day.unit.str.slice(0, 2)
    agg = day.groupby(["country", day.ts_utc.dt.date])["value"].sum().reset_index()
    st.bar_chart(agg.groupby("country")["value"].sum())


# ---------------------------------------------------------------- Quality
def page_quality():
    st.title("Data quality - what the suite knows")
    k = q("""SELECT count(*) AS n, count(*) FILTER (WHERE status='pass') AS n_pass,
                    count(*) FILTER (WHERE status='warn') AS n_warn,
                    count(*) FILTER (WHERE status='fail') AS n_fail,
                    count(*) FILTER (WHERE status='error') AS n_error
             FROM dq.results""").iloc[0]
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Checks", int(k.n))
    c2.metric("Pass", int(k.n_pass))
    c3.metric("Warn", int(k.n_warn))
    c4.metric("Fail (data defects)", int(k.n_fail))
    c5.metric("Suite errors", int(k.n_error))

    res = q("SELECT check_id, family, severity, status, observed, expected, message, "
            "drilldown_sql FROM dq.results ORDER BY CASE status "
            "WHEN 'fail' THEN 0 WHEN 'error' THEN 1 WHEN 'warn' THEN 2 ELSE 3 END, check_id")
    st.subheader("Findings")
    for _, r in res[res.status != "pass"].iterrows():
        with st.expander(f"[{r.status.upper()}] {r.check_id} - observed {r.observed}"):
            st.write(r.message)
            st.code(r.drilldown_sql, language="sql")

    st.subheader("Per-series coverage (lineage view)")
    cov = q("SELECT unit, country, metric, hours_loaded, coverage_pct, longest_gap_hours "
            "FROM analytics.series_coverage ORDER BY coverage_pct")
    st.dataframe(cov, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------- Renewables
def page_renewables():
    st.title("Renewable mix - wind + solar vs load")
    st.caption("Share is computed only for country-days where load, wind and solar are "
               "all 24/24 published; ENTSO-E load is a net figure, so >100% days (mostly DK) "
               "mean wind+solar exceeded domestically reported load - see DQ page.")
    yrs = q("SELECT country, year, avg_daily_share_pct, max_daily_share_pct, scored_days "
            "FROM analytics.v_renewable_share_year WHERE scored_days > 200 ORDER BY country, year")
    st.altair_chart(alt.Chart(yrs).mark_line().encode(
        x=alt.X("year:O", title="Year"),
        y=alt.Y("avg_daily_share_pct:Q", title="Wind+solar / load (%, daily avg)"),
        color="country:N",
        tooltip=["country", "year", "avg_daily_share_pct", "scored_days"],
    ).properties(height=340, title="Annual average renewable share of load"),
        use_container_width=True)

    st.subheader("Generation mix (daily MWh totals)")
    mx = q("""SELECT day, country, wind_mwh, solar_mwh FROM analytics.renewable_daily
              WHERE country IN ('DE','DK','NL','BE') AND wind_mwh IS NOT NULL
              ORDER BY day""")
    mx["day"] = pd.to_datetime(mx["day"])
    win = st.select_slider("Rolling window (days)", [7, 14, 30, 90], value=30)
    roll = mx.set_index("day").groupby("country").rolling(win).mean().reset_index()
    st.altair_chart(alt.Chart(roll).mark_area(opacity=0.55).encode(
        x=alt.X("day:T", title="Date (UTC)"),
        y=alt.Y("solar_mwh:Q", title=f"Daily MWh ({win}-day avg)"),
        color="country:N",
        tooltip=["day", "country", "solar_mwh"],
    ).properties(height=300, title="Solar"), use_container_width=True)
    st.altair_chart(alt.Chart(roll).mark_area(opacity=0.55, color="#1e88e5").encode(
        x=alt.X("day:T", title="Date (UTC)"),
        y=alt.Y("wind_mwh:Q", title=f"Wind MWh ({win}-day avg)"),
        color="country:N",
        tooltip=["day", "country", "wind_mwh"],
    ).properties(height=300, title="Wind"), use_container_width=True)

    st.subheader("Forecast vs actual (day-ahead)")
    fm = q("""SELECT country, month, mape_pct FROM analytics.forecast_month
              WHERE country IN ('DE','FR','ES','NL','BE','IT','PL') ORDER BY month""")
    fm["month"] = pd.to_datetime(fm["month"])
    st.altair_chart(alt.Chart(fm).mark_line().encode(
        x=alt.X("month:T", title="Month"),
        y=alt.Y("mape_pct:Q", title="MAPE %"),
        color="country:N",
        tooltip=["country", "month", "mape_pct"],
    ).properties(height=320, title="Day-ahead forecast error by country-month"),
        use_container_width=True)


# ---------------------------------------------------------------- About
def page_about():
    st.title("About this warehouse")
    st.markdown(
        """
**Pipeline**: `etl/fetch.py` (sha256-pinned download) -> `etl/ingest.py`
(streaming COPY, all-text landing) -> `etl/transform.py` (SQL staging views,
year-partitioned typed core, modelled marts) -> `etl/dq.py` (22 checks, every
drill-down executed) -> this dashboard.

**Source**: Open Power System Data, `time_series_60min_singleindex.csv`,
release 2020-10-06 (hourly UTC, 2015-01-01 .. 2020-09-30, 50,400 hours x 88
country-level series: ENTSO-E actual load + day-ahead forecast for 31
countries, wind generation for 5, solar generation for 21).

**Storage**: PostgreSQL 16 in Docker (`docker-compose.yml`), landing raw as
text, typed `core.observation` partitioned by year with a (ts, unit) primary
key, marts in `analytics.*`.

**Honest limits**: PT wind is a near-empty published series; a handful of
published cells are physically impossible (SK 2.17 TWh-scale spike) and are
kept in core, flagged by the suite, and quarantined from marts by a
documented plausibility band.
""")


PAGES = {
    "Overview": page_overview,
    "Time explorer": page_explorer,
    "Data quality": page_quality,
    "Renewables": page_renewables,
    "About": page_about,
}
choice = st.sidebar.radio("Page", list(PAGES))
PAGES[choice]()

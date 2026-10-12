"""Transform: execute the SQL layer in order. The modelling lives in sql/,
not here - this runner only sequences files and reports row counts."""
import sys
from pathlib import Path

from config import connect

FILES = ["01_schema.sql", "03_staging.sql", "04_core.sql", "05_analytics.sql"]
SQL = Path(__file__).resolve().parents[1] / "sql"


def main():
    conn = connect()
    conn.autocommit = False
    cur = conn.cursor()
    for name in FILES:
        sql = (SQL / name).read_text(encoding="utf-8")
        cur.execute(sql)
        conn.commit()
        print(f"applied sql/{name}")
    for t in ("raw.observation", "core.observation", "analytics.load_daily",
              "analytics.renewable_daily", "analytics.series_coverage",
              "analytics.forecast_month"):
        cur.execute(f"SELECT count(*) FROM {t}")
        print(f"{t:32s} {cur.fetchone()[0]:>12,}")
    conn.close()


if __name__ == "__main__":
    sys.exit(main())

"""Extract-Load: stream the pinned CSV into raw.observation as text.

One pass over 130 MB / 50,400 hourly rows x 88 selected series columns. Every
non-empty cell becomes one landing row; the run manifest records how many
cells the scan saw so the COPY count can be reconciled to the byte (dq
check reconcile.cells_vs_manifest). Idempotent by design: a reload truncates
the landing and re-runs, so the warehouse is a deterministic function of the
pinned file.
"""
import argparse
import csv
import io
import sys
import time
from pathlib import Path

from config import connect
from units import EXPECTED_HOURS, classify_column, country_of
from fetch import LOCAL, SHA256, SIZE_BYTES, sha256_of

BATCH = 250_000


def main(dry_run: bool):
    if not LOCAL.exists():
        sys.exit("run etl/fetch.py first")
    digest = sha256_of(LOCAL)
    if digest != SHA256:
        sys.exit(f"sha256 mismatch: {digest} != {SHA256} - refuse to land changed bytes")

    with LOCAL.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        units_row = next(reader)            # OPSD unit/metadata row, not data
        series = [h for h in header if classify_column(h) is not None]
        idx = [header.index(h) for h in series]

        rows_scanned = 0
        cells_seen = 0
        buffer = []
        t0 = time.time()

        def sink(batch_rows):
            nonlocal buffer
            if dry_run:
                buffer = []
                return
            buf = io.StringIO()
            w = csv.writer(buf, delimiter="\t", quoting=csv.QUOTE_NONE, escapechar="\\")
            for r in batch_rows:
                w.writerow(r)
            buf.seek(0)
            cur.copy_expert(
                "COPY raw.observation (ts_text, unit, value_text, source_row, run_id) "
                "FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '')",
                buf,
            )
            buffer = []

        if dry_run:
            landed = None
        else:
            conn = connect()
            conn.autocommit = False
            cur = conn.cursor()
            cur.execute("TRUNCATE raw.observation")
            cur.execute(
                "INSERT INTO meta.load_run (mode, file_sha256, file_bytes, header_cols, series_selected) "
                "VALUES ('full-reload', %s, %s, %s, %s) RETURNING run_id",
                (SHA256, SIZE_BYTES, len(header), len(series)),
            )
            run_id = cur.fetchone()[0]

        for rownum, row in enumerate(reader, start=1):
            ts = row[0]
            rows_scanned += 1
            for col_name, pos in zip(series, idx):
                v = row[pos]
                if v != "":
                    cells_seen += 1
                    buffer.append((ts, col_name, v, rownum, 0 if dry_run else run_id))
                    if len(buffer) >= BATCH:
                        sink(buffer)
        sink(buffer)

        if not dry_run:
            cur.execute("SELECT count(*) FROM raw.observation")
            landed = cur.fetchone()[0]
            cur.execute(
                "UPDATE meta.load_run SET finished_at=now(), cells_expected=%s, rows_landed=%s "
                "WHERE run_id=%s",
                (cells_seen, landed, run_id),
            )
            conn.commit()
            print(f"run_id={run_id}")
            cur.execute("SELECT to_regclass('stg.observation')")
            if cur.fetchone()[0]:
                cur.execute("SELECT count(*), count(*) FILTER (WHERE ts_utc IS NULL),"
                            " count(*) FILTER (WHERE value IS NULL) FROM stg.observation")
                total, ts_bad, val_bad = cur.fetchone()
                print(f"landing rows={landed:,} (cells counted {cells_seen:,}) "
                      f"staged={total:,} ts_unparseable={ts_bad} value_nonnumeric={val_bad}")
            else:
                print(f"landing rows={landed:,} (cells counted {cells_seen:,}) - "
                      "run etl/transform.py to stage and model")
            conn.close()
        else:
            print(f"dry scan: rows={rows_scanned:,} series={len(series)} "
                  f"(countries: {len(set(country_of(s) for s in series))}) "
                  f"non-empty cells={cells_seen:,} expected max={len(series)*EXPECTED_HOURS:,}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="scan and count only, no DB")
    a = ap.parse_args()
    main(a.dry_run)

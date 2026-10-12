"""Download the exact bytes of the OPSD hourly grid release and pin them.

The sha256 below is the only fingerprint the pipeline trusts: every number in
this repo was produced against these bytes, so a silent upstream change must
fail here rather than mid-analysis.
"""
import argparse
import hashlib
import sys
from pathlib import Path

URL = ("https://data.open-power-system-data.org/time_series/2020-10-06/"
       "time_series_60min_singleindex.csv")
SHA256 = "6a7f2bc571314cbf9c321cc03437691cd4be95c3a6f075e60ff99e8035c704c8"
SIZE_BYTES = 130339665
LOCAL = Path(__file__).resolve().parents[1] / "data" / "time_series_60min_singleindex.csv"


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def verify(path: Path = LOCAL) -> bool:
    if not path.exists():
        print(f"MISSING: {path}")
        return False
    size = path.stat().st_size
    digest = sha256_of(path)
    ok = size == SIZE_BYTES and digest == SHA256
    print(f"size={size} (want {SIZE_BYTES}) sha256={digest[:16]}... "
          f"({'OK' if ok else 'MISMATCH'})")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--offline", action="store_true",
                    help="only verify what is already on disk")
    args = ap.parse_args()

    if LOCAL.exists() and verify(LOCAL):
        return
    if args.offline:
        sys.exit("offline mode and the pinned file is missing/corrupt")

    import urllib.request
    print(f"downloading {URL}")
    LOCAL.parent.mkdir(exist_ok=True)
    tmp = LOCAL.with_suffix(".part")
    with urllib.request.urlopen(URL) as resp, tmp.open("wb") as f:
        done = 0
        while True:
            chunk = resp.read(4 * 1024 * 1024)
            if not chunk:
                break
            f.write(chunk)
            done += len(chunk)
            print(f"\r  {done:,} / {SIZE_BYTES:,} bytes", end="", flush=True)
    print()
    tmp.replace(LOCAL)
    if not verify(LOCAL):
        sys.exit("downloaded bytes do not match the pinned sha256")
    print("pinned file verified")


if __name__ == "__main__":
    main()

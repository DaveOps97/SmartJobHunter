"""
Esegue scraping (main.py) e poi sincronizza CSV -> SQLite.

Uso manuale:
  python -m scripts.run_scrape_and_sync --db /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.db \
      --csv /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.csv

Pensato per schedulazione giornaliera (launchd/cron): esegue scraping e sync.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run scraping then sync CSV->SQLite")
    p.add_argument("--csv", default="/Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.csv", help="Percorso CSV canonico")
    p.add_argument("--db", default="/Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.db", help="Percorso DB SQLite")
    p.add_argument("--chunksize", type=int, default=3000)
    return p.parse_args()


def run_scraping() -> int:
    # Esegue main.py nello stesso interprete
    cmd = [sys.executable, os.path.abspath("/Users/davidelandolfi/PyProjects/ListScraper/main.py")]
    proc = subprocess.run(cmd, capture_output=False)
    return proc.returncode


def run_sync(csv_path: str, db_path: str, chunksize: int) -> int:
    cmd = [
        sys.executable,
        "-m",
        "storage.sync_csv_to_sqlite",
        "--csv",
        os.path.abspath(csv_path),
        "--db",
        os.path.abspath(db_path),
        "--chunksize",
        str(chunksize),
    ]
    proc = subprocess.run(cmd, capture_output=False)
    return proc.returncode


def main() -> None:
    args = parse_args()
    code = run_scraping()
    if code != 0:
        sys.exit(code)
    code = run_sync(args.csv, args.db, args.chunksize)
    sys.exit(code)


if __name__ == "__main__":
    main()



"""
Script di sync CSV -> SQLite con import a chunk e report inseriti/aggiornati.

Uso:
  python -m storage.sync_csv_to_sqlite --csv /percorso/jobs.csv --db /percorso/jobs.db \
      --chunksize 2000
"""

from __future__ import annotations

import argparse
import os
from typing import Tuple

from storage.sqlite_db import upsert_jobs_from_csv


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync CSV -> SQLite (chunked upsert)")
    p.add_argument("--csv", required=True, help="Percorso al CSV sorgente")
    p.add_argument("--db", required=True, help="Percorso al DB SQLite di destinazione")
    p.add_argument("--chunksize", type=int, default=2000, help="Dimensione chunk pandas.read_csv")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    csv_path = os.path.abspath(args.__dict__["csv"])  # preserva assoluto
    db_path = os.path.abspath(args.__dict__["db"])    # preserva assoluto

    inserted, updated = upsert_jobs_from_csv(csv_path=csv_path, db_path=db_path, chunksize=args.chunksize)
    print(f"Sync completata. Inseriti: {inserted}, Aggiornati: {updated}")


if __name__ == "__main__":
    main()



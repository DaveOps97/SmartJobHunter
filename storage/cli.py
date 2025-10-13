"""
CLI per consultare offerte (paginazione/ordinamento) e segnare viewed/applied/notes.

Esempi:
  # Importa CSV in SQLite
  python -m storage.sync_csv_to_sqlite --csv /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.csv \
      --db /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.db --chunksize 3000

  # Lista prima pagina ordinata per score decrescente (default)
  python -m storage.cli list --db /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.db \
      --page 1 --page-size 50 --order-by llm_score --order-dir desc

  # Mostra solo non visionati
  python -m storage.cli list --db ... --only-unviewed

  # Segna una riga come visionata e aggiungi nota
  python -m storage.cli set --db ... --id hc-123 --viewed true --note "interessante"

  # Segna come applicato
  python -m storage.cli set --db ... --id hc-123 --applied true
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

from storage.sqlite_db import query_jobs, set_job_flags


def _bool_from_str(v: str | None) -> bool | None:
    if v is None:
        return None
    lv = v.strip().lower()
    if lv in {"true", "1", "yes", "y"}:
        return True
    if lv in {"false", "0", "no", "n"}:
        return False
    return None


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="CLI ListScraper DB")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_list = sub.add_parser("list", help="Lista lavori con paginazione/ordinamento")
    p_list.add_argument("--db", required=True, help="Percorso DB SQLite")
    p_list.add_argument("--page", type=int, default=1)
    p_list.add_argument("--page-size", type=int, default=50)
    p_list.add_argument("--order-by", default="llm_score")
    p_list.add_argument("--order-dir", default="desc", choices=["asc", "desc", "ASC", "DESC"])
    p_list.add_argument("--only-unviewed", action="store_true")
    p_list.add_argument("--json", action="store_true", help="Output JSON grezzo")

    p_set = sub.add_parser("set", help="Aggiorna flag viewed/applied/notes per id")
    p_set.add_argument("--db", required=True)
    p_set.add_argument("--id", required=True, help="ID riga (`id`) da aggiornare")
    p_set.add_argument("--viewed", required=False, help="true/false")
    p_set.add_argument("--applied", required=False, help="true/false")
    p_set.add_argument("--note", required=False)

    return p


def cmd_list(args: argparse.Namespace) -> None:
    db_path = os.path.abspath(args.__dict__["db"])  # preserva assoluto
    rows, total_rows, total_pages = query_jobs(
        db_path=db_path,
        page=args.page,
        page_size=args.page_size,
        order_by=args.order_by,
        order_dir=args.order_dir,
        only_unviewed=args.only_unviewed,
    )

    if args.__dict__.get("json"):
        print(json.dumps({"rows": rows, "total_rows": total_rows, "total_pages": total_pages}, ensure_ascii=False))
        return

    # Stampa testo leggibile
    print(f"Totale: {total_rows} | Pagine: {total_pages} | Pagina: {args.page}")
    for r in rows:
        rid = r.get("id")
        score = r.get("llm_score")
        title = r.get("title") or "?"
        company = r.get("company") or "?"
        date_posted = r.get("date_posted") or "?"
        viewed = r.get("viewed")
        applied = r.get("applied")
        print(f"- [{score}] {title} @ {company} | {date_posted} | id={rid} | viewed={viewed} | applied={applied}")


def cmd_set(args: argparse.Namespace) -> None:
    db_path = os.path.abspath(args.__dict__["db"])  # preserva assoluto
    job_id = args.__dict__["id"]
    viewed = _bool_from_str(args.__dict__.get("viewed"))
    applied = _bool_from_str(args.__dict__.get("applied"))
    note = args.__dict__.get("note")
    set_job_flags(db_path=db_path, job_id=job_id, viewed=viewed, applied=applied, note=note)
    print("OK")


def main() -> None:
    p = build_parser()
    args = p.parse_args()
    if args.cmd == "list":
        cmd_list(args)
    elif args.cmd == "set":
        cmd_set(args)
    else:
        p.print_help()


if __name__ == "__main__":
    main()



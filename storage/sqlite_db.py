"""
Modulo SQLite: schema, import incrementale da CSV (chunked), query paginate/ordinate,
e aggiornamento flag utente (viewed/applied/notes).

Nota: pensato per CSV molto grandi (10k-20k+ righe, 38+ colonne) con memoria
stabile grazie a pandas.read_csv(..., chunksize=N).
"""

from __future__ import annotations

import os
import math
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


# Colonne note dallo schema del progetto (vedi scrapers/utils.get_expected_columns)
KNOWN_NUMERIC_COLUMNS = {
    "min_amount",
    "max_amount",
}
KNOWN_INTEGER_COLUMNS = {
    "llm_score",
    "llm_score_competenze",
    "llm_score_azienda",
    "llm_score_stipendio",
    "llm_score_località",
    "llm_score_crescita",
    "llm_score_coerenza",
}
KNOWN_BOOLEAN_COLUMNS = {
    "is_remote",
}


USER_FLAG_COLUMNS = [
    "viewed",
    "applied",
    "viewed_at",
    "applied_at",
    "notes",
]


@contextmanager
def get_connection(db_path: str):
    conn = sqlite3.connect(db_path)
    try:
        # Migliora performance su bulk insert
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        yield conn
        conn.commit()
    finally:
        conn.close()


def _map_sql_type(column_name: str) -> str:
    if column_name in KNOWN_INTEGER_COLUMNS:
        return "INTEGER"
    if column_name in KNOWN_NUMERIC_COLUMNS:
        return "REAL"
    if column_name in KNOWN_BOOLEAN_COLUMNS:
        return "INTEGER"  # 0/1
    # Default a TEXT per massima compatibilità
    return "TEXT"


def initialize_db(db_path: str, csv_columns: List[str]) -> None:
    """Crea lo schema se non esiste con colonne dinamiche dal CSV + flag utente.

    - Chiave primaria: id (TEXT) se presente nel CSV, altrimenti rowid implicito
    - Indici utili su colonne di ordinamento comuni
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with get_connection(db_path) as conn:
        cur = conn.cursor()

        has_id = "id" in csv_columns

        # Definisci colonne
        column_defs: List[str] = []
        for col in csv_columns:
            sql_type = _map_sql_type(col)
            column_defs.append(f"`{col}` {sql_type}")

        # Flag utente
        column_defs.extend([
            "`viewed` INTEGER",
            "`applied` INTEGER",
            "`viewed_at` TEXT",
            "`applied_at` TEXT",
            "`notes` TEXT",
        ])

        pk_clause = "PRIMARY KEY(`id`)" if has_id else ""
        create_sql = (
            "CREATE TABLE IF NOT EXISTS jobs ("
            + ",".join(column_defs + ([pk_clause] if pk_clause else []))
            + ")"
        )
        cur.execute(create_sql)

        # Indici utili
        if has_id:
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_id ON jobs(id)")
        for idx_col in ["llm_score", "date_posted", "company", "title", "scraping_date"]:
            if idx_col in csv_columns or idx_col in KNOWN_INTEGER_COLUMNS:
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_jobs_{idx_col} ON jobs({idx_col})"
                )


def _to_python_value(col: str, value: Any) -> Any:
    if pd.isna(value):
        return None
    if col in KNOWN_BOOLEAN_COLUMNS:
        # Normalizza a 0/1
        if isinstance(value, str):
            lv = value.strip().lower()
            return 1 if lv in {"true", "1", "yes", "y"} else 0
        return 1 if bool(value) else 0
    if col in KNOWN_INTEGER_COLUMNS:
        try:
            return int(value)
        except Exception:
            return None
    if col in KNOWN_NUMERIC_COLUMNS:
        try:
            return float(value)
        except Exception:
            return None
    return value


def upsert_jobs_from_csv(csv_path: str, db_path: str, chunksize: int = 2000) -> Tuple[int, int]:
    """Import incrementale del CSV in SQLite con UPSERT su `id`.

    Preserva i flag utente esistenti (viewed/applied/notes) durante gli update.

    Returns:
        (num_inserted, num_updated)
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)

    # Leggi primo chunk per colonne
    first_iter = pd.read_csv(csv_path, nrows=0)
    csv_columns = list(first_iter.columns)
    initialize_db(db_path, csv_columns)

    inserted = 0
    updated = 0

    has_id = "id" in csv_columns

    with get_connection(db_path) as conn:
        cur = conn.cursor()

        # Setup statements
        placeholders = ",".join(["?"] * len(csv_columns))

        if has_id:
            # On conflict su id aggiorna tutte le colonne del CSV ma non toccare i flag utente
            update_assignments = ",".join([f"`{c}`=excluded.`{c}`" for c in csv_columns if c != "id"])
            sql = (
                f"INSERT INTO jobs (" + ",".join([f"`{c}`" for c in csv_columns]) + ") "
                f"VALUES ({placeholders}) "
                f"ON CONFLICT(id) DO UPDATE SET {update_assignments}"
            )
        else:
            # Nessun id: inserimenti semplici
            sql = (
                f"INSERT INTO jobs (" + ",".join([f"`{c}`" for c in csv_columns]) + ") "
                f"VALUES ({placeholders})"
            )

        for chunk in pd.read_csv(csv_path, chunksize=chunksize):
            chunk = chunk.where(pd.notna(chunk), None)
            rows = []
            for _, row in chunk.iterrows():
                values = [_to_python_value(col, row.get(col)) for col in csv_columns]
                rows.append(tuple(values))

            if not rows:
                continue

            if has_id:
                # Per misurare inserted vs updated: contiamo quanti id esistono già
                ids = [str(r[csv_columns.index("id")]) for r in rows]
                q_marks = ",".join(["?"] * len(ids))
                cur.execute(f"SELECT COUNT(1) FROM jobs WHERE id IN ({q_marks})", ids)
                existing_count = cur.fetchone()[0]
                cur.executemany(sql, rows)
                inserted += len(rows) - existing_count
                updated += existing_count
            else:
                cur.executemany(sql, rows)
                inserted += len(rows)

    return inserted, updated


def query_jobs(
    db_path: str,
    page: int = 1,
    page_size: int = 50,
    order_by: str = "llm_score",
    order_dir: str = "DESC",
    only_unviewed: bool = False,
    only_viewed: bool = False,
) -> Tuple[List[Dict[str, Any]], int, int]:
    """Ritorna righe paginate e ordinate.

    Args:
        only_unviewed: se True, mostra solo job con viewed=0 o NULL
        only_viewed: se True, mostra solo job con viewed=1

    Returns:
        (rows, total_rows, total_pages)
    """
    assert page >= 1
    assert page_size >= 1
    order_dir = order_dir.upper()
    if order_dir not in ("ASC", "DESC"):
        order_dir = "DESC"

    with get_connection(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Costruisci WHERE clause
        where_clause = ""
        if only_unviewed and not only_viewed:
            where_clause = "WHERE (viewed IS NULL OR viewed = 0)"
        elif only_viewed and not only_unviewed:
            where_clause = "WHERE viewed = 1"
        # Se entrambi False o entrambi True, nessun filtro

        # Conteggio totale
        cur.execute(f"SELECT COUNT(1) FROM jobs {where_clause}")
        total_rows = int(cur.fetchone()[0])
        total_pages = max(1, math.ceil(total_rows / page_size))
        offset = (page - 1) * page_size

        # Protezione basilare su nome colonna: usa backticks
        order_col = order_by.replace("`", "")
        sql = (
            f"SELECT * FROM jobs {where_clause} "
            f"ORDER BY `{order_col}` {order_dir} NULLS LAST, id {order_dir} "
            f"LIMIT ? OFFSET ?"
        )
        cur.execute(sql, (page_size, offset))
        rows = [dict(r) for r in cur.fetchall()]

    return rows, total_rows, total_pages


def set_job_flags(
    db_path: str,
    job_id: str,
    viewed: Optional[bool] = None,
    applied: Optional[bool] = None,
    note: Optional[str] = None,
) -> None:
    """Aggiorna i flag utente per una riga identificata da `id`."""
    if job_id is None:
        raise ValueError("job_id richiesto per aggiornare i flag")

    updates: List[str] = []
    params: List[Any] = []

    now_iso = datetime.utcnow().isoformat(timespec="seconds")

    if viewed is not None:
        updates.append("viewed=?")
        params.append(1 if viewed else 0)
        updates.append("viewed_at=?")
        params.append(now_iso if viewed else None)

    if applied is not None:
        updates.append("applied=?")
        params.append(1 if applied else 0)
        updates.append("applied_at=?")
        params.append(now_iso if applied else None)

    if note is not None:
        updates.append("notes=?")
        params.append(note)

    if not updates:
        return

    params.append(job_id)
    sql = f"UPDATE jobs SET {', '.join(updates)} WHERE id=?"

    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
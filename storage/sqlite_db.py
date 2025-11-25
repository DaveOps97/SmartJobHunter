"""
Modulo SQLite: schema, upsert diretto da DataFrame (chunked), query paginate/ordinate,
e aggiornamento flag utente (viewed/interested/applied/notes).

Nota: pensato per DataFrame molto grandi (10k-20k+ righe, 38+ colonne) con memoria
stabile grazie a chunking manuale.
"""

from __future__ import annotations

import os
import math
import sqlite3
import logging
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

# Setup logging
logger = logging.getLogger(__name__)


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
    "interested",
    "applied",
    "viewed_at",
    "interested_at",
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
    except sqlite3.Error as e:
        conn.rollback()
        logger.error(f"SQLite error: {e}")
        raise
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


def initialize_db(db_path: str, columns: List[str]) -> None:
    """Crea lo schema se non esiste con colonne dinamiche dal DataFrame + flag utente.

    - Chiave primaria: id (TEXT) se presente nelle colonne, altrimenti rowid implicito
    - Indici utili su colonne di ordinamento comuni
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with get_connection(db_path) as conn:
        cur = conn.cursor()

        has_id = "id" in columns

        # Definisci colonne
        column_defs: List[str] = []
        for col in columns:
            sql_type = _map_sql_type(col)
            column_defs.append(f"`{col}` {sql_type}")

        # Flag utente (inclusi timestamp)
        column_defs.extend([
            "`viewed` INTEGER",
            "`interested` INTEGER",
            "`applied` INTEGER",
            "`viewed_at` TEXT",
            "`interested_at` TEXT",
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

        # Aggiungi colonne mancanti se il DB esiste già
        cur.execute("PRAGMA table_info(jobs)")
        existing_columns = {row[1] for row in cur.fetchall()}
        
        # Lista di tutte le colonne da verificare e aggiungere
        required_columns = [
            ("interested", "INTEGER"),
            ("interested_at", "TEXT"),
            ("viewed_at", "TEXT"),
            ("applied_at", "TEXT"),
        ]
        
        for col_name, col_type in required_columns:
            if col_name not in existing_columns:
                print(f"Aggiungendo colonna mancante: {col_name}")
                cur.execute(f"ALTER TABLE jobs ADD COLUMN `{col_name}` {col_type}")

        # Indici utili
        if has_id:
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_id ON jobs(id)")
        for idx_col in ["llm_score", "date_posted", "company", "location", "title", "scraping_date"]:
            if idx_col in columns or idx_col in KNOWN_INTEGER_COLUMNS:
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


def get_existing_job_ids(db_path: str, job_ids: List[str]) -> set[str]:
    """Identifica quali job ID sono già presenti nel database.
    
    Args:
        db_path: Percorso al database SQLite
        job_ids: Lista di ID da verificare
        
    Returns:
        Set di ID già presenti nel database
    """
    if not job_ids:
        return set()
    
    try:
        with get_connection(db_path) as conn:
            cur = conn.cursor()
            # Verifica se la tabella esiste
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='jobs'")
            if not cur.fetchone():
                return set()
            
            # Verifica se la colonna id esiste
            cur.execute("PRAGMA table_info(jobs)")
            columns = [row[1] for row in cur.fetchall()]
            if "id" not in columns:
                return set()
            
            # Query per trovare gli ID esistenti
            placeholders = ",".join(["?"] * len(job_ids))
            cur.execute(f"SELECT id FROM jobs WHERE id IN ({placeholders})", job_ids)
            existing_ids = {str(row[0]) for row in cur.fetchall()}
            return existing_ids
    except sqlite3.Error as e:
        logger.warning(f"Errore durante la verifica degli ID esistenti: {e}")
        return set()


def upsert_jobs(db_path: str, jobs_dataframe: pd.DataFrame, batch_size: int = 2000) -> Tuple[int, int]:
    """Upsert diretto di un DataFrame in SQLite con chunking per performance.

    Preserva i flag utente esistenti (viewed/interested/applied/notes) durante gli update.
    Aggiunge automaticamente scraping_date se mancante.

    Args:
        db_path: Percorso al database SQLite
        jobs_dataframe: DataFrame pandas con i job da inserire/aggiornare
        batch_size: Dimensione dei chunk per il processing (default: 2000)

    Returns:
        (num_inserted, num_updated)

    Raises:
        sqlite3.Error: In caso di errori SQLite (con retry automatico)
    """
    if jobs_dataframe is None or jobs_dataframe.empty:
        logger.info("DataFrame vuoto, nessun upsert necessario")
        return 0, 0

    # Aggiungi scraping_date se mancante
    if 'scraping_date' not in jobs_dataframe.columns:
        scraping_date = datetime.now().strftime("%Y-%m-%d")
        jobs_dataframe = jobs_dataframe.copy()
        jobs_dataframe['scraping_date'] = scraping_date
        logger.info(f"Aggiunto scraping_date automatico: {scraping_date}")

    # Ottieni colonne dal DataFrame
    df_columns = list(jobs_dataframe.columns)
    initialize_db(db_path, df_columns)

    inserted = 0
    updated = 0
    has_id = "id" in df_columns

    # Retry logic con exponential backoff
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with get_connection(db_path) as conn:
                cur = conn.cursor()

                # Setup statements
                placeholders = ",".join(["?"] * len(df_columns))

                if has_id:
                    # On conflict su id aggiorna tutte le colonne del DataFrame ma non toccare i flag utente
                    update_assignments = ",".join([f"`{c}`=excluded.`{c}`" for c in df_columns if c != "id"])
                    sql = (
                        f"INSERT INTO jobs (" + ",".join([f"`{c}`" for c in df_columns]) + ") "
                        f"VALUES ({placeholders}) "
                        f"ON CONFLICT(id) DO UPDATE SET {update_assignments}"
                    )
                else:
                    # Nessun id: inserimenti semplici
                    sql = (
                        f"INSERT INTO jobs (" + ",".join([f"`{c}`" for c in df_columns]) + ") "
                        f"VALUES ({placeholders})"
                    )

                # Processa in chunk per evitare memory overflow
                total_rows = len(jobs_dataframe)
                num_chunks = math.ceil(total_rows / batch_size)
                logger.info(f"Processing {total_rows} righe in {num_chunks} chunk(s) di {batch_size}")

                for chunk_idx in range(num_chunks):
                    start_idx = chunk_idx * batch_size
                    end_idx = min((chunk_idx + 1) * batch_size, total_rows)
                    chunk = jobs_dataframe.iloc[start_idx:end_idx].copy()

                    chunk = chunk.where(pd.notna(chunk), None)
                    rows = []
                    for _, row in chunk.iterrows():
                        values = [_to_python_value(col, row.get(col)) for col in df_columns]
                        rows.append(tuple(values))

                    if not rows:
                        continue

                    if has_id:
                        # Per misurare inserted vs updated: contiamo quanti id esistono già
                        ids = [str(r[df_columns.index("id")]) for r in rows]
                        q_marks = ",".join(["?"] * len(ids))
                        cur.execute(f"SELECT COUNT(1) FROM jobs WHERE id IN ({q_marks})", ids)
                        existing_count = cur.fetchone()[0]
                        cur.executemany(sql, rows)
                        inserted += len(rows) - existing_count
                        updated += existing_count
                    else:
                        cur.executemany(sql, rows)
                        inserted += len(rows)

                    logger.info(f"Chunk {chunk_idx + 1}/{num_chunks} processato: {len(rows)} righe")

            # Successo: esci dal retry loop
            break

        except sqlite3.Error as e:
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) * 0.1  # Exponential backoff: 0.1s, 0.2s, 0.4s
                logger.warning(f"SQLite error (tentativo {attempt + 1}/{max_retries}): {e}. Retry tra {wait_time}s...")
                time.sleep(wait_time)
            else:
                logger.error(f"SQLite error dopo {max_retries} tentativi: {e}")
                raise

    logger.info(f"Upsert completato: {inserted} inseriti, {updated} aggiornati")
    return inserted, updated


def query_jobs(
    db_path: str,
    page: int = 1,
    page_size: int = 50,
    order_by: str = "llm_score",
    order_dir: str = "DESC",
    mode: str = "not_viewed",
) -> Tuple[List[Dict[str, Any]], int, int]:
    """Ritorna righe paginate e ordinate.

    Args:
        mode: filtra per stato
            - "not_viewed": viewed=0/NULL AND interested=0/NULL AND applied=0/NULL
            - "viewed": viewed=1 AND interested=0/NULL AND applied=0/NULL
            - "interested": interested=1 AND applied=0/NULL
            - "applied": applied=1

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

        # Costruisci WHERE clause basata su mode
        where_clause = ""
        if mode == "not_viewed":
            where_clause = "WHERE (viewed IS NULL OR viewed = 0) AND (interested IS NULL OR interested = 0) AND (applied IS NULL OR applied = 0)"
        elif mode == "viewed":
            where_clause = "WHERE viewed = 1 AND (interested IS NULL OR interested = 0) AND (applied IS NULL OR applied = 0)"
        elif mode == "interested":
            where_clause = "WHERE interested = 1 AND (applied IS NULL OR applied = 0)"
        elif mode == "applied":
            where_clause = "WHERE applied = 1"

        # Conteggio totale
        cur.execute(f"SELECT COUNT(1) FROM jobs {where_clause}")
        total_rows = int(cur.fetchone()[0])
        total_pages = max(1, math.ceil(total_rows / page_size))
        offset = (page - 1) * page_size

        # Protezione basilare su nome colonna: usa backticks
        order_col = order_by.replace("`", "")

        # Aggiungi sempre scraping_date DESC come ordinamento secondario
        order_clause = f"ORDER BY `{order_col}` {order_dir} NULLS LAST, `scraping_date` DESC, id {order_dir}"

        sql = (
            f"SELECT * FROM jobs {where_clause} "
            f"{order_clause} "
            f"LIMIT ? OFFSET ?"
        )
        cur.execute(sql, (page_size, offset))
        rows = [dict(r) for r in cur.fetchall()]

    return rows, total_rows, total_pages


def set_job_flags(
    db_path: str,
    job_id: str,
    viewed: Optional[bool] = None,
    interested: Optional[bool] = None,
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

    if interested is not None:
        updates.append("interested=?")
        params.append(1 if interested else 0)
        updates.append("interested_at=?")
        params.append(now_iso if interested else None)

    if applied is not None:
        updates.append("applied=?")
        params.append(1 if applied else 0)
        updates.append("applied_at=?")
        params.append(now_iso if applied else None)

    if note is not None:
        updates.append("notes=?")
        params.append(note)

    if not updates:
        # Non sollevare errore, semplicemente ritorna
        return

    params.append(job_id)
    sql = f"UPDATE jobs SET {', '.join(updates)} WHERE id=?"

    with get_connection(db_path) as conn:
        cur = conn.cursor()
        result = cur.execute(sql, params)
        if result.rowcount == 0:
            raise ValueError(f"Job con id '{job_id}' non trovato")
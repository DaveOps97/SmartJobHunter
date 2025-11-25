"""
Esegue scraping (main.py) che scrive direttamente nel database SQLite.

Uso manuale:
  python -m scripts.run_scrape_and_sync

Pensato per schedulazione giornaliera (launchd/cron): esegue scraping che aggiorna direttamente il DB.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from storage.sqlite_db import get_connection

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "storage" / "jobs.db"


def run_scraping() -> int:
    """Esegue main.py che scrive direttamente nel database SQLite."""
    cmd = [sys.executable, os.path.abspath(str(PROJECT_ROOT / "main.py"))]
    proc = subprocess.run(cmd, capture_output=False)
    return proc.returncode


def cleanup_stale_jobs(
    db_path: Optional[str] = None,
    low_score_retention_days: int = 7,
    absolute_retention_days: int = 30,
    score_threshold: int = 5,
) -> None:
    """Rimuove job vecchi dal database secondo due criteri:
    
    1. Job con score basso (<=threshold) più vecchi di low_score_retention_days
    2. Job qualsiasi più vecchi di absolute_retention_days (esclusi quelli con applied=True)
    
    Args:
        db_path: Path al database SQLite (default: DEFAULT_DB_PATH)
        low_score_retention_days: Giorni di retention per job con score basso (default: 7)
        absolute_retention_days: Giorni oltre i quali rimuovere tutti i job non applicati (default: 30)
        score_threshold: Soglia punteggio - rimuove job con score <= threshold (default: 5)
    """
    path = Path(db_path) if db_path else DEFAULT_DB_PATH
    if not path.exists():
        print(f"[CLEANUP] Database non trovato in {path}, nessuna rimozione eseguita.")
        return

    low_score_cutoff = (datetime.now() - timedelta(days=low_score_retention_days)).strftime("%Y-%m-%d")
    absolute_cutoff = (datetime.now() - timedelta(days=absolute_retention_days)).strftime("%Y-%m-%d")
    
    try:
        with get_connection(str(path)) as conn:
            cur = conn.cursor()
            
            # Query con doppia logica usando OR [web:90][web:93]
            cur.execute(
                """
                DELETE FROM jobs
                WHERE scraping_date IS NOT NULL
                  AND (
                    -- Criterio 1: Score basso e vecchi di 7+ giorni
                    (
                        scraping_date <= ?
                        AND llm_score IS NOT NULL
                        AND llm_score <= ?
                    )
                    OR
                    -- Criterio 2: Qualsiasi job vecchio di 30+ giorni (non applicato)
                    (
                        scraping_date <= ?
                        AND (applied IS NULL OR applied = 0)
                    )
                  )
                """,
                (low_score_cutoff, score_threshold, absolute_cutoff),
            )
            removed = cur.rowcount if cur.rowcount is not None else 0
            
            # Statistiche dettagliate per logging
            cur.execute(
                """
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN llm_score <= ? THEN 1 END) as low_score,
                    COUNT(CASE WHEN scraping_date < ? THEN 1 END) as very_old
                FROM jobs
                WHERE scraping_date IS NOT NULL
                """,
                (score_threshold, absolute_cutoff)
            )
            stats = cur.fetchone()
            
    except Exception as exc:
        print(f"[CLEANUP] Errore durante la pulizia del DB: {exc}")
        raise

    print(f"[CLEANUP] Rimosse {removed} righe:")
    print(f"  - Job con score <= {score_threshold} e date < {low_score_cutoff}")
    print(f"  - Job con date < {absolute_cutoff} e applied != True")
    if stats:
        print(f"[CLEANUP] Rimasti nel DB: {stats[0]} job totali "
              f"({stats[1]} con score <= {score_threshold}, {stats[2]} più vecchi di {absolute_retention_days} giorni)")


def main() -> None:
    """Esegue lo scraping e pulisce il DB dai job vecchi."""
    code = run_scraping()
    if code == 0:
        cleanup_stale_jobs(
            db_path=os.getenv("LISTSCRAPER_DB_PATH"),
            low_score_retention_days=int(os.getenv("LOW_SCORE_RETENTION_DAYS", "7")),
            absolute_retention_days=int(os.getenv("ABSOLUTE_RETENTION_DAYS", "30")),
            score_threshold=int(os.getenv("SCORE_THRESHOLD", "5"))
        )
    sys.exit(code)


if __name__ == "__main__":
    main()

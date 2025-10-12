"""
Utility functions comuni per tutti gli scraper
"""

import pandas as pd
from scrapers.llm import enrich_dataframe_with_llm
import csv
import os
from pathlib import Path
import re
from html import unescape


def clean_html_text(text: str) -> str:
    """
    Rimuove tutti i tag HTML e CSS dalle descrizioni dei lavori, mantenendo solo il testo pulito.
    
    Args:
        text: Testo contenente HTML/CSS da pulire
        
    Returns:
        Testo pulito senza tag HTML/CSS
    """
    if not text or not isinstance(text, str):
        return text
    
    # Decodifica le entità HTML (es. &amp; -> &, &lt; -> <)
    text = unescape(text)
    
    # Rimuovi tutti i tag HTML (inclusi quelli con attributi CSS)
    # Pattern per catturare tag con attributi: <tag attributi>contenuto</tag>
    text = re.sub(r'<[^>]+>', ' ', text)
    
    # Rimuovi caratteri di controllo e spazi multipli
    text = re.sub(r'\s+', ' ', text)
    
    # Rimuovi spazi all'inizio e alla fine
    text = text.strip()
    
    return text


def align_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Allinea un DataFrame alle colonne attese, aggiungendo colonne mancanti con None"""
    if df is None or df.empty:
        return pd.DataFrame(columns=columns)
    for col in columns:
        if col not in df.columns:
            df[col] = None
    # Conserva solo colonne attese nell'ordine
    return df[columns]


def get_expected_columns(existing_df: pd.DataFrame = None, fallback_df: pd.DataFrame = None) -> tuple[list[str], bool]:
    """
    Determina le colonne attese dello schema finale.
    
    Returns:
        tuple: (expected_columns, schema_upgrade_required)
    """
    # Schema fisso definito a priori
    FIXED_SCHEMA = [
        # Identificatori
        'id', 'site', 'job_url', 'job_url_direct', 'title', 'company',
        
        # Posizione e tempo
        'location', 'date_posted', 'job_type', 'is_remote', 'work_from_home_type',
        
        # Compenso
        'interval', 'min_amount', 'max_amount', 'currency',
        
        # Ruolo e competenze
        'job_level', 'job_function', 'skills', 'description',
        
        # Azienda
        'company_url', 'company_logo', 'company_num_employees', 'company_revenue', 
        'company_description', 'company_industries', 'company_activities',
        
        # Campi aggiuntivi HiringCafe
        'language_requirements', 'role_activities',
        
        # Colonne arricchimento LLM
        'llm_score', 'llm_motivazione', 'llm_match_competenze',
        
        # Data di scraping
        'data_scraping',
    ]
    
    if existing_df is not None:
        # Se esiste un CSV, usa le sue colonne ma aggiungi quelle mancanti dello schema fisso
        existing_columns = list(existing_df.columns)
        missing_columns = [col for col in FIXED_SCHEMA if col not in existing_columns]
        expected_columns = existing_columns + missing_columns
        schema_upgrade_required = len(missing_columns) > 0
    else:
        # Nessun CSV esistente: usa schema fisso
        expected_columns = FIXED_SCHEMA.copy()
        schema_upgrade_required = False
        
    return expected_columns, schema_upgrade_required


def save_jobs_to_csv(all_jobs_df: pd.DataFrame, existing_path: str) -> None:
    """
    Salva i job nel CSV, gestendo upgrade di schema e append incrementale.
    """
    existing_df = None
    if os.path.exists(existing_path):
        try:
            existing_df = pd.read_csv(existing_path)
            print(f"\n[CSV] {existing_path} esistente caricato: {len(existing_df)} righe")
        except Exception as e:
            print(f"[CSV] Errore lettura {existing_path}: {e}")
            existing_df = None
    
    expected_columns, schema_upgrade_required = get_expected_columns(existing_df, all_jobs_df)
    
    # Allinea le colonne
    all_jobs_aligned = align_columns(all_jobs_df, expected_columns)
    
    # Calcolo differenza e scrittura incrementale
    if existing_df is not None and not existing_df.empty:
        existing_ids = set(existing_df['id'].astype(str)) if 'id' in existing_df.columns else set()
        mask_new = ~all_jobs_aligned['id'].astype(str).isin(existing_ids)
        new_rows = all_jobs_aligned[mask_new]
        num_new = len(new_rows)
        print(f"[CSV] Nuovi job rispetto a {existing_path}: {num_new}")
        
        if schema_upgrade_required:
            # In caso di upgrade schema, mantieni righe esistenti allineate e arricchisci solo le nuove
            print("[CSV] Upgrade schema rilevato: riscrivo file con merge esistente + nuove righe arricchite")
            existing_aligned = align_columns(existing_df, expected_columns)
            enriched_new = enrich_dataframe_with_llm(new_rows) if num_new > 0 else new_rows
            merged = pd.concat([existing_aligned, enriched_new], ignore_index=True)
            merged.to_csv(existing_path, quoting=csv.QUOTE_NONNUMERIC, escapechar="\\", index=False)
            print(f"[CSV] {existing_path} riscritto con {len(merged)} righe")
        elif num_new > 0:
            # Append senza header
            enriched_new = enrich_dataframe_with_llm(new_rows)
            enriched_new.to_csv(existing_path, mode='a', header=False, quoting=csv.QUOTE_NONNUMERIC, escapechar="\\", index=False)
            print(f"[CSV] Appesi {num_new} nuovi annunci a {existing_path}")
        else:
            print("[CSV] Nessun nuovo annuncio da aggiungere")
    else:
        # Nessun csv esistente: crea file con tutto
        if not all_jobs_aligned.empty:
            # Assicurati che la directory esista
            Path(existing_path).parent.mkdir(parents=True, exist_ok=True)
            # Primo file: considera tutte le righe come "nuove" da arricchire
            enriched = enrich_dataframe_with_llm(all_jobs_aligned)
            enriched.to_csv(existing_path, quoting=csv.QUOTE_NONNUMERIC, escapechar="\\", index=False)
            print(f"[CSV] Creato {existing_path} con {len(all_jobs_aligned)} righe")
        else:
            print("[CSV] Nessun dato da scrivere")

"""
Utility functions comuni per tutti gli scraper
"""

import pandas as pd
import csv
import os
from pathlib import Path


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
    # Nuove colonne multi-valore da HiringCafe che vogliamo includere sempre
    EXTRA_HC_COLUMNS = [
        'company_industries',
        'company_activities', 
        'language_requirements',
        'role_activities',
    ]
    
    if existing_df is not None:
        expected_columns = list(existing_df.columns)
    elif fallback_df is not None and not fallback_df.empty:
        expected_columns = list(fallback_df.columns)
    else:
        # fallback minimo, verrà arricchito quando arriveranno dati
        expected_columns = [
            'id','site','job_url','job_url_direct','title','company','location','date_posted','job_type',
            'interval','min_amount','max_amount','currency','is_remote','job_level','job_function',
            'emails','description','company_url','company_logo',
            'company_num_employees','company_revenue','company_description','skills',
            'work_from_home_type'
        ]
    
    # Se il CSV esistente non ha queste colonne, segniamo un upgrade di schema
    missing_extra = [c for c in EXTRA_HC_COLUMNS if c not in expected_columns]
    if missing_extra:
        expected_columns.extend(missing_extra)
        schema_upgrade_required = existing_df is not None
    else:
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
            # Riscrive l'intero CSV con il nuovo schema (header aggiornato)
            print("[CSV] Upgrade schema rilevato: riscrivo {existing_path} con nuovo header")
            all_jobs_aligned.to_csv(existing_path, quoting=csv.QUOTE_NONNUMERIC, escapechar="\\", index=False)
            print(f"[CSV] {existing_path} riscritto con {len(all_jobs_aligned)} righe")
        elif num_new > 0:
            # Append senza header
            new_rows.to_csv(existing_path, mode='a', header=False, quoting=csv.QUOTE_NONNUMERIC, escapechar="\\", index=False)
            print(f"[CSV] Appesi {num_new} nuovi annunci a {existing_path}")
        else:
            print("[CSV] Nessun nuovo annuncio da aggiungere")
    else:
        # Nessun csv esistente: crea file con tutto
        if not all_jobs_aligned.empty:
            # Assicurati che la directory esista
            Path(existing_path).parent.mkdir(parents=True, exist_ok=True)
            all_jobs_aligned.to_csv(existing_path, quoting=csv.QUOTE_NONNUMERIC, escapechar="\\", index=False)
            print(f"[CSV] Creato {existing_path} con {len(all_jobs_aligned)} righe")
        else:
            print("[CSV] Nessun dato da scrivere")

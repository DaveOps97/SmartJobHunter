"""
Utility functions comuni per tutti gli scraper
"""

import pandas as pd
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
        'scraping_date',
    ]
    
    if existing_df is not None:
        # Se esiste un DataFrame, usa le sue colonne ma aggiungi quelle mancanti dello schema fisso
        existing_columns = list(existing_df.columns)
        missing_columns = [col for col in FIXED_SCHEMA if col not in existing_columns]
        expected_columns = existing_columns + missing_columns
        schema_upgrade_required = len(missing_columns) > 0
    else:
        # Nessun DataFrame esistente: usa schema fisso
        expected_columns = FIXED_SCHEMA.copy()
        schema_upgrade_required = False
        
    return expected_columns, schema_upgrade_required

def combine_sources(*dataframes: pd.DataFrame, expected_columns: list[str]) -> pd.DataFrame:
    """
    Combina multiple fonti di job scraping in un unico DataFrame.
    
    Args:
        *dataframes: Uno o più DataFrame da combinare
        expected_columns: Lista delle colonne attese nello schema finale
        
    Returns:
        DataFrame combinato e deduplicato, o DataFrame vuoto se nessuna fonte ha dati
    """
    frames = []
    
    for df in dataframes:
        aligned_df = align_columns(df, expected_columns)
        if not aligned_df.empty:
            # Estrai il nome della fonte dalla colonna 'site' (usa il primo valore)
            source_name = aligned_df['site'].iloc[0] if 'site' in aligned_df.columns else "Unknown"
            frames.append(aligned_df)
            print(f"{source_name}: {len(aligned_df)} job raccolti")
    
    if not frames:
        print("⚠️  Nessun job raccolto dalle fonti configurate")
        return pd.DataFrame(columns=expected_columns)
    
    all_sources = pd.concat(frames, ignore_index=True)
    
    # Deduplicazione (protezione race condition multithreading)
    all_sources = all_sources.drop_duplicates(subset=['id'], keep='first')
    
    print(f"Totale raccolti: {len(all_sources)} job unici")
    return all_sources



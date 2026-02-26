"""
Main script per il scraping di job da multiple fonti
"""

import pandas as pd
from datetime import datetime
from pathlib import Path
from scrapers import scrape_all_locations, fetch_hiring_cafe_dataframe
from scrapers.utils import get_expected_columns, combine_sources
from scrapers.llm import initialize_api_keys, enrich_dataframe_with_llm
from storage.sqlite_db import get_db_path, get_jobs_to_enrich, upsert_jobs, get_connection


def load_env_from_root():
    """Carica variabili d'ambiente dal .env"""
    import os
    from scrapers.llm import initialize_api_keys
    
    project_root = Path(__file__).parent
    env_file = project_root / ".env"
    
    if env_file.exists():
        with open(env_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()
    
    # Carica multiple keys
    api_keys = []
    for i in range(1, 11):  # Supporta fino a 10 progetti
        key = os.getenv(f"GEMINI_API_KEY_{i}")
        if key:
            api_keys.append(key)
    
    # Fallback su key singola (compatibilità)
    if not api_keys:
        single = os.getenv("FREE_GEMINI_API_KEY") or os.getenv("GEMINI_API_KEY")
        if single:
            api_keys = [single]
    
    if not api_keys:
        raise RuntimeError("Nessuna API key trovata nel .env")
    
    is_free = len(api_keys) > 1 or bool(os.getenv("FREE_GEMINI_API_KEY"))
    
    print(f"🔑 Progetti configurati: {len(api_keys)}")
    initialize_api_keys(api_keys, is_free)


# Lista delle città italiane da cercare
locations = [
        "Milano, Lombardia",      # Hub tech principale (70% job tech Italia)
        # "Torino, Piemonte",       # Automotive, fintech, AI
        # "Bologna, Emilia-Romagna", # Tech hub emergente, pharma-tech
        # "Firenze, Toscana",       # Scale-up, turismo tech
        # "Verona, Veneto",         # Logistica, manufacturing tech
        # "Genova, Liguria",        # Porto, shipping tech
        # "Brescia, Lombardia",     # Manufacturing, industria 4.0
        # "Venezia, Veneto",        # Turismo tech, port tech
        "Padova, Veneto",         # Healthcare tech, università
        # "Parma, Emilia-Romagna",  # Food tech, automotive
        "Foligno, Umbria", 
        "Perugia, Umbria",
        #"Roma, Lazio",            #
        "Napoli, Campania",        #
        "Caserta, Campania",        #
]

jobspy_search_term = (
    '(data OR python OR java OR backend OR software OR machine OR learning OR AI OR ML OR ETL OR big data) '
    '(engineer OR developer OR scientist) '
    '(python OR java OR spark OR pyspark OR docker OR kubernetes OR '
    'scrapy OR mongodb OR postgresql OR rest OR api OR fastapi OR '
    'langchain OR haystack OR llm OR git OR spring OR kafka OR microservices OR arangodb OR pinecone) '
    '-senior -lead -manager -architect -principal'
)


def main():
    """Funzione principale che coordina tutti gli scraper"""
    
    # === Setup ===
    load_env_from_root()
    
    scraping_date = datetime.now().strftime("%Y-%m-%d")
    print(f"[DATA] Data di scraping: {scraping_date}")
    
    expected_columns, _ = get_expected_columns()
    
    # === Scraping ===
    print("=== INIZIO SCRAPING JOBSPY ===")
    jobspy_df = scrape_all_locations(
        locations=locations,
        search_term=jobspy_search_term,
        hours_old=26,
        results_wanted=60
    ) # hours_old:results_wanted -> 26:60, 60:120, 128:150
    
    print("\n=== INIZIO SCRAPING HIRINGCAFE ===")
    hiring_df = None
    # hiring_df = fetch_hiring_cafe_dataframe(...)
    
    # === Combinazione fonti ===
    print(f"\n=== COMBINAZIONE FONTI ===")
    all_sources = combine_sources(jobspy_df, hiring_df, expected_columns=expected_columns)
    
    if all_sources.empty:
        print("⚠️  Nessun job da processare.")
        return
    
    all_sources['scraping_date'] = scraping_date
    
    # === Identifica job da arricchire ===
    jobs_to_enrich = get_jobs_to_enrich(all_sources)
    
    if jobs_to_enrich.empty:
        print("Nessun job da arricchire.")
        return
    
    # === Arricchimento LLM ===
    print(f"\nProcessando {len(jobs_to_enrich)} job con LLM...")
    enriched_jobs = enrich_dataframe_with_llm(jobs_to_enrich, batch_size=1)
    
    # === Salvataggio ===
    print(f"\n=== SALVATAGGIO NEL DATABASE ===")
    inserted, updated = upsert_jobs(get_db_path(), enriched_jobs, batch_size=2000)
    print(f"✅ DB aggiornato: {inserted} nuovi, {updated} aggiornati")
    
    # === Verifica job NULL residui ===
    try:
        with get_connection(get_db_path()) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM jobs WHERE llm_score IS NULL")
            remaining_null = cur.fetchone()[0]
            print(f"📊 Job con llm_score NULL rimasti nel DB: {remaining_null}")
    except Exception as e:
        print(f"⚠️  Errore verifica job NULL: {e}")


if __name__ == "__main__":
    main()

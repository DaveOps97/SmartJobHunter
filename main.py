"""
Main script per il scraping di job da multiple fonti
"""

import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from scrapers import scrape_all_locations, fetch_hiring_cafe_dataframe
from scrapers.utils import align_columns, get_expected_columns, save_jobs_to_csv
from scrapers.llm import initialize_api_key


def load_env_from_root():
    """Carica variabili d'ambiente dal file .env nella root del progetto"""
    # Trova la root del progetto (directory che contiene main.py)
    project_root = Path(__file__).parent
    env_file = project_root / ".env"
    
    if env_file.exists():
        with open(env_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()
    
    # Carica l'API key e determina il tipo
    api_key = os.getenv("FREE_GEMINI_API_KEY") or os.getenv("GEMINI_API_KEY")
    is_free_api_key = bool(os.getenv("FREE_GEMINI_API_KEY"))
    
    if not api_key:
        raise RuntimeError("API key mancante: imposta la variabile d'ambiente FREE_GEMINI_API_KEY o GEMINI_API_KEY")
    
    # Stampa il messaggio di caricamento una sola volta
    if is_free_api_key:
        print("🔑 Utilizzo FREE_GEMINI_API_KEY (rate limiting attivo: 15 req/min)")
    else:
        print("🔑 Utilizzo GEMINI_API_KEY (nessun rate limiting)")
    
    # Inizializza le variabili globali nel modulo llm
    initialize_api_key(api_key, is_free_api_key)


# Lista delle città italiane da cercare
locations = [
        "Milano, Lombardia",      # Hub tech principale (70% job tech Italia)
        "Torino, Piemonte",       # Automotive, fintech, AI
        "Bologna, Emilia-Romagna", # Tech hub emergente, pharma-tech
        "Firenze, Toscana",       # Scale-up, turismo tech
        "Verona, Veneto",         # Logistica, manufacturing tech
        "Genova, Liguria",        # Porto, shipping tech
        "Brescia, Lombardia",     # Manufacturing, industria 4.0
        "Venezia, Veneto",        # Turismo tech, port tech
        "Padova, Veneto",         # Healthcare tech, università
        "Parma, Emilia-Romagna",  # Food tech, automotive
        "Perugia, Umbria",
        "Roma, Lazio",            #
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
    
    # === Caricamento API key globale ===
    load_env_from_root()
    
    # === Data di scraping ===
    scraping_date = datetime.now().strftime("%Y-%m-%d")
    print(f"[DATA] Data di scraping: {scraping_date}")
    
    # === Schema fisso definito a priori ===
    expected_columns, _ = get_expected_columns()
   # print(f"[SCHEMA] Schema fisso definito con {len(expected_columns)} colonne")
    
    # === JobSpy Scraping ===
    print("=== INIZIO SCRAPING JOBSPY ===")
    jobspy_df = scrape_all_locations(locations=locations, search_term=jobspy_search_term, hours_old=26, results_wanted=60) # 26, 60
    
    # === HiringCafe Scraping ===
    print("\n=== INIZIO SCRAPING HIRINGCAFE ===")
    hiring_query = ''
    
    # Fetch da HiringCafe
    hiring_df = fetch_hiring_cafe_dataframe(expected_columns=expected_columns, search_query=hiring_query, date_filter= "1_day", max_pages=3)
    
    # === Combinazione e salvataggio ===
    print(f"\n=== COMBINAZIONE FONTI ===")
    
    # Allinea colonne
    jobspy_aligned = align_columns(jobspy_df, expected_columns)
    hiring_aligned = align_columns(hiring_df, expected_columns)
    
    # Unione fonti e deduplicazione (filtra DF vuoti per evitare FutureWarning)
    frames = []
    for df in [jobspy_aligned, hiring_aligned]:
        if df is not None and not df.empty and len(df) > 0:
            # Assicurati che il DataFrame abbia tutte le colonne attese prima della concatenazione
            df_with_all_cols = align_columns(df, expected_columns)
            frames.append(df_with_all_cols)
    
    if frames:
        all_sources = pd.concat(frames, ignore_index=True)
    else:
        all_sources = pd.DataFrame(columns=expected_columns)
    all_sources_unique = all_sources.dropna(subset=['id']) if 'id' in all_sources.columns else all_sources
    all_sources_unique = all_sources_unique.drop_duplicates(subset=['id'], keep='first') if 'id' in all_sources_unique.columns else all_sources_unique
    
    # Aggiungi la data di scraping a tutte le righe
    if not all_sources_unique.empty:
        all_sources_unique['scraping_date'] = scraping_date
    
    print(f"Totale raccolti (jobspy + hiring.cafe): {len(all_sources_unique)} unici")
    
    # Salvataggio finale (CSV "canonico" usato come sorgente per il DB)
    file_path = "/Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.csv"
    save_jobs_to_csv(all_sources_unique, file_path)



if __name__ == "__main__":
    main()
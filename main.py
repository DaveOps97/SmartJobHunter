"""
Main script per il scraping di job da multiple fonti
"""

import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from scrapers import scrape_all_locations, fetch_hiring_cafe_dataframe
from scrapers.utils import align_columns, get_expected_columns
from scrapers.llm import initialize_api_key, enrich_dataframe_with_llm
from storage.sqlite_db import upsert_jobs, get_existing_job_ids


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
    jobspy_df = scrape_all_locations(locations=locations, search_term=jobspy_search_term, hours_old=26, results_wanted=60) # 26(60), 60(120), 128(150)
    
    # === HiringCafe Scraping ===
    print("\n=== INIZIO SCRAPING HIRINGCAFE ===")
    hiring_query = ''
    
    # Fetch da HiringCafe (può essere commentato se non si vuole usare)
    hiring_df = None
    # hiring_df = fetch_hiring_cafe_dataframe(expected_columns=expected_columns, search_query=hiring_query, date_filter= "1_day", max_pages=3)
    
    # === Combinazione e salvataggio ===
    print(f"\n=== COMBINAZIONE FONTI ===")
    
    # Allinea colonne e prepara i DataFrame (gestisce None e DataFrame vuoti)
    frames = []
    
    if jobspy_df is not None and not jobspy_df.empty:
        jobspy_aligned = align_columns(jobspy_df, expected_columns)
        if not jobspy_aligned.empty:
            frames.append(jobspy_aligned)
            print(f"JobSpy: {len(jobspy_aligned)} job raccolti")
    
    if hiring_df is not None and not hiring_df.empty:
        hiring_aligned = align_columns(hiring_df, expected_columns)
        if not hiring_aligned.empty:
            frames.append(hiring_aligned)
            print(f"HiringCafe: {len(hiring_aligned)} job raccolti")
    
    # Unione fonti e deduplicazione
    if frames:
        all_sources = pd.concat(frames, ignore_index=True)
    else:
        all_sources = pd.DataFrame(columns=expected_columns)
        print("⚠️  Nessun job raccolto dalle fonti configurate")
    all_sources_unique = all_sources.dropna(subset=['id']) if 'id' in all_sources.columns else all_sources
    all_sources_unique = all_sources_unique.drop_duplicates(subset=['id'], keep='first') if 'id' in all_sources_unique.columns else all_sources_unique
    
    # Aggiungi la data di scraping a tutte le righe
    if not all_sources_unique.empty:
        all_sources_unique['scraping_date'] = scraping_date
    
    print(f"Totale raccolti: {len(all_sources_unique)} unici")
    
    # Salvataggio diretto nel database SQLite
    DB_PATH = "/Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.db"
    
    if all_sources_unique.empty:
        print("Nessun job da processare.")
        return
    
    try:
        # === Identificazione job nuovi vs esistenti ===
        print(f"\n=== IDENTIFICAZIONE JOB NUOVI ===")
        updated_jobs_df = None
        
        if 'id' not in all_sources_unique.columns:
            print("⚠️  Attenzione: colonna 'id' mancante, tutti i job saranno considerati nuovi")
            new_jobs_df = all_sources_unique.copy()
            existing_job_ids = set()
        else:
            job_ids = all_sources_unique['id'].astype(str).tolist()
            existing_job_ids = get_existing_job_ids(DB_PATH, job_ids)
            new_jobs_mask = ~all_sources_unique['id'].astype(str).isin(existing_job_ids)
            new_jobs_df = all_sources_unique[new_jobs_mask].copy()
            updated_jobs_df = all_sources_unique[~new_jobs_mask].copy()
            
            print(f"Job già presenti nel DB: {len(existing_job_ids)}")
            print(f"Job nuovi da processare: {len(new_jobs_df)}")
            if updated_jobs_df is not None and not updated_jobs_df.empty:
                print(f"Job da aggiornare: {len(updated_jobs_df)}")
        
        # === Arricchimento LLM solo per job nuovi ===
        if not new_jobs_df.empty:
            print(f"\n=== ARRICCHIMENTO LLM PER JOB NUOVI ===")
            new_jobs_enriched = enrich_dataframe_with_llm(new_jobs_df)
            
            # Combina job nuovi arricchiti con job aggiornati (senza LLM)
            if updated_jobs_df is not None and not updated_jobs_df.empty:
                all_jobs_final = pd.concat([new_jobs_enriched, updated_jobs_df], ignore_index=True)
            else:
                all_jobs_final = new_jobs_enriched
        else:
            print("\n=== NESSUN JOB NUOVO, AGGIORNAMENTO SOLO JOB ESISTENTI ===")
            all_jobs_final = all_sources_unique
        
        # === Salvataggio finale nel database ===
        print(f"\n=== SALVATAGGIO NEL DATABASE ===")
        inserted, updated = upsert_jobs(DB_PATH, all_jobs_final, batch_size=2000)
        print(f"✅ DB aggiornato: {inserted} nuovi, {updated} aggiornati")
        
    except Exception as e:
        print(f"❌ Errore durante il salvataggio nel database: {e}")
        import traceback
        traceback.print_exc()
        raise



if __name__ == "__main__":
    main()
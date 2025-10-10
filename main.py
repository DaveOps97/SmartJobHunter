"""
Main script per il scraping di job da multiple fonti
"""

import pandas as pd
from scrapers import scrape_all_locations, fetch_hiring_cafe_dataframe
from scrapers.utils import align_columns, get_expected_columns, save_jobs_to_csv
 


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
        # "Padova, Veneto",         # Healthcare tech, università
        # "Parma, Emilia-Romagna",  # Food tech, automotive
        # "Roma, Lazio",            #
        # "Napoli, Campania",        #
]

search_term = (
    '(data OR python OR java OR backend OR software OR machine OR learning OR AI OR ML OR ETL) '
    '(engineer OR developer) '
    '(python OR java OR spark OR pyspark OR docker OR kubernetes OR '
    'scrapy OR mongodb OR postgresql OR rest OR api OR fastapi OR '
    'langchain OR haystack OR llm OR git OR spring OR kafka OR microservices) '
    '-senior -lead -manager -architect -principal'
)

def main():
    """Funzione principale che coordina tutti gli scraper"""
    
    # === JobSpy Scraping ===
    print("=== INIZIO SCRAPING JOBSPY ===")
    jobspy_df = scrape_all_locations(locations=locations, search_term=search_term, hours_old=168, results_wanted=10)
    
    # === HiringCafe Scraping ===
    print("\n=== INIZIO SCRAPING HIRINGCAFE ===")
    hiring_query = 'developer'
    
    # Determina schema colonne attese
    expected_columns, _ = get_expected_columns(jobspy_df if not jobspy_df.empty else None)
    
    # Fetch da HiringCafe
    hiring_df = fetch_hiring_cafe_dataframe(expected_columns=expected_columns, search_query=hiring_query, date_filter= "1_week", max_pages=5)
    
    # === Combinazione e salvataggio ===
    print(f"\n=== COMBINAZIONE FONTI ===")
    
    # Allinea colonne
    jobspy_aligned = align_columns(jobspy_df, expected_columns)
    hiring_aligned = align_columns(hiring_df, expected_columns)
    
    # Unione fonti e deduplicazione (filtra DF vuoti per evitare FutureWarning)
    frames = [df for df in [jobspy_aligned, hiring_aligned] if df is not None and not df.empty and len(df) > 0]
    if frames:
        all_sources = pd.concat(frames, ignore_index=True)
    else:
        all_sources = pd.DataFrame(columns=expected_columns)
    all_sources_unique = all_sources.dropna(subset=['id']) if 'id' in all_sources.columns else all_sources
    all_sources_unique = all_sources_unique.drop_duplicates(subset=['id'], keep='first') if 'id' in all_sources_unique.columns else all_sources_unique
    
    print(f"Totale raccolti (jobspy + hiring.cafe): {len(all_sources_unique)} unici")
    
    # Salvataggio finale
    file_path = "/Users/davidelandolfi/PyProjects/ListScraper/storage/jobs_test_ai.csv"
    save_jobs_to_csv(all_sources_unique, file_path)



if __name__ == "__main__":
    main()
"""
JobSpy scraper module
"""

import time
import pandas as pd
from jobspy import scrape_jobs
from scrapers.utils import clean_html_text


def clean_jobspy_descriptions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pulisce le descrizioni HTML dai DataFrame di JobSpy.
    
    Args:
        df: DataFrame con le descrizioni da pulire
        
    Returns:
        DataFrame con descrizioni pulite
    """
    if df is None or df.empty or 'description' not in df.columns:
        return df
    
    # Applica la pulizia HTML alle descrizioni
    df = df.copy()
    df['description'] = df['description'].apply(clean_html_text)
    
    return df


def scrape_location_with_retries(location: str, search_term: str, hours_old: int = 168, results_wanted: int = 500, max_retries: int = 3, base_delay: float = 2.0) -> pd.DataFrame | None:
    """
    Scraping di una singola località con retry automatico.
    
    Args:
        location: Nome della città da cercare
        max_retries: Numero massimo di tentativi
        base_delay: Delay base tra i retry (esponenziale)
        
    Returns:
        DataFrame con i job trovati o None se fallisce
    """
    for attempt in range(1, max_retries + 1):
        try:
            # Indeed + LinkedIn con location originale (città + regione)
            df_il = scrape_jobs(
                site_name=["indeed", "linkedin"],
                search_term=search_term,
                location=location,
                distance=50,
                hours_old=hours_old,
                results_wanted=results_wanted,
                job_type="fulltime",
                is_remote=False,
                country_indeed='Italy',
                linkedin_fetch_description=True,
                description_format="markdown",
                verbose=1,
            )

            # Glassdoor con sola città
            city_only = location.split(",")[0].strip()
            df_gd = scrape_jobs(
                site_name=["glassdoor"],
                search_term=search_term,
                location=city_only,
                distance=50,
                hours_old=hours_old,
                results_wanted=results_wanted,
                job_type="fulltime",
                is_remote=False,
                linkedin_fetch_description=True,
                description_format="markdown",
                verbose=1,
            )

            # Filtro post-scrape solo per Glassdoor: location deve contenere una parola di city_only oppure Italia/Italy
            if isinstance(df_gd, pd.DataFrame) and not df_gd.empty and 'location' in df_gd.columns:
                import re
                tokens = [t for t in city_only.split() if t]
                loc_series = df_gd['location'].astype(str)
                mask = loc_series.str.contains(r"italia|italy", case=False, na=False)
                for tok in tokens:
                    pattern = rf"\\b{re.escape(tok)}\\b"
                    mask = mask | loc_series.str.contains(pattern, case=False, na=False)
                df_gd = df_gd[mask]

            frames = [d for d in [df_il, df_gd] if isinstance(d, pd.DataFrame) and not d.empty and len(d) > 0]
            if frames:
                df = pd.concat(frames, ignore_index=True)
            else:
                df = pd.DataFrame()
        except Exception as e:
            wait_s = base_delay * (2 ** (attempt - 1))
            print(f"[{location}] Errore tentativo {attempt}/{max_retries}: {e}. Retry tra {wait_s:.1f}s")
            time.sleep(wait_s)
            continue

        if isinstance(df, pd.DataFrame) and not df.empty:
            # Pulisci le descrizioni HTML
            df = clean_jobspy_descriptions(df)
            print(f"[{location}] Successo: {len(df)} annunci")
            return df
        else:
            wait_s = base_delay * (2 ** (attempt - 1))
            print(f"[{location}] Nessun risultato al tentativo {attempt}. Retry tra {wait_s:.1f}s")
            time.sleep(wait_s)

    print(f"[{location}] Fallito dopo {max_retries} tentativi")
    return None


def scrape_all_locations(locations: list[str], search_term: str, hours_old: int = 168, results_wanted: int = 500, max_retries: int = 3, base_delay: float = 3.0) -> pd.DataFrame:
    scrape_params = {'search_term': search_term, 'hours_old': hours_old, 'results_wanted': results_wanted, 'max_retries': max_retries, 'base_delay': base_delay}
    """
    Scraping di tutte le località specificate.
    
    Args:
        locations: Lista delle città da cercare
        max_retries: Numero massimo di tentativi per località
        base_delay: Delay base tra i retry
        
    Returns:
        DataFrame combinato con tutti i job unici
    """
    all_jobs = []
    
    for location in locations:
        print(f"\n=== Scraping {location} ===")
        df_loc = scrape_location_with_retries(location, **scrape_params)
        if df_loc is not None:
            all_jobs.append(df_loc)
        # Pausa breve tra località per ridurre limiti
        time.sleep(1.0)
    
    # Merge e deduplicazione
    if not all_jobs:
        print("\nNessun job valido trovato da jobspy.")
        return pd.DataFrame()
    
    combined_jobs = pd.concat(all_jobs, ignore_index=True)
    combined_jobs_unique = combined_jobs.drop_duplicates(subset=['id'], keep='first')
    
    # Pulisci le descrizioni HTML (doppio controllo)
    # combined_jobs_unique = clean_jobspy_descriptions(combined_jobs_unique)
    
    print(f"[JobSpy] Totale job unici: {len(combined_jobs_unique)}")
    return combined_jobs_unique

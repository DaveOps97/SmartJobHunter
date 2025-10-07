import csv
import time
import os
import json
import requests
import pandas as pd
from jobspy import scrape_jobs

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
        "Roma, Lazio",            #
        "Napoli, Campania",        #
]

# === VALORI OSSERVATI (VERIFICATI DAL SITO) ===
DATE_FILTERS = {
    # Chiave: descrizione filtro → Valore: giorni per API, buffer usato per tenere conto degli offset dalle date di publicazione del sito
    "1_day": 2,              # 1 giorno → 2 giorni API
    "3_days": 4,             # 3 giorni → 4 giorni API
    "1_week": 14,            # 1 settimana (7 giorni) → 14 giorni API
    "1_month": 61,           # 1 mese (30 giorni) → 61 giorni API
    "2_months": 91,          # 2 mesi (60 giorni) → 91 giorni API
    "3_months": 121,         # 3 mesi (90 giorni) → 121 giorni API
    "6_months": 211,         # 6 mesi (180 giorni) → 211 giorni API
    "1_year": 750,           # 1 anno (365 giorni) → 750 giorni API
    "2_years": 1080,         # 2 anni (730 giorni) → 1080 giorni API
}

def scrape_location_with_retries(location: str, max_retries: int = 3, base_delay: float = 2.0) -> pd.DataFrame | None:
    for attempt in range(1, max_retries + 1):
        try:
            df = scrape_jobs(
                site_name=["indeed", "linkedin", "glassdoor"],
                search_term = (
                    '(data OR python OR java OR backend OR software OR machine OR learning OR AI OR ML OR ETL) '
                    '(engineer OR developer) '
                    '(python OR java OR spark OR pyspark OR docker OR kubernetes OR '
                    'scrapy OR mongodb OR postgresql OR rest OR api OR fastapi OR '
                    'langchain OR haystack OR llm OR git OR spring OR kafka OR microservices) '
                    '-senior -lead -manager -architect -principal'
                ),
                location=location,
                distance=50,
                hours_old=168,
                results_wanted=500, # milano in una settimana ne fa 250~
                job_type="fulltime",
                is_remote=False,
                country_indeed='Italy',
                linkedin_fetch_description=True,
                description_format="markdown",
                verbose=1,
            )
        except Exception as e:
            wait_s = base_delay * (2 ** (attempt - 1))
            print(f"[{location}] Errore tentativo {attempt}/{max_retries}: {e}. Retry tra {wait_s:.1f}s")
            time.sleep(wait_s)
            continue

        if isinstance(df, pd.DataFrame) and not df.empty:
            print(f"[{location}] Successo: {len(df)} annunci")
            return df
        else:
            wait_s = base_delay * (2 ** (attempt - 1))
            print(f"[{location}] Nessun risultato al tentativo {attempt}. Retry tra {wait_s:.1f}s")
            time.sleep(wait_s)

    print(f"[{location}] Fallito dopo {max_retries} tentativi")
    return None


# === Hiring Cafe integration ===
def search_hiring_cafe(search_query: str = "java developer", date_filter="1_week", page: int = 0):
    """Versione ridotta della ricerca HiringCafe API"""

    url = "https://hiring.cafe/api/search-jobs"

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:143.0) Gecko/20100101 Firefox/143.0",
        "Origin": "https://hiring.cafe",
        "Referer": "https://hiring.cafe/",
    }

    payload = {
        "size": 100, # n risultati per pagina
        "page": page,
        "searchState": {
            "searchQuery": search_query,
            "jobTitleQuery": "(\"developer\" OR \"engineer\")",
            "commitmentTypes": ["Full Time"],
            "seniorityLevel": ["Entry Level"],
            "departments": ["Engineering", "Software Development"],
            "bachelorsDegreeRequirements": ["Required"],
            "mastersDegreeRequirements": ["Required", "Preferred", "Not Mentioned"],
            "languageRequirements": ["english"],
            "dateFetchedPastNDays": DATE_FILTERS[date_filter],
            "locations": [{
                "formatted_address": "Italy",
                "types": ["country"],
                "geometry": {"location": {"lat": "45.6634", "lon": "9.1521"}},
                "address_components": [{"long_name": "Italy", "short_name": "IT", "types": ["country"]}],
                "options": {"flexible_regions": ["anywhere_in_continent", "anywhere_in_world"]},
            }],
            "sortBy": "default",
        }
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"[HiringCafe] Errore richiesta pagina {page}: {e}")
    return None


def normalize_hiring_cafe_jobs_to_schema(jobs: list[dict], expected_columns: list[str]) -> pd.DataFrame:
    """Mappa i job HiringCafe (schema results[v5_processed_job_data,...]) allo schema CSV esistente."""

    def first_or_none(value):
        if isinstance(value, list) and value:
            return value[0]
        return value if not isinstance(value, list) else None

    def map_one(job: dict) -> dict:
        job_data = job.get("v5_processed_job_data", {}) or {}
        company_data = job.get("v5_processed_company_data", {}) or {}
        job_info = job.get("job_information", {}) or {}

        # Id e URL
        raw_id = job.get("id") or job.get("objectID")
        job_id = f"hc-{raw_id}" if raw_id else None
        apply_url = job.get("apply_url")

        # Titolo / azienda
        title = job_info.get("title") or job_info.get("job_title_raw")
        company_name = job_data.get("company_name") or company_data.get("name")

        # Location
        location = job_data.get("formatted_workplace_location")
        if not location:
            cities = job_data.get("workplace_cities")
            if isinstance(cities, list) and cities:
                location = ", ".join(cities)

        # Data pubblicazione
        date_posted = job_data.get("estimated_publish_date")

        # Job type e remoto
        commitment = first_or_none(job_data.get("commitment"))
        job_type = str(commitment).lower() if commitment else None
        workplace_type = job_data.get("workplace_type")  # e.g., Remote/Hybrid/On-site
        is_remote = True if str(workplace_type).lower() == "remote" else False

        # Descrizione (HTML)
        description = job_info.get("description")

        # Compenso
        currency = job_data.get("listed_compensation_currency")
        interval = job_data.get("listed_compensation_frequency")  # e.g., Yearly/Monthly
        # Scegliamo min/max nell'ordine Yearly > Monthly > Weekly > Daily > Hourly
        min_amount = (
            job_data.get("yearly_min_compensation")
            or job_data.get("monthly_min_compensation")
            or job_data.get("weekly_min_compensation")
            or job_data.get("daily_min_compensation")
            or job_data.get("hourly_min_compensation")
            or job_data.get("bi-weekly_min_compensation")
        )
        max_amount = (
            job_data.get("yearly_max_compensation")
            or job_data.get("monthly_max_compensation")
            or job_data.get("weekly_max_compensation")
            or job_data.get("daily_max_compensation")
            or job_data.get("hourly_max_compensation")
            or job_data.get("bi-weekly_max_compensation")
        )

        # Seniority / funzione / skills
        job_level = job_data.get("seniority_level")
        job_function = job_data.get("job_category")
        technical_tools = job_data.get("technical_tools")
        skills = ", ".join(technical_tools) if isinstance(technical_tools, list) else None

        mapped = {
            "id": job_id,
            "site": "hiring_cafe",
            "job_url": apply_url,
            "job_url_direct": apply_url,
            "title": title,
            "company": company_name,
            "location": location,
            "date_posted": date_posted,
            "job_type": job_type,
            "salary_source": None,
            "interval": interval,
            "min_amount": min_amount,
            "max_amount": max_amount,
            "currency": currency,
            "is_remote": is_remote,
            "job_level": job_level,
            "job_function": job_function,
            "listing_type": None,
            "emails": None,
            "description": description,
            "company_industry": company_data.get("industries")[0] if isinstance(company_data.get("industries"), list) and company_data.get("industries") else None,
            "company_url": company_data.get("website") or job_data.get("company_website"),
            "company_logo": company_data.get("image_url"),
            "company_url_direct": company_data.get("website") or job_data.get("company_website"),
            "company_addresses": None,
            "company_num_employees": company_data.get("num_employees"),
            "company_revenue": company_data.get("latest_revenue"),
            "company_description": company_data.get("tagline") or job_data.get("company_tagline"),
            "skills": skills,
            "experience_range": None,
            "company_rating": None,
            "company_reviews_count": None,
            "vacancy_count": None,
            "work_from_home_type": workplace_type,
        }

        # Riduci alle colonne attese, riempi mancanti a None
        out = {col: mapped.get(col) for col in expected_columns}
        for col in expected_columns:
            if col not in out:
                out[col] = None
        return out

    rows = [map_one(j) for j in jobs]
    if not rows:
        return pd.DataFrame(columns=expected_columns)
    df = pd.DataFrame(rows)
    if "is_remote" in df.columns:
        df["is_remote"] = df["is_remote"].astype(bool)
    return df


def fetch_hiring_cafe_dataframe(expected_columns: list[str], search_query: str, max_pages: int = 5) -> pd.DataFrame:
    all_jobs = []
    for page in range(max_pages):
        data = search_hiring_cafe(search_query=search_query, page=page)
        jobs = (data or {}).get("results") if isinstance(data, dict) else None
        if not jobs:
            if page == 0:
                print("[HiringCafe] Nessun risultato nella prima pagina")
            break
        print(f"[HiringCafe] Pagina {page}: {len(jobs)} annunci")
        all_jobs.extend(jobs)
        time.sleep(0.5)

    if not all_jobs:
        print("[HiringCafe] Nessun job trovato")
        return pd.DataFrame(columns=expected_columns)
    df = normalize_hiring_cafe_jobs_to_schema(all_jobs, expected_columns)
    # Togli eventuali duplicati senza id
    df = df.dropna(subset=["id"]) if "id" in df.columns else df
    df = df.drop_duplicates(subset=["id"], keep="first") if "id" in df.columns else df
    print(f"[HiringCafe] Totale unici: {len(df)}")
    return df


all_jobs = []
# for location in locations:
#     print(f"\n=== Scraping {location} ===")
#     df_loc = scrape_location_with_retries(location, max_retries=3, base_delay=3.0)
#     if df_loc is not None:
#         all_jobs.append(df_loc)
#     # Pausa breve tra località per ridurre limiti
#     time.sleep(1.0)

# Merge iniziale (jobspy)
if not all_jobs:
    print("\nNessun job valido trovato da jobspy.")
    combined_jobs_unique = pd.DataFrame()
else:
    combined_jobs = pd.concat(all_jobs, ignore_index=True)
    combined_jobs_unique = combined_jobs.drop_duplicates(subset=['id'], keep='first')

# Carica jobs.csv esistente (se presente) per derivare schema colonne
existing_path = "jobs.csv"
existing_df = None
if os.path.exists(existing_path):
    try:
        existing_df = pd.read_csv(existing_path)
        print(f"\n[CSV] jobs.csv esistente caricato: {len(existing_df)} righe")
    except Exception as e:
        print(f"[CSV] Errore lettura jobs.csv: {e}")
        existing_df = None

# Determina colonne attese dello schema finale
if existing_df is not None:
    expected_columns = list(existing_df.columns)
elif not combined_jobs_unique.empty:
    expected_columns = list(combined_jobs_unique.columns)
else:
    # fallback minimo, verrà arricchito quando arriveranno dati
    expected_columns = [
        'id','site','job_url','job_url_direct','title','company','location','date_posted','job_type',
        'salary_source','interval','min_amount','max_amount','currency','is_remote','job_level','job_function',
        'listing_type','emails','description','company_industry','company_url','company_logo','company_url_direct',
        'company_addresses','company_num_employees','company_revenue','company_description','skills','experience_range',
        'company_rating','company_reviews_count','vacancy_count','work_from_home_type'
    ]

# Integra Hiring Cafe usando query simile a jobspy
hiring_query = 'developer'
df_hiring = fetch_hiring_cafe_dataframe(expected_columns=expected_columns, search_query=hiring_query, max_pages=5)

# Uniforma colonne jobspy al set atteso (aggiunge mancanti con None)
def align_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=columns)
    for col in columns:
        if col not in df.columns:
            df[col] = None
    # Conserva solo colonne attese nell'ordine
    return df[columns]

combined_jobspy_aligned = align_columns(combined_jobs_unique, expected_columns)
df_hiring_aligned = align_columns(df_hiring, expected_columns)

# Unione fonti e unici per id
all_sources = pd.concat([combined_jobspy_aligned, df_hiring_aligned], ignore_index=True)
all_sources_unique = all_sources.dropna(subset=['id']) if 'id' in all_sources.columns else all_sources
all_sources_unique = all_sources_unique.drop_duplicates(subset=['id'], keep='first') if 'id' in all_sources_unique.columns else all_sources_unique

print(f"\n=== TOTALE RACCOLTI (jobspy + hiring.cafe): {len(all_sources_unique)} unici ===")

# Calcolo differenza e scrittura incrementale
if existing_df is not None and not existing_df.empty:
    existing_ids = set(existing_df['id'].astype(str)) if 'id' in existing_df.columns else set()
    mask_new = ~all_sources_unique['id'].astype(str).isin(existing_ids)
    new_rows = all_sources_unique[mask_new]
    num_new = len(new_rows)
    print(f"[CSV] Nuovi job rispetto a jobs.csv: {num_new}")
    if num_new > 0:
        # Append senza header
        new_rows.to_csv(existing_path, mode='a', header=False, quoting=csv.QUOTE_NONNUMERIC, escapechar="\\", index=False)
        print(f"[CSV] Appesi {num_new} nuovi annunci a jobs.csv")
    else:
        print("[CSV] Nessun nuovo annuncio da aggiungere")
else:
    # Nessun csv esistente: crea file con tutto
    if not all_sources_unique.empty:
        all_sources_unique.to_csv(existing_path, quoting=csv.QUOTE_NONNUMERIC, escapechar="\\", index=False)
        print(f"[CSV] Creato jobs.csv con {len(all_sources_unique)} righe")
    else:
        print("[CSV] Nessun dato da scrivere")

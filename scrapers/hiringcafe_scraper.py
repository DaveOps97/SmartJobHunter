"""
HiringCafe scraper module
"""

import time
import requests
import pandas as pd
from scrapers.utils import clean_html_text


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


def search_hiring_cafe(search_query: str = "", date_filter="1_week", page: int = 0, max_retries: int = 3):
    """Versione ridotta della ricerca HiringCafe API con retry automatico"""

    url = "https://hiring.cafe/api/search-jobs"

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:143.0) Gecko/20100101 Firefox/143.0",
        "Origin": "https://hiring.cafe",
        "Referer": "https://hiring.cafe/",
    }

    payload = {
        "size": 100, # n risultati per pagina
        "page": page,
        "searchState": {
            "searchQuery": search_query,
            "jobTitleQuery": "(\"developer\" OR \"engineer\" OR \"scientist\")",
            "commitmentTypes": ["Full Time"],
            "seniorityLevel": ["Entry Level", "No Prior Experience Required"],
            "departments": [], # "Engineering", "Software Development"
            "bachelorsDegreeRequirements": ["Required"],
            "mastersDegreeRequirements": ["Required", "Preferred", "Not Mentioned"],
            "languageRequirements": ["english","italian"],
            "languageRequirementsOperator": "OR",
            "dateFetchedPastNDays": DATE_FILTERS[date_filter],
            "locations": [{
                "formatted_address": "Italy",
                "types": ["country"],
                # "geometry": {"location": {"lat": "45.6634", "lon": "9.1521"}}, # non sembra esserci nel body originale
                "address_components": [{"long_name": "Italy", "short_name": "IT", "types": ["country"]}],
                "options": {"flexible_regions": ["anywhere_in_continent", "anywhere_in_world"]},
            }],
            "sortBy": "default",
        }
    }

    for attempt in range(1, max_retries + 1):
        try:
            timeout = 30 + (attempt - 1) * 10  # timeout crescente: 30, 40, 50s
            response = requests.post(url, headers=headers, json=payload, timeout=timeout)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"[HiringCafe] Pagina {page} - HTTP {response.status_code} al tentativo {attempt}")
        except Exception as e:
            if attempt < max_retries:
                wait_time = 2 ** attempt  # 2, 4, 8 secondi
                print(f"[HiringCafe] Errore pagina {page} tentativo {attempt}/{max_retries}: {e}. Retry tra {wait_time}s")
                time.sleep(wait_time)
            else:
                print(f"[HiringCafe] Errore pagina {page} dopo {max_retries} tentativi: {e}")
    
    return None


def normalize_hiring_cafe_jobs_to_schema(jobs: list[dict], expected_columns: list[str]) -> pd.DataFrame:
    """Mappa i job HiringCafe (schema results[v5_processed_job_data,...]) allo schema CSV esistente."""

    # Helper per serializzare liste in una singola stringa contenente TUTTI i valori
    def join_all(values, sep: str = "; "):
        if isinstance(values, list):
            return sep.join([str(v) for v in values if v is not None]) if values else None
        return values

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
        if date_posted:
            date_posted = date_posted.split("T")[0]

        # Job type e remoto
        commitment_all = job_data.get("commitment")
        # Manteniamo job_type singolo (se c'è un solo elemento), altrimenti serializziamo tutti
        if isinstance(commitment_all, list):
            job_type = "; ".join([str(c).lower() for c in commitment_all]) if commitment_all else None
        else:
            job_type = str(commitment_all).lower() if commitment_all else None
        workplace_type = job_data.get("workplace_type")  # e.g., Remote/Hybrid/On-site
        is_remote = True if str(workplace_type).lower() == "remote" else False

        # Descrizione (HTML) - pulita dai tag HTML/CSS
        description_raw = job_info.get("description")
        description = clean_html_text(description_raw)

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

        # Campi multi-valore aggiuntivi (tutti i campi, non solo il primo)
        company_industries_all = join_all((job.get("v5_processed_company_data", {}) or {}).get("industries"))
        company_activities_all = join_all((job.get("v5_processed_company_data", {}) or {}).get("activities"))
        language_requirements_all = join_all(job_data.get("language_requirements"))
        role_activities_all = join_all(job_data.get("role_activities"))

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
            "interval": interval,
            "min_amount": min_amount,
            "max_amount": max_amount,
            "currency": currency,
            "is_remote": is_remote,
            "job_level": job_level,
            "job_function": job_function,
            #"emails": None,
            "description": description,
            "company_url": company_data.get("website") or job_data.get("company_website"),
            "company_logo": company_data.get("image_url"),
            "company_industries": company_industries_all,
            "company_activities": company_activities_all,
            "company_num_employees": company_data.get("num_employees"),
            "company_revenue": company_data.get("latest_revenue"),
            "company_description": company_data.get("tagline") or job_data.get("company_tagline"),
            "skills": skills,
            "work_from_home_type": workplace_type,
            "language_requirements": language_requirements_all,
            "role_activities": role_activities_all,
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


def fetch_hiring_cafe_dataframe(expected_columns: list[str], search_query: str, date_filter:str = "1_week", max_pages: int = 5) -> pd.DataFrame:
    """
    Fetch completo da HiringCafe con paginazione.
    
    Args:
        expected_columns: Colonne attese per il DataFrame finale
        search_query: Query di ricerca
        max_pages: Numero massimo di pagine da scaricare
        
    Returns:
        DataFrame con tutti i job unici trovati
    """
    all_jobs = []
    for page in range(max_pages):
        data = search_hiring_cafe(search_query=search_query, date_filter=date_filter, page=page)
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

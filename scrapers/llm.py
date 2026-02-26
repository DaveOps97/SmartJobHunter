from __future__ import annotations


import os
import re
import time
import json
from pathlib import Path
from typing import Any, Dict, Optional, List
from collections import deque
from threading import Lock


import pandas as pd
from tqdm import tqdm
from google import genai
from google.genai import types as genai_types




class PerKeyRateLimiter:
    """Rate limiter che traccia separatamente ogni API key"""
    
    def __init__(self, max_requests_per_minute: int = 10):
        """
        Args:
            max_requests_per_minute: Limite RPM per singola key (10 per free tier)
        """
        self.max_requests = max_requests_per_minute
        self.key_requests: Dict[str, deque] = {}  # key -> deque di timestamps
        self.lock = Lock()
    
    def wait_if_needed(self, api_key: str):
        """
        Controlla e rispetta il rate limit per questa specifica API key
        
        Args:
            api_key: La chiave API da controllare
        """
        with self.lock:
            # Inizializza deque per questa key se non esiste
            if api_key not in self.key_requests:
                self.key_requests[api_key] = deque()
            
            requests = self.key_requests[api_key]
            now = time.time()
            
            # Rimuovi richieste più vecchie di 1 minuto PER QUESTA KEY
            while requests and now - requests[0] > 60:
                requests.popleft()
            
            # Se questa key ha raggiunto il limite, aspetta
            if len(requests) >= self.max_requests:
                oldest_request = requests[0]
                wait_time = 60 - (now - oldest_request) + 0.1
                if wait_time > 0:
                    key_suffix = api_key[-6:] if len(api_key) > 6 else api_key
                    print(f"⏳ Rate limit per key ...{key_suffix}: attesa {wait_time:.1f}s")
                    time.sleep(wait_time)
                    now = time.time()
                    # Pulisci di nuovo dopo l'attesa
                    while requests and now - requests[0] > 60:
                        requests.popleft()
            
            # Registra questa richiesta per questa key
            requests.append(now)
    
    def get_stats(self) -> Dict[str, Dict[str, int]]:
        """Restituisce statistiche per ogni key"""
        with self.lock:
            stats = {}
            now = time.time()
            for api_key, requests in self.key_requests.items():
                key_suffix = api_key[-6:] if len(api_key) > 6 else api_key
                # Conta richieste nell'ultimo minuto
                recent = sum(1 for ts in requests if now - ts <= 60)
                stats[f"...{key_suffix}"] = {
                    "last_minute": recent,
                    "total": len(requests)
                }
            return stats



class MultiProjectManager:
    """Gestisce rotazione tra progetti con rate limiting per-key integrato"""
    
    def __init__(self, api_keys: List[str], max_rpm_per_key: int = 10):
        """
        Args:
            api_keys: Lista di API keys da progetti diversi
            max_rpm_per_key: Rate limit RPM per singola key (default 10 per free tier)
        """
        if not api_keys:
            raise ValueError("Almeno una API key richiesta")
        
        # Deque usata solo per il round-robin semplice sulle key (batch)
        self.api_keys = deque(api_keys)
        # Lista ordinata e immutabile usata per la sequenza key×model (singolo job)
        self._api_keys_list: List[str] = list(api_keys)
        # Contatore piatto di slot key×model
        self._slot_index: int = 0

        self.lock = Lock()
        self.usage_stats = {key: 0 for key in api_keys}

        # Rate limiter per-key integrato
        self.rate_limiter = PerKeyRateLimiter(max_requests_per_minute=max_rpm_per_key)
    
    def get_next_key_with_rate_limit(self) -> str:
        """
        Ottiene la prossima key con round-robin E applica rate limiting
        
        Returns:
            API key pronta all'uso (già controllata per rate limit)
        """
        # Round-robin: ottieni la prossima key
        with self.lock:
            self.api_keys.rotate(-1)
            current_key = self.api_keys[0]
            self.usage_stats[current_key] += 1
        
        # Applica rate limiting per questa specifica key (fuori dal lock)
        self.rate_limiter.wait_if_needed(current_key)
        
        return current_key

    def get_next_key_and_model(self) -> tuple[str, str]:
        """
        Ottiene la prossima API key e il relativo modello da usare,
        alternando i modelli separatamente per ogni chiave.
        """
        with self.lock:
            num_models = len(SINGLE_EVAL_MODELS)
            total_slots = len(self._api_keys_list) * num_models
            if total_slots <= 0:
                raise RuntimeError("Nessuna combinazione key×modello disponibile")

            # Sequenza lineare su griglia key×model
            idx = self._slot_index % total_slots
            key_idx = idx // num_models
            model_idx = idx % num_models
            self._slot_index += 1

            current_key = self._api_keys_list[key_idx]
            current_model = SINGLE_EVAL_MODELS[model_idx]
            self.usage_stats[current_key] += 1

        # Applica il rate limit sulla singola chiave fuori dal lock
        self.rate_limiter.wait_if_needed(current_key)

        return current_key, current_model
    
    def get_stats(self) -> dict:
        """Statistiche complete di utilizzo"""
        with self.lock:
            distribution = {
                f"...{k[-6:]}": count 
                for k, count in self.usage_stats.items()
            }
            total = sum(self.usage_stats.values())
        
        rate_stats = self.rate_limiter.get_stats()
        
        return {
            "total_requests": total,
            "distribution": distribution,
            "num_projects": len(self._api_keys_list),
            "rate_limits": rate_stats
        }


# Variabili globali
GEMINI_API_KEYS: List[str] = []
IS_FREE_API_KEY = False
_project_manager: Optional[MultiProjectManager] = None

# Modelli usati nella valutazione singola (per il calcolo degli slot RPD)
# Ordine importante: indice 0 → modello "lite"
SINGLE_EVAL_MODELS: List[str] = ["gemini-2.5-flash-lite", "gemini-2.5-flash"]

# Stato globale per gestione esaurimento quota RPD (per-day)
_rpd_exhausted_count: int = 0
_rpd_exhausted_lock: Lock = Lock()

FALLBACK_RESULT_RPD = {
    "score_competenze": None,
    "score_azienda": None,
    "score_stipendio": None,
    "score_località": None,
    "score_crescita": None,
    "score": None,
    "motivazione": "Quota RPD giornaliera esaurita per tutte le API key e modelli disponibili.",
    "match_competenze": None,
}



def initialize_api_keys(api_keys: List[str], is_free: bool):
    """Inizializza il multi-project manager con rate limiting per-key"""
    global GEMINI_API_KEYS, IS_FREE_API_KEY, _project_manager
    
    GEMINI_API_KEYS = api_keys
    IS_FREE_API_KEY = is_free
    
    if len(api_keys) > 1:
        # Multi-project: rate limit 10 RPM per key
        _project_manager = MultiProjectManager(
            api_keys,
            max_rpm_per_key=10
        )
        print(f"✅ Multi-project rotation: {len(api_keys)} progetti")
        print(f"📊 Rate limit: 10 RPM per progetto")
        print(f"🚀 Throughput max: {len(api_keys) * 10} RPM totali")
    elif len(api_keys) == 1:
        # Singolo progetto: rate limit 15 RPM (più conservativo)
        _project_manager = MultiProjectManager(
            api_keys,
            max_rpm_per_key=15
        )
        print("⚠️  Singolo progetto - rate limit 15 RPM")
    else:
        raise ValueError("Nessuna API key fornita")



SYSTEM_INSTRUCTIONS = """PERSONA: Sei un esperto di selezione del personale che valuta offerte di lavoro in relazione al mio profilo professionale.


TASK: Valuta se l'offerta di lavoro è rilevante per me, assegnando punteggi separati per ciascun criterio dopo aver analizzato l'offerta.


PROFILO PROFESSIONALE:
- Laurea Magistrale in Ingegneria Informatica - Data Engineering and AI
- Livello: Junior/Entry Level
- Esperienza: sviluppo software, Big Data, NLP, RAG
- Linguaggi: Python, Java
- Database: ArangoDB, MongoDB, PostgreSQL, Pinecone
- Framework: Scrapy, Haystack, LangChain, Spring, REST APIs, FastAPI, Apache Spark (PySpark)
- Strumenti: Git, Docker, VSCode, Linux, macOS
- Metodologie: Agile (Scrum), TDD, CI/CD
- Competenze: Data Engineering, Web Scraping, Graph Data Modeling, Object-Oriented Programming
- Lingue: Italiano (madrelingua), Inglese C1


PREFERENZE:
- Ruoli: Data Engineer, Software Engineer, Big Data Engineer, AI Engineer, Backend Developer, Web Developer
- Settori: Data Science, AI, Cybersecurity, Software Development, Cloud & Automation
- Località: Città del nord Italia e del sud-est Francia (ad esempio Sophia-Antipolis). Vanno bene anche città in Umbria e regioni limitrofe, in Campania o Roma, 
purché l'offerta di lavoro sia interessante; stesso discorso per altri paesi esteri. Bonus se full remote, penalizza se always on site.
- Tipo azienda: preferenza per startup strutturate o aziende tecnologiche innovative, ma apertura a realtà grandi se riconosciute per qualità e ambiente collaborativo
- Crescita: interesse per opportunità di apprendimento (mentorship, training, progetti stimolanti)
- Benefit: stipendi competitivi, smart working, formazione, tool moderni
- Lingua: preferenza per posizioni con uso attivo dell'inglese


CRITERI DI VALUTAZIONE:
Assegna uno score 0-10 per ciascun criterio, dove i pesi indicano l'importanza relativa nel calcolo finale:


1. score_competenze (peso 40% - CRITICO): allineamento con linguaggi, framework, metodologie e dominio del ruolo. IMPORTANTE: Se il ruolo è senior o mid level, dillo in modo esplicito e assegna score 0.
2. score_azienda (peso 25% - IMPORTANTE): reputazione, cultura collaborativa, innovazione, formazione, tecnologie moderne; preferenza per PMI Consolidate e Scaleup
3. score_stipendio (peso 15% - IMPORTANTE): competitività per profilo junior, benefit significativi, smart working, flessibilità
4. score_località (peso 10% - MODERATO): corrispondenza con aree preferite o remote work/relocation accettabile
5. score_crescita (peso 10% - MODERATO): mentorship, percorsi di crescita, formazione, progetti sfidanti


SCALA PUNTEGGI (0-10):
- 0-2: Molto scarso/assente
- 3-4: Insufficiente/parziale
- 5-6: Sufficiente/adeguato
- 7-8: Buono/allineato
- 9-10: Eccellente/perfetto


PROCESSO DI VALUTAZIONE:
1. Analizza l'offerta rispetto a profilo e preferenze
2. Per OGNI criterio, valuta prima l'allineamento, POI assegna il punteggio 0-10
3. Nel campo motivazione, fornisci overview dell'offerta seguita da analisi bilanciata:
    - **Punti Positivi (+):** lista elementi allineati a profilo/preferenze
    - **Punti Negativi (-):** lista criticità, mancanze o disallineamenti
    - **Analisi Punteggi:** riepiloga i singoli punteggi assegnati ai cinque criteri, riportando un breve commento per ciascuno nel formato:
        - [Nome_criterio] (x/10): commento sintetico
4. Nel campo match_competenze, elenca le competenze tecniche specifiche che matchano
Rispondi in italiano, mantenendo i termini tecnici in inglese."""



def _enforce_competenze_zero_for_senior(
    scores: Dict[str, int],
    motivazione: Optional[str] = None,
) -> None:
    """
    Applica il vincolo di progetto:
    se nella motivazione viene indicato che il ruolo è mid/senior, forza score_competenze a 0.
    Modifica il dict scores in-place.
    """
    try:
        if not isinstance(motivazione, str) or not motivazione.strip():
            return

        m = motivazione.lower()
        patterns = (
            r"\bsenior\b",
            r"\bsr\.?\b",
            r"\bmid[\s-]?level\b",
            r"\bmidlevel\b",
        )
        if any(re.search(p, m) for p in patterns):
            scores["score_competenze"] = 0
    except Exception:
        # In caso di problemi con i dati, non bloccare il flusso
        pass


def _calculate_final_score(scores: Dict[str, int]) -> int:
    """
    Calcola lo score finale utilizzando la formula di somma ponderata.
    
    Args:
        scores: Dizionario con i punteggi dei 6 criteri
        
    Returns:
        Score finale arrotondato per eccesso (0-10)
    """
    # Pesi dei criteri
    weights = {
        "score_competenze": 0.40,  # 40% - CRITICO
        "score_azienda": 0.25,     # 25% - IMPORTANTE
        "score_stipendio": 0.15,   # 15% - IMPORTANTE
        "score_località": 0.10,    # 10% - MODERATO
        "score_crescita": 0.10,     # 10% - MODERATO
    }
    
    # Calcola la somma ponderata
    weighted_sum = 0.0
    for criterion, weight in weights.items():
        score = scores.get(criterion, 0)
        weighted_sum += score * weight
    
    # Arrotonda per eccesso e assicura che sia tra 0 e 10
    final_score = min(10, max(0, int(weighted_sum + 0.5)))
    return final_score



def _get_client() -> Any:
    """
    Ottiene client Gemini con key rotation E rate limiting automatici
    
    Returns:
        Client Gemini configurato con la key corretta
    """
    if not _project_manager:
        raise RuntimeError("Project manager non inizializzato")
    
    if genai is None:
        raise RuntimeError("google-genai non installato")
    
    # Ottieni la prossima key CON rate limiting già applicato
    current_key = _project_manager.get_next_key_with_rate_limit()
    
    return genai.Client(api_key=current_key)



def _build_job_structured_data(row_data: Dict[str, Any]) -> str:
    """
    Costruisce il blocco di dati strutturati per una singola offerta.
    Questa funzione mantiene il formato originale del prompt.
    
    Args:
        row_data: Dizionario con tutti i campi della job
        
    Returns:
        Stringa con dati strutturati formattati
    """
    title = row_data.get("title")
    company = row_data.get("company")
    location = row_data.get("location")
    job_type = row_data.get("job_type")
    job_level = row_data.get("job_level")
    job_function = row_data.get("job_function")
    skills = row_data.get("skills")
    min_amount = row_data.get("min_amount")
    max_amount = row_data.get("max_amount")
    currency = row_data.get("currency")
    interval = row_data.get("interval")
    is_remote = row_data.get("is_remote")
    work_from_home_type = row_data.get("work_from_home_type")
    company_description = row_data.get("company_description")
    company_num_employees = row_data.get("company_num_employees")
    company_revenue = row_data.get("company_revenue")
    company_industries = row_data.get("company_industries")
    company_activities = row_data.get("company_activities")
    language_requirements = row_data.get("language_requirements")
    role_activities = row_data.get("role_activities")
    description = row_data.get("description")
    
    return f"""
OFFERTA DI LAVORO - DATI STRUTTURATI:


IDENTIFICAZIONE:
- Titolo: {title or 'N/A'}
- Azienda: {company or 'N/A'}
- Posizione: {location or 'N/A'}


RUOLO E SENIORITY:
- Tipo di contratto: {job_type or 'N/A'}
- Livello: {job_level or 'N/A'}
- Funzione: {job_function or 'N/A'}
- Competenze richieste: {skills or 'N/A'}
- Attività ruolo: {role_activities or 'N/A'}
- Lingue: {language_requirements or 'N/A'}


COMPENSO:
- Range: {min_amount or 'N/A'} - {max_amount or 'N/A'} {currency or ''} ({interval or 'N/A'})


MODALITÀ LAVORO:
- Remoto: {is_remote if is_remote is not None else 'N/A'}
- Tipo lavoro: {work_from_home_type or 'N/A'}


AZIENDA:
- Descrizione: {company_description or 'N/A'}
- Dipendenti: {company_num_employees or 'N/A'}
- Fatturato: {company_revenue or 'N/A'}
- Settori: {company_industries or 'N/A'}
- Attività: {company_activities or 'N/A'}


DESCRIZIONE COMPLETA:
{description}
"""



def evaluate_jobs_batch(jobs_data: List[Dict[str, Any]], max_retries: int = 3, base_delay: float = 1.5) -> List[Dict[str, Any]]:
    """
    Valuta multiple offerte di lavoro in un singolo prompt batch.
    
    Args:
        jobs_data: Lista di dizionari con tutti i campi delle job
        max_retries: Numero massimo di tentativi
        base_delay: Delay base tra i retry
        
    Returns:
        Lista di dizionari con i risultati della valutazione per ogni job
    """
    if not jobs_data:
        return []
    
    # Verifica che tutte le job abbiano description valida
    valid_jobs = []
    empty_results = []
    
    for job in jobs_data:
        description = job.get("description")
        if not description or not isinstance(description, str) or description.strip() == "":
            empty_results.append({
                "score_competenze": 0,
                "score_azienda": 0,
                "score_stipendio": 0,
                "score_località": 0,
                "score_crescita": 0,
                "score": 0,
                "motivazione": "Nessuna descrizione disponibile",
                "match_competenze": [],
            })
        else:
            valid_jobs.append(job)
    
    # Se non ci sono job valide, ritorna solo i risultati vuoti
    if not valid_jobs:
        return empty_results
    
    # Costruisci prompt batch con separatori neutri
    jobs_text = ""
    for idx, job in enumerate(valid_jobs, start=1):
        # MODIFICA: Usa separatore neutro senza numero visibile
        jobs_text += f"\n{'='*80}\n"  # Rimuovi "### OFFERTA #X ###"
        jobs_text += _build_job_structured_data(job)
    
    # MODIFICA: Prompt più esplicito sul formato
    prompt = (
        f"Valuta attentamente ed in modo indipendente tra loro le seguenti {len(valid_jobs)} offerte di lavoro in base alle istruzioni di sistema. "
        f"Le offerte sono separate da una linea di uguale (=). "
        f"Rispondi con un JSON array contenente esattamente {len(valid_jobs)} oggetti valutazione, "
        "uno per ogni offerta, nello stesso ordine in cui appaiono. "
        "NON menzionare il numero dell'offerta nella motivazione. "
        "Ogni oggetto deve avere tutti i campi richiesti.\n\n"
        + jobs_text
    )
    
    client = _get_client()
    
    # Schema per singola valutazione
    evaluation_schema = genai_types.Schema(
        type=genai_types.Type.OBJECT,
        required=["job_id", "score_competenze", "score_azienda", "score_stipendio",
                 "score_località", "score_crescita", "motivazione", "match_competenze"],
        properties={
            "job_id": genai_types.Schema(
                type=genai_types.Type.INTEGER,
                description="Numero progressivo dell'offerta (1-based)"
            ),
            "score_competenze": genai_types.Schema(type=genai_types.Type.INTEGER, minimum=0, maximum=10),
            "score_azienda": genai_types.Schema(type=genai_types.Type.INTEGER, minimum=0, maximum=10),
            "score_stipendio": genai_types.Schema(type=genai_types.Type.INTEGER, minimum=0, maximum=10),
            "score_località": genai_types.Schema(type=genai_types.Type.INTEGER, minimum=0, maximum=10),
            "score_crescita": genai_types.Schema(type=genai_types.Type.INTEGER, minimum=0, maximum=10),
            "motivazione": genai_types.Schema(type=genai_types.Type.STRING),
            "match_competenze": genai_types.Schema(
                type=genai_types.Type.ARRAY,
                items=genai_types.Schema(type=genai_types.Type.STRING)
            ),
        }
    )
    
    # Schema per array di valutazioni
    response_schema = genai_types.Schema(
        type=genai_types.Type.OBJECT,
        required=["evaluations"],
        properties={
            "evaluations": genai_types.Schema(
                type=genai_types.Type.ARRAY,
                items=evaluation_schema,
                minItems=len(valid_jobs),
                maxItems=len(valid_jobs)
            )
        }
    )
    
    last_err: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            contents = [genai_types.Content(
                role="user",
                parts=[genai_types.Part.from_text(text=prompt)]
            )]
            
            cfg = genai_types.GenerateContentConfig(
                temperature=0.2,
                thinking_config=genai_types.ThinkingConfig(thinking_budget=0),
                response_mime_type="application/json",
                response_schema=response_schema,
                system_instruction=[genai_types.Part.from_text(text=SYSTEM_INSTRUCTIONS)]
            )
            
            stream = client.models.generate_content_stream(
                model="gemini-flash-lite-latest",
                contents=contents,
                config=cfg
            )
            
            accum = []
            for chunk in stream:
                if getattr(chunk, "text", None):
                    accum.append(chunk.text)
            
            text = ("".join(accum)).strip()
            
            # Risposta vuota: possibile safety block, timeout o modello che non restituisce testo
            if not text:
                raise ValueError(
                    "Risposta API vuota (possibile safety block, timeout o contenuto filtrato)"
                )
            
            json_str = _extract_json(text)
            parsed = json.loads(json_str)
            evaluations = parsed.get("evaluations", [])
            
            # Verifica che il numero di valutazioni corrisponda
            if len(evaluations) != len(valid_jobs):
                # Diagnostica: aiuta a capire se è risposta vuota, JSON sbagliato o struttura diversa
                msg = (
                    f"Mismatch valutazioni: attese {len(valid_jobs)}, ricevute {len(evaluations)}. "
                    f"Chiavi nel JSON: {list(parsed.keys())}. "
                )
                if text:
                    preview = text[:400] + "..." if len(text) > 400 else text
                    msg += f"Anteprima risposta ({len(text)} chars): {preview!r}"
                else:
                    msg += "Risposta API vuota."
                raise ValueError(msg)
            
            # Processa risultati
            results = []
            for eval_data in evaluations:
                # Usa job_id per recuperare l'offerta originale e applicare le regole di progetto
                job_id = int(eval_data.get("job_id", 0) or 0)
                job_idx = job_id - 1  # job_id è 1-based
                row_data = valid_jobs[job_idx] if 0 <= job_idx < len(valid_jobs) else {}

                scores = {
                    "score_competenze": int(eval_data.get("score_competenze", 0)),
                    "score_azienda": int(eval_data.get("score_azienda", 0)),
                    "score_stipendio": int(eval_data.get("score_stipendio", 0)),
                    "score_località": int(eval_data.get("score_località", 0)),
                    "score_crescita": int(eval_data.get("score_crescita", 0)),
                }

                motivazione = str(eval_data.get("motivazione", ""))
                # Forza score_competenze a 0 se la motivazione indica mid/senior
                _enforce_competenze_zero_for_senior(scores, motivazione=motivazione)
                
                results.append({
                    **scores,
                    "score": _calculate_final_score(scores),
                    "motivazione": motivazione,
                    "match_competenze": list(eval_data.get("match_competenze", []) or []),
                })
            
            # Ricombina con i risultati vuoti iniziali
            final_results = []
            valid_idx = 0
            empty_idx = 0
            
            for job in jobs_data:
                description = job.get("description")
                if not description or not isinstance(description, str) or description.strip() == "":
                    final_results.append(empty_results[empty_idx])
                    empty_idx += 1
                else:
                    final_results.append(results[valid_idx])
                    valid_idx += 1
            
            return final_results
            
        except Exception as e:
            last_err = e
            err_type = type(e).__name__
            status = getattr(e, "status_code", None)
            print(f"⚠️ Errore batch LLM al tentativo {attempt}/{max_retries}: {err_type}: {e}")
            if status is not None:
                print(f"   HTTP status: {status}")
            
            is_429 = (
                status == 429
                or "429" in str(e)
                or (getattr(e, "message", "") or "").upper().find("RESOURCE_EXHAUSTED") >= 0
            )
            is_503 = status == 503 or "503" in str(e) or "UNAVAILABLE" in str(e).upper()
            if is_429:
                wait = _get_retry_seconds_from_error(e) or min(60, base_delay * (2 ** attempt))
                if "free_tier" in str(e) or "quotaValue" in str(e) or "RPD" in str(e).upper():
                    print(f"   ⏳ 429 quota (es. RPD free tier): attesa {wait:.0f}s")
                else:
                    print(f"   ⏳ 429 rilevato: attesa {wait:.0f}s prima del retry")
                time.sleep(wait)
            elif is_503:
                wait = min(60, base_delay * (2 ** (attempt + 1))) # 6s → 12s → 24s
                print(f"   ⏳ 503 modello sotto carico: attesa {wait:.0f}s, riprovo dopo")
                time.sleep(wait)
            else:
                time.sleep(base_delay * attempt)
            client = _get_client()
    
    # Fallback: ritorna valutazioni con errore
    print(f"⚠️ Batch fallito dopo {max_retries} tentativi, uso fallback singolo")
    return [evaluate_job(job, max_retries=1) for job in jobs_data]



def evaluate_job(row_data: Dict[str, Any], max_retries: int = 3, base_delay: float = 1.5) -> Dict[str, Any]:
    """
    Valuta un'offerta di lavoro usando tutti i campi disponibili.
    NOTA: Questa funzione è mantenuta come fallback per il batch.
    
    Args:
        row_data: Dizionario con tutti i campi della riga del DataFrame
        max_retries: Numero massimo di tentativi
        base_delay: Delay base tra i retry
        
    Returns:
        Dizionario con i risultati della valutazione LLM
    """
    global _rpd_exhausted_count

    description = row_data.get("description")
    
    if not description or not isinstance(description, str) or description.strip() == "":
        return {
            "score_competenze": 0,
            "score_azienda": 0,
            "score_stipendio": 0,
            "score_località": 0,
            "score_crescita": 0,
            "score": 0,
            "motivazione": "Nessuna descrizione disponibile",
            "match_competenze": [],
        }

    # Usa la funzione helper per costruire i dati strutturati
    structured_data = _build_job_structured_data(row_data)

    # Prompt originale invariato
    prompt = (
        "Valuta la seguente offerta di lavoro in base alle istruzioni di sistema. "
        "Rispondi esclusivamente con JSON valido senza testo extra.\n\n"
        + structured_data
    )

    last_err: Optional[Exception] = None
    current_key: str = ""
    model_name: str = ""

    for attempt in range(1, max_retries + 1):
        try:
            if not _project_manager:
                raise RuntimeError("Project manager non inizializzato")

            # Slot assegnato solo al primo tentativo; i retry RPM restano sullo stesso slot,
            # mentre i retry RPD ruotano esplicitamente lo slot nel blocco di gestione errori.
            if attempt == 1 or not current_key:
                current_key, model_name = _project_manager.get_next_key_and_model()

            client = genai.Client(api_key=current_key)

            response_schema = genai_types.Schema(
                type=genai_types.Type.OBJECT,
                required=["score_competenze", "score_azienda", "score_stipendio", 
                         "score_località", "score_crescita", 
                         "motivazione", "match_competenze"],
                properties={
                    "score_competenze": genai_types.Schema(
                        type=genai_types.Type.INTEGER,
                        minimum=0,
                        maximum=10,
                    ),
                    "score_azienda": genai_types.Schema(
                        type=genai_types.Type.INTEGER,
                        minimum=0,
                        maximum=10,
                    ),
                    "score_stipendio": genai_types.Schema(
                        type=genai_types.Type.INTEGER,
                        minimum=0,
                        maximum=10,
                    ),
                    "score_località": genai_types.Schema(
                        type=genai_types.Type.INTEGER,
                        minimum=0,
                        maximum=10,
                    ),
                    "score_crescita": genai_types.Schema(
                        type=genai_types.Type.INTEGER,
                        minimum=0,
                        maximum=10,
                    ),
                    "motivazione": genai_types.Schema(
                        type=genai_types.Type.STRING,
                    ),
                    "match_competenze": genai_types.Schema(
                        type=genai_types.Type.ARRAY,
                        items=genai_types.Schema(
                            type=genai_types.Type.STRING,
                        ),
                    ),
                },
            )

            contents = [
                genai_types.Content(
                    role="user",
                    parts=[
                        genai_types.Part.from_text(text=prompt),
                    ],
                )
            ]
            cfg = genai_types.GenerateContentConfig(
                temperature=0.2,
                thinking_config=genai_types.ThinkingConfig(thinking_budget=0),
                response_mime_type="application/json",
                response_schema=response_schema,
                system_instruction=[
                    genai_types.Part.from_text(text=SYSTEM_INSTRUCTIONS),
                ],
            )
            stream = client.models.generate_content_stream(
                model=model_name,
                contents=contents,
                config=cfg,
            )
            accum = []
            for chunk in stream:
                if getattr(chunk, "text", None):
                    accum.append(chunk.text)
            text = ("".join(accum)).strip()
            if not text:
                raise ValueError(
                    "Risposta API vuota (possibile safety block, timeout o contenuto filtrato)"
                )
            json_str = _extract_json(text)
            parsed = json.loads(json_str)
            
            scores = {
                "score_competenze": int(parsed.get("score_competenze", 0)),
                "score_azienda": int(parsed.get("score_azienda", 0)),
                "score_stipendio": int(parsed.get("score_stipendio", 0)),
                "score_località": int(parsed.get("score_località", 0)),
                "score_crescita": int(parsed.get("score_crescita", 0)),
            }

            motivazione = str(parsed.get("motivazione", ""))
            # Applica la regola: se la motivazione indica mid/senior, score_competenze deve essere 0
            #_enforce_competenze_zero_for_senior(scores, motivazione=motivazione)
            
            final_score = _calculate_final_score(scores)
            
            result = {
                "score_competenze": scores["score_competenze"],
                "score_azienda": scores["score_azienda"],
                "score_stipendio": scores["score_stipendio"],
                "score_località": scores["score_località"],
                "score_crescita": scores["score_crescita"],
                "score": final_score,
                "motivazione": motivazione,
                "match_competenze": list(parsed.get("match_competenze", []) or []),
            }
            return result
            
        except Exception as e:
            last_err = e
            err_type = type(e).__name__
            status = getattr(e, "status_code", None)
            err_str = str(e)
            print(f"⚠️ Errore LLM al tentativo {attempt}/{max_retries}: {err_type}: {err_str}")
            if status is not None:
                print(f"   HTTP status: {status}")
            is_429 = (
                status == 429
                or "429" in err_str
                or (getattr(e, "message", "") or "").upper().find("RESOURCE_EXHAUSTED") >= 0
            )
            is_503 = status == 503 or "503" in err_str or "UNAVAILABLE" in err_str.upper()

            if is_429:
                if _is_rpd_error(err_str):
                    # Caso 2: 429 RPD – quota per-day esaurita per questo slot
                    print("   ⏳ 429 RPD: attesa 1s, ruoto slot key/modello")
                    time.sleep(1)
                    with _rpd_exhausted_lock:
                        _rpd_exhausted_count += 1
                        exhausted = _rpd_exhausted_count
                    threshold = _get_rpd_exhaustion_threshold()
                    if threshold > 0:
                        print(f"   [RPD] Slot esauriti: {exhausted}/{threshold}")
                        if exhausted >= threshold:
                            print("   ⚠️ Quota RPD giornaliera esaurita per tutte le key/modelli. Uso fallback.")
                            return FALLBACK_RESULT_RPD.copy()
                    # Avanza esplicitamente: il prossimo attempt userà il nuovo slot
                    if _project_manager:
                        current_key, model_name = _project_manager.get_next_key_and_model()
                else:
                    # Caso 1: 429 RPM – rispetta retryDelay e resta sullo stesso slot
                    wait = _get_retry_seconds_from_error(e) or min(60, base_delay * (2 ** attempt))
                    print(f"   ⏳ 429 RPM: attesa {wait:.0f}s, stesso slot (key/model invariati)")
                    time.sleep(wait)
            elif is_503:
                wait = min(60, 20 + base_delay * (2 ** attempt))
                print(f"   ⏳ 503 modello sotto carico: attesa {wait:.0f}s, riprovo dopo")
                time.sleep(wait)
            else:
                time.sleep(base_delay * attempt)

    # fallback robusto
    return {
        "score_competenze": None,
        "score_azienda": None,
        "score_stipendio": None,
        "score_località": None,
        "score_crescita": None,
        "score": None,
        "motivazione": f"⚠️ Errore valutazione LLM: {type(last_err).__name__ if last_err else 'sconosciuto'}",
        "match_competenze": None,
    }



def _get_retry_seconds_from_error(e: Exception) -> Optional[float]:
    """
    Estrae il delay di retry suggerito dall'errore API (es. 429 con 'Please retry in 59s').
    Restituisce None se non trovato.
    """
    s = str(e)
    # "Please retry in 59.504675799s." o "retry in 59s"
    m = re.search(r"[Rr]etry in (\d+(?:\.\d+)?)\s*s", s)
    if m:
        return min(120, max(1, float(m.group(1))))
    # details RetryInfo.retryDelay come "59s"
    err = getattr(e, "error", None) or getattr(e, "details", None)
    if isinstance(err, dict):
        for d in err.get("details") or err.get("error", {}).get("details") or []:
            if isinstance(d, dict) and d.get("retryDelay") is not None:
                delay_str = str(d["retryDelay"]).strip().rstrip("s")
                try:
                    return min(120, max(1, float(delay_str)))
                except ValueError:
                    pass
    return None


def _get_rpd_exhaustion_threshold() -> int:
    """
    Calcola dinamicamente la soglia di esaurimento RPD come
    numero di coppie key × modello disponibili.
    """
    try:
        return max(0, len(GEMINI_API_KEYS) * len(SINGLE_EVAL_MODELS))
    except Exception:
        return 0


def _is_rpd_error(error_message: str) -> bool:
    """
    True se il 429 è probabilmente dovuto a quota giornaliera (RPD) e non a RPM.
    Si basa su parole chiave tipiche degli errori di quota giornaliera.
    """
    if not error_message:
        return False
    msg = error_message.lower()
    rpd_keywords = ["free_tier", "quotavalue", "perday", "generaterequestsperday"]
    return any(kw in msg for kw in rpd_keywords)


def _extract_json(text: str) -> str:
    """Estrae JSON robusto anche con testo extra"""
    text = text.strip()

    # Caso 1: JSON pulito
    if text.startswith("{") and text.endswith("}"):
        try:
            json.loads(text)
            return text
        except json.JSONDecodeError:
            pass  # Fallback ai metodi successivi

    # Caso 2: JSON con testo extra DOPO
    # "{ ... } \n\nAlcune note extra..."
    start = text.find("{")
    if start == -1:
        return "{}"

    # Trova la chiusura bilanciata
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                candidate = text[start : i + 1]
                try:
                    json.loads(candidate)
                    return candidate
                except json.JSONDecodeError:
                    continue  # Prova la prossima }

    # Caso 3: JSON con testo extra PRIMA
    # "Ecco la valutazione: { ... }"
    end = text.rfind("}")
    if end != -1 and end > start:
        candidate = text[start : end + 1]
        try:
            json.loads(candidate)
            return candidate
        except json.JSONDecodeError:
            pass

    # Caso 4: Multiple JSON objects (prendi il primo completo)
    json_pattern = r"\{(?:[^{}]|(?:\{(?:[^{}]|(?:\{[^{}]*\})*)*\}))*\}"
    matches = re.findall(json_pattern, text, re.DOTALL)

    for match in matches:
        try:
            parsed = json.loads(match)
            # Verifica che abbia la struttura attesa
            if "evaluations" in parsed:
                return match
        except json.JSONDecodeError:
            continue

    # Fallback finale
    return "{}"



def enrich_dataframe_with_llm(df: pd.DataFrame, batch_size: int = 10) -> pd.DataFrame:
    """
    Arricchisce DataFrame con valutazioni LLM processando job in batch.
    
    Args:
        df: DataFrame con job descriptions
        batch_size: Numero di job per richiesta API (default 10)
                    Usa 1 per disabilitare batching
    
    Returns:
        DataFrame arricchito con colonne llm_*
    """
    if df is None or df.empty:
        return df

    # Reset del contatore RPD all'inizio di ogni esecuzione per evitare stato sporco
    global _rpd_exhausted_count
    with _rpd_exhausted_lock:
        _rpd_exhausted_count = 0
    threshold = _get_rpd_exhaustion_threshold()
    if threshold > 0:
        print(f"🔄 Reset contatore RPD. Slot key×modello disponibili: {threshold}")

    new_cols = {
        "llm_score": [],
        "llm_score_competenze": [],
        "llm_score_azienda": [],
        "llm_score_stipendio": [],
        "llm_score_località": [],
        "llm_score_crescita": [],
        "llm_motivazione": [],
        "llm_match_competenze": [],
    }

    total_rows = len(df)
    
    # Se batch_size è 1, usa il metodo originale
    if batch_size <= 1:
        print(f"\n=== ELABORAZIONE LLM SINGOLA ===")
        print(f"Elaborazione di {total_rows} offerte di lavoro (1 per richiesta)...")
        
        progress_bar = tqdm(
            df.iterrows(), 
            total=total_rows,
            ncols=100,
            desc="Elaborazione LLM",
            unit="job",
            miniters=10,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
        )

        for idx, row in progress_bar:
            # Early termination: se tutti gli slot RPD sono esauriti, usa direttamente il fallback
            use_fallback = False
            threshold = _get_rpd_exhaustion_threshold()
            if threshold > 0:
                with _rpd_exhausted_lock:
                    if _rpd_exhausted_count >= threshold:
                        use_fallback = True

            if use_fallback:
                print(f"   [RPD] Soglia {threshold} raggiunta. Job {idx} skippato con fallback.")
                res = FALLBACK_RESULT_RPD.copy()
            else:
                res = evaluate_job(row.to_dict())

            new_cols["llm_score"].append(res.get("score"))
            new_cols["llm_score_competenze"].append(res.get("score_competenze"))
            new_cols["llm_score_azienda"].append(res.get("score_azienda"))
            new_cols["llm_score_stipendio"].append(res.get("score_stipendio"))
            new_cols["llm_score_località"].append(res.get("score_località"))
            new_cols["llm_score_crescita"].append(res.get("score_crescita"))
            new_cols["llm_motivazione"].append(res.get("motivazione", ""))
            
            match_comp = res.get("match_competenze")
            new_cols["llm_match_competenze"].append(
                json.dumps(match_comp, ensure_ascii=False) if match_comp is not None else None
            )

        progress_bar.close()
    
    else:
        # Elaborazione batch
        num_batches = (total_rows + batch_size - 1) // batch_size
        rpd_saved = total_rows - num_batches
        
        print(f"\n=== ELABORAZIONE LLM IN BATCH ===")
        print(f"Job totali: {total_rows}")
        print(f"Batch size: {batch_size}")
        print(f"Numero di richieste API: {num_batches}")
        print(f"📉 Risparmio RPD: {rpd_saved} richieste (-{rpd_saved/total_rows*100:.1f}%)")
        
        progress_bar = tqdm(
            range(0, total_rows, batch_size),
            total=num_batches,
            ncols=100,
            desc="Batch LLM",
            unit="batch",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
        )
        
        for start_idx in progress_bar:
            end_idx = min(start_idx + batch_size, total_rows)
            batch_rows = df.iloc[start_idx:end_idx]
            
            # Converti batch in lista di dizionari
            jobs_data = [row.to_dict() for _, row in batch_rows.iterrows()]
            
            # Valuta batch intero con singola richiesta
            batch_results = evaluate_jobs_batch(jobs_data)
            
            # Verifica corrispondenza risultati
            if len(batch_results) != len(jobs_data):
                print(f"\n⚠️ Mismatch risultati batch {start_idx}-{end_idx}: "
                      f"attesi {len(jobs_data)}, ricevuti {len(batch_results)}")
                # Padding con risultati None
                while len(batch_results) < len(jobs_data):
                    batch_results.append({
                        "score": None,
                        "score_competenze": None,
                        "score_azienda": None,
                        "score_stipendio": None,
                        "score_località": None,
                        "score_crescita": None,
                        "motivazione": "Risultato mancante dal batch",
                        "match_competenze": None
                    })
            
            # Aggiungi risultati alle colonne
            for res in batch_results:
                new_cols["llm_score"].append(res.get("score"))
                new_cols["llm_score_competenze"].append(res.get("score_competenze"))
                new_cols["llm_score_azienda"].append(res.get("score_azienda"))
                new_cols["llm_score_stipendio"].append(res.get("score_stipendio"))
                new_cols["llm_score_località"].append(res.get("score_località"))
                new_cols["llm_score_crescita"].append(res.get("score_crescita"))
                new_cols["llm_motivazione"].append(res.get("motivazione", ""))
                
                match_comp = res.get("match_competenze")
                new_cols["llm_match_competenze"].append(
                    json.dumps(match_comp, ensure_ascii=False) if match_comp is not None else None
                )
        
        progress_bar.close()
    
    print(f"=== ELABORAZIONE LLM COMPLETATA ===")

    for k, v in new_cols.items():
        df[k] = v
    
    return df

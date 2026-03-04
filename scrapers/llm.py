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
    """Rate limiter che traccia separatamente ogni API key e modello"""
    
    def __init__(self, limits_per_model: Dict[str, int]):
        """
        Args:
            limits_per_model: dizionario modello → RPM massimo.
                              Es. {"gemini-2.5-flash-lite": 10, "gemini-2.5-flash": 5}
        """
        self.limits = limits_per_model
        self.key_model_requests: Dict[str, deque] = {}  # chiave: "apikey::modelname"
        self.key_model_total_count: Dict[str, int] = {}  # contatore storico per ogni bucket (Fix #2)
        self.lock = Lock()
    
    def wait_if_needed(self, api_key: str, model_name: str) -> None:
        """
        Controlla e rispetta il rate limit per questa specifica API key e modello
        (thread‑safe, rilasciando il lock prima di dormire).
        
        Args:
            api_key: La chiave API da controllare
            model_name: Il nome del modello da controllare
        """
        bucket_key = f"{api_key}::{model_name}"
        max_requests = self.limits.get(model_name, 10)  # fallback conservativo

        while True:
            with self.lock:
                if bucket_key not in self.key_model_requests:
                    self.key_model_requests[bucket_key] = deque()
                requests = self.key_model_requests[bucket_key]
                now = time.time()

                # rimuovi richieste scadute
                while requests and now - requests[0] > 60:
                    requests.popleft()

                if len(requests) < max_requests:
                    # c'è spazio: registra e termina
                    requests.append(now)
                    # Incrementa contatore storico (Fix #2)
                    self.key_model_total_count[bucket_key] = self.key_model_total_count.get(bucket_key, 0) + 1
                    return

                # calcola attesa, ma fallisce fuori dal lock
                oldest_request = requests[0]
                wait_time = 60 - (now - oldest_request) + 0.1

            # fuori dal lock
            if wait_time > 0:
                key_suffix = api_key[-6:] if len(api_key) >= 6 else api_key
                print(f"⏱️ Rate limit key ...{key_suffix} / {model_name}: attesa {wait_time:.1f}s")
                time.sleep(wait_time)
            # loop di nuovo per ricalcolare e verificare di nuovo il bucket
    
    def get_stats(self) -> Dict[str, Dict[str, int]]:
        """Restituisce statistiche per ogni coppia key×modello"""
        with self.lock:
            stats = {}
            now = time.time()
            for bucket_key, requests in self.key_model_requests.items():
                recent = sum(1 for ts in requests if now - ts < 60)
                # Usa contatore storico invece di len(requests) (Fix #2)
                stats[bucket_key] = {"last_minute": recent, "total": self.key_model_total_count.get(bucket_key, len(requests))}
            return stats



class MultiProjectManager:
    """Gestisce rotazione tra progetti con rate limiting per-key e per-modello integrato"""
    
    def __init__(self, api_keys: List[str]):
        """
        Args:
            api_keys: Lista di API keys da progetti diversi
        """
        if not api_keys:
            raise ValueError("Almeno una API key richiesta")
        
        # Lista ordinata e immutabile usata per la sequenza key×model (singolo job)
        self.api_keys_list: List[str] = list(api_keys)
        # Contatore piatto di slot key×model
        self.slot_index: int = 0

        self.lock = Lock()
        self.usage_stats = {key: 0 for key in api_keys}

        # Rate limiter per-model integrato
        model_limits = {
            "gemini-2.5-flash-lite": 10,
            "gemini-2.5-flash": 5,
        }
        self.rate_limiter = PerKeyRateLimiter(limits_per_model=model_limits)
    
    def get_next_key_and_model(self) -> tuple[str, str]:
        """
        Ottiene la prossima API key e il relativo modello da usare,
        alternando i modelli separatamente per ogni chiave.
        """
        with self.lock:
            num_models = len(SINGLE_EVAL_MODELS)
            total_slots = len(self.api_keys_list) * num_models
            if total_slots == 0:
                raise RuntimeError("Nessuna combinazione key×modello disponibile")

            # Sequenza lineare su griglia key×model
            idx = self.slot_index % total_slots
            key_idx = idx // num_models
            model_idx = idx % num_models
            self.slot_index += 1

            current_key = self.api_keys_list[key_idx]
            current_model = SINGLE_EVAL_MODELS[model_idx]
            self.usage_stats[current_key] += 1

        # Fuori dal lock: applica rate limiting con limite specifico per modello
        self.rate_limiter.wait_if_needed(current_key, current_model)

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
            "num_projects": len(self.api_keys_list),
            "rate_limits": rate_stats
        }


# Variabili globali
GEMINI_API_KEYS: List[str] = []
_project_manager: Optional[MultiProjectManager] = None

# Modelli usati nella valutazione singola (per il calcolo degli slot RPD)
# Ordine importante: indice 0 → modello "lite"
SINGLE_EVAL_MODELS: List[str] = ["gemini-2.5-flash-lite", "gemini-2.5-flash-lite", "gemini-2.5-flash"]

# Stato globale per gestione esaurimento quota RPD (per-day)
# ora traccia gli slot esauriti (key::model) per evitare doppi conteggi
_rpd_exhausted_slots: set[str] = set()
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

FALLBACK_RESULT_DLQ = {
    "score_competenze": None,
    "score_azienda": None,
    "score_stipendio": None,
    "score_località": None,
    "score_crescita": None,
    "score": None,
    "motivazione": "DLQ: 429 generico, da riprocessare",
    "match_competenze": None,
}



def initialize_api_keys(api_keys: List[str]):
    """Inizializza il multi-project manager con rate limiting per-key e per-modello"""
    global GEMINI_API_KEYS, _project_manager
    
    GEMINI_API_KEYS = api_keys
    
    if len(api_keys) > 1:
        # Multi-project: rate limit specifico per modello
        _project_manager = MultiProjectManager(api_keys)
        # print(f"✅ Multi-project rotation: {len(api_keys)} progetti")
        # print(f"📊 Rate limit: 10 RPM per gemini-2.5-flash-lite, 5 RPM per gemini-2.5-flash")
        # print(f"🚀 Throughput max: {len(api_keys) * 10} RPM totali (lite)")
    elif len(api_keys) == 1:
        # Singolo progetto: rate limit specifico per modello
        _project_manager = MultiProjectManager(api_keys)
        # print("⚠️  Singolo progetto - rate limit per modello")
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



def evaluate_job(row_data: Dict[str, Any], max_retries: int = 3, base_delay: float = 1.5) -> Dict[str, Any]:
    """
    Valuta un'offerta di lavoro usando tutti i campi disponibili.
    
    Args:
        row_data: Dizionario con tutti i campi della riga del DataFrame
        max_retries: Numero massimo di tentativi
        base_delay: Delay base tra i retry
        
    Returns:
        Dizionario con i risultati della valutazione LLM
    """
    global _rpd_exhausted_slots

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

            # Slot assegnato al primo tentativo; i retry ruotano esplicitamente lo slot
            # nel blocco di gestione errori (429 e 503), tranne errori generici.
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
            _enforce_competenze_zero_for_senior(scores, motivazione=motivazione)
            
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
            # print(f"⚠️ Errore LLM al tentativo {attempt}/{max_retries}: {err_type}: {err_str}")
            print(f"⚠️ LLM {err_type} tent {attempt}/{max_retries}")
            # if status is not None:
            #     print(f"   HTTP status: {status}")
            is_429 = (
                status == 429
                or "429" in err_str
                or (getattr(e, "message", "") or "").upper().find("RESOURCE_EXHAUSTED") >= 0
            )
            is_503 = status == 503 or "503" in err_str or "UNAVAILABLE" in err_str.upper()

            if is_429:
                if _is_rpd_error(err_str):
                    # Caso RPD: comportamento invariato
                    # Caso 2: 429 RPD – quota per-day esaurita per questo slot
                    # print("   ⏳ 429 RPD: attesa 1s, ruoto slot key/modello")
                    print(f"⚠️ RPD {model_name} tent {attempt}")
                    time.sleep(1)
                    slot_id = f"{current_key}::{model_name}"
                    with _rpd_exhausted_lock:
                        _rpd_exhausted_slots.add(slot_id)
                        exhausted = len(_rpd_exhausted_slots)
                    threshold = _get_rpd_exhaustion_threshold()
                    if threshold > 0:
                        # print(f"   [RPD] Slot esauriti: {exhausted}/{threshold}")
                        if exhausted >= threshold:
                            print("   ⚠️ Quota RPD giornaliera esaurita per tutte le key/modelli. Uso fallback.")
                            return FALLBACK_RESULT_RPD.copy()
                    # Avanza esplicitamente: il prossimo attempt userà il nuovo slot
                    if _project_manager:
                        current_key, model_name = _project_manager.get_next_key_and_model()
                else:
                    # Caso RPM classico o 429 generico (non RPD)
                    # Ruota slot per evitare lo stesso endpoint
                    if _project_manager:
                        current_key, model_name = _project_manager.get_next_key_and_model()
                    # Attesa: usa retryDelay esplicito se disponibile, altrimenti backoff esponenziale con cap a 60s
                    wait = _get_retry_seconds_from_error(e) or min(60, 10 * (2 ** (attempt - 1)))
                    print(f"   ⏳ 429: attesa {wait:.0f}s, rotato slot key/modello")
                    time.sleep(wait)
                    # Se siamo all'ultimo tentativo, invia in DLQ
                    if attempt == max_retries:
                        return FALLBACK_RESULT_DLQ.copy()
            elif is_503:
                # Rotazione slot su 503 (Fix #3)
                key_suffix = current_key[-6:] if len(current_key) >= 6 else current_key
                print(f"   ⏳ 503 overload su ...{key_suffix}/{model_name}: ruoto slot e riprovo")
                if _project_manager:
                    current_key, model_name = _project_manager.get_next_key_and_model()
                    time.sleep(5)  # Attesa minima anche con multi-project per server transitorio
                else:
                    wait = min(60, 20 + base_delay * (2 ** attempt))
                    print(f"   ⏳ 503: attesa {wait:.0f}s")
                    time.sleep(wait)
            else:
                time.sleep(base_delay * attempt)
                # Se siamo all'ultimo tentativo, invia in DLQ anche per errori non 429/503
                if attempt == max_retries:
                    return FALLBACK_RESULT_DLQ.copy()

    # Fallback per errori non gestiti (non dovrebbe essere raggiunto normalmente)
    return FALLBACK_RESULT_DLQ.copy()



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
        return max(0, len(GEMINI_API_KEYS) * len(set(SINGLE_EVAL_MODELS)))
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
            # Per il singolo job, cerca i campi attesi
            if "score_competenze" in parsed:
                return match
        except json.JSONDecodeError:
            continue

    # Fallback finale
    return "{}"



def enrich_dataframe_with_llm(df: pd.DataFrame) -> pd.DataFrame:
    """
    Arricchisce DataFrame con valutazioni LLM processando job singolarmente.
    
    Args:
        df: DataFrame con job descriptions
    
    Returns:
        DataFrame arricchito con colonne llm_*
    """
    if df is None or df.empty:
        return df

    # Reset del contatore RPD all'inizio di ogni esecuzione per evitare stato sporco
    global _rpd_exhausted_slots
    with _rpd_exhausted_lock:
        _rpd_exhausted_slots.clear()
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

    dlq: list[tuple[int, Any]] = []  # (DataFrame index, row)

    total_rows = len(df)
    
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
        with _rpd_exhausted_lock:
            use_fallback = len(_rpd_exhausted_slots) >= _get_rpd_exhaustion_threshold()

        if use_fallback:
            print(f"   [RPD] Soglia {threshold} raggiunta. Job {idx} skippato con fallback.")
            res = FALLBACK_RESULT_RPD.copy()
        else:
            res = evaluate_job(row.to_dict(), max_retries=len(GEMINI_API_KEYS) * 2)

        if res.get("motivazione", "").startswith("DLQ:"):
            dlq.append((idx, row))
            new_cols["llm_score"].append(None)
            new_cols["llm_score_competenze"].append(None)
            new_cols["llm_score_azienda"].append(None)
            new_cols["llm_score_stipendio"].append(None)
            new_cols["llm_score_località"].append(None)
            new_cols["llm_score_crescita"].append(None)
            new_cols["llm_motivazione"].append("DLQ: in attesa di riprocessamento")
            new_cols["llm_match_competenze"].append(None)
        else:
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
    
    # Applica new_cols PRIMA del DLQ processing (Bug #1 fix)
    for k, v in new_cols.items():
        df[k] = v
    
    if dlq:
        cooldown = 60
        print(f"\n♻️ DLQ: {len(dlq)} job da riprocessare. Cooldown {cooldown}s...")
        time.sleep(cooldown)
        
        # Reset stato RPD: i job DLQ vanno ritentati senza precondizioni (Bug #2 fix)
        with _rpd_exhausted_lock:
            _rpd_exhausted_slots.clear()
        
        print(f"♻️ Inizio riprocessamento DLQ...")

        for dlq_idx, (df_idx, row) in enumerate(dlq, start=1):
            print(f"  DLQ job {dlq_idx}/{len(dlq)}...")
            res = evaluate_job(row.to_dict(), max_retries=len(GEMINI_API_KEYS) * len(SINGLE_EVAL_MODELS))
            # log esplicito per DLQ falliti definitivamente
            motiv = res.get("motivazione", "")
            if motiv.startswith("DLQ:") or motiv.startswith("Quota RPD"):
                print(f"  ⚠️ DLQ job {dlq_idx} non risolto: {motiv[:60]}")
            # use the result directly; evaluate_job already applies
            # _enforce_competenze_zero_for_senior internally
            df.at[df_idx, "llm_score"]            = res.get("score")
            df.at[df_idx, "llm_score_competenze"] = res.get("score_competenze")
            df.at[df_idx, "llm_score_azienda"]    = res.get("score_azienda")
            df.at[df_idx, "llm_score_stipendio"]  = res.get("score_stipendio")
            df.at[df_idx, "llm_score_località"]   = res.get("score_località")
            df.at[df_idx, "llm_score_crescita"]   = res.get("score_crescita")
            df.at[df_idx, "llm_motivazione"]      = res.get("motivazione")
            df.at[df_idx, "llm_match_competenze"] = (
                json.dumps(res.get("match_competenze"), ensure_ascii=False)
                if res.get("match_competenze") is not None else None
            )

        print(f"✅ DLQ completata: {len(dlq)} job riprocessati.")

    print(f"=== ELABORAZIONE LLM COMPLETATA ===")
    
    return df

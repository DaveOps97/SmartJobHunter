from __future__ import annotations

import os
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
        
        self.api_keys = deque(api_keys)
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
            "num_projects": len(self.api_keys),
            "rate_limits": rate_stats
        }

# Variabili globali
GEMINI_API_KEYS: List[str] = []
IS_FREE_API_KEY = False
_project_manager: Optional[MultiProjectManager] = None


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

1. score_competenze (peso 40% - CRITICO): allineamento con linguaggi, framework, metodologie e dominio del ruolo. Dai score 0 se il ruolo è senior o mid level
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
	# Estrai i campi principali
	description = row_data.get("description")
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

	# Costruisci prompt strutturato con tutti i campi
	structured_data = f"""
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

	prompt = (
		"Valuta la seguente offerta di lavoro in base alle istruzioni di sistema. "
		"Rispondi esclusivamente con JSON valido senza testo extra.\n\n" 
		+ structured_data
	)
	
	# _get_client() ora gestisce key rotation e rate limiting per-key
	client = _get_client()

	last_err: Optional[Exception] = None
	for attempt in range(1, max_retries + 1):
		try:
			# Definisci lo schema JSON per la risposta
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
				model="gemini-flash-lite-latest",
				contents=contents,
				config=cfg,
			)
			accum = []
			for chunk in stream:
				if getattr(chunk, "text", None):
					accum.append(chunk.text)
			text = ("".join(accum)).strip()
			# estrai JSON puro se il modello aggiunge testo
			json_str = _extract_json(text)
			parsed = json.loads(json_str)
			
			# Estrai i punteggi dei singoli criteri
			scores = {
				"score_competenze": int(parsed.get("score_competenze", 0)),
				"score_azienda": int(parsed.get("score_azienda", 0)),
				"score_stipendio": int(parsed.get("score_stipendio", 0)),
				"score_località": int(parsed.get("score_località", 0)),
				"score_crescita": int(parsed.get("score_crescita", 0)),
			}
			
			# Calcola lo score finale
			final_score = _calculate_final_score(scores)
			
			# Ritorna tutti i campi richiesti
			result = {
				"score_competenze": scores["score_competenze"],
				"score_azienda": scores["score_azienda"],
				"score_stipendio": scores["score_stipendio"],
				"score_località": scores["score_località"],
				"score_crescita": scores["score_crescita"],
				"score": final_score,
				"motivazione": str(parsed.get("motivazione", "")),
				"match_competenze": list(parsed.get("match_competenze", []) or []),
			}
			return result
			
		except Exception as e:
			# Salva l'errore per il fallback finale
			last_err = e
			# Log dettagliato dell'eccezione
			err_type = type(e).__name__
			status = getattr(e, "status_code", None)
			print(f"⚠️ Errore LLM al tentativo {attempt}/{max_retries}: {err_type}: {e}")
			if status is not None:
				print(f"   HTTP status: {status}")
			# Backoff semplice tra i retry
			time.sleep(base_delay * attempt)
			
			# Nel retry, ottieni un NUOVO client (con rate limiting per-key)
			client = _get_client()


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


def _extract_json(text: str) -> str:
	text = text.strip()
	# Se già JSON
	if text.startswith("{") and text.endswith("}"):
		return text
	# Cerca il primo blocco JSON
	start = text.find("{")
	end = text.rfind("}")
	if start != -1 and end != -1 and end > start:
		return text[start : end + 1]
	# Ultimo resort
	return "{}"


def enrich_dataframe_with_llm(df: pd.DataFrame) -> pd.DataFrame:
	"""
	Per ogni riga del DF, invia la colonna `description` al modello e aggiunge
	le colonne: llm_score (int), llm_motivazione (str),
	llm_match_competenze (json string), e i singoli punteggi dei criteri.
	"""
	if df is None or df.empty:
		return df

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

	# Barra di progresso per tracciare l'elaborazione LLM
	total_rows = len(df)
	
	# Stampa il messaggio di avvio
	print(f"\n=== INIZIO ELABORAZIONE LLM ===")
	print(f"Elaborazione di {total_rows} offerte di lavoro...")
	
	# Usa tqdm per la barra di progresso con informazioni dettagliate
	# miniters=50: aggiorna ogni 50 iterazioni invece di ogni 1
	# mininterval=1.0: aggiorna almeno ogni secondo
	# position=0, leave=True: mantiene la barra sulla stessa riga
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
		res = evaluate_job(row.to_dict())
		
		# Gestisci None esplicitamente
		new_cols["llm_score"].append(res.get("score"))  # Può essere None
		new_cols["llm_score_competenze"].append(res.get("score_competenze"))
		new_cols["llm_score_azienda"].append(res.get("score_azienda"))
		new_cols["llm_score_stipendio"].append(res.get("score_stipendio"))
		new_cols["llm_score_località"].append(res.get("score_località"))
		new_cols["llm_score_crescita"].append(res.get("score_crescita"))
		new_cols["llm_motivazione"].append(res.get("motivazione", ""))
		# Per match_competenze: se None, salva None invece di JSON
		match_comp = res.get("match_competenze")
		new_cols["llm_match_competenze"].append(
			json.dumps(match_comp, ensure_ascii=False) if match_comp is not None else None
		)


	# Chiudi la barra di progresso
	progress_bar.close()
	print(f"=== ELABORAZIONE LLM COMPLETATA ===")

	for k, v in new_cols.items():
		df[k] = v
	return df

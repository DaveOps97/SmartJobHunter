from __future__ import annotations

import os
import time
import json
from pathlib import Path
from typing import Any, Dict, Optional
from collections import deque
from threading import Lock

import pandas as pd
from tqdm import tqdm
from google import genai
from google.genai import types as genai_types

# Variabili globali per l'API key
GEMINI_API_KEY = None
IS_FREE_API_KEY = False


def initialize_api_key(api_key: str, is_free: bool):
    """Inizializza le variabili globali dell'API key"""
    global GEMINI_API_KEY, IS_FREE_API_KEY
    GEMINI_API_KEY = api_key
    IS_FREE_API_KEY = is_free


class RateLimiter:
	"""Rate limiter per controllare il numero di richieste per minuto"""
	
	def __init__(self, max_requests_per_minute: int = 15):
		self.max_requests = max_requests_per_minute
		self.requests = deque()
		self.lock = Lock()
	
	def wait_if_needed(self):
		"""Aspetta se necessario per rispettare il limite di richieste per minuto"""
		with self.lock:
			now = time.time()
			
			# Rimuovi richieste più vecchie di 1 minuto
			while self.requests and now - self.requests[0] > 60:
				self.requests.popleft()
			
			# Se abbiamo raggiunto il limite, aspetta
			if len(self.requests) >= self.max_requests:
				oldest_request = self.requests[0]
				wait_time = 60 - (now - oldest_request) + 0.1  # +0.1 per sicurezza
				if wait_time > 0:
					time.sleep(wait_time)
					# Aggiorna il timestamp dopo l'attesa
					now = time.time()
					# Rimuovi di nuovo le richieste vecchie
					while self.requests and now - self.requests[0] > 60:
						self.requests.popleft()
			
			# Registra questa richiesta
			self.requests.append(now)


# Istanza globale del rate limiter
_rate_limiter = None


def _get_rate_limiter() -> Optional[RateLimiter]:
	"""Ottiene il rate limiter appropriato basato sulla variabile globale"""
	global _rate_limiter
	
	if _rate_limiter is None:
		# Usa le variabili globali dal main invece di caricare ogni volta
		if IS_FREE_API_KEY:
			_rate_limiter = RateLimiter(max_requests_per_minute=15)
		else:
			_rate_limiter = None  # Nessun rate limiting
	
	return _rate_limiter


SYSTEM_INSTRUCTIONS = """PERSONA: Sei un esperto di selezione del personale che valuta offerte di lavoro in relazione al mio profilo professionale.

TASK: Valuta se l'offerta di lavoro è rilevante per me, assegnando punteggi separati per ciascun criterio dopo aver analizzato l'offerta.

PROFILO PROFESSIONALE:
- Laurea Magistrale in Ingegneria Informatica - Data Engineering and AI
- Livello: Junior/Entry Level
- Esperienza: sviluppo software, Big Data, NLP, RAG
- Linguaggi: Python, Java
- Database: ArangoDB, MongoDB, PostgreSQL, Pinecone
- Framework: Scrapy, Haystack, LangChain, Spring, REST APIs, Apache Spark (PySpark)
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

1. score_competenze (peso 40% - CRITICO): allineamento con linguaggi, framework, metodologie e dominio del ruolo
2. score_azienda (peso 20% - IMPORTANTE): reputazione, cultura collaborativa, innovazione, formazione, tecnologie moderne; preferenza per startup/aziende giovani e dinamiche
3. score_stipendio (peso 15% - IMPORTANTE): competitività per profilo junior, benefit significativi, smart working, flessibilità
4. score_località (peso 10% - MODERATO): corrispondenza con aree preferite o remote work/relocation accettabile
5. score_crescita (peso 10% - MODERATO): mentorship, percorsi di crescita, formazione, progetti sfidanti
6. score_coerenza (peso 5% - MINORE): adeguatezza responsabilità e mansioni al livello junior

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
	- **Analisi Punteggi:** riepiloga i singoli punteggi assegnati ai sei criteri, riportando un breve commento per ciascuno nel formato:
	* Nome criterio (x/10):* commento sintetico
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
		"score_azienda": 0.20,     # 20% - IMPORTANTE
		"score_stipendio": 0.15,   # 15% - IMPORTANTE
		"score_località": 0.10,    # 10% - MODERATO
		"score_crescita": 0.10,     # 10% - MODERATO
		"score_coerenza": 0.05,     # 5% - MINORE
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
	# Usa la variabile globale invece di caricare ogni volta
	if not GEMINI_API_KEY:
		raise RuntimeError("API key mancante: imposta la variabile d'ambiente FREE_GEMINI_API_KEY o GEMINI_API_KEY")
	if genai is None:
		raise RuntimeError("google-genai non installato. Aggiungi la dipendenza google-genai.")
	return genai.Client(api_key=GEMINI_API_KEY)


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
			"score_coerenza": 0,
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
- Livello: {job_level or 'N/A'}
- Funzione: {job_function or 'N/A'}
- Competenze richieste: {skills or 'N/A'}

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

REQUISITI AGGIUNTIVI:
- Lingue: {language_requirements or 'N/A'}
- Attività ruolo: {role_activities or 'N/A'}

DESCRIZIONE COMPLETA:
{description}
"""

	prompt = (
		"Valuta la seguente offerta di lavoro in base alle istruzioni di sistema. "
		"Rispondi esclusivamente con JSON valido senza testo extra.\n\n" 
		+ structured_data
	)

	client = _get_client()
	
	# Applica rate limiting se necessario
	rate_limiter = _get_rate_limiter()
	if rate_limiter:
		rate_limiter.wait_if_needed()

	last_err: Optional[Exception] = None
	for attempt in range(1, max_retries + 1):
		try:
			# Definisci lo schema JSON per la risposta
			response_schema = genai_types.Schema(
				type=genai_types.Type.OBJECT,
				required=["score_competenze", "score_azienda", "score_stipendio", 
						 "score_località", "score_crescita", "score_coerenza", 
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
					"score_coerenza": genai_types.Schema(
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
				"score_coerenza": int(parsed.get("score_coerenza", 0)),
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
				"score_coerenza": scores["score_coerenza"],
				"score": final_score,
				"motivazione": str(parsed.get("motivazione", "")),
				"match_competenze": list(parsed.get("match_competenze", []) or []),
			}
			return result
			
		except Exception as e:  # rete, rate limit, parsing
			last_err = e
			time.sleep(base_delay * attempt)
			# Applica rate limiting anche nei retry
			if rate_limiter:
				rate_limiter.wait_if_needed()

	# fallback robusto
	return {
		"score_competenze": 0,
		"score_azienda": 0,
		"score_stipendio": 0,
		"score_località": 0,
		"score_crescita": 0,
		"score_coerenza": 0,
		"score": 0,
		"motivazione": f"Errore valutazione: {type(last_err).__name__ if last_err else 'sconosciuto'}",
		"match_competenze": [],
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
		"llm_score_coerenza": [],
		"llm_motivazione": [],
		"llm_match_competenze": [],
	}

	# Barra di progresso per tracciare l'elaborazione LLM
	total_rows = len(df)
	
	# Stampa il messaggio di avvio
	print(f"\n=== INIZIO ELABORAZIONE LLM ===")
	print(f"Elaborazione di {total_rows} offerte di lavoro...")
	
	# Usa tqdm per la barra di progresso con informazioni dettagliate
	progress_bar = tqdm(
		df.iterrows(), 
		total=total_rows,
		ncols = 100,
		desc="Elaborazione LLM",
		unit="offerta",
		bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
	)

	for idx, row in progress_bar:
		# Aggiorna la descrizione della barra con il numero di riga corrente
		progress_bar.set_description(f"Elaborazione LLM (riga {idx+1}/{total_rows})")
		
		res = evaluate_job(row.to_dict())
		new_cols["llm_score"].append(int(res.get("score", 0)))
		new_cols["llm_score_competenze"].append(int(res.get("score_competenze", 0)))
		new_cols["llm_score_azienda"].append(int(res.get("score_azienda", 0)))
		new_cols["llm_score_stipendio"].append(int(res.get("score_stipendio", 0)))
		new_cols["llm_score_località"].append(int(res.get("score_località", 0)))
		new_cols["llm_score_crescita"].append(int(res.get("score_crescita", 0)))
		new_cols["llm_score_coerenza"].append(int(res.get("score_coerenza", 0)))
		new_cols["llm_motivazione"].append(str(res.get("motivazione", "")))
		# serializza lista in JSON per CSV
		new_cols["llm_match_competenze"].append(json.dumps(res.get("match_competenze", []), ensure_ascii=False))

	# Chiudi la barra di progresso
	progress_bar.close()
	print(f"=== ELABORAZIONE LLM COMPLETATA ===")

	for k, v in new_cols.items():
		df[k] = v
	return df

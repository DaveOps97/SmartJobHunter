from __future__ import annotations

import os
import time
import json
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

try:
	from google import genai
	from google.genai import types as genai_types
except Exception:  # pragma: no cover
	genai = None
	genai_types = None


SYSTEM_INSTRUCTIONS = """
Sei un esperto di selezione del personale. Il tuo compito è valutare se un'offerta di lavoro
è rilevante per il candidato in base a competenze, interessi e qualità complessiva della posizione.

PROFILO DEL CANDIDATO:
- Laurea Magistrale in Ingegneria Informatica – Data Engineering and AI
- Esperienza in sviluppo software, Big Data e sistemi basati su NLP e RAG
- Linguaggi: Python, Java
- Database: ArangoDB, MongoDB, PostgreSQL, Pinecone
- Framework e tecnologie: Scrapy, Haystack, LangChain, Spring, REST APIs, Apache Spark (PySpark)
- Strumenti: Git, Docker, VSCode, Linux, macOS
- Metodologie: Agile (Scrum), TDD, CI/CD
- Competenze tecniche: Data Engineering, Web Scraping, Graph Data Modeling, Object-Oriented Programming
- Livello linguistico: Inglese C1

PREFERENZE:
- Ruoli: Data Engineer, Software Engineer, Big Data Engineer, AI Engineer, Backend Developer
- Settori: Data Science, AI, Cybersecurity, Software Development, Cloud & Automation
- Località preferite: Nord Italia o Francia (es. zona Sophia-Antipolis), ma apprezzo anche la regione Campania se l'opportunità è interessante
- Interesse per aziende solide o innovative (startup strutturate o aziende con buon rating su Glassdoor)
- Interesse per posizioni con opportunità di crescita e apprendimento (mentorship, training, progetti stimolanti)
- Preferenza per stipendi competitivi e benefit tecnologici (smart working, formazione, tool moderni)

ISTRUZIONI:
Analizza la descrizione dell'offerta e rispondi SOLO in formato JSON:
{
  "rilevante": true/false,
  "score": 0-10,
  "motivazione": "breve spiegazione (max 500 caratteri)",
  "match_competenze": ["competenza1", "competenza2"],
  "segnali_positivi": ["punto1", "punto2", ...],
  "segnali_negativi": ["punto1", "punto2", ...]
}

CRITERI DI VALUTAZIONE E PESI:
1. Competenze tecniche e ruolo (40%)
   - Allineamento con linguaggi, framework, metodologie e dominio del ruolo.
2. Qualità e reputazione dell’azienda (20%)
   - Buon rating Glassdoor (>3.5), solidità o innovazione tecnologica.
3. Retribuzione e benefit (15%)
   - Stipendio competitivo per un profilo junior, benefit e flessibilità lavorativa.
4. Località e modalità di lavoro (10%)
   - Zone preferite, smart working o relocation accettabile.
5. Possibilità di crescita e formazione (10%)
   - Mentorship, training, percorsi di crescita chiari, uso di tecnologie avanzate.
6. Coerenza del ruolo (5%)
   - Adeguatezza delle responsabilità al livello di esperienza.

Criteri di interpretazione del punteggio finale:
- Score >= 7: altamente rilevante
- Score 4–6: parzialmente rilevante
- Score < 4: non rilevante
"""


def _load_env_from_root():
	"""Carica variabili d'ambiente dal file .env nella root del progetto"""
	# Trova la root del progetto (directory che contiene main.py)
	current_dir = Path(__file__).parent
	project_root = current_dir.parent  # scrapers/ -> root/
	env_file = project_root / ".env"
	
	if env_file.exists():
		with open(env_file, 'r', encoding='utf-8') as f:
			for line in f:
				line = line.strip()
				if line and not line.startswith('#') and '=' in line:
					key, value = line.split('=', 1)
					os.environ[key.strip()] = value.strip()


def _get_client() -> Any:
	# Carica variabili dal .env se presente
	_load_env_from_root()
	
	api_key = os.getenv("GEMINI_API_KEY")
	if not api_key:
		raise RuntimeError("API key mancante: imposta la variabile d'ambiente GEMINI_API_KEY o GOOGLE_API_KEY")
	if genai is None:
		raise RuntimeError("google-genai non installato. Aggiungi la dipendenza google-genai.")
	return genai.Client(api_key=api_key)


def evaluate_description(description: Optional[str], title: Optional[str] = None, company: Optional[str] = None, max_retries: int = 3, base_delay: float = 1.5) -> Dict[str, Any]:
	if not description or not isinstance(description, str) or description.strip() == "":
		return {
			"rilevante": False,
			"score": 0,
			"motivazione": "Nessuna descrizione disponibile",
			"match_competenze": [],
			"segnali_positivi": [],
			"segnali_negativi": ["mancanza descrizione"],
		}

	prompt = (
		"Valuta la seguente offerta in base alle istruzioni di sistema. "
		"Rispondi esclusivamente con JSON valido senza testo extra.\n\n" 
		f"Titolo: {title or ''}\n"
		f"Azienda: {company or ''}\n\n"
		"Descrizione:\n" + description
	)

	client = _get_client()

	last_err: Optional[Exception] = None
	for attempt in range(1, max_retries + 1):
		try:
			contents = [
				genai_types.Content(
					role="user",
					parts=[
						genai_types.Part.from_text(text=SYSTEM_INSTRUCTIONS),
						genai_types.Part.from_text(text=prompt),
					],
				)
			]
			cfg = genai_types.GenerateContentConfig(
				thinking_config=genai_types.ThinkingConfig(thinking_budget=0),
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
			# normalizza campi attesi
			return {
				"rilevante": bool(parsed.get("rilevante", False)),
				"score": int(parsed.get("score", 0)),
				"motivazione": str(parsed.get("motivazione", ""))[:500],
				"match_competenze": list(parsed.get("match_competenze", []) or []),
				"segnali_positivi": list(parsed.get("segnali_positivi", []) or []),
				"segnali_negativi": list(parsed.get("segnali_negativi", []) or []),
			}
		except Exception as e:  # rete, rate limit, parsing
			last_err = e
			time.sleep(base_delay * attempt)

	# fallback robusto
	return {
		"rilevante": False,
		"score": 0,
		"motivazione": f"Errore valutazione: {type(last_err).__name__ if last_err else 'sconosciuto'}",
		"match_competenze": [],
		"segnali_positivi": [],
		"segnali_negativi": ["errore chiamata LLM"],
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
	le colonne: llm_relevant (bool), llm_score (int), llm_motivazione (str),
	llm_match_competenze (json string), llm_segnali_positivi (json string),
	llm_segnali_negativi (json string).
	"""
	if df is None or df.empty:
		return df

	new_cols = {
		"llm_relevant": [],
		"llm_score": [],
		"llm_motivazione": [],
		"llm_match_competenze": [],
		"llm_segnali_positivi": [],
		"llm_segnali_negativi": [],
	}

	for _, row in df.iterrows():
		res = evaluate_description(
			description=row.get("description"),
			title=row.get("title"),
			company=row.get("company"),
		)
		new_cols["llm_relevant"].append(bool(res.get("rilevante", False)))
		new_cols["llm_score"].append(int(res.get("score", 0)))
		new_cols["llm_motivazione"].append(str(res.get("motivazione", "")))
		# serializza liste in JSON per CSV
		new_cols["llm_match_competenze"].append(json.dumps(res.get("match_competenze", []), ensure_ascii=False))
		new_cols["llm_segnali_positivi"].append(json.dumps(res.get("segnali_positivi", []), ensure_ascii=False))
		new_cols["llm_segnali_negativi"].append(json.dumps(res.get("segnali_negativi", []), ensure_ascii=False))

	for k, v in new_cols.items():
		df[k] = v
	return df

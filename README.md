# ListScraper

**ListScraper** è un job scraper intelligente che raccoglie automaticamente offerte di lavoro da piattaforme come LinkedIn, Indeed, Glassdoor e HiringCafe, analizzandole tramite un Large Language Model (LLM) per identificare quelle più rilevanti in base alle preferenze e competenze dell'utente.

## 📋 Descrizione del Progetto

ListScraper automatizza il processo di ricerca del lavoro attraverso un sistema in due fasi:

1. **Scraping**: Il sistema esegue web scraping su diverse piattaforme di recruitment usando la libreria JobSpy e API dedicate, raccogliendo dati strutturati sulle offerte di lavoro disponibili. Le informazioni estratte includono titolo della posizione, azienda, località, descrizione del lavoro e altri dettagli rilevanti.

2. **Arricchimento LLM**: Ogni offerta viene processata da un Large Language Model (Google Gemini) che analizza la job description e altre informazioni pertinenti. L'LLM valuta se l'offerta corrisponde alle preferenze personali e alle competenze specificate, assegnando punteggi su diversi criteri (competenze, azienda, stipendio, località, crescita) e filtrando automaticamente le opportunità più rilevanti.

Il progetto utilizza un'**architettura DB-first**: lo scraping scrive direttamente nel database SQLite con upsert incrementale, paginazione e flag utente (visionato/interessato/applicato).

## 📁 Struttura del Progetto

```
ListScraper/
├── main.py                 # Script principale per scraping e arricchimento LLM
├── requirements.txt        # Dipendenze Python
├── README.md              # Questa documentazione
│
├── api/                   # Server web per consultazione offerte
│   └── server.py         # FastAPI con frontend HTML integrato
│
├── scrapers/              # Moduli per scraping da diverse fonti
│   ├── __init__.py
│   ├── jobspy_scraper.py # Scraper per LinkedIn, Indeed, Glassdoor (via JobSpy)
│   ├── hiringcafe_scraper.py # Scraper per HiringCafe (API diretta)
│   ├── llm.py            # Logica di arricchimento con Google Gemini
│   └── utils.py          # Funzioni utility comuni (pulizia HTML, combinazione fonti)
│
├── storage/               # Database e interfaccia CLI
│   ├── sqlite_db.py      # Gestione database SQLite (schema, upsert, query)
│   ├── cli.py            # CLI per consultazione e aggiornamento flag
│   └── jobs.db           # Database SQLite (non versionato)
│
├── scripts/               # Script di utilità e orchestrazione
│   ├── run_scrape_and_sync.py # Script orchestratore (main + maintenance)
│   └── migrate_db.py     # Script per migrazione schema database
│
└── cron/                  # Configurazione schedulazione automatica (macOS)
    ├── cron_ls.sh        # Script bash per esecuzione schedulata
    └── com.davidelandolfi.listscraper.plist # Configurazione launchd
```

## 🚀 Setup

### Prerequisiti

- Python 3.8+
- Virtual environment (consigliato)

### Installazione

1. **Clona il repository** (se applicabile) o naviga nella directory del progetto

2. **Crea e attiva un virtual environment**:
```bash
python -m venv .venv
source .venv/bin/activate  # Su macOS/Linux
# oppure
.venv\Scripts\activate  # Su Windows
```

3. **Installa le dipendenze**:
```bash
pip install -r requirements.txt
```

4. **Configura la chiave API Gemini**:

   Crea un file `.env` nella root del progetto:
```bash
FREE_GEMINI_API_KEY=your_api_key_here
```

   Oppure esporta la variabile d'ambiente:
```bash
export FREE_GEMINI_API_KEY=your_api_key_here
```

   **Nota**: Se usi `FREE_GEMINI_API_KEY`, il sistema applica automaticamente un rate limiting di 15 richieste/minuto. Per rate illimitati, usa `GEMINI_API_KEY`.

## 💻 Utilizzo

### Esecuzione Manuale

Esegui lo scraping completo (raccolta + arricchimento LLM):

```bash
python main.py
```

Oppure usa lo script orchestratore che include anche la pulizia automatica:

```bash
python -m scripts.run_scrape_and_sync
```

### Consultazione via CLI

**Lista offerte** (paginazione e ordinamento):

```bash
# Lista prima pagina ordinata per score decrescente (default)
python -m storage.cli list \
  --db storage/jobs.db \
  --page 1 \
  --page-size 50 \
  --order-by llm_score \
  --order-dir desc

# Solo offerte non visionate
python -m storage.cli list \
  --db storage/jobs.db \
  --only-unviewed

# Output JSON
python -m storage.cli list \
  --db storage/jobs.db \
  --json
```

**Aggiorna flag** (viewed/interested/applied/notes):

```bash
# Segna come visionata con nota
python -m storage.cli set \
  --db storage/jobs.db \
  --id <JOB_ID> \
  --viewed true \
  --note "interessante"

# Segna come applicata
python -m storage.cli set \
  --db storage/jobs.db \
  --id <JOB_ID> \
  --applied true
```

**Opzioni di ordinamento disponibili**:
- `--order-by`: `llm_score`, `date_posted`, `company`, `title`, `scraping_date`
- `--order-dir`: `asc` o `desc`

### Consultazione via Web Interface

Avvia il server FastAPI:

```bash
LISTSCRAPER_DB=storage/jobs.db \
  uvicorn api.server:app --host 127.0.0.1 --port 8000
```

Apri il browser su `http://127.0.0.1:8000/` per accedere all'interfaccia web con:
- Paginazione e ordinamento
- Filtri per stato (not viewed, viewed, interested, applied)
- Toggle flag direttamente dalla tabella
- Visualizzazione dettagliata della motivazione LLM
- Copia URL delle offerte interessate

## ⏰ Esecuzione Automatica (macOS)

Il progetto include configurazione per esecuzione automatica giornaliera tramite `launchd` (macOS).

### Setup Launchd

1. **Copia il file plist** nella directory LaunchAgents:
```bash
cp cron/com.davidelandolfi.listscraper.plist ~/Library/LaunchAgents/
```

2. **Modifica il percorso dello script** nel plist se necessario (attualmente configurato per `/Users/davidelandolfi/PyProjects/ListScraper/`)

3. **Carica il job**:
```bash
launchctl load ~/Library/LaunchAgents/com.davidelandolfi.listscraper.plist
```

4. **Avvia manualmente** (opzionale, per test):
```bash
launchctl start com.davidelandolfi.listscraper
```

### Configurazione

Il job è configurato per eseguirsi **ogni giorno alle 10:00**. Per modificare l'orario, edita il file plist:

```xml
<key>StartCalendarInterval</key>
<dict>
    <key>Hour</key>
    <integer>10</integer>  <!-- Modifica qui -->
    <key>Minute</key>
    <integer>0</integer>   <!-- Modifica qui -->
</dict>
```

Lo script `cron/cron_ls.sh` gestisce automaticamente:
- Verifica connessione internet con retry
- Prevenzione sleep durante esecuzione (`caffeinate`)
- Retry automatico in caso di errori
- Notifiche macOS in caso di successo/errore
- Logging dettagliato

## 🔧 Configurazione Avanzata

### Personalizzazione Criteri di Ricerca

Modifica `main.py` per personalizzare:

- **Località**: Modifica la lista `locations` (riga ~44)
- **Termini di ricerca**: Modifica `jobspy_search_term` (riga ~61)
- **Filtri temporali**: Modifica `hours_old` e `results_wanted` nelle chiamate di scraping

### Personalizzazione Profilo LLM

Modifica `scrapers/llm.py` per personalizzare:

- **Profilo professionale**: Modifica `SYSTEM_INSTRUCTIONS` (riga ~79)
- **Preferenze**: Aggiorna la sezione "PREFERENZE" nelle istruzioni di sistema
- **Criteri di valutazione**: Modifica i pesi nella funzione `_calculate_final_score` (riga ~133)

### Pulizia Automatica Database

Lo script `run_scrape_and_sync.py` esegue automaticamente la pulizia del database rimuovendo:

1. Job con score basso (≤5) più vecchi di 7 giorni
2. Job qualsiasi più vecchi di 30 giorni (esclusi quelli con `applied=true`)

Configura tramite variabili d'ambiente:
```bash
export LOW_SCORE_RETENTION_DAYS=7
export ABSOLUTE_RETENTION_DAYS=30
export SCORE_THRESHOLD=5
```

## 📊 Architettura

### Workflow

```
┌─────────────────┐
│   Scraping      │  JobSpy (LinkedIn, Indeed, Glassdoor)
│   +             │  HiringCafe (API diretta)
│   HiringCafe    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Combinazione   │  Deduplicazione per ID
│  e Deduplicazione│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Identificazione │  Nuovi job + job con score NULL
│ Job da Arricchire│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Arricchimento   │  Google Gemini Flash Lite
│      LLM        │  Rate limiting automatico
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Upsert DB     │  SQLite con preservazione flag utente
│   (SQLite)      │
└─────────────────┘
```

### Database Schema

Il database SQLite (`storage/jobs.db`) contiene una tabella `jobs` con:

- **Colonne di scraping**: Tutti i campi raccolti dalle piattaforme (title, company, location, description, ecc.)
- **Colonne LLM**: `llm_score`, `llm_score_competenze`, `llm_score_azienda`, `llm_score_stipendio`, `llm_score_località`, `llm_score_crescita`, `llm_motivazione`, `llm_match_competenze`
- **Flag utente**: `viewed`, `interested`, `applied`, `viewed_at`, `interested_at`, `applied_at`, `notes`
- **Metadati**: `scraping_date`, `id` (chiave primaria)

### Rate Limiting

Il sistema gestisce automaticamente il rate limiting per l'API Gemini:
- **FREE_GEMINI_API_KEY**: 15 richieste/minuto
- **GEMINI_API_KEY**: Nessun limite (rate limiting disabilitato)

## 🛠️ Sviluppo

### Aggiungere una Nuova Fonte di Scraping

1. Crea un nuovo modulo in `scrapers/`
2. Implementa una funzione che restituisce un DataFrame con le colonne standard (vedi `scrapers/utils.py::get_expected_columns`)
3. Importa e integra in `main.py`

### Migrazione Database

Per aggiungere nuove colonne al database:

```bash
python -m scripts.migrate_db
```

Oppure modifica direttamente `scripts/migrate_db.py` per aggiungere le colonne desiderate.

## 🐛 Troubleshooting

### Problema: API Key non riconosciuta

Assicurati di aver esportato la variabile d'ambiente o creato il file `.env` nella root del progetto.

### Problema: Rate limiting eccessivo

Se usi `FREE_GEMINI_API_KEY`, il sistema applica automaticamente 15 req/min. Considera di passare a `GEMINI_API_KEY` per rate più alti.

### Problema: Database locked

Il database potrebbe essere bloccato da un'altra istanza. Assicurati che non ci siano altri processi in esecuzione.

### Problema: Scraping fallisce

Verifica la connessione internet e controlla i log in `storage/launchd.stderr.log` per dettagli sull'errore.

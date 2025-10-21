# ListScraper
**ListScraper** è un job scraper intelligente che raccoglie automaticamente offerte di lavoro da piattaforme come LinkedIn, Indeed, Glassdoor e HiringCafe, analizzandole tramite un Large Language Model per identificare quelle più rilevanti in base alle preferenze e competenze dell'utente.

## Descrizione del Progetto

ListScraper automatizza il processo di ricerca del lavoro attraverso un sistema in due fasi:

1) Il sistema esegue web scraping su diverse piattaforme di recruitment usando la libreria JobSpy, raccogliendo dati strutturati sulle offerte di lavoro disponibili. Le informazioni estratte includono titolo della posizione, azienda, località, descrizione del lavoro e altri dettagli rilevanti. I dati vengono salvati in formato CSV per facilitare l'elaborazione successiva.
2) Ogni riga del CSV viene processata da un Large Language Model che analizza la job description e altre informazioni pertinenti. L'LLM valuta se l'offerta corrisponde alle preferenze personali e alle competenze specificate, filtrando automaticamente le opportunità più rilevanti.

Per gestire il CSV di grandi dimensioni contenente le offerte raccolte, il progetto include un layer SQLite con import a chunk, paginazione e flag utente (visionato/interessato/applicato).

### 1) Sincronizza il CSV in SQLite

```bash
python -m storage.sync_csv_to_sqlite --csv /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.csv \
  --db /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.db --chunksize 3000
```

L'import è idempotente: usa `id` come chiave e fa upsert preservando i flag utente.

### 2) Consulta e aggiorna via CLI

Lista paginata e ordinata (default: score decrescente):

```bash
python -m storage.cli list --db /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.db \
  --page 1 --page-size 50 --order-by llm_score --order-dir desc
```

Solo non visionati:

```bash
python -m storage.cli list --db /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.db --only-unviewed
```

Segna viewed/applied e aggiungi note:

```bash
python -m storage.cli set --db /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.db \
  --id <JOB_ID> --viewed true --note "interessante"

python -m storage.cli set --db /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.db \
  --id <JOB_ID> --applied true
```

Opzioni utili per l'ordinamento: `--order-by llm_score|date_posted|company|title|scraping_date` con `--order-dir asc|desc`.

### Alternative e varianti

- DuckDB: eccellente per query analitiche e formato colonnare; può leggere Parquet direttamente (`SELECT * FROM 'jobs.parquet' LIMIT ...`). Se preferisci evitare uno step di import, valuta un export diretto a Parquet e query DuckDB on-the-fly.
- Parquet + Polars: lettura lazy/paginata efficiente; ottimo se vuoi trasformazioni complesse in locale.
- SQLite resta la scelta più semplice per flagging transazionale (viewed/applied) e un'interfaccia CLI snella.

## Setup rapido

```bash
pip install -r requirements.txt
```

Se usi l’arricchimento LLM, esporta la chiave:
```bash
export FREE_GEMINI_API_KEY=...
```

### Esecuzione giornaliera (scraping → CSV → DB)

```bash
python -m scripts.run_scrape_and_sync \
  --csv /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.csv \
  --db /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.db \
  --chunksize 3000
```

### Consulta e aggiorna dal terminale

```bash
# Lista (paginazione e ordine per score desc)
python -m storage.cli list --db /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.db \
  --page 1 --page-size 50 --order-by llm_score --order-dir desc

# Solo non visionati
python -m storage.cli list --db /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.db --only-unviewed

# Segna viewed/applied/notes
python -m storage.cli set --db /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.db \
  --id <JOB_ID> --viewed true --applied false --note "interessante"
```

Parametri principali: `--db`, `--page`, `--page-size`, `--order-by` (es. `llm_score|date_posted|company|title|scraping_date`), `--order-dir` (`asc|desc`).

### API + Frontend minimale

Avvia server:
```bash
LISTSCRAPER_DB=/Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.db \
  uvicorn api.server:app --host 127.0.0.1 --port 8000
```
Apri `http://127.0.0.1:8000/` per la pagina con paginazione/ordinamento e toggle viewed/applied/notes.

### Solo sync CSV → DB (opzionale)

```bash
python -m storage.sync_csv_to_sqlite \
  --csv /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.csv \
  --db /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.db \
  --chunksize 3000
```

## Esecuzione giornaliera automatica (scraping + sync)

Per separare scraping e consultazione, usa lo script che lancia scraping e poi sincronizza il DB:

```bash
python -m scripts.run_scrape_and_sync --csv /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.csv \
  --db /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.db --chunksize 3000
```

### macOS (launchd)

1. Crea un plist in `~/Library/LaunchAgents/com.davidelandolfi.listscraper.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.davidelandolfi.listscraper</string>
    
    <key>ProgramArguments</key>
    <array>
        <string>/Users/davidelandolfi/cron_ls.sh</string>
    </array>
    
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>10</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    
    <key>WakeSystem</key>
    <true/>
    
    <key>StandardOutPath</key>
    <string>/Users/davidelandolfi/PyProjects/ListScraper/storage/launchd.out</string>
    
    <key>StandardErrorPath</key>
    <string>/Users/davidelandolfi/PyProjects/ListScraper/storage/launchd.err</string>
    
    <key>EnvironmentVariables</key>
    <dict>
        <key>FREE_GEMINI_API_KEY</key>
        <string>AIzaSyCfrSMHbHTIhVu6ZZaAw1irlBvk7_lczqM</string>
        <key>TQDM_DISABLE</key>
        <string>1</string>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin</string>
    </dict>
</dict>
</plist>
```

2. Carica il job:

```bash
launchctl load ~/Library/LaunchAgents/com.davidelandolfi.listscraper.plist
launchctl start com.listscraper.daily
```

### File cron di riferimento

```bash
#!/bin/bash

# Imposta variabili d'ambiente
export FREE_GEMINI_API_KEY="INSERT_KEY"
export TQDM_DISABLE="1"
export PATH="/usr/local/bin:/usr/bin:/bin"

# Cambia nella directory del progetto
cd /Users/davidelandolfi/PyProjects/ListScraper

# Esegui lo script principale
caffeinate -s /Users/davidelandolfi/PyProjects/ListScraper/.venv/bin/python -m scripts.run_scrape_and_sync \
  --csv /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.csv \
  --db /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.db \
  --chunksize 3000

echo "Script completato: $(date)" >> /Users/davidelandolfi/PyProjects/ListScraper/storage/completion.log

```


# ListScraper
**ListScraper** è un job scraper intelligente che raccoglie automaticamente offerte di lavoro da piattaforme come LinkedIn, Indeed, Glassdoor e HiringCafe, analizzandole tramite un Large Language Model per identificare quelle più rilevanti in base alle preferenze e competenze dell'utente.

## Descrizione del Progetto

ListScraper automatizza il processo di ricerca del lavoro attraverso un sistema in due fasi:

1) Il sistema esegue web scraping su diverse piattaforme di recruitment usando la libreria JobSpy, raccogliendo dati strutturati sulle offerte di lavoro disponibili. Le informazioni estratte includono titolo della posizione, azienda, località, descrizione del lavoro e altri dettagli rilevanti. I dati vengono salvati direttamente nel database SQLite durante lo scraping.
2) Ogni riga viene processata da un Large Language Model che analizza la job description e altre informazioni pertinenti. L'LLM valuta se l'offerta corrisponde alle preferenze personali e alle competenze specificate, filtrando automaticamente le opportunità più rilevanti.

Il progetto utilizza un'architettura **DB-first**: lo scraping scrive direttamente nel database SQLite con upsert incrementale, paginazione e flag utente (visionato/interessato/applicato).

## Architettura

**Workflow attuale**: Scraping → Upsert diretto DB

Il sistema esegue lo scraping da multiple fonti, combina e deduplica i risultati, e scrive direttamente nel database SQLite tramite upsert incrementale. I flag utente (viewed/interested/applied/notes) vengono preservati durante gli aggiornamenti.

### Consulta e aggiorna via CLI

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


## Setup rapido

```bash
pip install -r requirements.txt
```

Se usi l’arricchimento LLM, esporta la chiave:
```bash
export FREE_GEMINI_API_KEY=...
```

### Esecuzione giornaliera (scraping diretto nel DB)

```bash
python -m scripts.run_scrape_and_sync
```

Oppure direttamente:

```bash
python main.py
```

Lo script `main.py` scrive direttamente nel database SQLite (`storage/jobs.db`).

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

## Esecuzione giornaliera automatica

Lo script `run_scrape_and_sync` esegue lo scraping che aggiorna direttamente il database:

```bash
python -m scripts.run_scrape_and_sync
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

# Esegui lo script principale (scraping diretto nel DB)
	caffeinate -s /Users/davidelandolfi/PyProjects/ListScraper/.venv/bin/python -m scripts.run_scrape_and_sync

echo "Script completato: $(date)" >> /Users/davidelandolfi/PyProjects/ListScraper/storage/completion.log

```

## TODO
- Spostare i log nella cartella cron
- Far funzionare la ricerca su altri paesi
#!/bin/bash

# Script schedulato tramite launchd che esegue ListScraper ogni giorno alle 10:00.
# Gestisce sleep/risveglio (anche a batteria), riconnessioni di rete e logging.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_BIN="$PROJECT_DIR/.venv/bin/python"
LOG_FILE="$PROJECT_DIR/storage/completion.log"
ENV_FILE="$PROJECT_DIR/.env"
MAX_NET_WAIT_SECONDS=$((6 * 60 * 60))  # 6 ore
INITIAL_BACKOFF=10                      # Parte da 10 secondi
MAX_BACKOFF=300                         # Backoff massimo: 5 minuti
MAX_RETRY_ATTEMPTS=5                    # Numero massimo di tentativi scraper

PYTHON_PID=""
CAFFEINATE_PID=""

log() {
    local message="$1"
    mkdir -p "$(dirname "$LOG_FILE")"
    echo "$(date '+%Y-%m-%d %H:%M:%S') [cron_ls] $message" >> "$LOG_FILE"
}

cleanup() {
    log "Ricevuto segnale, avvio cleanup..."
    if [[ -n "$CAFFEINATE_PID" ]]; then
        kill -TERM "$CAFFEINATE_PID" 2>/dev/null || true
        wait "$CAFFEINATE_PID" 2>/dev/null || true
    fi

    if [[ -n "$PYTHON_PID" ]]; then
        kill -TERM "$PYTHON_PID" 2>/dev/null || true
        wait "$PYTHON_PID" 2>/dev/null || true
    fi
    log "Cleanup completato."
    exit 0
}

trap cleanup TERM INT

require_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        log "Comando richiesto mancante: $1"
        exit 1
    fi
}

# Health check con endpoint multipli per evitare falsi negativi
check_internet() {
    # Cloudflare DNS
    if curl -Is --max-time 5 https://1.1.1.1 >/dev/null 2>&1; then
        return 0
    fi
    
    # Google DNS
    if curl -Is --max-time 5 https://8.8.8.8 >/dev/null 2>&1; then
        return 0
    fi
    
    # Fallback su dominio pubblico
    if curl -Is --max-time 5 https://www.google.com >/dev/null 2>&1; then
        return 0
    fi
    
    return 1
}

# Attesa connessione con exponential backoff
wait_for_internet() {
    local waited=0
    local backoff=$INITIAL_BACKOFF
    local check_count=0
    
    until check_internet; do
        check_count=$((check_count + 1))
        
        if (( waited >= MAX_NET_WAIT_SECONDS )); then
            log "Connessione assente da più di $((MAX_NET_WAIT_SECONDS/3600)) ore ($check_count controlli effettuati)."
            notify "Scraper Fallito" "Connessione non disponibile dopo $((MAX_NET_WAIT_SECONDS/3600)) ore"
            return 1
        fi
        
        log "Connessione assente (tentativo #$check_count), nuovo controllo tra ${backoff}s..."
        sleep "$backoff"
        waited=$((waited + backoff))
        
        # Exponential backoff con cap al massimo
        backoff=$((backoff * 2))
        if (( backoff > MAX_BACKOFF )); then
            backoff=$MAX_BACKOFF
        fi
    done
    
    log "Connessione ripristinata dopo ${waited}s ($check_count controlli)"
    return 0
}

# Notifiche native macOS
notify() {
    local title="$1"
    local message="$2"
    osascript -e "display notification \"$message\" with title \"$title\" sound name \"Basso\"" 2>/dev/null || true
}

run_scraper() {
    set +e
    "$PYTHON_BIN" -m scripts.run_scrape_and_sync &
    PYTHON_PID=$!

    # Previene sleep durante l'esecuzione
    caffeinate -dims -w "$PYTHON_PID" &
    CAFFEINATE_PID=$!

    wait "$PYTHON_PID"
    local exit_code=$?

    wait "$CAFFEINATE_PID" 2>/dev/null || true
    CAFFEINATE_PID=""
    PYTHON_PID=""
    set -e
    return "$exit_code"
}

main() {
    require_command curl
    require_command caffeinate
    require_command osascript

    if [[ ! -x "$PYTHON_BIN" ]]; then
        log "Python virtualenv non trovato in $PYTHON_BIN"
        notify "Scraper Errore" "Virtualenv Python non trovato"
        exit 1
    fi

    if [[ -f "$ENV_FILE" ]]; then
        set -a
        # shellcheck disable=SC1090
        source "$ENV_FILE"
        set +a
    fi

    export PATH="/usr/local/bin:/usr/bin:/bin"
    export TQDM_DISABLE="1"

    if ! cd "$PROJECT_DIR"; then
        log "Impossibile accedere a $PROJECT_DIR"
        notify "Scraper Errore" "Directory progetto inaccessibile"
        exit 1
    fi

    log "=== Esecuzione programmata avviata ==="

    # Verifica iniziale connessione
    if ! wait_for_internet; then
        log "Connessione non disponibile all'avvio, interruzione del job."
        exit 70
    fi

    # Loop di retry con metriche
    local attempt=1
    local start_time=$(date +%s)
    
    while (( attempt <= MAX_RETRY_ATTEMPTS )); do
        log ">>> Tentativo $attempt/$MAX_RETRY_ATTEMPTS: avvio scripts.run_scrape_and_sync"
        
        if run_scraper; then
            local end_time=$(date +%s)
            local duration=$((end_time - start_time))
            log "=== Job completato con successo in ${duration}s dopo $attempt tentativo(i) ==="
            notify "Scraper Completato" "Job eseguito con successo in $((duration/60)) minuti"
            break
        fi

        local exit_code=$?
        log "Job terminato con exit code $exit_code al tentativo $attempt"
        
        # Verifica se è un problema di rete
        if ! check_internet; then
            log "Job interrotto per mancanza di connessione; attendo ripristino..."
            
            if wait_for_internet; then
                attempt=$((attempt + 1))
                if (( attempt <= MAX_RETRY_ATTEMPTS )); then
                    log "Connessione ripristinata, riprovo (tentativo $attempt/$MAX_RETRY_ATTEMPTS)"
                    continue
                else
                    log "Raggiunto limite massimo di tentativi ($MAX_RETRY_ATTEMPTS)"
                    notify "Scraper Fallito" "Troppi tentativi dopo problemi di rete"
                    exit "$exit_code"
                fi
            fi
            
            log "Connessione non tornata disponibile, esco con codice $exit_code."
            exit "$exit_code"
        fi

        # Errore non di rete, interrompe immediatamente
        log "Job terminato con errore (exit $exit_code). Nessun problema di rete rilevato."
        notify "Scraper Errore" "Job fallito con exit code $exit_code (errore applicativo)"
        exit "$exit_code"
    done
}

main "$@"

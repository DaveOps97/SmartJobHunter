'''
Script utile per aggiungere nuove colonne nel db
'''

import sqlite3
import os

# Percorso relativo alla root del progetto
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "storage", "jobs.db")

def migrate():
    if not os.path.exists(DB_PATH):
        print(f"Database non trovato: {DB_PATH}")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Controlla quali colonne esistono
    cur.execute("PRAGMA table_info(jobs)")
    existing_columns = {row[1] for row in cur.fetchall()}
    
    # Aggiungi colonne mancanti
    columns_to_add = [
        ("viewed_at", "TEXT"),
        ("interested_at", "TEXT"),
        ("applied_at", "TEXT"),
    ]
    
    for col_name, col_type in columns_to_add:
        if col_name not in existing_columns:
            print(f"Aggiungendo colonna: {col_name}")
            cur.execute(f"ALTER TABLE jobs ADD COLUMN `{col_name}` {col_type}")
        else:
            print(f"Colonna già esistente: {col_name}")
    
    conn.commit()
    conn.close()
    print("Migrazione completata!")

if __name__ == "__main__":
    migrate()

# ListScraper
**ListScraper** è un job scraper intelligente che raccoglie automaticamente offerte di lavoro da piattaforme come LinkedIn, Indeed, Glassdoor e HiringCafe, analizzandole tramite un Large Language Model per identificare quelle più rilevanti in base alle preferenze e competenze dell'utente.

## Descrizione del Progetto

ListScraper automatizza il processo di ricerca del lavoro attraverso un sistema in due fasi:

1) Il sistema esegue web scraping su diverse piattaforme di recruitment usando la libreria JobSpy, raccogliendo dati strutturati sulle offerte di lavoro disponibili. Le informazioni estratte includono titolo della posizione, azienda, località, descrizione del lavoro e altri dettagli rilevanti. I dati vengono salvati in formato CSV per facilitare l'elaborazione successiva.
2) Ogni riga del CSV viene processata da un Large Language Model che analizza la job description e altre informazioni pertinenti. L'LLM valuta se l'offerta corrisponde alle preferenze personali e alle competenze specificate, filtrando automaticamente le opportunità più rilevanti.

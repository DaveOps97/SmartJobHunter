"""
FastAPI per consultazione e flagging delle offerte.

Endpoint principali:
- GET  /health
- GET  /jobs               (paginazione/ordinamento, filtro per mode)
- POST /jobs/{job_id}/flags  (aggiorna viewed/interested/applied/notes)

Configurazione DB:
- Env var LISTSCRAPER_DB (default: /Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.db)

Esecuzione:
  uvicorn api.server:app --host 127.0.0.1 --port 8000
"""

from __future__ import annotations

import os
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from storage.sqlite_db import query_jobs, set_job_flags


DEFAULT_DB = "/Users/davidelandolfi/PyProjects/ListScraper/storage/jobs.db"


def get_db_path() -> str:
    return os.getenv("LISTSCRAPER_DB", DEFAULT_DB)


app = FastAPI(title="ListScraper API", version="1.0.0")


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok"}


@app.get("/jobs")
def list_jobs(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    order_by: str = Query("llm_score"),
    order_dir: str = Query("DESC"),
    mode: str = Query("not_viewed"),
):
    """
    Elenca i job con paginazione e filtri.
    
    Args:
        mode: filtra per stato:
            - "not_viewed": nessuna flag attiva
            - "viewed": solo viewed=true
            - "interested": interested=true
            - "applied": applied=true
    """
    try:
        rows, total_rows, total_pages = query_jobs(
            db_path=get_db_path(),
            page=page,
            page_size=page_size,
            order_by=order_by,
            order_dir=order_dir,
            mode=mode,
        )
        return {"rows": rows, "total_rows": total_rows, "total_pages": total_pages, "page": page}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class FlagsIn(BaseModel):
    viewed: Optional[bool] = Field(default=None)
    interested: Optional[bool] = Field(default=None)
    applied: Optional[bool] = Field(default=None)
    note: Optional[str] = Field(default=None)


@app.post("/jobs/{job_id}/flags")
def update_flags(job_id: str, body: FlagsIn):
    """Aggiorna le flag utente per un job specifico."""
    try:
        set_job_flags(
            db_path=get_db_path(),
            job_id=job_id,
            viewed=body.viewed,
            interested=body.interested,
            applied=body.applied,
            note=body.note,
        )
        return {"status": "ok", "job_id": job_id}  # Conferma con job_id
    except Exception as e:
        import traceback
        traceback.print_exc()  # Log completo dell'errore
        raise HTTPException(status_code=400, detail=str(e))



@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return '''<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>ListScraper</title>
    <style>
      body { 
        font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; 
        margin: 24px; 
        background-color: #2b2b2b;
        color: #e0e0e0;
      }
      header { display:flex; gap:30px; align-items:center; flex-wrap:wrap; margin-bottom: 16px; }
      input, select, button { 
        padding:8px; 
        background-color: #3a3a3a;
        color: #e0e0e0;
        border: 1px solid #555;
        border-radius: 4px;
      }
      button:hover { background-color: #4a4a4a; cursor: pointer; }
      table { width:100%; border-collapse: collapse; background-color: #333; }
      th, td { text-align:left; padding:8px; border-bottom:1px solid #444; }
      th { cursor:pointer; background-color: #3a3a3a; }
      tr:hover { background-color: #3a3a3a; }
      a { color: #5ca9ff; }
      a:visited { color: #9d7cff; }
      .meta { color:#999; font-size:12px; }
      input[type="text"] { background-color: #3a3a3a; color: #e0e0e0; }
    </style>
  </head>
<body>
  <header>
    <select id="orderBy">
      <option value="llm_score">llm_score</option>
      <option value="scraping_date">scraping_date</option>
      <option value="date_posted">date_posted</option>
      <option value="company">company</option>
      <option value="location">location</option>
      <option value="title">title</option>
    </select>
    <label>Dir <select id="orderDir"><option value="DESC">DESC</option><option value="ASC">ASC</option></select></label>
    <label>Table view <select id="mainFlagFilter">
      <option value="not_viewed">Not viewed</option>
      <option value="viewed">Viewed</option>
      <option value="interested">Interested</option>
      <option value="applied">Applied</option>
    </select></label>
    <button id="reload">Reload</button>
    <button id="copyInterestedUrls">Copy URLs of interested</button>
  </header>
  <div class="meta" id="meta"></div>
  <table><thead><tr>
    <th>score</th><th>title</th><th>company</th><th>location</th><th>date</th><th>scraping_date</th><th>url</th><th>motivazione</th><th>flag</th><th>note</th>
  </tr></thead><tbody id="rows"></tbody></table>
  <div style="margin-top:12px; display:flex; gap:8px; align-items:center;"><button id="prev">Prev</button><span id="pageInfo" class="meta"></span><button id="next">Next</button></div>
  <script>
    let page = 1;
    const pageSize = 50;
    const orderByEl = document.getElementById('orderBy');
    const orderDirEl = document.getElementById('orderDir');
    const mainFlagFilterEl = document.getElementById('mainFlagFilter');
    const rowsEl = document.getElementById('rows');
    const metaEl = document.getElementById('meta');
    const pageInfoEl = document.getElementById('pageInfo');
    function esc(x) {
      return String(x ?? '').replace(/[<>&"']/g, c => ({'<':'&lt;','>':'&gt;','&':'&amp;','"':'&quot;',"'":'&#039;'}[c]));
    }
    function tronc(x, max=100) {
      const s = String(x??'');
      return s.length > max ? esc(s.slice(0,max))+'…' : esc(s);
    }
    async function load() {
      const params = new URLSearchParams({
        page: String(page),
        page_size: String(pageSize),
        order_by: orderByEl.value,
        order_dir: orderDirEl.value,
        mode: document.getElementById('mainFlagFilter').value,
      });
      try {
        const res = await fetch('/jobs?' + params.toString());
        if (!res.ok) throw new Error(`HTTP ${res.status}: ${res.statusText}`);
        const data = await res.json();
        rowsEl.innerHTML = '';
        data.rows.forEach(r => {
          const flagVal = r.applied ? 'applied' : r.interested ? 'interested' : r.viewed ? 'viewed' : 'not_viewed';
          const tr = document.createElement('tr');
          tr.innerHTML = `
<td>${esc(r.llm_score)}</td>
<td>${tronc(r.title,50)}</td>
<td>${tronc(r.company,40)}</td>
<td>${tronc(r.location,40)}</td>
<td>${esc(r.date_posted)}</td>
<td>${esc(r.scraping_date)}</td>
<td>${r.job_url?`<a href="${esc(r.job_url)}" target="_blank">link</a>`:''}</td>
<td title="${esc(r.llm_motivazione)}">${tronc(r.llm_motivazione,80)}</td>
<td><select class="flag-select" data-id="${r.id}">
<option value="not_viewed" ${flagVal === 'not_viewed' ? 'selected' : ''}>Not viewed</option>
<option value="viewed" ${flagVal === 'viewed' ? 'selected' : ''}>Viewed</option>
<option value="interested" ${flagVal === 'interested' ? 'selected' : ''}>Interested</option>
<option value="applied" ${flagVal === 'applied' ? 'selected' : ''}>Applied</option>
</select></td>
<td><input type="text" value="${esc(r.notes)}" data-id="${r.id}" class="note" style="width:140px"/></td>
  `;
          rowsEl.appendChild(tr);
          tr.querySelector('td[title]').style.cursor = 'pointer';
          tr.querySelector('td[title]').onclick = function() {
            showMotivazione(this.getAttribute('title'));
          };
        });
        pageInfoEl.textContent = `Page ${data.page} / ${data.total_pages} — ${data.total_rows} rows`;
        metaEl.textContent = `order_by=${orderByEl.value} ${orderDirEl.value} | mode=${document.getElementById('mainFlagFilter').value}`;
        document.querySelectorAll('.flag-select').forEach(sel => {
          sel.onchange = async function(e) {
            const id = this.getAttribute('data-id');
            const val = this.value;
            
            const body = {
              viewed: val === 'viewed',
              interested: val === 'interested',
              applied: val === 'applied'
            };
            
            try {
              const response = await fetch(`/jobs/${id}/flags`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(body)
              });
              
              if (!response.ok) {
                const errorText = await response.text();
                console.error('Update failed:', errorText);
                alert(`Errore durante l'aggiornamento: ${errorText}`);
                load(); // Ricarica solo in caso di errore
              } else {
                // Successo: semplicemente logga, NON rimuovere la riga
                console.log(`Flag aggiornato per job ${id}: ${val}`);
                // La riga rimane visibile anche se non appartiene più al filtro corrente
              }
            } catch (error) {
              console.error('Network error:', error);
              alert(`Errore di rete: ${error.message}`);
            }
          };
        });
        document.querySelectorAll('.note').forEach(inp => inp.onchange = async e => {
          const id = inp.getAttribute('data-id');
          await fetch(`/jobs/${id}/flags`, {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({note:inp.value})});
        });
      } catch (error) {
        rowsEl.innerHTML = `<tr><td colspan="10" style="color:red;">Error: ${error.message}</td></tr>`;
        pageInfoEl.textContent = 'Error loading data';
        metaEl.textContent = 'Error';
      }
    }
    document.getElementById('reload').onclick = () => { page = 1; load(); };
    document.getElementById('prev').onclick = () => { if(page>1){page--;load();}};
    document.getElementById('next').onclick = () => {page++;load();};
    document.getElementById('mainFlagFilter').onchange = () => { page = 1; load(); };
    document.getElementById('copyInterestedUrls').onclick = async () => {
      // Trova tutte le righe con il dropdown impostato su "interested"
      const interestedSelects = Array.from(document.querySelectorAll('.flag-select'))
        .filter(sel => sel.value === 'interested');
      
      const urls = [];
      interestedSelects.forEach(sel => {
        const row = sel.closest('tr');
        const linkEl = row.querySelector('a[href]');
        if (linkEl) {
          urls.push(linkEl.href);
        }
      });
      
      if (urls.length > 0) { 
        await navigator.clipboard.writeText(urls.join('\\n')); 
        alert(`Copied ${urls.length} URLs to clipboard`); 
      } else { 
        alert('No viewed jobs with URLs in current view'); 
      }
    };
    load();
    // Modal per visualizzare motivazione completa
    function showMotivazione(text) {
      const overlay = document.createElement('div');
      overlay.style.cssText = `
        position:fixed;
        top:0;left:0;width:100%;height:100%;
        background:rgba(0,0,0,0.7);
        display:flex;
        align-items:center;
        justify-content:center;
        z-index:1000;
      `;
      
      const modal = document.createElement('div');
      modal.style.cssText = `
        background:#2a2a2a;
        color:#f0f0f0;
        padding:24px;
        border-radius:12px;
        max-width:1000px;
        max-height:80vh;
        overflow-y:auto;
        box-shadow:0 0 20px rgba(0,0,0,0.6);
      `;
      
      const sections = text.split(/(\*\*Punti Positivi \(\+\):\*\*|\*\*Punti Negativi \(-\):\*\*|\*\*Analisi Punteggi:\*\*)/);
      
      let currentBg = '';
      sections.forEach(section => {
        if (section.includes('Punti Positivi')) {
          currentBg = '#245c3a';
        } else if (section.includes('Punti Negativi')) {
          currentBg = '#5c2a2a';
        } else if (section.includes('Analisi Punteggi')) {
          currentBg = '#24465c';
        }

        if (section.trim() && !section.startsWith('**')) {
          const div = document.createElement('div');
          div.style.cssText = `
            background:${currentBg};
            padding:12px;
            margin:8px 0;
            border-radius:8px;
            white-space:pre-wrap;
          `;
          div.textContent = section.trim();
          modal.appendChild(div);
        } else if (section.startsWith('**')) {
          const title = document.createElement('h3');
          title.style.cssText = `
            margin:16px 0 8px 0;
            color:#fff;
            border-bottom:1px solid #444;
            padding-bottom:4px;
          `;
          title.textContent = section.replace(/\*\*/g, '');
          modal.appendChild(title);
        }
      });
      
      overlay.onclick = () => overlay.remove();
      overlay.appendChild(modal);
      modal.onclick = (e) => e.stopPropagation();
      document.body.appendChild(overlay);
    }

  </script>
</body></html>'''
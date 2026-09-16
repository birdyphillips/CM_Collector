"""
CM Collector API
================
FastAPI backend + browser UI for CM Collector.

    GET  /                        — dashboard UI (Start/Stop buttons, session table)
    POST /sessions/start          — start a collection session
    POST /sessions/{id}/stop      — stop a running session
    GET  /sessions                — list all sessions (JSON)
    GET  /sessions/{id}           — session detail (JSON)
    GET  /sessions/{id}/report    — generate + return HTML chart report

Usage:
    pip install fastapi uvicorn plotly
    uvicorn cm_collector_api:app --reload --port 8000
"""
import json
import logging
import os
import re
import shutil
import sqlite3
import threading
import uuid
from datetime import datetime
from typing import Optional

# Load .env from this file's directory before importing cm_collector so that
# _load_env() in cm_collector.py picks up the correct values regardless of
# the working directory uvicorn is launched from.
_HERE = os.path.dirname(os.path.abspath(__file__))
_ENV_PATH = os.path.join(_HERE, '.env')
if os.path.exists(_ENV_PATH):
    with open(_ENV_PATH) as _f:
        for _line in _f:
            _line = _line.strip()
            if not _line or _line.startswith('#') or '=' not in _line:
                continue
            _k, _, _v = _line.partition('=')
            os.environ[_k.strip()] = _v.strip()

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

import cm_collector as cc

# ---------------------------------------------------------------------------
# Logging — file + console
# ---------------------------------------------------------------------------
_LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cm_collector_api.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(_LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger('cm_collector_api')

app = FastAPI(title='CM Collector API', version='1.0')


@app.middleware('http')
async def _log_requests(request: Request, call_next):
    response = await call_next(request)
    # Skip noisy polling — only log non-GET or non-200, plus mutations and dashboard
    if request.method == 'GET' and response.status_code == 200 and request.url.path not in ('/', ):
        return response
    if request.url.path.startswith('/sessions') or request.url.path == '/':
        log.info('%s %s  →  %s', request.method, request.url.path, response.status_code)
    return response


@app.exception_handler(Exception)
async def _unhandled(request: Request, exc: Exception):
    log.error('Unhandled error on %s %s: %s', request.method, request.url.path, exc, exc_info=True)
    return JSONResponse(status_code=500, content={'detail': str(exc)})

# ---------------------------------------------------------------------------
# In-memory session store
# ---------------------------------------------------------------------------
_sessions: dict[str, dict] = {}

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class StartRequest(BaseModel):
    mac: str
    cmts_type: str = 'icmts'
    session_name: Optional[str] = None
    snmp_jumpserver: Optional[str] = None
    snmp_username: Optional[str] = None
    cmts_host: Optional[str] = None
    cmts_password: Optional[str] = None
    target_ip: Optional[str] = None
    icmts_target: Optional[str] = None
    kafka_broker: Optional[str] = None
    kafka_topic: Optional[str] = None
    snmp_poll_interval: int = cc.DEFAULT_SNMP_POLL_INTERVAL
    baseline_polls: int = 3

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _build_cfg(req: StartRequest) -> dict:
    mac_norm = cc._norm_mac(req.mac)
    if len(mac_norm) != 12:
        log.warning('Invalid MAC address received: %r', req.mac)
        raise HTTPException(400, 'Invalid MAC address — expected 12 hex digits (any separator)')
    mac_colon   = cc._mac_colon(mac_norm)
    mac_decimal = cc._mac_to_decimal(mac_norm)
    cmts_type   = req.cmts_type.lower()
    ts_str      = datetime.now().strftime('%Y%m%d_%H%M%S')

    safe_name   = re.sub(r'[^\w\-]', '_', (req.session_name or 'session')).strip('_')[:40]
    mac_tail    = mac_norm[-4:]
    folder_name = f'{ts_str}_{safe_name}_{mac_tail}_{cmts_type}'
    session_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        cc.DEFAULT_RESULTS_DIR,
        folder_name,
    )
    os.makedirs(session_dir, exist_ok=True)
    csv_paths   = {'us': os.path.join(session_dir, f'snmp_us_{mac_norm}_{ts_str}.csv')}
    if cmts_type == 'icmts':
        csv_paths['ds'] = os.path.join(session_dir, f'snmp_ds_{mac_norm}_{ts_str}.csv')
    kafka_csv = os.path.join(session_dir, f'kafka_{mac_norm}_{ts_str}.csv') if cmts_type == 'vcmts' else None

    return {
        'mac_norm':           mac_norm,
        'mac_colon':          mac_colon,
        'mac_decimal':        mac_decimal,
        'cmts_type':          cmts_type,
        'target_ip':          req.target_ip or '',
        'modem_community':    cc.DEFAULT_MODEM_COMMUNITY,
        'icmts_community':    cc.DEFAULT_ICMTS_COMMUNITY,
        'icmts_target':       req.icmts_target or (cc.DEFAULT_ICMTS_TARGET_IP if cmts_type == 'icmts' else ''),
        'kafka_broker':       req.kafka_broker or (cc.DEFAULT_KAFKA_BROKER if cmts_type == 'vcmts' else ''),
        'kafka_topic':        req.kafka_topic  or (cc.DEFAULT_KAFKA_TOPIC  if cmts_type == 'vcmts' else ''),
        'cmts_host':          req.cmts_host    or (cc.DEFAULT_VCMTS_IP or cc.DEFAULT_VCMTS_HOST if cmts_type == 'vcmts' else cc.DEFAULT_CMTS_HOST),
        'cmts_password':      req.cmts_password or cc.DEFAULT_TACACS_PASSWORD,
        'snmp_jumpserver':    req.snmp_jumpserver or cc.DEFAULT_SNMP_JUMPSERVER,
        'snmp_username':      req.snmp_username   or cc.DEFAULT_SNMP_USERNAME,
        'snmp_timeout':       cc.DEFAULT_SNMP_TIMEOUT,
        'snmp_retries':       cc.DEFAULT_SNMP_RETRIES,
        'snmp_poll_interval': req.snmp_poll_interval,
        'duration':           None,
        'session_dir':        session_dir,
        'csv_paths':          csv_paths,
        'kafka_csv':          kafka_csv,
        'modem_info':         None,
    }

# ---------------------------------------------------------------------------
# Dashboard UI
# ---------------------------------------------------------------------------
_UI = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>CM Collector</title>
<style>
  :root {
    --bg:      #0d1b2a;
    --panel:   #112240;
    --border:  #1e3a5f;
    --accent:  #1a73e8;
    --green:   #34a853;
    --red:     #ea4335;
    --text:    #e8eaed;
    --sub:     #8ab4f8;
    --radius:  8px;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; user-select: text; -webkit-user-select: text; }
  body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', sans-serif; min-height: 100vh; }

  header {
    background: var(--panel);
    border-bottom: 1px solid var(--border);
    padding: 18px 32px;
    display: flex;
    align-items: center;
    gap: 16px;
  }
  header .logo { font-size: 20px; font-weight: 700; color: var(--sub); letter-spacing: .5px; }
  header .sub  { font-size: 12px; color: #556; margin-top: 2px; }

  main { max-width: 960px; margin: 40px auto; padding: 0 24px; }

  .card {
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 28px 32px;
    margin-bottom: 28px;
  }
  .card h2 { font-size: 14px; font-weight: 600; color: var(--sub); text-transform: uppercase;
             letter-spacing: 1px; margin-bottom: 20px; }

  .form-row { display: flex; gap: 16px; flex-wrap: wrap; align-items: flex-end; }
  .field { display: flex; flex-direction: column; gap: 6px; }
  .field label { font-size: 12px; color: #8ab4f8; font-weight: 500; }
  .field input, .field select {
    background: #0d1b2a;
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--text);
    font-size: 14px;
    padding: 9px 14px;
    outline: none;
    transition: border-color .2s;
  }
  .field input:focus, .field select:focus { border-color: var(--accent); }
  .field input { width: 220px; font-family: monospace; letter-spacing: 1px; }
  .field select { width: 140px; cursor: pointer; }

  .btn {
    padding: 10px 28px;
    border: none;
    border-radius: 6px;
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
    transition: opacity .15s, transform .1s;
  }
  .btn:active { transform: scale(.97); }
  .btn:disabled { opacity: .4; cursor: not-allowed; }
  .btn-start { background: var(--green); color: #fff; }
  .btn-stop  { background: var(--red);   color: #fff; }

  #status-bar {
    font-size: 13px;
    padding: 10px 14px;
    border-radius: 6px;
    margin-top: 16px;
    display: none;
  }
  .status-ok  { background: #0d2a1a; border: 1px solid #34a853; color: #34a853; }
  .status-err { background: #2a0d0d; border: 1px solid #ea4335; color: #ea4335; }
  .status-inf { background: #0d1e2a; border: 1px solid #1a73e8; color: #8ab4f8; }

  table { width: 100%; border-collapse: collapse; font-size: 13px; table-layout: auto; }
  th { text-align: left; padding: 10px 14px; color: var(--sub); font-weight: 600;
       font-size: 11px; text-transform: uppercase; letter-spacing: .8px;
       border-bottom: 1px solid var(--border); white-space: nowrap; }
  td { padding: 11px 14px; border-bottom: 1px solid #0d1b2a; vertical-align: middle; word-break: break-word; }
  td:last-child { min-width: 160px; width: 1%; white-space: nowrap; }
  tr:hover td { background: #0d1e30; }

  .badge {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
  }
  .badge-running  { background: #0d2a1a; color: #34a853; border: 1px solid #34a853; }
  .badge-stopping { background: #2a1e0a; color: #fa7b17; border: 1px solid #fa7b17; }
  .badge-stopped  { background: #1a1a2a; color: #8ab4f8; border: 1px solid #445; }

  .mac-cell { font-family: monospace; color: var(--sub); }
  .action-cell { display: flex; gap: 6px; flex-wrap: wrap; align-items: center; }

  .btn-sm {
    padding: 5px 14px;
    font-size: 12px;
    border-radius: 5px;
    border: none;
    font-weight: 600;
    cursor: pointer;
    transition: opacity .15s;
  }
  .btn-sm:disabled { opacity: .35; cursor: not-allowed; }
  .btn-sm-stop   { background: var(--red);   color: #fff; }
  .btn-sm-report { background: var(--accent); color: #fff; }
  .btn-sm-delete { background: #2a1a1a; color: #ea4335; border: 1px solid #ea4335; }

  .id-cell { font-family: monospace; cursor: pointer; color: var(--sub); }
  .id-cell:hover { color: #fff; text-decoration: underline dotted; }
  .row-error { font-size: 11px; color: #ea4335; margin-top: 4px; }
  .row-has-error td { border-left: 2px solid #ea4335; }
</style>
</head>
<body>

<header>
  <div>
    <div class="logo">⬡ CM Collector</div>
    <div class="sub">Spectrum — Access Engineering  |  LLD Telemetry Collection</div>
  </div>
</header>

<main>

  <!-- Start form -->
  <div class="card">
    <h2>New Collection Session</h2>
    <div class="form-row">
      <div class="field">
        <label>Modem MAC Address</label>
        <input id="mac-input" type="text" placeholder="0cb9.3764.3ab0 or xx:xx:xx:xx:xx:xx" maxlength="20" spellcheck="false">
      </div>
      <div class="field">
        <label>CMTS Type</label>
        <select id="cmts-select">
          <option value="icmts">iCMTS</option>
          <option value="vcmts">vCMTS</option>
        </select>
      </div>
      <div class="field">
        <label>Session Name</label>
        <input id="session-name-input" type="text" placeholder="e.g. Netflix L4S Test" style="width:240px;font-family:inherit;letter-spacing:normal">
      </div>
      <div class="field">
        <label>&nbsp;</label>
        <div style="display:flex;gap:10px;">
          <button class="btn btn-start" id="btn-start" onclick="startSession()">▶ Start</button>
          <button class="btn btn-stop" id="btn-stop" onclick="stopActive()" disabled>■ Stop</button>
        </div>
      </div>
    </div>
    <div id="status-bar"></div>
  </div>

  <!-- Sessions table -->
  <div class="card">
    <h2>Sessions</h2>
    <table>
      <thead>
        <tr>
          <th>ID</th>
          <th>MAC</th>
          <th>Type</th>
          <th>Session Name</th>
          <th>Started</th>
          <th>Elapsed</th>
          <th>Status</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody id="session-tbody">
        <tr id="empty-row"><td colspan="8">No sessions yet</td></tr>
      </tbody>
    </table>
  </div>

</main>

<script>
let activeSessionId = null;

function setStatus(msg, type='inf') {
  const bar = document.getElementById('status-bar');
  bar.textContent = msg;
  bar.className = `status-${type}`;
  bar.style.display = 'block';
}

async function startSession() {
  const mac  = document.getElementById('mac-input').value.trim();
  const type = document.getElementById('cmts-select').value;
  if (!mac) { setStatus('Enter a modem MAC address.', 'err'); return; }

  document.getElementById('btn-start').disabled = true;
  setStatus('Starting session — collecting baseline polls…', 'inf');

  try {
    const name = document.getElementById('session-name-input').value.trim();
    const res = await fetch('/sessions/start', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ mac, cmts_type: type, session_name: name || null })
    });
    const data = await res.json();
    if (!res.ok) { setStatus(`Error: ${data.detail || res.statusText}`, 'err'); return; }

    activeSessionId = data.session_id;
    document.getElementById('btn-stop').disabled = false;
    setStatus('Collecting baseline polls — please wait…', 'inf');
    await refreshSessions();
  } catch(e) {
    setStatus(`Request failed: ${e}`, 'err');
  } finally {
    document.getElementById('btn-start').disabled = false;
  }
}

async function stopActive() {
  if (!activeSessionId) return;
  await stopSession(activeSessionId);
  document.getElementById('btn-stop').disabled = true;
  activeSessionId = null;
}

async function stopSession(id) {
  setStatus(`Stopping session ${id} — collecting 3 cooldown polls…`, 'inf');
  try {
    const res  = await fetch(`/sessions/${id}/stop`, { method: 'POST' });
    const data = await res.json();
    if (!res.ok) { setStatus(`Error stopping session: ${data.detail || res.statusText}`, 'err'); return; }
    await refreshSessions();
  } catch(e) {
    setStatus(`Stop failed: ${e}`, 'err');
  }
}

async function deleteSession(id) {
  if (!confirm(`Delete session ${id}? This cannot be undone.`)) return;
  try {
    const res  = await fetch(`/sessions/${id}`, { method: 'DELETE' });
    const data = await res.json();
    if (!res.ok) { setStatus(`Delete failed: ${data.detail || res.statusText}`, 'err'); return; }
    if (activeSessionId === id) { activeSessionId = null; document.getElementById('btn-stop').disabled = true; }
    setStatus(`Session ${id} deleted.`, 'ok');
    await refreshSessions();
  } catch(e) {
    setStatus(`Delete failed: ${e}`, 'err');
  }
}

async function refreshSessions() {
  const res      = await fetch('/sessions');
  const sessions = await res.json();
  const tbody    = document.getElementById('session-tbody');

  // Update status bar when active session finishes cooldown
  if (activeSessionId) {
    const active = sessions.find(s => s.id === activeSessionId);
    if (active && active.status === 'stopped') {
      setStatus(`✔ Collection complete — session ${activeSessionId} ready for report.`, 'ok');
      activeSessionId = null;
      document.getElementById('btn-stop').disabled = true;
    }
  }

  tbody.innerHTML = '';

  if (!sessions.length) {
    tbody.innerHTML = '<tr id="empty-row"><td colspan="8">No sessions yet</td></tr>';
    return;
  }

  sessions.slice().reverse().forEach(s => {
    const running  = s.status === 'running';
    const stopping = s.status === 'stopping';
    const started  = s.started_at ? s.started_at.replace('T',' ').slice(0,16) + ' UTC' : '—';
    const elapsedSec = s.started_at
      ? Math.floor(((running || stopping ? Date.now() : new Date((s.stopped_at || s.started_at) + 'Z')) - new Date(s.started_at + 'Z')) / 1000)
      : 0;
    const maxSec     = 600;
    const remSec     = Math.max(0, maxSec - elapsedSec);
    const elapsedStr = (running || stopping)
      ? `${Math.floor(elapsedSec/60)}m ${elapsedSec%60}s <span style="color:#556;font-size:11px">(${Math.floor(remSec/60)}m ${remSec%60}s left)</span>`
      : `${Math.floor(elapsedSec/60)}m ${elapsedSec%60}s`;
    const phase = s.phase || '';
    const badge   = phase === 'baseline'
      ? '<span class="badge badge-stopping">⏳ Baseline</span>'
      : (running || stopping) && phase === 'ready'
        ? '<span class="badge badge-running">✅ Ready — Start Your Test</span>'
        : running
          ? '<span class="badge badge-running">● Running</span>'
          : stopping
            ? '<span class="badge badge-stopping">⏳ Cooldown</span>'
            : '<span class="badge badge-stopped">✔ Collection Complete</span>';
    const errBadge = s.last_error
      ? `<div class="row-error">⚠ ${s.last_error}</div>` : '';
    const delBtn   = `<button class="btn-sm btn-sm-delete" onclick="deleteSession('${s.id}')">Delete</button>`;
    const stopBtn  = `<button class="btn-sm btn-sm-stop" ${(running && !stopping) ? '' : 'disabled'} onclick="stopSession('${s.id}')">Stop</button>`;
    const rptBtn  = '';
    tbody.innerHTML += `
      <tr class="${s.last_error ? 'row-has-error' : ''}">
        <td><span class="id-cell" title="Click to copy" onclick="copyId(this,'${s.id}')">${s.id}</span></td>
        <td class="mac-cell">${s.mac || '—'}</td>
        <td>${(s.cmts_type||'').toUpperCase()}</td>
        <td>${s.session_name || '—'}</td>
        <td>${started}</td>
        <td>${elapsedStr}</td>
        <td>${badge}${errBadge}</td>
        <td><div class="action-cell">${stopBtn}${rptBtn}${delBtn}</div></td>
      </tr>`;
  });
}

function copyId(el, id) {
  navigator.clipboard.writeText(id).then(() => {
    const orig = el.textContent;
    el.textContent = 'Copied!';
    el.style.color = 'var(--green)';
    setTimeout(() => { el.textContent = orig; el.style.color = ''; }, 1200);
  });
}

// Accept any MAC format — strip separators, validate hex only, leave as-is
document.getElementById('mac-input').addEventListener('input', function() {
  // Allow hex digits plus common separators (: - .)
  this.value = this.value.replace(/[^0-9a-fA-F:.\\-]/g, '');
});

// Poll session list every 5s
refreshSessions();
setInterval(refreshSessions, 1000);
</script>
</body>
</html>"""

# ---------------------------------------------------------------------------
# SQLite session persistence
# ---------------------------------------------------------------------------
_DB_PATH = os.path.join(_HERE, 'sessions.db')
_SAFE_CFG_KEYS = {
    'mac_norm', 'mac_colon', 'mac_decimal', 'cmts_type', 'target_ip',
    'modem_community', 'icmts_community', 'icmts_target', 'kafka_broker',
    'kafka_topic', 'cmts_host', 'snmp_jumpserver', 'snmp_username',
    'snmp_timeout', 'snmp_retries', 'snmp_poll_interval', 'duration',
    'session_dir', 'csv_paths', 'kafka_csv',
}

def _db():
    con = sqlite3.connect(_DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def _db_init():
    with _db() as con:
        con.execute('''
            CREATE TABLE IF NOT EXISTS sessions (
                id           TEXT PRIMARY KEY,
                mac          TEXT,
                cmts_type    TEXT,
                status       TEXT,
                phase        TEXT,
                started_at   TEXT,
                stopped_at   TEXT,
                session_name TEXT,
                last_error   TEXT,
                cfg_json     TEXT
            )
        ''')
        # snmp_upstream_delta_rows — SNMP upstream with raw + delta columns
        con.execute('''
            CREATE TABLE IF NOT EXISTS snmp_upstream_delta_rows (
                session_id TEXT, captured_utc TEXT, poll_index INTEGER, phase TEXT,
                target_ip TEXT, target_label TEXT, cmts_type TEXT, sfid TEXT,
                sf_buffer_size TEXT,
                ps_scn TEXT, ps_priority TEXT, ps_max_rate TEXT,
                ps_max_burst TEXT, ps_max_concat_burst TEXT, ps_aqm_latency_target TEXT,
                ps_min_buffer TEXT, ps_target_buffer TEXT, ps_max_buffer TEXT,
                flow_pkts TEXT, flow_octets TEXT, flow_policed_drop TEXT,
                flow_policed_delay TEXT, flow_aqm_drop TEXT,
                lat_bin_scn TEXT, lat_aqm_target TEXT,
                lat_edge_bin1 TEXT, lat_edge_bin2 TEXT, lat_edge_bin3 TEXT,
                lat_edge_bin4 TEXT, lat_edge_bin5 TEXT, lat_edge_bin6 TEXT,
                lat_edge_bin7 TEXT, lat_edge_bin8 TEXT, lat_edge_bin9 TEXT,
                lat_edge_bin10 TEXT, lat_edge_bin11 TEXT, lat_edge_bin12 TEXT,
                lat_edge_bin13 TEXT, lat_edge_bin14 TEXT, lat_edge_bin15 TEXT,
                lat_max_usec TEXT, lat_updates TEXT,
                lat_bin1 TEXT, lat_bin2 TEXT, lat_bin3 TEXT, lat_bin4 TEXT,
                lat_bin5 TEXT, lat_bin6 TEXT, lat_bin7 TEXT, lat_bin8 TEXT,
                lat_bin9 TEXT, lat_bin10 TEXT, lat_bin11 TEXT, lat_bin12 TEXT,
                lat_bin13 TEXT, lat_bin14 TEXT, lat_bin15 TEXT, lat_bin16 TEXT,
                cong_sanctioned TEXT, cong_ect0 TEXT, cong_ect1 TEXT,
                cong_ce_marked TEXT, cong_arrived_ce TEXT,
                delta_flow_pkts TEXT, delta_flow_octets TEXT,
                delta_flow_policed_drop TEXT, delta_flow_policed_delay TEXT,
                delta_flow_aqm_drop TEXT, delta_lat_updates TEXT,
                delta_lat_bin1 TEXT, delta_lat_bin2 TEXT, delta_lat_bin3 TEXT,
                delta_lat_bin4 TEXT, delta_lat_bin5 TEXT, delta_lat_bin6 TEXT,
                delta_lat_bin7 TEXT, delta_lat_bin8 TEXT, delta_lat_bin9 TEXT,
                delta_lat_bin10 TEXT, delta_lat_bin11 TEXT, delta_lat_bin12 TEXT,
                delta_lat_bin13 TEXT, delta_lat_bin14 TEXT, delta_lat_bin15 TEXT,
                delta_lat_bin16 TEXT,
                delta_cong_sanctioned TEXT, delta_cong_ect0 TEXT, delta_cong_ect1 TEXT,
                delta_cong_ce_marked TEXT, delta_cong_arrived_ce TEXT
            )
        ''')
        # snmp_downstream_delta_rows — iCMTS DS SNMP with raw + delta columns
        con.execute('''
            CREATE TABLE IF NOT EXISTS snmp_downstream_delta_rows (
                session_id TEXT, captured_utc TEXT, poll_index INTEGER,
                target_ip TEXT, target_label TEXT, cmts_type TEXT, sfid TEXT,
                sf_buffer_size TEXT,
                ps_scn TEXT, ps_priority TEXT, ps_max_rate TEXT,
                ps_max_burst TEXT, ps_max_concat_burst TEXT, ps_aqm_latency_target TEXT,
                ps_min_buffer TEXT, ps_target_buffer TEXT, ps_max_buffer TEXT,
                flow_pkts TEXT, flow_octets TEXT, flow_policed_drop TEXT,
                flow_policed_delay TEXT, flow_aqm_drop TEXT,
                lat_bin_scn TEXT, lat_aqm_target TEXT,
                lat_edge_bin1 TEXT, lat_edge_bin2 TEXT, lat_edge_bin3 TEXT,
                lat_edge_bin4 TEXT, lat_edge_bin5 TEXT, lat_edge_bin6 TEXT,
                lat_edge_bin7 TEXT, lat_edge_bin8 TEXT, lat_edge_bin9 TEXT,
                lat_edge_bin10 TEXT, lat_edge_bin11 TEXT, lat_edge_bin12 TEXT,
                lat_edge_bin13 TEXT, lat_edge_bin14 TEXT, lat_edge_bin15 TEXT,
                lat_max_usec TEXT, lat_updates TEXT,
                lat_bin1 TEXT, lat_bin2 TEXT, lat_bin3 TEXT, lat_bin4 TEXT,
                lat_bin5 TEXT, lat_bin6 TEXT, lat_bin7 TEXT, lat_bin8 TEXT,
                lat_bin9 TEXT, lat_bin10 TEXT, lat_bin11 TEXT, lat_bin12 TEXT,
                lat_bin13 TEXT, lat_bin14 TEXT, lat_bin15 TEXT, lat_bin16 TEXT,
                cong_sanctioned TEXT, cong_ect0 TEXT, cong_ect1 TEXT,
                cong_ce_marked TEXT, cong_arrived_ce TEXT,
                delta_flow_pkts TEXT, delta_flow_octets TEXT,
                delta_flow_policed_drop TEXT, delta_flow_policed_delay TEXT,
                delta_flow_aqm_drop TEXT, delta_lat_updates TEXT,
                delta_lat_bin1 TEXT, delta_lat_bin2 TEXT, delta_lat_bin3 TEXT,
                delta_lat_bin4 TEXT, delta_lat_bin5 TEXT, delta_lat_bin6 TEXT,
                delta_lat_bin7 TEXT, delta_lat_bin8 TEXT, delta_lat_bin9 TEXT,
                delta_lat_bin10 TEXT, delta_lat_bin11 TEXT, delta_lat_bin12 TEXT,
                delta_lat_bin13 TEXT, delta_lat_bin14 TEXT, delta_lat_bin15 TEXT,
                delta_lat_bin16 TEXT,
                delta_cong_sanctioned TEXT, delta_cong_ect0 TEXT, delta_cong_ect1 TEXT,
                delta_cong_ce_marked TEXT, delta_cong_arrived_ce TEXT
            )
        ''')
        # kafka_rows — Kafka US+DS telemetry
        con.execute('''
            CREATE TABLE IF NOT EXISTS kafka_rows (
                session_id TEXT, captured_utc TEXT, kafka_timestamp_ms TEXT,
                dir TEXT, sfIndex TEXT, sfid TEXT, scn TEXT,
                mdName TEXT, node TEXT, pod TEXT, cluster TEXT,
                delta_octets TEXT, delta_pkts TEXT, delta_pkts_dropped TEXT,
                total_octets TEXT, total_pkts TEXT,
                lat_avg_usec TEXT, lat_max_usec TEXT,
                aqm_drop_pkts TEXT, aqm_marked_pkts TEXT, sanctioned_pkts TEXT,
                lat_bin01 TEXT, lat_bin02 TEXT, lat_bin03 TEXT, lat_bin04 TEXT,
                lat_bin05 TEXT, lat_bin06 TEXT, lat_bin07 TEXT, lat_bin08 TEXT,
                lat_bin09 TEXT, lat_bin10 TEXT, lat_bin11 TEXT, lat_bin12 TEXT,
                lat_bin13 TEXT, lat_bin14 TEXT, lat_bin15 TEXT, lat_bin16 TEXT,
                bin01_lower_msec TEXT, bin16_upper_msec TEXT,
                max_rate_bps TEXT, aqm_target_msecs TEXT
            )
        ''')

def _db_upsert(session_id: str, s: dict):
    cfg = {k: v for k, v in s['cfg'].items() if k in _SAFE_CFG_KEYS}
    try:
        with _db() as con:
            con.execute('''
                INSERT INTO sessions (id, mac, cmts_type, status, phase, started_at, stopped_at, session_name, last_error, cfg_json)
                VALUES (:id, :mac, :cmts_type, :status, :phase, :started_at, :stopped_at, :session_name, :last_error, :cfg_json)
                ON CONFLICT(id) DO UPDATE SET
                    status       = excluded.status,
                    phase        = excluded.phase,
                    stopped_at   = excluded.stopped_at,
                    last_error   = excluded.last_error,
                    cfg_json     = excluded.cfg_json
            ''', {
                'id':           session_id,
                'mac':          s['mac'],
                'cmts_type':    s['cmts_type'],
                'status':       s['status'],
                'phase':        s.get('phase', ''),
                'started_at':   s['started_at'],
                'stopped_at':   s.get('stopped_at'),
                'session_name': s['session_name'],
                'last_error':   s.get('last_error'),
                'cfg_json':     json.dumps(cfg),
            })
    except Exception as e:
        log.warning('DB upsert failed for %s: %s', session_id, e)


def _db_migrate():
    """Add columns/tables missing from older DB schemas."""
    with _db() as con:
        tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        # Add phase column if missing (older DBs)
        try:
            con.execute('ALTER TABLE sessions ADD COLUMN phase TEXT')
        except Exception:
            pass  # already exists
        # Rename legacy table if it still exists
        if 'snmp_delta_rows' in tables:
            con.execute('ALTER TABLE snmp_delta_rows RENAME TO snmp_upstream_delta_rows')
        for tbl in ('snmp_upstream_delta_rows', 'snmp_downstream_delta_rows', 'kafka_rows'):
            try:
                con.execute(f'ALTER TABLE {tbl} ADD COLUMN phase TEXT')
            except Exception:
                pass  # already exists
        # Drop redundant tables
        for dead in ('snmp_rows', 'snmp_upstream_rows', 'kafka_downstream_rows'):
            con.execute(f'DROP TABLE IF EXISTS {dead}')
        con.execute('''
            CREATE TABLE IF NOT EXISTS snmp_upstream_delta_rows (
                session_id TEXT, captured_utc TEXT, poll_index INTEGER,
                target_ip TEXT, target_label TEXT, cmts_type TEXT, sfid TEXT,
                sf_buffer_size TEXT,
                ps_scn TEXT, ps_priority TEXT, ps_max_rate TEXT,
                ps_max_burst TEXT, ps_max_concat_burst TEXT, ps_aqm_latency_target TEXT,
                ps_min_buffer TEXT, ps_target_buffer TEXT, ps_max_buffer TEXT,
                flow_pkts TEXT, flow_octets TEXT, flow_policed_drop TEXT,
                flow_policed_delay TEXT, flow_aqm_drop TEXT,
                lat_bin_scn TEXT, lat_aqm_target TEXT,
                lat_edge_bin1 TEXT, lat_edge_bin2 TEXT, lat_edge_bin3 TEXT,
                lat_edge_bin4 TEXT, lat_edge_bin5 TEXT, lat_edge_bin6 TEXT,
                lat_edge_bin7 TEXT, lat_edge_bin8 TEXT, lat_edge_bin9 TEXT,
                lat_edge_bin10 TEXT, lat_edge_bin11 TEXT, lat_edge_bin12 TEXT,
                lat_edge_bin13 TEXT, lat_edge_bin14 TEXT, lat_edge_bin15 TEXT,
                lat_max_usec TEXT, lat_updates TEXT,
                lat_bin1 TEXT, lat_bin2 TEXT, lat_bin3 TEXT, lat_bin4 TEXT,
                lat_bin5 TEXT, lat_bin6 TEXT, lat_bin7 TEXT, lat_bin8 TEXT,
                lat_bin9 TEXT, lat_bin10 TEXT, lat_bin11 TEXT, lat_bin12 TEXT,
                lat_bin13 TEXT, lat_bin14 TEXT, lat_bin15 TEXT, lat_bin16 TEXT,
                cong_sanctioned TEXT, cong_ect0 TEXT, cong_ect1 TEXT,
                cong_ce_marked TEXT, cong_arrived_ce TEXT,
                delta_flow_pkts TEXT, delta_flow_octets TEXT,
                delta_flow_policed_drop TEXT, delta_flow_policed_delay TEXT,
                delta_flow_aqm_drop TEXT, delta_lat_updates TEXT,
                delta_lat_bin1 TEXT, delta_lat_bin2 TEXT, delta_lat_bin3 TEXT,
                delta_lat_bin4 TEXT, delta_lat_bin5 TEXT, delta_lat_bin6 TEXT,
                delta_lat_bin7 TEXT, delta_lat_bin8 TEXT, delta_lat_bin9 TEXT,
                delta_lat_bin10 TEXT, delta_lat_bin11 TEXT, delta_lat_bin12 TEXT,
                delta_lat_bin13 TEXT, delta_lat_bin14 TEXT, delta_lat_bin15 TEXT,
                delta_lat_bin16 TEXT,
                delta_cong_sanctioned TEXT, delta_cong_ect0 TEXT, delta_cong_ect1 TEXT,
                delta_cong_ce_marked TEXT, delta_cong_arrived_ce TEXT
            )
        ''')
        con.execute('''
            CREATE TABLE IF NOT EXISTS snmp_downstream_delta_rows (
                session_id TEXT, captured_utc TEXT, poll_index INTEGER,
                target_ip TEXT, target_label TEXT, cmts_type TEXT, sfid TEXT,
                sf_buffer_size TEXT,
                ps_scn TEXT, ps_priority TEXT, ps_max_rate TEXT,
                ps_max_burst TEXT, ps_max_concat_burst TEXT, ps_aqm_latency_target TEXT,
                ps_min_buffer TEXT, ps_target_buffer TEXT, ps_max_buffer TEXT,
                flow_pkts TEXT, flow_octets TEXT, flow_policed_drop TEXT,
                flow_policed_delay TEXT, flow_aqm_drop TEXT,
                lat_bin_scn TEXT, lat_aqm_target TEXT,
                lat_edge_bin1 TEXT, lat_edge_bin2 TEXT, lat_edge_bin3 TEXT,
                lat_edge_bin4 TEXT, lat_edge_bin5 TEXT, lat_edge_bin6 TEXT,
                lat_edge_bin7 TEXT, lat_edge_bin8 TEXT, lat_edge_bin9 TEXT,
                lat_edge_bin10 TEXT, lat_edge_bin11 TEXT, lat_edge_bin12 TEXT,
                lat_edge_bin13 TEXT, lat_edge_bin14 TEXT, lat_edge_bin15 TEXT,
                lat_max_usec TEXT, lat_updates TEXT,
                lat_bin1 TEXT, lat_bin2 TEXT, lat_bin3 TEXT, lat_bin4 TEXT,
                lat_bin5 TEXT, lat_bin6 TEXT, lat_bin7 TEXT, lat_bin8 TEXT,
                lat_bin9 TEXT, lat_bin10 TEXT, lat_bin11 TEXT, lat_bin12 TEXT,
                lat_bin13 TEXT, lat_bin14 TEXT, lat_bin15 TEXT, lat_bin16 TEXT,
                cong_sanctioned TEXT, cong_ect0 TEXT, cong_ect1 TEXT,
                cong_ce_marked TEXT, cong_arrived_ce TEXT,
                delta_flow_pkts TEXT, delta_flow_octets TEXT,
                delta_flow_policed_drop TEXT, delta_flow_policed_delay TEXT,
                delta_flow_aqm_drop TEXT, delta_lat_updates TEXT,
                delta_lat_bin1 TEXT, delta_lat_bin2 TEXT, delta_lat_bin3 TEXT,
                delta_lat_bin4 TEXT, delta_lat_bin5 TEXT, delta_lat_bin6 TEXT,
                delta_lat_bin7 TEXT, delta_lat_bin8 TEXT, delta_lat_bin9 TEXT,
                delta_lat_bin10 TEXT, delta_lat_bin11 TEXT, delta_lat_bin12 TEXT,
                delta_lat_bin13 TEXT, delta_lat_bin14 TEXT, delta_lat_bin15 TEXT,
                delta_lat_bin16 TEXT,
                delta_cong_sanctioned TEXT, delta_cong_ect0 TEXT, delta_cong_ect1 TEXT,
                delta_cong_ce_marked TEXT, delta_cong_arrived_ce TEXT
            )
        ''')
        con.execute('''
            CREATE TABLE IF NOT EXISTS kafka_rows (
                session_id TEXT, captured_utc TEXT, kafka_timestamp_ms TEXT,
                dir TEXT, sfIndex TEXT, sfid TEXT, scn TEXT,
                mdName TEXT, node TEXT, pod TEXT, cluster TEXT,
                delta_octets TEXT, delta_pkts TEXT, delta_pkts_dropped TEXT,
                total_octets TEXT, total_pkts TEXT,
                lat_avg_usec TEXT, lat_max_usec TEXT,
                aqm_drop_pkts TEXT, aqm_marked_pkts TEXT, sanctioned_pkts TEXT,
                lat_bin01 TEXT, lat_bin02 TEXT, lat_bin03 TEXT, lat_bin04 TEXT,
                lat_bin05 TEXT, lat_bin06 TEXT, lat_bin07 TEXT, lat_bin08 TEXT,
                lat_bin09 TEXT, lat_bin10 TEXT, lat_bin11 TEXT, lat_bin12 TEXT,
                lat_bin13 TEXT, lat_bin14 TEXT, lat_bin15 TEXT, lat_bin16 TEXT,
                bin01_lower_msec TEXT, bin16_upper_msec TEXT,
                max_rate_bps TEXT, aqm_target_msecs TEXT
            )
        ''')


def _db_insert_snmp_upstream_delta_rows(session_id: str, rows: list):
    if not rows:
        return
    cols = ['session_id'] + cc.SNMP_DELTA_CSV_FIELDS
    placeholders = ','.join('?' * len(cols))
    try:
        with _db() as con:
            con.executemany(
                f'INSERT INTO snmp_upstream_delta_rows ({",".join(cols)}) VALUES ({placeholders})',
                [tuple([session_id] + [row.get(c, '') for c in cc.SNMP_DELTA_CSV_FIELDS]) for row in rows]
            )
    except Exception as e:
        log.warning('snmp_upstream_delta_rows insert failed for %s: %s', session_id, e)


def _db_insert_snmp_downstream_delta_rows(session_id: str, rows: list):
    if not rows:
        return
    cols = ['session_id'] + cc.SNMP_DELTA_CSV_FIELDS
    placeholders = ','.join('?' * len(cols))
    try:
        with _db() as con:
            con.executemany(
                f'INSERT INTO snmp_downstream_delta_rows ({",".join(cols)}) VALUES ({placeholders})',
                [tuple([session_id] + [row.get(c, '') for c in cc.SNMP_DELTA_CSV_FIELDS]) for row in rows]
            )
    except Exception as e:
        log.warning('snmp_downstream_delta_rows insert failed for %s: %s', session_id, e)


def _db_insert_kafka_rows(session_id: str, rows: list):
    """Insert Kafka rows."""
    if not rows:
        return
    cols = ['session_id'] + cc.KAFKA_CSV_FIELDS
    placeholders = ','.join('?' * len(cols))
    try:
        with _db() as con:
            con.executemany(
                f'INSERT INTO kafka_rows ({",".join(cols)}) VALUES ({placeholders})',
                [tuple([session_id] + [row.get(c, '') for c in cc.KAFKA_CSV_FIELDS]) for row in rows]
            )
    except Exception as e:
        log.warning('kafka_rows insert failed for %s: %s', session_id, e)



def _db_load_sessions():
    try:
        with _db() as con:
            rows = con.execute('SELECT * FROM sessions ORDER BY started_at').fetchall()
        recovered = 0
        for row in rows:
            sid = row['id']
            if sid in _sessions:
                continue
            cfg = json.loads(row['cfg_json'] or '{}')
            cfg.setdefault('modem_info', None)
            cfg.setdefault('cmts_password', cc.DEFAULT_TACACS_PASSWORD)
            _sessions[sid] = {
                'id':           sid,
                'mac':          row['mac'],
                'cmts_type':    row['cmts_type'],
                'status':       row['status'],
                'phase':        row['phase'] or 'stopped',
                'started_at':   row['started_at'],
                'stopped_at':   row['stopped_at'],
                'session_name': row['session_name'],
                'last_error':   row['last_error'],
                'cfg':          cfg,
                'stop_event':   threading.Event(),
                'threads':      [],
            }
            recovered += 1
        if recovered:
            log.info('Loaded %d session(s) from DB', recovered)
    except Exception as e:
        log.warning('Failed to load sessions from DB: %s', e)


@app.on_event('startup')
def _on_startup():
    _db_init()
    _db_migrate()
    _db_load_sessions()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get('/', response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(content=_UI)


@app.post('/sessions/start', status_code=201)
def start_session(req: StartRequest):
    log.info('START request  mac=%s  cmts_type=%s  name=%r', req.mac, req.cmts_type, req.session_name)
    try:
        cfg = _build_cfg(req)
    except HTTPException:
        raise
    except Exception as e:
        log.error('Failed to build session config: %s', e, exc_info=True)
        raise HTTPException(500, f'Config error: {e}')

    if not cfg['target_ip']:
        log.info('[%s] Resolving modem IPv6  cmts_host=%r  jumpserver=%r',
                 cfg['mac_colon'], cfg.get('cmts_host') or 'not set', cfg.get('snmp_jumpserver') or 'not set')
        try:
            modem_info = cc.modem_info_collector(cfg)
        except Exception as e:
            log.error('[%s] modem_info_collector failed: %s', cfg['mac_colon'], e, exc_info=True)
            # For vCMTS, Kafka can still collect without SNMP — don't block session start
            if cfg['cmts_type'] == 'icmts':
                raise HTTPException(500, f'CMTS lookup failed: {e}')
            modem_info = None
        if modem_info and modem_info.get('not_found'):
            log.warning('[%s] Modem not found on %s', cfg['mac_colon'], cfg['cmts_host'])
            # For iCMTS this is fatal; for vCMTS Kafka still works
            if cfg['cmts_type'] == 'icmts':
                raise HTTPException(404, f'Modem {cfg["mac_colon"]} not found on {cfg["cmts_host"]}')
        if modem_info and modem_info.get('cm_ipv6'):
            cfg['target_ip'] = modem_info['cm_ipv6']
            log.info('[%s] Resolved IPv6: %s', cfg['mac_colon'], cfg['target_ip'])
        else:
            log.warning('[%s] IPv6 not resolved — SNMP will be skipped, Kafka will still run', cfg['mac_colon'])
        cfg['modem_info'] = modem_info
    else:
        # Still collect modem info for CSV headers/sidecar even when IP is pre-supplied
        try:
            modem_info = cc.modem_info_collector(cfg)
            cfg['modem_info'] = modem_info
        except Exception as e:
            log.warning('[%s] modem_info_collector failed (non-fatal): %s', cfg['mac_colon'], e)
            cfg['modem_info'] = None

    session_id   = str(uuid.uuid4())[:8]
    session_name = req.session_name or f'{cfg["cmts_type"].upper()} Session'
    stop_event   = threading.Event()
    poll_index   = [1]
    threads      = []

    # Pass DB insertion callbacks into cfg so collector threads can write rows
    cfg['session_id'] = session_id
    cfg['db_insert_snmp_delta']   = lambda rows: _db_insert_snmp_upstream_delta_rows(session_id, rows)
    cfg['db_insert_snmp_ds_delta'] = lambda rows: _db_insert_snmp_downstream_delta_rows(session_id, rows)
    cfg['db_insert_kafka']         = lambda rows: _db_insert_kafka_rows(session_id, rows)
    cfg['baseline_polls'] = req.baseline_polls
    cfg['cooldown_polls'] = 3

    def _phase_callback(new_phase):
        s = _sessions.get(session_id)
        if s:
            s['phase'] = new_phase
            _db_upsert(session_id, s)
            log.info('[%s] Phase → %s', session_id, new_phase)

    cfg['phase_callback'] = _phase_callback

    def _error_callback(err):
        s = _sessions.get(session_id)
        if s:
            s['last_error'] = err
            s['status'] = 'stopped'
            s['stopped_at'] = datetime.utcnow().isoformat()
            _db_upsert(session_id, s)
            s['stop_event'].set()
            log.error('[%s] %s', session_id, err)

    cfg['error_callback'] = _error_callback

    _sessions[session_id] = {
        'id':           session_id,
        'mac':          cfg['mac_colon'],
        'cmts_type':    cfg['cmts_type'],
        'status':       'running',
        'phase':        'baseline',
        'started_at':   datetime.utcnow().isoformat(),
        'stopped_at':   None,
        'session_name': session_name,
        'last_error':   None,
        'cfg':          cfg,
        'stop_event':   stop_event,
        'threads':      threads,
    }

    def _snmp_wrapper():
        try:
            cc.snmp_collector_thread(cfg, stop_event, cfg['csv_paths'], poll_index)
        except Exception as e:
            log.error('[%s][%s] SNMP thread crashed: %s', session_id, cfg['mac_colon'], e, exc_info=True)
            _sessions[session_id]['last_error'] = f'SNMP thread error: {e}'
        finally:
            s = _sessions.get(session_id)
            if s and s['status'] == 'running' and not stop_event.is_set():
                err = s['last_error'] or (
                    f'SNMP thread exited — target_ip not resolved '
                    f'(cmts_host={cfg.get("cmts_host") or "not set"}, '
                    f'jumpserver={cfg.get("snmp_jumpserver") or "not set"})'
                )
                log.error('[%s] SNMP thread exited early: %s', session_id, err)
                s['last_error'] = err
                s['status'] = 'error'
                s['stopped_at'] = datetime.utcnow().isoformat()
                _db_upsert(session_id, s)
                stop_event.set()

    threads.append(threading.Thread(target=_snmp_wrapper, daemon=True, name=f'snmp-{session_id}'))

    if cfg['cmts_type'] == 'vcmts' and cfg['kafka_csv']:
        def _kafka_wrapper():
            try:
                cc.kafka_collector_thread(cfg, stop_event, cfg['kafka_csv'])
            except Exception as e:
                log.error('[%s][%s] Kafka thread crashed: %s', session_id, cfg['mac_colon'], e, exc_info=True)
                _sessions[session_id]['last_error'] = f'Kafka thread error: {e}'
        threads.append(threading.Thread(target=_kafka_wrapper, daemon=True, name=f'kafka-{session_id}'))

    # Watchdog — auto-stop after 10 minutes
    MAX_SESSION_SECS = 600
    def _watchdog():
        if stop_event.wait(timeout=MAX_SESSION_SECS):
            return  # stopped normally before timeout
        log.info('[%s] Auto-stopping after %ds timeout', session_id, MAX_SESSION_SECS)
        s = _sessions.get(session_id)
        if s and s['status'] in ('running', 'stopping'):
            stop_event.set()
            for t in s['threads']:
                if t.name != threading.current_thread().name:
                    t.join(timeout=15)
            s['status']     = 'stopped'
            s['stopped_at'] = datetime.utcnow().isoformat()
            s['last_error']  = s['last_error'] or 'Auto-stopped after 10 min timeout'
            _db_upsert(session_id, s)
            log.info('[%s] Session auto-stopped', session_id)

    watchdog = threading.Thread(target=_watchdog, daemon=True, name=f'watchdog-{session_id}')
    threads.append(watchdog)

    for t in threads:
        t.start()

    _db_upsert(session_id, _sessions[session_id])
    log.info('[%s] Session started  mac=%s  type=%s  dir=%s  timeout=%ds',
             session_id, cfg['mac_colon'], cfg['cmts_type'], cfg['session_dir'], MAX_SESSION_SECS)

    return {
        'session_id':  session_id,
        'session_dir': cfg['session_dir'],
        'mac':         cfg['mac_colon'],
        'cmts_type':   cfg['cmts_type'],
        'status':      'running',
    }


@app.post('/sessions/{session_id}/stop')
def stop_session(session_id: str):
    s = _sessions.get(session_id)
    if not s:
        log.warning('STOP request for unknown session %s', session_id)
        raise HTTPException(404, 'Session not found')
    if s['status'] not in ('running',):
        return {'session_id': session_id, 'status': s['status']}

    log.info('[%s] Stop requested  mac=%s', session_id, s['mac'])
    s['status'] = 'stopping'
    s['stop_event'].set()
    _db_upsert(session_id, s)

    def _finalize():
        poll_interval = s['cfg'].get('snmp_poll_interval', 15)
        cooldown_secs = poll_interval * 3 + poll_interval  # 3 polls + 1 buffer
        for t in s['threads']:
            if t.is_alive() and t.name != f'watchdog-{session_id}':
                t.join(timeout=cooldown_secs)
        s['status']     = 'stopped'
        s['stopped_at'] = datetime.utcnow().isoformat()
        _db_upsert(session_id, s)
        log.info('[%s] Session stopped', session_id)

    threading.Thread(target=_finalize, daemon=True, name=f'finalize-{session_id}').start()
    return {'session_id': session_id, 'status': 'stopping'}


@app.get('/sessions')
def list_sessions():
    return [
        {k: v for k, v in s.items() if k not in ('cfg', 'stop_event', 'threads')}
        for s in _sessions.values()
    ]


@app.get('/sessions/{session_id}')
def get_session(session_id: str):
    s = _sessions.get(session_id)
    if not s:
        raise HTTPException(404, 'Session not found')
    return {k: v for k, v in s.items() if k not in ('cfg', 'stop_event', 'threads')}



@app.delete('/sessions/{session_id}')
def delete_session(session_id: str):
    s = _sessions.get(session_id)
    if s and s['status'] == 'running':
        raise HTTPException(400, 'Stop the session before deleting')
    # Delete results folder
    if s:
        session_dir = s['cfg'].get('session_dir', '')
        if session_dir and os.path.isdir(session_dir):
            try:
                shutil.rmtree(session_dir)
                log.info('[%s] Deleted results dir: %s', session_id, session_dir)
            except Exception as e:
                log.warning('[%s] Could not delete results dir: %s', session_id, e)
    try:
        with _db() as con:
            con.execute('DELETE FROM sessions WHERE id=?', (session_id,))
            for tbl in ('snmp_upstream_delta_rows', 'snmp_downstream_delta_rows', 'kafka_rows'):
                con.execute(f'DELETE FROM {tbl} WHERE session_id=?', (session_id,))
    except Exception as e:
        raise HTTPException(500, f'DB error: {e}')
    _sessions.pop(session_id, None)
    log.info('[%s] Session deleted', session_id)
    return {'session_id': session_id, 'deleted': True}


@app.get('/sessions/{session_id}/errors')
def get_errors(session_id: str):
    s = _sessions.get(session_id)
    if not s:
        raise HTTPException(404, 'Session not found')
    return {'session_id': session_id, 'last_error': s.get('last_error')}


if __name__ == '__main__':
    import uvicorn
    uvicorn.run('cm_collector_api:app', host='0.0.0.0', port=8000, reload=True)

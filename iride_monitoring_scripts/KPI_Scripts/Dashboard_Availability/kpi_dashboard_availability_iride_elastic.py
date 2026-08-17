"""
kpi_dashboard_availability_iride_elastic.py
===========================================
KPI: Availability of visualization and user dashboards services.

Sonda la disponibilita' del servizio di visualizzazione / dashboard utente
IRIDE CyberItaly (applicazione "perception").

DUE LIVELLI DI MONITORAGGIO (come da HIS-Central):
  Livello A (reachability front-end): HTTP GET sulla URL della dashboard,
            status code + latenza. Prova che il front-end (SPA) risponde.
  Livello B (backend autenticato): login Keycloak (password grant) + una
            chiamata all'API Insula (/jobs) che alimenta la dashboard.
            Prova che i dati arrivano davvero, non solo che carica la SPA.

RATIONALE:
  La dashboard e' una Single Page App: un semplice 200 sul front-end
  dimostra solo che il bundle statico e' servito, NON che i dati siano
  raggiungibili. Per questo aggiungiamo il livello B autenticato: se il
  front-end e' su ma l'API dietro e' giu', il servizio dashboard NON e'
  realmente disponibile per l'utente.

Output: 1 evento JSON in stile DESP per esecuzione, con reachability
front-end (frontend_*) + backend autenticato (api_*) + response_status
aggregato (OK / SLOW / FAIL).

Su file locale (sempre) + Elastic (se ELASTIC_ENABLED=True).
Index Elastic: logs-iride-dashboard-availability.monitoring-default
"""

import sys
import os
import json
import socket
import time
import logging
import configparser
import urllib3
import warnings
from datetime import datetime, timezone

import requests

# Silenzia rumore cosmetico (TLS self-signed Elastic, deprecation warnings)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

# ============================================================
# Setup logging
# ============================================================
hostname = socket.gethostname()
script_name = os.path.splitext(os.path.basename(__file__))[0]
log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        f"{script_name}.log")

logger = logging.getLogger(script_name)
logger.setLevel(logging.INFO)
fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(fmt)
logger.addHandler(ch)

# Silenzia librerie noisy (POST verbosi, TLS warning)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)

# ============================================================
# Config
# ============================================================
base_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_dir, 'dashboard_availability_iride_elastic.ini')
config = configparser.ConfigParser()
config.read(config_path)

# Elastic
ELASTIC_ENABLED = config.getboolean('CONFIG', 'ELASTIC_ENABLED', fallback=False)
MONITORING_URL = config.get('CONFIG', 'MONITORING_URL', fallback='')
MONITORING_APIKEY = config.get('CONFIG', 'MONITORING_APIKEY', fallback='')
MONITORING_VERIFY_CERTS = config.getboolean(
    'CONFIG', 'MONITORING_VERIFY_CERTS', fallback=True)
SYNTHETIC_INDEX = config.get(
    'CONFIG', 'SYNTHETIC_INDEX',
    fallback='logs-iride-dashboard-availability.monitoring-default')

# Front-end dashboard (livello A)
DASHBOARD_URL = config.get('DASHBOARD', 'FRONTEND_URL')
HTTP_TIMEOUT = config.getint('DASHBOARD', 'HTTP_TIMEOUT', fallback=30)
LATENCY_OK_MS = config.getint('DASHBOARD', 'LATENCY_OK_MS', fallback=3000)
LATENCY_SLOW_MS = config.getint('DASHBOARD', 'LATENCY_SLOW_MS', fallback=10000)

# Backend API Insula (livello B) — dati che alimentano la dashboard
INSULA_BASE_URL = config.get('INSULA', 'BASE_URL',
                             fallback='https://iride-cyberitaly.space/secure/api/v2.0')
JOBS_PROBE_PATH = config.get('INSULA', 'JOBS_PROBE_PATH', fallback='/jobs?size=1')

# Keycloak (auth backend)
KEYCLOAK_URL = config.get('KEYCLOAK', 'URL',
                          fallback='https://identity.iride-cyberitaly.space')
KEYCLOAK_REALM = config.get('KEYCLOAK', 'REALM', fallback='cyberitaly')
KEYCLOAK_CLIENT_ID = config.get('KEYCLOAK', 'CLIENT_ID', fallback='admin-cli')
KEYCLOAK_CLIENT_SECRET = config.get('KEYCLOAK', 'CLIENT_SECRET', fallback='')
KEYCLOAK_USERNAME = config.get('KEYCLOAK', 'USERNAME', fallback='')
KEYCLOAK_PASSWORD = config.get('KEYCLOAK', 'PASSWORD', fallback='')

logger.info(f"[AVVIO] {script_name} — hostname={hostname}")
logger.info(f"[CONFIG] Dashboard front-end: {DASHBOARD_URL}")
logger.info(f"[CONFIG] Backend API: {INSULA_BASE_URL}{JOBS_PROBE_PATH}")
logger.info(f"[CONFIG] Keycloak: {KEYCLOAK_URL} (realm={KEYCLOAK_REALM})")
logger.info(f"[CONFIG] Latency thresholds: OK<{LATENCY_OK_MS}ms, "
            f"SLOW<{LATENCY_SLOW_MS}ms")
if ELASTIC_ENABLED:
    logger.info(f"[CONFIG] Elastic ENABLED → {MONITORING_URL}")
    logger.info(f"[CONFIG] Index: {SYNTHETIC_INDEX}")
else:
    logger.info(f"[CONFIG] >>> DRY-RUN MODE (no Elastic) <<<")
logger.info("")


# ============================================================
# Helpers
# ============================================================
def write_log(entry):
    """Scrive una riga JSON nel file di log."""
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(json.dumps(entry, ensure_ascii=False) + '\n')


def ship_to_elastic(entry):
    """Invia il documento a Elasticsearch (se abilitato)."""
    if not ELASTIC_ENABLED:
        return
    try:
        url = f"{MONITORING_URL.rstrip('/')}/{SYNTHETIC_INDEX}/_doc"
        headers = {
            'Authorization': f'ApiKey {MONITORING_APIKEY}',
            'Content-Type': 'application/json',
        }
        r = requests.post(url, headers=headers, json=entry,
                          verify=MONITORING_VERIFY_CERTS, timeout=30)
        if r.status_code not in (200, 201):
            logger.error(f"[ELASTIC] HTTP {r.status_code}: {r.text[:200]}")
        else:
            logger.info(f"[ELASTIC] documento inviato a {SYNTHETIC_INDEX}")
    except Exception as e:
        logger.error(f"[ELASTIC] {type(e).__name__}: {e}")


def get_keycloak_token(session):
    """OpenID Connect password grant → access_token. None se fallisce."""
    token_url = (f"{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}"
                 f"/protocol/openid-connect/token")
    data = {
        "grant_type": "password",
        "client_id": KEYCLOAK_CLIENT_ID,
        "username": KEYCLOAK_USERNAME,
        "password": KEYCLOAK_PASSWORD,
    }
    if KEYCLOAK_CLIENT_SECRET:
        data["client_secret"] = KEYCLOAK_CLIENT_SECRET
    try:
        r = session.post(token_url, data=data, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            logger.error(f"[AUTH] HTTP {r.status_code}: {r.text[:200]}")
            return None, r.status_code
        return r.json().get("access_token"), r.status_code
    except Exception as e:
        logger.error(f"[AUTH] {type(e).__name__}: {e}")
        return None, None


def count_jobs(payload):
    """Estrae il numero di job dalla risposta HATEOAS Insula (/jobs)."""
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        items = (payload.get("_embedded", {}).get("jobs")
                 or payload.get("content")
                 or [])
        return len(items) if isinstance(items, list) else 0
    return 0


# ============================================================
# Main probe
# ============================================================
def main():
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    session = requests.Session()

    status = "OK"
    error_message = None

    # ---- Livello A: reachability front-end dashboard ----
    frontend_http_code = None
    frontend_latency_ms = None
    frontend_ok = False
    frontend_size = 0

    t0 = time.perf_counter()
    try:
        r = session.get(DASHBOARD_URL, timeout=HTTP_TIMEOUT,
                        allow_redirects=True, verify=True)
        frontend_latency_ms = (time.perf_counter() - t0) * 1000
        frontend_http_code = r.status_code
        frontend_size = len(r.content)
        frontend_ok = (200 <= r.status_code < 300)
        if not frontend_ok:
            status = "FAIL"
            error_message = f"Front-end HTTP {r.status_code}"
    except requests.exceptions.Timeout:
        frontend_latency_ms = (time.perf_counter() - t0) * 1000
        status = "FAIL"
        error_message = f"Front-end timeout after {HTTP_TIMEOUT}s"
    except requests.exceptions.ConnectionError as e:
        frontend_latency_ms = (time.perf_counter() - t0) * 1000
        status = "FAIL"
        error_message = f"Front-end ConnectionError: {str(e)[:150]}"
    except Exception as e:
        frontend_latency_ms = (time.perf_counter() - t0) * 1000
        status = "FAIL"
        error_message = f"Front-end {type(e).__name__}: {str(e)[:150]}"

    # ---- Livello B: backend autenticato (Keycloak + API Insula /jobs) ----
    auth_http_code = None
    api_http_code = None
    api_latency_ms = None
    api_ok = False
    jobs_returned = None
    token = None

    token, auth_http_code = get_keycloak_token(session)
    if not token:
        if status == "OK":
            status = "FAIL"
            error_message = "Keycloak auth failed (no token)"
    else:
        probe_url = INSULA_BASE_URL.rstrip('/') + JOBS_PROBE_PATH
        headers = {"Authorization": f"Bearer {token}"}
        t1 = time.perf_counter()
        try:
            r = session.get(probe_url, headers=headers, timeout=HTTP_TIMEOUT)
            api_latency_ms = (time.perf_counter() - t1) * 1000
            api_http_code = r.status_code
            api_ok = (200 <= r.status_code < 300)
            if api_ok:
                try:
                    jobs_returned = count_jobs(r.json())
                except ValueError:
                    api_ok = False
                    if status == "OK":
                        status = "FAIL"
                        error_message = "Backend API response not JSON"
            else:
                if status == "OK":
                    status = "FAIL"
                    error_message = f"Backend API HTTP {r.status_code}"
        except requests.exceptions.Timeout:
            api_latency_ms = (time.perf_counter() - t1) * 1000
            if status == "OK":
                status = "FAIL"
                error_message = f"Backend API timeout after {HTTP_TIMEOUT}s"
        except Exception as e:
            api_latency_ms = (time.perf_counter() - t1) * 1000
            if status == "OK":
                status = "FAIL"
                error_message = f"Backend API {type(e).__name__}: {str(e)[:150]}"

    # ---- Latency check (solo se tutto e' reachable) ----
    if status == "OK":
        worst_latency = max(
            [x for x in (frontend_latency_ms, api_latency_ms) if x is not None],
            default=0.0)
        if worst_latency > LATENCY_SLOW_MS:
            status = "FAIL"
            error_message = (f"Latency {worst_latency:.0f}ms > "
                             f"{LATENCY_SLOW_MS}ms")
        elif worst_latency > LATENCY_OK_MS:
            status = "SLOW"

    # ---- Costruzione documento ----
    entry = {
        "@timestamp": timestamp,
        "event_timestamp": timestamp,
        "platform": "iride",
        "service_provider_log": "iride_dashboard",
        "service_name": "iride-cyberitaly",
        "event_type": "dashboard_availability_probe",
        "response_status": status,
        "hostname": hostname,
        "endpoint": DASHBOARD_URL,
        "error_message": error_message,
        "latency_threshold_ok_ms": LATENCY_OK_MS,
        "latency_threshold_slow_ms": LATENCY_SLOW_MS,

        # Livello A — front-end
        "frontend_url": DASHBOARD_URL,
        "frontend_http_code": frontend_http_code,
        "frontend_latency_ms": (round(frontend_latency_ms, 2)
                                if frontend_latency_ms is not None else None),
        "frontend_response_size_bytes": frontend_size,
        "frontend_ok": frontend_ok,

        # Livello B — backend autenticato
        "auth_http_code": auth_http_code,
        "auth_ok": bool(token),
        "api_endpoint": INSULA_BASE_URL.rstrip('/') + JOBS_PROBE_PATH,
        "api_http_code": api_http_code,
        "api_latency_ms": (round(api_latency_ms, 2)
                           if api_latency_ms is not None else None),
        "api_ok": api_ok,
        "api_jobs_returned": jobs_returned,
    }

    write_log(entry)

    # ---- Console output ----
    logger.info(f"[FRONTEND] {frontend_http_code} in "
                f"{frontend_latency_ms:.0f}ms ({frontend_size} bytes) "
                f"ok={frontend_ok}"
                if frontend_latency_ms is not None
                else f"[FRONTEND] errore: {error_message}")
    logger.info(f"[AUTH] token={'OK' if token else 'FAIL'} "
                f"(HTTP {auth_http_code})")
    if token:
        logger.info(f"[API] {api_http_code} in "
                    f"{api_latency_ms:.0f}ms ok={api_ok} "
                    f"jobs={jobs_returned}"
                    if api_latency_ms is not None
                    else "[API] nessuna risposta")

    if status == "OK":
        logger.info("[RESULT] OK — front-end + backend entrambi disponibili")
    elif status == "SLOW":
        logger.warning(f"[RESULT] SLOW — {error_message}")
    else:
        logger.error(f"[RESULT] FAIL — {error_message}")

    ship_to_elastic(entry)

    logger.info("[FINE]")
    return 0 if status == "OK" else 1


if __name__ == "__main__":
    sys.exit(main())
"""
kpi_data_access_availability_iride_elastic.py
=============================================
KPI: Availability of data retrieval, ingestion and access.

Sonda la disponibilita' dell'accesso ai dati IRIDE CyberItaly esposti via
SFTPGo (host ftp.dingest.iride-cyberitaly.space), l'endpoint di ingestion /
retrieval dei prodotti.

DUE LIVELLI DI MONITORAGGIO:
  Livello A (auth): GET /api/v2/token con Basic Auth → JWT. Prova che il
            servizio SFTPGo risponde e l'autenticazione funziona.
  Livello B (retrieval reale): GET /api/v2/user/dirs?path=... con API key
            user-scope (X-SFTPGO-API-KEY). Elenca una directory: prova che
            il recupero effettivo dei dati funziona end-to-end, non solo
            che il servizio di auth e' su.

RATIONALE:
  Un semplice /token 200 dimostra che il servizio e' raggiungibile ma NON
  che i dati siano accessibili. Il livello B elenca davvero una directory
  (retrieval): se l'auth e' su ma il listing fallisce, l'accesso ai dati
  NON e' realmente disponibile.

Output: 1 evento JSON in stile DESP per esecuzione, con auth_* (livello A),
retrieval_* (livello B) e response_status aggregato (OK / SLOW / FAIL).

Su file locale (sempre) + Elastic (se ELASTIC_ENABLED=True).
Index Elastic: logs-iride-data-access-availability.monitoring-default
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
config_path = os.path.join(base_dir, 'data_access_availability_iride_elastic.ini')
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
    fallback='logs-iride-data-access-availability.monitoring-default')

# SFTPGo endpoint + credenziali
SFTPGO_BASE_URL = config.get('SFTPGO', 'BASE_URL')
SFTPGO_USERNAME = config.get('SFTPGO', 'USERNAME', fallback='')
SFTPGO_PASSWORD = config.get('SFTPGO', 'PASSWORD', fallback='')
SFTPGO_API_KEY = config.get('SFTPGO', 'API_KEY', fallback='')
PROBE_PATH = config.get('SFTPGO', 'PROBE_PATH', fallback='/')
HTTP_TIMEOUT = config.getint('SFTPGO', 'HTTP_TIMEOUT', fallback=30)
LATENCY_OK_MS = config.getint('SFTPGO', 'LATENCY_OK_MS', fallback=3000)
LATENCY_SLOW_MS = config.getint('SFTPGO', 'LATENCY_SLOW_MS', fallback=10000)

logger.info(f"[AVVIO] {script_name} — hostname={hostname}")
logger.info(f"[CONFIG] SFTPGo endpoint: {SFTPGO_BASE_URL}")
logger.info(f"[CONFIG] Probe path (retrieval): {PROBE_PATH}")
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


def count_entries(payload):
    """Conta gli item elencati da /api/v2/user/dirs (lista di dict)."""
    if isinstance(payload, list):
        return len(payload)
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

    # ---- Livello A: auth SFTPGo (Basic Auth → JWT) ----
    auth_http_code = None
    auth_latency_ms = None
    auth_ok = False
    token = None

    t0 = time.perf_counter()
    try:
        r = session.get(f"{SFTPGO_BASE_URL}/api/v2/token",
                        auth=(SFTPGO_USERNAME, SFTPGO_PASSWORD),
                        timeout=HTTP_TIMEOUT)
        auth_latency_ms = (time.perf_counter() - t0) * 1000
        auth_http_code = r.status_code
        if r.status_code == 200:
            token = r.json().get("access_token")
            auth_ok = bool(token)
            if not auth_ok:
                status = "FAIL"
                error_message = "Auth 200 ma nessun access_token nel body"
        else:
            status = "FAIL"
            error_message = f"Auth HTTP {r.status_code}"
    except requests.exceptions.Timeout:
        auth_latency_ms = (time.perf_counter() - t0) * 1000
        status = "FAIL"
        error_message = f"Auth timeout after {HTTP_TIMEOUT}s"
    except requests.exceptions.ConnectionError as e:
        auth_latency_ms = (time.perf_counter() - t0) * 1000
        status = "FAIL"
        error_message = f"Auth ConnectionError: {str(e)[:150]}"
    except Exception as e:
        auth_latency_ms = (time.perf_counter() - t0) * 1000
        status = "FAIL"
        error_message = f"Auth {type(e).__name__}: {str(e)[:150]}"

    # ---- Livello B: retrieval reale (listing directory) ----
    retrieval_http_code = None
    retrieval_latency_ms = None
    retrieval_ok = False
    n_entries = None

    if auth_ok:
        if not SFTPGO_API_KEY:
            if status == "OK":
                status = "FAIL"
                error_message = "SFTPGO_API_KEY non configurata (retrieval impossibile)"
        else:
            headers = {"X-SFTPGO-API-KEY": SFTPGO_API_KEY}
            t1 = time.perf_counter()
            try:
                r = session.get(f"{SFTPGO_BASE_URL}/api/v2/user/dirs",
                                headers=headers,
                                params={"path": PROBE_PATH},
                                timeout=HTTP_TIMEOUT)
                retrieval_latency_ms = (time.perf_counter() - t1) * 1000
                retrieval_http_code = r.status_code
                if r.status_code == 200:
                    try:
                        n_entries = count_entries(r.json())
                        retrieval_ok = True
                    except ValueError:
                        if status == "OK":
                            status = "FAIL"
                            error_message = "Retrieval response not JSON"
                else:
                    if status == "OK":
                        status = "FAIL"
                        error_message = f"Retrieval HTTP {r.status_code}"
            except requests.exceptions.Timeout:
                retrieval_latency_ms = (time.perf_counter() - t1) * 1000
                if status == "OK":
                    status = "FAIL"
                    error_message = f"Retrieval timeout after {HTTP_TIMEOUT}s"
            except Exception as e:
                retrieval_latency_ms = (time.perf_counter() - t1) * 1000
                if status == "OK":
                    status = "FAIL"
                    error_message = f"Retrieval {type(e).__name__}: {str(e)[:150]}"

    # ---- Latency check (solo se tutto e' andato) ----
    if status == "OK":
        worst_latency = max(
            [x for x in (auth_latency_ms, retrieval_latency_ms)
             if x is not None],
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
        "service_provider_log": "iride_data_access",
        "service_name": "iride-cyberitaly",
        "event_type": "data_access_availability_probe",
        "response_status": status,
        "hostname": hostname,
        "endpoint": SFTPGO_BASE_URL,
        "error_message": error_message,
        "latency_threshold_ok_ms": LATENCY_OK_MS,
        "latency_threshold_slow_ms": LATENCY_SLOW_MS,

        # Livello A — auth
        "auth_http_code": auth_http_code,
        "auth_latency_ms": (round(auth_latency_ms, 2)
                            if auth_latency_ms is not None else None),
        "auth_ok": auth_ok,

        # Livello B — retrieval reale
        "probe_path": PROBE_PATH,
        "retrieval_http_code": retrieval_http_code,
        "retrieval_latency_ms": (round(retrieval_latency_ms, 2)
                                 if retrieval_latency_ms is not None else None),
        "retrieval_ok": retrieval_ok,
        "n_entries": n_entries,
    }

    write_log(entry)

    # ---- Console output ----
    logger.info(f"[AUTH] {auth_http_code} in "
                f"{auth_latency_ms:.0f}ms ok={auth_ok}"
                if auth_latency_ms is not None
                else f"[AUTH] errore: {error_message}")
    if auth_ok:
        logger.info(f"[RETRIEVAL] {retrieval_http_code} in "
                    f"{retrieval_latency_ms:.0f}ms ok={retrieval_ok} "
                    f"entries={n_entries}"
                    if retrieval_latency_ms is not None
                    else "[RETRIEVAL] nessuna risposta")

    if status == "OK":
        logger.info("[RESULT] OK — auth + retrieval dati entrambi disponibili")
    elif status == "SLOW":
        logger.warning(f"[RESULT] SLOW — {error_message}")
    else:
        logger.error(f"[RESULT] FAIL — {error_message}")

    ship_to_elastic(entry)

    logger.info("[FINE]")
    return 0 if status == "OK" else 1


if __name__ == "__main__":
    sys.exit(main())
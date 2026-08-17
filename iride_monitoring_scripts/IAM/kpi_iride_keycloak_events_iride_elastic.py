"""
kpi_iride_keycloak_events_iride_elastic.py
============================================
Monitoring eventi login Keycloak IRIDE (Login / Login Error).

Reference: CIMS-38 (Davide Foschi, CGI).
Endpoint pattern:
  GET /admin/realms/cyberitaly/events?type=LOGIN&max=100&client=webapp-client
  GET /admin/realms/cyberitaly/events?type=LOGIN_ERROR&max=100&client=webapp-client

Cosa fa:
  1. Login OAuth2 su Keycloak (grant_type=password con account monitoring)
  2. Recupera eventi LOGIN + LOGIN_ERROR nella finestra di lookback (paginati)
  3. Emette 1 doc per evento su Elastic (event_type=keycloak_event) con _id
     deterministico -> idempotente, nessun duplicato tra run sovrapposte
  4. Emette 1 doc riepilogo con conteggi + tempi (event_type=keycloak_events_summary)

Indice Elastic target (suggerito):
  logs-iride-keycloak-events.monitoring-default

Pattern cron consigliato: ogni 5-10 min (LOOKBACK_MINUTES >= 2x intervallo cron).

CHANGELOG v2:
  - @timestamp = ora REALE dell'evento Keycloak (era l'ora della run:
    tutti gli eventi si impilavano sul minuto del cron)
  - ingest_timestamp = ora della run (conservata per debug pipeline)
  - _id deterministico (sha1) + bulk index -> riprocessare la stessa
    finestra sovrascrive invece di duplicare
  - Bulk API: 1 chiamata HTTP ogni BULK_SIZE doc invece di 1 per doc
  - Paginazione first/max + cutoff su LOOKBACK_MINUTES: nessun evento perso
    se il traffico supera EVENTS_MAX tra due run
  - Token refresh se scade durante la paginazione

Mapping richiesto lato index template (altrimenti i pannelli Terms non funzionano):
  kc_event_type, error, ip_address, client_id_kc, user_id, session_id -> keyword
  details -> flattened   (Keycloak ci mette chiavi dinamiche: mapping explosion)
"""

import configparser
import datetime as dt
import hashlib
import json
import logging
import socket
import ssl
import sys
import time
import urllib.request
import urllib.error
import urllib3
import warnings
from pathlib import Path

import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

# ============================================================
# CONFIG
# ============================================================
SCRIPT_DIR = Path(__file__).resolve().parent
INI_PATH = SCRIPT_DIR / "kpi_iride_keycloak_events_iride_elastic.ini"

config = configparser.ConfigParser()
if not INI_PATH.exists():
    print(f"FATAL: INI non trovato a {INI_PATH}", file=sys.stderr)
    sys.exit(1)
config.read(INI_PATH)

# Elastic
ELASTIC_ENABLED = config.getboolean('CONFIG', 'ELASTIC_ENABLED', fallback=False)
MONITORING_URL = config.get('CONFIG', 'MONITORING_URL', fallback='')
MONITORING_APIKEY = config.get('CONFIG', 'MONITORING_APIKEY', fallback='')
MONITORING_VERIFY_CERTS = config.getboolean('CONFIG',
                                            'MONITORING_VERIFY_CERTS',
                                            fallback=False)
MONITORING_INDEX = config.get(
    'CONFIG', 'MONITORING_INDEX',
    fallback='logs-iride-keycloak-events.monitoring-default')
BULK_SIZE = config.getint('CONFIG', 'BULK_SIZE', fallback=500)
ELASTIC_TIMEOUT = config.getint('CONFIG', 'ELASTIC_TIMEOUT_SEC', fallback=30)

LOG_FILE_NAME = config.get(
    'CONFIG', 'LOG_FILE_NAME',
    fallback='kpi_iride_keycloak_events_iride_elastic.log')

SERVICE_NAME_FIELD = config.get('CONFIG', 'SERVICE_NAME_FIELD',
                                fallback='iride-cyberitaly')

# Keycloak
KEYCLOAK_URL = config.get('KEYCLOAK', 'KEYCLOAK_URL').rstrip('/')
REALM_NAME = config.get('KEYCLOAK', 'REALM_NAME')
CLIENT_ID = config.get('KEYCLOAK', 'CLIENT_ID')
CLIENT_SECRET = config.get('KEYCLOAK', 'CLIENT_SECRET', fallback='')
ADMIN_USERNAME = config.get('KEYCLOAK', 'ADMIN_USERNAME')
ADMIN_PASSWORD = config.get('KEYCLOAK', 'ADMIN_PASSWORD')
TARGET_CLIENT = config.get('KEYCLOAK', 'TARGET_CLIENT',
                           fallback='webapp-client')
EVENT_TYPES = [t.strip() for t in config.get(
    'KEYCLOAK', 'EVENT_TYPES', fallback='LOGIN,LOGIN_ERROR').split(',')
    if t.strip()]
EVENTS_PAGE_SIZE = config.getint('KEYCLOAK', 'EVENTS_PAGE_SIZE', fallback=100)
EVENTS_MAX_PAGES = config.getint('KEYCLOAK', 'EVENTS_MAX_PAGES', fallback=50)
LOOKBACK_MINUTES = config.getint('KEYCLOAK', 'LOOKBACK_MINUTES', fallback=30)
KEYCLOAK_TIMEOUT = config.getint('KEYCLOAK', 'TIMEOUT_SEC', fallback=60)

# ============================================================
# LOGGING
# ============================================================
script_name = Path(__file__).stem
hostname = socket.gethostname()

logger = logging.getLogger(script_name)
logger.setLevel(logging.INFO)
logger.handlers.clear()
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] %(message)s'))
logger.addHandler(console_handler)
logger.propagate = False

log_file_path = SCRIPT_DIR / LOG_FILE_NAME
json_log_handler = logging.FileHandler(log_file_path, mode='a',
                                       encoding='utf-8')
json_log_handler.setFormatter(logging.Formatter('%(message)s'))
json_logger = logging.getLogger(f"{script_name}_json")
json_logger.setLevel(logging.INFO)
json_logger.handlers.clear()
json_logger.addHandler(json_log_handler)
json_logger.propagate = False

logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('elasticsearch').setLevel(logging.WARNING)


def write_log(doc):
    json_logger.info(json.dumps(doc, default=str))


# ============================================================
# ELASTIC — BULK
# ============================================================
_ssl_ctx = None
if not MONITORING_VERIFY_CERTS:
    _ssl_ctx = ssl.create_default_context()
    _ssl_ctx.check_hostname = False
    _ssl_ctx.verify_mode = ssl.CERT_NONE

_bulk_buffer = []          # lista di (doc_id | None, doc)
_bulk_stats = {'ok': 0, 'failed': 0, 'batches': 0}


def _post_bulk(payload_lines):
    """POST NDJSON su _bulk. Ritorna (ok_count, failed_count)."""
    body = ("\n".join(payload_lines) + "\n").encode('utf-8')
    url = f"{MONITORING_URL.rstrip('/')}/{MONITORING_INDEX}/_bulk"
    req = urllib.request.Request(
        url, data=body,
        headers={
            'Authorization': f'ApiKey {MONITORING_APIKEY}',
            'Content-Type': 'application/x-ndjson',
        }, method='POST')

    last_err = None
    for attempt in (1, 2):
        try:
            with urllib.request.urlopen(req, timeout=ELASTIC_TIMEOUT,
                                        context=_ssl_ctx) as resp:
                raw = resp.read().decode('utf-8', errors='replace')
            result = json.loads(raw)
            if not result.get('errors'):
                return len(result.get('items', [])), 0
            ok = failed = 0
            first_reason = None
            for item in result.get('items', []):
                action = next(iter(item.values()))
                if action.get('status', 500) < 300:
                    ok += 1
                else:
                    failed += 1
                    if first_reason is None:
                        first_reason = action.get('error', {}).get('reason')
            logger.warning(f"[ELASTIC] bulk parziale: {failed} ko — "
                           f"primo errore: {first_reason}")
            return ok, failed
        except Exception as e:
            last_err = e
            if attempt == 1:
                time.sleep(2)

    logger.error(f"[ELASTIC] bulk fallito: "
                 f"{type(last_err).__name__}: {last_err}")
    return 0, len(payload_lines) // 2


def _flush_bulk():
    global _bulk_buffer
    if not _bulk_buffer:
        return
    if not ELASTIC_ENABLED:
        _bulk_buffer = []
        return

    lines = []
    for doc_id, doc in _bulk_buffer:
        meta = {'index': {}}
        if doc_id:
            meta['index']['_id'] = doc_id
        lines.append(json.dumps(meta))
        lines.append(json.dumps(doc, default=str))

    ok, failed = _post_bulk(lines)
    _bulk_stats['ok'] += ok
    _bulk_stats['failed'] += failed
    _bulk_stats['batches'] += 1
    _bulk_buffer = []


def ship(doc, doc_id=None):
    """Accoda un doc: file NDJSON sempre, Elastic in bulk."""
    write_log(doc)
    _bulk_buffer.append((doc_id, doc))
    if len(_bulk_buffer) >= BULK_SIZE:
        _flush_bulk()


# ============================================================
# KEYCLOAK
# ============================================================
_access_token = None
_token_expiry = 0
_login_status = 'OK'
_login_time_sec = 0


def get_access_token():
    global _access_token, _token_expiry, _login_status, _login_time_sec

    token_url = (f"{KEYCLOAK_URL}/realms/{REALM_NAME}"
                 f"/protocol/openid-connect/token")
    data = {
        'grant_type': 'password',
        'client_id': CLIENT_ID,
        'username': ADMIN_USERNAME,
        'password': ADMIN_PASSWORD,
    }
    if CLIENT_SECRET:
        data['client_secret'] = CLIENT_SECRET

    start = time.time()
    try:
        r = requests.post(token_url, data=data, timeout=KEYCLOAK_TIMEOUT,
                          verify=MONITORING_VERIFY_CERTS)
        _login_time_sec = round(time.time() - start, 3)
    except Exception as e:
        _login_time_sec = round(time.time() - start, 3)
        _login_status = 'NOK'
        logger.error(f"[KEYCLOAK] login error: {type(e).__name__}: {e}")
        return None

    if r.status_code != 200:
        _login_status = 'NOK'
        logger.error(f"[KEYCLOAK] login HTTP {r.status_code}: {r.text[:200]}")
        return None

    try:
        j = r.json()
        _access_token = j['access_token']
        _token_expiry = time.time() + j.get('expires_in', 60) - 10
        _login_status = 'OK'
        logger.info(f"[KEYCLOAK] login OK ({_login_time_sec}s)")
        return _access_token
    except (KeyError, ValueError) as e:
        _login_status = 'NOK'
        logger.error(f"[KEYCLOAK] parse token error: {e}")
        return None


def get_auth_headers():
    """Token valido, rinnovato se scaduto durante la paginazione."""
    if time.time() >= _token_expiry:
        get_access_token()
    return {'Authorization': f'Bearer {_access_token}'}


def get_events(event_type, cutoff_ms):
    """
    Recupera eventi Keycloak (type + client), paginando finche' non si esce
    dalla finestra di lookback. Keycloak restituisce i piu' recenti per primi.
    """
    url = f"{KEYCLOAK_URL}/admin/realms/{REALM_NAME}/events"
    collected = []
    first = 0

    for page_num in range(EVENTS_MAX_PAGES):
        params = {
            'type': event_type,
            'client': TARGET_CLIENT,
            'first': first,
            'max': EVENTS_PAGE_SIZE,
        }
        try:
            r = requests.get(url, headers=get_auth_headers(), params=params,
                             timeout=KEYCLOAK_TIMEOUT,
                             verify=MONITORING_VERIFY_CERTS)
        except Exception as e:
            logger.error(f"[KEYCLOAK] events {event_type} error: "
                         f"{type(e).__name__}: {e}")
            return collected, 'NOK'

        if r.status_code != 200:
            logger.error(f"[KEYCLOAK] GET events type={event_type} -> "
                         f"HTTP {r.status_code}: {r.text[:200]}")
            return collected, 'NOK'

        try:
            page = r.json()
        except ValueError as e:
            logger.error(f"[KEYCLOAK] events {event_type} parse error: {e}")
            return collected, 'NOK'

        if not page:
            break

        reached_cutoff = False
        for ev in page:
            ev_ms = ev.get('time')
            if ev_ms is not None and ev_ms < cutoff_ms:
                reached_cutoff = True
                break
            collected.append(ev)

        if reached_cutoff or len(page) < EVENTS_PAGE_SIZE:
            break
        first += EVENTS_PAGE_SIZE
    else:
        logger.warning(f"[KEYCLOAK] {event_type}: raggiunto EVENTS_MAX_PAGES "
                       f"({EVENTS_MAX_PAGES}), possibili eventi non letti")

    return collected, 'OK'


def build_event_id(kc_event, event_type):
    """
    _id deterministico: la stessa run ripetuta sovrascrive invece di
    duplicare. Keycloak non espone un id evento, quindi si usa la
    combinazione di campi che identifica univocamente l'evento.
    """
    parts = [
        str(kc_event.get('realmId')),
        str(kc_event.get('time')),
        str(kc_event.get('userId')),
        str(kc_event.get('sessionId')),
        str(kc_event.get('clientId')),
        str(kc_event.get('ipAddress')),
        event_type,
        str(kc_event.get('error')),
    ]
    return hashlib.sha1("|".join(parts).encode('utf-8')).hexdigest()


def event_to_doc(kc_event, event_type, ingest_ts):
    """Trasforma un evento Keycloak in doc Elastic-ready DESP-style."""
    ev_time_ms = kc_event.get('time')
    ev_time_iso = None
    if ev_time_ms:
        ev_time_iso = dt.datetime.fromtimestamp(
            ev_time_ms / 1000, tz=dt.timezone.utc
        ).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

    return {
        # @timestamp = ora REALE dell'evento: e' l'asse X di ogni pannello
        "@timestamp": ev_time_iso or ingest_ts,
        "event_timestamp": ev_time_iso or ingest_ts,
        "ingest_timestamp": ingest_ts,
        "platform": "iride",
        "service_provider_log": "iride_keycloak",
        "service_name": SERVICE_NAME_FIELD,
        "event_type": "keycloak_event",
        "kc_event_type": event_type,
        "response_status": "OK",
        "hostname": hostname,

        # Campi Keycloak
        "kc_event_time_iso": ev_time_iso,
        "kc_event_time_ms": ev_time_ms,
        "realm_id": kc_event.get('realmId'),
        "realm_name": REALM_NAME,
        "client_id_kc": kc_event.get('clientId'),
        "user_id": kc_event.get('userId'),
        "session_id": kc_event.get('sessionId'),
        "ip_address": kc_event.get('ipAddress'),
        "error": kc_event.get('error'),
        "details": kc_event.get('details', {}) or {},
    }


# ============================================================
# MAIN
# ============================================================
def main():
    now = dt.datetime.now(dt.timezone.utc)
    ingest_ts = now.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    cutoff_ms = int((now.timestamp() - LOOKBACK_MINUTES * 60) * 1000)
    cutoff_iso = dt.datetime.fromtimestamp(
        cutoff_ms / 1000, tz=dt.timezone.utc).isoformat(timespec='seconds')

    logger.info("=" * 70)
    logger.info(f"[AVVIO] {script_name} — hostname={hostname}")
    logger.info(f"[CONFIG] Keycloak: {KEYCLOAK_URL}")
    logger.info(f"[CONFIG] Realm: {REALM_NAME}, Client target: {TARGET_CLIENT}")
    logger.info(f"[CONFIG] Event types: {', '.join(EVENT_TYPES)}")
    logger.info(f"[CONFIG] Lookback: {LOOKBACK_MINUTES} min (da {cutoff_iso})")
    if ELASTIC_ENABLED:
        logger.info(f"[CONFIG] Elastic: {MONITORING_URL} -> {MONITORING_INDEX} "
                    f"(bulk={BULK_SIZE})")
    else:
        logger.info("[CONFIG] >>> DRY-RUN (no Elastic) <<<")
    logger.info("=" * 70)

    total_start = time.time()

    # 1. Login
    if not get_access_token():
        err_doc = {
            "@timestamp": ingest_ts,
            "event_timestamp": ingest_ts,
            "ingest_timestamp": ingest_ts,
            "platform": "iride",
            "service_provider_log": "iride_keycloak",
            "service_name": SERVICE_NAME_FIELD,
            "event_type": "keycloak_events_summary",
            "response_status": "ERROR",
            "login_status": "NOK",
            "login_time": _login_time_sec,
            "hostname": hostname,
            "error_detail": "login failed, see logs",
        }
        ship(err_doc)
        _flush_bulk()
        logger.error("[FINE] login fallito, abort")
        return 1

    # 2. Fetch + ship per ogni tipo evento
    counts = {}
    fetch_status = 'OK'
    events_shipped = 0

    for ev_type in EVENT_TYPES:
        logger.info(f"[KEYCLOAK] recupero eventi {ev_type}...")
        events, status = get_events(ev_type, cutoff_ms)
        if status != 'OK':
            fetch_status = 'PARTIAL'
        counts[ev_type] = len(events)
        logger.info(f"[KEYCLOAK] {ev_type} events: {len(events)}")

        for ev in events:
            doc = event_to_doc(ev, ev_type, ingest_ts)
            ship(doc, doc_id=build_event_id(ev, ev_type))
            events_shipped += 1

    _flush_bulk()
    total_elapsed = round(time.time() - total_start, 2)

    # 3. Doc summary aggregato (senza _id: e' una metrica di run)
    login_count = counts.get('LOGIN', 0)
    login_error_count = counts.get('LOGIN_ERROR', 0)
    denom = login_count + login_error_count
    error_rate = round(login_error_count / denom, 4) if denom else 0.0

    summary_doc = {
        "@timestamp": ingest_ts,
        "event_timestamp": ingest_ts,
        "ingest_timestamp": ingest_ts,
        "platform": "iride",
        "service_provider_log": "iride_keycloak",
        "service_name": SERVICE_NAME_FIELD,
        "event_type": "keycloak_events_summary",
        "response_status": "OK" if fetch_status == 'OK' else "PARTIAL",
        "hostname": hostname,
        "realm_name": REALM_NAME,
        "target_client": TARGET_CLIENT,
        "lookback_minutes": LOOKBACK_MINUTES,
        "login_events_count": login_count,
        "login_error_events_count": login_error_count,
        "login_error_rate": error_rate,
        "total_events_shipped": events_shipped,
        "elastic_docs_ok": _bulk_stats['ok'],
        "elastic_docs_failed": _bulk_stats['failed'],
        "elastic_bulk_batches": _bulk_stats['batches'],
        "response_time": total_elapsed,
        "login_time": _login_time_sec,
        "login_status": _login_status,
    }
    ship(summary_doc)
    _flush_bulk()

    logger.info(f"[SUMMARY] LOGIN={login_count} "
                f"LOGIN_ERROR={login_error_count} "
                f"error_rate={error_rate} shipped={events_shipped} "
                f"elastic_ok={_bulk_stats['ok']} "
                f"elastic_ko={_bulk_stats['failed']} "
                f"runtime={total_elapsed}s")
    logger.info("[FINE]")
    return 0 if fetch_status == 'OK' else 2


if __name__ == "__main__":
    sys.exit(main())

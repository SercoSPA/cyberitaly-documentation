"""
kpi_insula_awareness_storage_iride_elastic.py
=============================================
Collector KPI Awareness (wallet crediti + storage) da Insula API per il
monitoring IRIDE.

Risponde alle metriche di consumo D-100:
  - Crediti residui per utente / totali di piattaforma
  - Storage occupato vs quota per utente
  - Utenti vicini/oltre la quota storage

Endpoint usati (da CIMS-39):
  - POST identity.iride-cyberitaly.space/realms/cyberitaly/.../token
  - GET  iride-cyberitaly.space/secure/api/v2.0/wallets?size=100        (paginato)
  - GET  iride-cyberitaly.space/secure/api/v2.0/quotas?size=100         (paginato)
  - GET  iride-cyberitaly.space/secure/api/v2.0/reports/storage/{id}/CSV (1/utente)

Logica (da CIMS-39, "Putting it together"):
  1. /wallets  -> lista utenti (id, name, role) + saldo crediti.
  2. /quotas   -> filtra usageType.name == "FILES_STORAGE_MB",
                  mappa override quota per owner.id (default 5000 MB).
  3. /reports/storage/{id}/CSV per ogni utente -> usa l'ultima riga (bytes)
                  -> converte in MB. Nessun endpoint bulk: 1 call/utente.

Output JSON in stile DESP:
  - 1 doc per utente          (event_type=insula_awareness_user)
  - 1 doc aggregato globale    (event_type=insula_awareness_aggregate)

Indice Elastic dedicato:
    metrics-iride-insula-awareness.monitoring-default

NOTA operativa: al 24/06 (CIMS-39) il microservizio "server" di Insula tornava
nginx 500 su /wallets. Se in modalita' reale vedi 500 con body <center>nginx</center>
NON e' il token: e' il backend giu' lato loro (CIMS-47/deploy). Un 401/403 invece
e' il token/scope. Il collector logga lo status e non crasha.
"""

import sys
import os
import csv
import json
import time
import socket
import logging
import configparser
import urllib3
import warnings
from datetime import datetime, timezone
from collections import Counter

import requests

# Silenzia rumore cosmetico
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
logging.getLogger('elasticsearch').setLevel(logging.WARNING)
logging.getLogger('elastic_transport').setLevel(logging.WARNING)


# ============================================================
# Config
# ============================================================
base_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_dir,
                           'kpi_insula_awareness_storage_iride_elastic.ini')
config = configparser.ConfigParser()
config.read(config_path)

ELASTIC_ENABLED = config.getboolean('CONFIG', 'ELASTIC_ENABLED', fallback=False)
MONITORING_URL = config.get('CONFIG', 'MONITORING_URL', fallback='')
MONITORING_APIKEY = config.get('CONFIG', 'MONITORING_APIKEY', fallback='')
MONITORING_VERIFY_CERTS = config.getboolean(
    'CONFIG', 'MONITORING_VERIFY_CERTS', fallback=True)
AWARENESS_INDEX = config.get(
    'CONFIG', 'AWARENESS_INDEX',
    fallback='metrics-iride-insula-awareness.monitoring-default')
MOCK_MODE = config.getboolean('CONFIG', 'MOCK_MODE', fallback=False)

INSULA_BASE_URL = config.get('INSULA', 'BASE_URL')

KEYCLOAK_URL = config.get('KEYCLOAK', 'URL')
KEYCLOAK_REALM = config.get('KEYCLOAK', 'REALM')
KEYCLOAK_CLIENT_ID = config.get('KEYCLOAK', 'CLIENT_ID')
KEYCLOAK_CLIENT_SECRET = config.get('KEYCLOAK', 'CLIENT_SECRET', fallback='')
KEYCLOAK_USERNAME = config.get('KEYCLOAK', 'USERNAME')
KEYCLOAK_PASSWORD = config.get('KEYCLOAK', 'PASSWORD')

PAGE_SIZE = config.getint('INSULA_AWARENESS', 'PAGE_SIZE', fallback=100)
MAX_PAGES = config.getint('INSULA_AWARENESS', 'MAX_PAGES', fallback=50)
DEFAULT_QUOTA_MB = config.getint('INSULA_AWARENESS', 'DEFAULT_QUOTA_MB',
                                 fallback=5000)
STORAGE_ALERT_PCT = config.getfloat('INSULA_AWARENESS', 'STORAGE_ALERT_PCT',
                                    fallback=80.0)
EMIT_INDIVIDUAL_USERS = config.getboolean(
    'INSULA_AWARENESS', 'EMIT_INDIVIDUAL_USERS', fallback=True)
INTER_CALL_DELAY = config.getfloat('INSULA_AWARENESS', 'INTER_CALL_DELAY',
                                   fallback=0.0)
HTTP_TIMEOUT = config.getint('INSULA_AWARENESS', 'HTTP_TIMEOUT', fallback=30)

# Lista esplicita account test/servizio (match esatto, case-insensitive)
KNOWN_TEST_ACCOUNTS = {
    u.strip().lower()
    for u in config.get('INSULA_AWARENESS', 'KNOWN_TEST_ACCOUNTS',
                        fallback='').split(',')
    if u.strip()
}

BYTES_PER_MB = 1048576  # 1024*1024 (CIMS-39)

logger.info(f"[AVVIO] {script_name} — hostname={hostname}")
logger.info(f"[CONFIG] Insula base: {INSULA_BASE_URL}")
logger.info(f"[CONFIG] Keycloak realm: {KEYCLOAK_REALM}")
logger.info(f"[CONFIG] Quota default: {DEFAULT_QUOTA_MB} MB — "
            f"soglia alert: {STORAGE_ALERT_PCT}%")
if MOCK_MODE:
    logger.info("[CONFIG] >>> MOCK MODE attivo <<<")
if not ELASTIC_ENABLED:
    logger.info("[CONFIG] >>> DRY-RUN MODE (no Elastic) <<<")
else:
    logger.info(f"[CONFIG] Elastic ENABLED → {AWARENESS_INDEX}")
logger.info("")


# ============================================================
# Mock data
# ============================================================
def mock_wallets():
    """Wallet finti dei 4 DT + un utente test."""
    return [
        {"id": 1, "balance": 8200,
         "owner": {"id": "u-arpav", "name": "user-arpa-veneto", "role": "USER"}},
        {"id": 2, "balance": 450,
         "owner": {"id": "u-arpae", "name": "user-arpae-er", "role": "USER"}},
        {"id": 3, "balance": 12000,
         "owner": {"id": "u-cmcc", "name": "cmcc-dev", "role": "POWER_USER"}},
        {"id": 4, "balance": 30,
         "owner": {"id": "u-ispra", "name": "user-ispra", "role": "USER"}},
        {"id": 5, "balance": 999999,
         "owner": {"id": "u-test", "name": "user-test", "role": "ADMIN"}},
    ]


def mock_quotas():
    """Solo utenti con override (CIMS-39: gli altri hanno il default)."""
    return [
        {"id": 91, "value": 20000,
         "usageType": {"name": "FILES_STORAGE_MB", "defaultValue": 5000},
         "owner": {"id": "u-cmcc", "name": "cmcc-dev"}},
        {"id": 92, "value": 2000,
         "usageType": {"name": "FILES_STORAGE_MB", "defaultValue": 5000},
         "owner": {"id": "u-ispra", "name": "user-ispra"}},
        # rumore da scartare (usageType != FILES_STORAGE_MB)
        {"id": 93, "value": 10,
         "usageType": {"name": "MAX_RUNNABLE_JOBS", "defaultValue": 5},
         "owner": {"id": "u-arpav", "name": "user-arpa-veneto"}},
    ]


def mock_storage_csv(user_id):
    """CSV finto in bytes, ultima riga = valore corrente."""
    fixtures = {
        "u-arpav": 1382408350,   # ~1318 MB su 5000 default
        "u-arpae": 4718592000,   # ~4500 MB su 5000 default -> ~90%
        "u-cmcc": 5242880000,    # ~5000 MB su 20000 override
        "u-ispra": 2000000000,   # ~1907 MB su 2000 override -> ~95%
        "u-test": 104857600,     # ~100 MB
    }
    used = fixtures.get(user_id, 0)
    today = datetime.now(timezone.utc).date().isoformat()
    return f"date,usage\n{today},{used}\n"


# ============================================================
# Helpers
# ============================================================
def write_log(entry):
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(json.dumps(entry, ensure_ascii=False, default=str) + '\n')


def ship_to_elastic(entry):
    if not ELASTIC_ENABLED:
        return
    try:
        url = f"{MONITORING_URL.rstrip('/')}/{AWARENESS_INDEX}/_doc"
        headers = {'Authorization': f'ApiKey {MONITORING_APIKEY}',
                   'Content-Type': 'application/json'}
        r = requests.post(url, headers=headers, json=entry,
                          verify=MONITORING_VERIFY_CERTS, timeout=30)
        if r.status_code not in (200, 201):
            logger.error(f"[ELASTIC] HTTP {r.status_code}: {r.text[:200]}")
    except Exception as e:
        logger.error(f"[ELASTIC] {type(e).__name__}: {e}")


def classify_owner(name):
    """is_test deterministico: match ESATTO contro la lista in .ini.
    Niente euristiche sul nome (cmcc-dev/predev/test sono utenti CMCC reali)."""
    return (name or "").strip().lower() in KNOWN_TEST_ACCOUNTS


def get_keycloak_token(session):
    if MOCK_MODE:
        return "MOCK_TOKEN"
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
            return None
        return r.json().get("access_token")
    except Exception as e:
        logger.error(f"[AUTH] {type(e).__name__}: {e}")
        return None


def fetch_paginated(session, headers, path, label, mock_provider=None):
    """Itera tutte le pagine di un endpoint HATEOAS (?size=&page=)."""
    if MOCK_MODE and mock_provider is not None:
        return mock_provider()

    all_items = []
    page = 0
    while page < MAX_PAGES:
        try:
            r = session.get(
                f"{INSULA_BASE_URL}{path}",
                headers=headers,
                params={"size": PAGE_SIZE, "page": page},
                timeout=HTTP_TIMEOUT,
            )
            if r.status_code != 200:
                logger.error(f"[{label}] HTTP {r.status_code}: {r.text[:200]}")
                break
            data = r.json()
            items = (data.get("_embedded", {}).get(label.lower())
                     or data.get("content")
                     or (data if isinstance(data, list) else []))
            if not items:
                break
            all_items.extend(items)
            page_info = data.get("page", {}) if isinstance(data, dict) else {}
            total_pages = page_info.get("totalPages", 1)
            page += 1
            if page >= total_pages:
                break
        except Exception as e:
            logger.error(f"[{label}] {type(e).__name__}: {e}")
            break
    return all_items


def fetch_storage_used_mb(session, headers, user_id, day_iso):
    """GET /reports/storage/{id}/CSV -> (used_mb, report_date) o (None, None)."""
    if MOCK_MODE:
        raw = mock_storage_csv(user_id)
    else:
        try:
            r = session.get(
                f"{INSULA_BASE_URL}/reports/storage/{user_id}/CSV",
                headers=headers,
                params={"startDateTime": day_iso, "endDateTime": day_iso},
                timeout=HTTP_TIMEOUT,
            )
            if r.status_code != 200:
                logger.error(f"[STORAGE] user={user_id} HTTP "
                             f"{r.status_code}: {r.text[:120]}")
                return None, None
            raw = r.text
        except Exception as e:
            logger.error(f"[STORAGE] user={user_id} {type(e).__name__}: {e}")
            return None, None

    # Parsing CSV: ultima riga dati = valore corrente (CIMS-39)
    rows = [row for row in csv.reader(raw.splitlines()) if row]
    data_rows = [r for r in rows if r and not r[0].strip().lower().startswith("date")]
    if not data_rows:
        return None, None
    last = data_rows[-1]
    try:
        report_date = last[0].strip()
        used_bytes = float(last[1].strip())
        return round(used_bytes / BYTES_PER_MB, 2), report_date
    except (IndexError, ValueError):
        logger.error(f"[STORAGE] user={user_id} riga CSV non parsabile: {last}")
        return None, None


# ============================================================
# Main
# ============================================================
def main():
    now = datetime.now(timezone.utc)
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    day_iso = f"{now.date().isoformat()}T00:00:00Z"  # snapshot di oggi

    session = requests.Session()

    # Auth
    token = get_keycloak_token(session)
    if not token:
        logger.error("[FINE] Token non ottenuto, abort.")
        return 1
    logger.info(f"[AUTH] token OK: {token[:30]}...")
    headers = {"Authorization": f"Bearer {token}"}

    # 1. Wallets -> lista utenti + crediti
    wallets = fetch_paginated(session, headers, "/wallets", "Wallets",
                              mock_provider=mock_wallets)
    logger.info(f"[WALLETS] {len(wallets)} wallet recuperati")

    # 2. Quotas -> mappa override storage per owner.id
    quotas = fetch_paginated(session, headers, "/quotas", "Quotas",
                             mock_provider=mock_quotas)
    override_map = {}
    for q in quotas:
        ut = q.get("usageType", {}) or {}
        if ut.get("name") != "FILES_STORAGE_MB":
            continue  # scarta MAX_RUNNABLE_JOBS ecc. (CIMS-39)
        owner_id = (q.get("owner", {}) or {}).get("id")
        if owner_id is not None:
            override_map[owner_id] = q.get("value")
    logger.info(f"[QUOTAS] {len(override_map)} override FILES_STORAGE_MB")

    # 3. Per ogni utente: storage usato + calcolo KPI
    user_docs = []
    n_storage_ok = 0
    n_storage_fail = 0

    for w in wallets:
        owner = w.get("owner", {}) or {}
        uid = owner.get("id")
        uname = owner.get("name")
        role = owner.get("role")
        credits = w.get("balance")

        used_mb, report_date = fetch_storage_used_mb(
            session, headers, uid, day_iso)
        if used_mb is None:
            n_storage_fail += 1
        else:
            n_storage_ok += 1

        quota_override = override_map.get(uid)
        quota_mb = quota_override if quota_override is not None else DEFAULT_QUOTA_MB
        used_pct = (round(used_mb / quota_mb * 100, 2)
                    if (used_mb is not None and quota_mb) else None)

        doc = {
            "@timestamp": now_iso,
            "event_timestamp": now_iso,
            "platform": "iride",
            "service_provider_log": "iride_insula_awareness",
            "event_type": "insula_awareness_user",
            "hostname": hostname,
            "endpoint": INSULA_BASE_URL,
            "user_id": uid,
            "username": uname,
            "role": role,
            "is_test": classify_owner(uname),
            "credits_balance": credits,
            "storage_quota_mb": quota_mb,
            "storage_quota_is_override": quota_override is not None,
            "storage_used_mb": used_mb,
            "storage_used_pct": used_pct,
            "storage_report_date": report_date,
            "storage_over_threshold": (used_pct is not None
                                       and used_pct >= STORAGE_ALERT_PCT),
            "mock_mode": MOCK_MODE,
        }
        user_docs.append(doc)

        if EMIT_INDIVIDUAL_USERS:
            write_log(doc)
            ship_to_elastic(doc)

        logger.info(
            f"  {uname:<20} crediti={credits!s:>8}  "
            f"storage={used_mb if used_mb is not None else '?'}"
            f"/{quota_mb} MB  "
            f"({used_pct if used_pct is not None else '?'}%)"
            f"{'  ⚠ OVER' if doc['storage_over_threshold'] else ''}")

        if INTER_CALL_DELAY > 0:
            time.sleep(INTER_CALL_DELAY)

    # 4. Aggregato globale
    real = [d for d in user_docs if not d["is_test"]]
    total_credits = sum(d["credits_balance"] or 0 for d in real)
    total_used = sum(d["storage_used_mb"] or 0 for d in real)
    total_quota = sum(d["storage_quota_mb"] or 0 for d in real)
    n_over_thr = sum(1 for d in real if d["storage_over_threshold"])
    n_over_quota = sum(1 for d in real
                       if d["storage_used_pct"] is not None
                       and d["storage_used_pct"] >= 100)
    roles = Counter(d["role"] for d in user_docs)

    agg = {
        "@timestamp": now_iso,
        "event_timestamp": now_iso,
        "platform": "iride",
        "service_provider_log": "iride_insula_awareness",
        "event_type": "insula_awareness_aggregate",
        "hostname": hostname,
        "endpoint": INSULA_BASE_URL,
        "total_users": len(user_docs),
        "total_users_real": len(real),
        "total_credits": total_credits,
        "total_storage_used_mb": round(total_used, 2),
        "total_storage_quota_mb": total_quota,
        "storage_fill_pct": (round(total_used / total_quota * 100, 2)
                             if total_quota else None),
        "n_users_over_threshold": n_over_thr,
        "n_users_over_quota": n_over_quota,
        "n_storage_report_ok": n_storage_ok,
        "n_storage_report_fail": n_storage_fail,
        "roles_breakdown": dict(roles),
        "alert_threshold_pct": STORAGE_ALERT_PCT,
        "mock_mode": MOCK_MODE,
    }
    write_log(agg)
    ship_to_elastic(agg)

    logger.info("")
    logger.info(f"[REPORT] utenti={len(user_docs)} (real={len(real)})  "
                f"crediti_tot={total_credits}  "
                f"storage={round(total_used, 1)}/{total_quota} MB  "
                f"over_soglia={n_over_thr}  over_quota={n_over_quota}  "
                f"csv_fail={n_storage_fail}")
    logger.info("[FINE]")
    return 0


if __name__ == "__main__":
    sys.exit(main())


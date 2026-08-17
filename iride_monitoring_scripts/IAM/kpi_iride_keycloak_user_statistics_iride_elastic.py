"""
kpi_iride_keycloak_user_statistics_iride_elastic.py
=====================================================
Monitoring statistiche utenti IAM Keycloak IRIDE CyberItaly.

Consolidato da 2 script forniti da Davide Foschi (CGI, CIMS-38):
  - user_statistics.py                   → conteggi globali + sessioni attive
  - user_statistics_country_userprofile.py → breakdown per country/profile/gender

Cosa fa:
  1. Login OAuth2 su Keycloak (grant_type=password con account monitoring)
  2. Recupera lista TUTTI gli utenti del realm
  3. Aggrega: totali, federati vs registrati, DPAD_Services members
  4. Recupera sessioni attive attraverso tutti i client
  5. Break down per Country, userProfile, Gender
  6. Emette 2 tipi di doc su Elastic:
     - 1 doc "summary" con totali globali (event_type=keycloak_user_statistics)
     - N doc "breakdown" per category/value (event_type=keycloak_user_breakdown)

Indice Elastic target (suggerito):
  logs-iride-keycloak-users.monitoring-default

Pattern cron consigliato: giornaliero (utenti crescono lento, no scan orario).

Fix stile IRIDE rispetto agli script Davide:
  - Config in INI
  - Silenziamento logger noisy (requests, urllib3, elasticsearch)
  - Ingestion diretta Elastic (client urllib.request, no lib pesante)
  - Fallback file .log NDJSON se Elastic ELASTIC_ENABLED=False
  - Timestamp coerente @timestamp in prima posizione
"""

import configparser
import datetime as dt
import json
import logging
import os
import socket
import sys
import time
import urllib3
import warnings
from pathlib import Path

import requests

# Silenzia rumore cosmetico
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

# ============================================================
# CONFIG
# ============================================================
SCRIPT_DIR = Path(__file__).resolve().parent
INI_PATH = SCRIPT_DIR / "kpi_iride_keycloak_user_statistics_iride_elastic.ini"

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
    fallback='logs-iride-keycloak-users.monitoring-default')

LOG_FILE_NAME = config.get(
    'CONFIG', 'LOG_FILE_NAME',
    fallback='kpi_iride_keycloak_user_statistics_iride_elastic.log')

SERVICE_NAME_FIELD = config.get('CONFIG', 'SERVICE_NAME_FIELD',
                                fallback='iride-cyberitaly')

# Keycloak
KEYCLOAK_URL = config.get('KEYCLOAK', 'KEYCLOAK_URL')
REALM_NAME = config.get('KEYCLOAK', 'REALM_NAME')
CLIENT_ID = config.get('KEYCLOAK', 'CLIENT_ID')
CLIENT_SECRET = config.get('KEYCLOAK', 'CLIENT_SECRET', fallback='')
ADMIN_USERNAME = config.get('KEYCLOAK', 'ADMIN_USERNAME')
ADMIN_PASSWORD = config.get('KEYCLOAK', 'ADMIN_PASSWORD')
DPAD_SERVICES_GROUP_ID = config.get('KEYCLOAK', 'DPAD_SERVICES_GROUP_ID',
                                    fallback='')
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

log_file_path = SCRIPT_DIR / LOG_FILE_NAME
json_log_handler = logging.FileHandler(log_file_path, mode='a',
                                       encoding='utf-8')
json_log_handler.setFormatter(logging.Formatter('%(message)s'))
json_logger = logging.getLogger(f"{script_name}_json")
json_logger.setLevel(logging.INFO)
json_logger.handlers.clear()
json_logger.addHandler(json_log_handler)
json_logger.propagate = False

# Silenzia librerie noisy
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('elasticsearch').setLevel(logging.WARNING)


def write_log(doc):
    json_logger.info(json.dumps(doc, default=str))


def ship_to_elastic(doc):
    if not ELASTIC_ENABLED:
        return
    try:
        import urllib.request
        url = f"{MONITORING_URL.rstrip('/')}/{MONITORING_INDEX}/_doc"
        body = json.dumps(doc).encode('utf-8')
        req = urllib.request.Request(
            url, data=body,
            headers={
                'Authorization': f'ApiKey {MONITORING_APIKEY}',
                'Content-Type': 'application/json',
            }, method='POST')
        if not MONITORING_VERIFY_CERTS:
            import ssl
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            urllib.request.urlopen(req, timeout=10, context=ctx)
        else:
            urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        logger.warning(f"[ELASTIC] ship failed: {type(e).__name__}: {e}")


# ============================================================
# KEYCLOAK TOKEN
# ============================================================
_access_token = None
_token_expiry = 0
_login_time_sec = 0
_login_status = 'OK'


def get_access_token():
    """OAuth2 password grant. Ritorna access_token o None se fallisce."""
    global _access_token, _token_expiry, _login_time_sec, _login_status

    token_url = f"{KEYCLOAK_URL}/realms/{REALM_NAME}/protocol/openid-connect/token"
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
        r = requests.post(token_url, data=data, timeout=KEYCLOAK_TIMEOUT)
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
        _token_expiry = time.time() + j.get('expires_in', 60) - 5
        _login_status = 'OK'
        logger.info(f"[KEYCLOAK] login OK ({_login_time_sec}s)")
        return _access_token
    except (KeyError, ValueError) as e:
        _login_status = 'NOK'
        logger.error(f"[KEYCLOAK] token parse error: {e}")
        return None


def get_auth_headers():
    """Ritorna headers con token valido, rinnovando se scaduto."""
    if time.time() >= _token_expiry:
        get_access_token()
    return {
        'Authorization': f'Bearer {_access_token}',
        'Content-Type': 'application/json',
    }


# ============================================================
# KEYCLOAK API CALLS
# ============================================================
def kc_get(url, params=None):
    """GET su Keycloak con retry token. Ritorna JSON o None."""
    try:
        r = requests.get(url, headers=get_auth_headers(),
                         params=params, timeout=KEYCLOAK_TIMEOUT)
        if r.status_code != 200:
            logger.error(f"[KEYCLOAK] GET {url.split('/')[-1]} → "
                         f"HTTP {r.status_code}: {r.text[:200]}")
            return None
        return r.json()
    except Exception as e:
        logger.error(f"[KEYCLOAK] GET error: {type(e).__name__}: {e}")
        return None


def get_dpad_members():
    """Recupera membri DPAD_Services con paginazione."""
    if not DPAD_SERVICES_GROUP_ID:
        return set()
    url = f"{KEYCLOAK_URL}/admin/realms/{REALM_NAME}/groups/{DPAD_SERVICES_GROUP_ID}/members"
    all_members = []
    first = 0
    page_size = 100
    while True:
        page = kc_get(url, params={'first': first, 'max': page_size})
        if not page:
            break
        all_members.extend(page)
        if len(page) < page_size:
            break
        first += page_size
    logger.info(f"[KEYCLOAK] DPAD_Services members: {len(all_members)}")
    return set(m['id'] for m in all_members)


def get_all_users(brief=False):
    """Recupera lista utenti realm. brief=True per performance quando basta l'ID."""
    url = f"{KEYCLOAK_URL}/admin/realms/{REALM_NAME}/users"
    params = {
        'max': 100000000,
        'briefRepresentation': str(brief).lower(),
    }
    users = kc_get(url, params=params)
    if users is None:
        return []
    return users


def get_active_sessions_total():
    """Somma sessioni attive per tutti i client."""
    url = f"{KEYCLOAK_URL}/admin/realms/{REALM_NAME}/clients"
    clients = kc_get(url)
    if not clients:
        return 0
    total = 0
    for c in clients:
        cid = c['id']
        sessions_url = (f"{KEYCLOAK_URL}/admin/realms/{REALM_NAME}"
                        f"/clients/{cid}/user-sessions")
        sessions = kc_get(sessions_url)
        if sessions:
            total += len(sessions)
    return total


# ============================================================
# MAIN
# ============================================================
def main():
    now = dt.datetime.now(dt.timezone.utc)
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%S.") + \
        f"{now.microsecond // 1000:03d}Z"

    logger.info("=" * 70)
    logger.info(f"[AVVIO] {script_name} — hostname={hostname}")
    logger.info(f"[CONFIG] Keycloak: {KEYCLOAK_URL}")
    logger.info(f"[CONFIG] Realm: {REALM_NAME}")
    logger.info(f"[CONFIG] Admin user: {ADMIN_USERNAME}")
    if ELASTIC_ENABLED:
        logger.info(f"[CONFIG] Elastic: {MONITORING_URL} → {MONITORING_INDEX}")
    else:
        logger.info("[CONFIG] >>> DRY-RUN (no Elastic) <<<")
    logger.info("=" * 70)

    total_start = time.time()

    # 1. Login
    if not get_access_token():
        # emetti doc di errore, exit
        err_doc = {
            "@timestamp": timestamp,
            "event_timestamp": timestamp,
            "platform": "iride",
            "service_provider_log": "iride_keycloak",
            "service_name": SERVICE_NAME_FIELD,
            "event_type": "keycloak_user_statistics",
            "response_status": "ERROR",
            "login_status": "NOK",
            "login_time": _login_time_sec,
            "hostname": hostname,
            "error_detail": "login failed, see logs",
        }
        write_log(err_doc)
        ship_to_elastic(err_doc)
        logger.error("[FINE] Login failed, aborting")
        return 1

    # 2. DPAD_Services members
    dpad_set = get_dpad_members()

    # 3. Lista utenti completa (brief=False per avere federatedIdentities + attributes)
    logger.info("[KEYCLOAK] recupero lista utenti...")
    users_start = time.time()
    users = get_all_users(brief=False)
    users_elapsed = round(time.time() - users_start, 2)
    logger.info(f"[KEYCLOAK] {len(users)} utenti recuperati in {users_elapsed}s")

    # 4. Aggrega contatori
    tot_users = len(users)
    reg_users = 0
    fed_users = 0
    dpad_users = 0
    country_count = {}
    user_profile_count = {}
    gender_count = {}

    for u in users:
        # federatedIdentities richiede briefRepresentation=false
        if u.get('federatedIdentities'):
            fed_users += 1
        else:
            reg_users += 1

        if u.get('id') in dpad_set:
            dpad_users += 1

        attrs = u.get('attributes', {}) or {}
        country = (attrs.get('Country', ['Not Defined']) or ['Not Defined'])[0]
        profile = (attrs.get('userProfile', ['Not Defined']) or ['Not Defined'])[0]
        gender = (attrs.get('Gender', ['Not Defined']) or ['Not Defined'])[0]
        country_count[country] = country_count.get(country, 0) + 1
        user_profile_count[profile] = user_profile_count.get(profile, 0) + 1
        gender_count[gender] = gender_count.get(gender, 0) + 1

    # 5. Sessioni attive
    logger.info("[KEYCLOAK] recupero sessioni attive...")
    active_sessions = get_active_sessions_total()
    logger.info(f"[KEYCLOAK] sessioni attive totali: {active_sessions}")

    total_elapsed = round(time.time() - total_start, 2)

    # 6. Doc summary
    summary_doc = {
        "@timestamp": timestamp,
        "event_timestamp": timestamp,
        "platform": "iride",
        "service_provider_log": "iride_keycloak",
        "service_name": SERVICE_NAME_FIELD,
        "event_type": "keycloak_user_statistics",
        "response_status": "OK",
        "hostname": hostname,
        "keycloak_url": KEYCLOAK_URL,
        "realm_name": REALM_NAME,
        "tot_users": tot_users,
        "federated_users": fed_users,
        "registered_users": reg_users,
        "dpadgroup_users": dpad_users,
        "actv_sessions": active_sessions,
        "response_time": total_elapsed,
        "login_time": _login_time_sec,
        "login_status": _login_status,
    }
    write_log(summary_doc)
    ship_to_elastic(summary_doc)

    logger.info(f"[SUMMARY] tot={tot_users} fed={fed_users} reg={reg_users} "
                f"dpad={dpad_users} sessions={active_sessions}")

    # 7. Doc breakdown (1 per key di ciascuna category)
    breakdown_docs_count = 0
    for category, counts in [
        ("country", country_count),
        ("userProfile", user_profile_count),
        ("gender", gender_count),
    ]:
        for key, count in sorted(counts.items()):
            doc = {
                "@timestamp": timestamp,
                "event_timestamp": timestamp,
                "platform": "iride",
                "service_provider_log": "iride_keycloak",
                "service_name": SERVICE_NAME_FIELD,
                "event_type": "keycloak_user_breakdown",
                "response_status": "OK",
                "hostname": hostname,
                "keycloak_url": KEYCLOAK_URL,
                "realm_name": REALM_NAME,
                "breakdown_category": category,
                "breakdown_value": key,
                "user_count": count,
            }
            write_log(doc)
            ship_to_elastic(doc)
            breakdown_docs_count += 1

    logger.info(f"[BREAKDOWN] emessi {breakdown_docs_count} doc "
                f"(country={len(country_count)}, "
                f"profile={len(user_profile_count)}, "
                f"gender={len(gender_count)})")
    logger.info(f"[FINE] runtime totale {total_elapsed}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())


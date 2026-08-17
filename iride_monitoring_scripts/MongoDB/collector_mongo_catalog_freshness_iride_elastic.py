"""
collector_mongo_catalog_freshness_iride_elastic.py
==================================================
Collector freshness del catalogo Insula via MongoDB diretto.

Bypass strategico: invece di interrogare Insula API REST (che richiede
credenziali Keycloak ancora in attesa via CIMS-47), interroga direttamente
il database MongoDB del catalogo, sfruttando le credenziali read-only
fornite da MEEO su CIMS-45 (16/06/2026).

Questo permette di rispondere alla domanda KILLER del progetto IRIDE:
    "I dati di ISPRA / fonti esterne arrivano nel catalogo?"
prima che il fronte Keycloak sia sbloccato.

Output JSON in stile DESP per ciascuna collection del catalogo:
- conteggio totale documenti
- ultimo documento aggiunto (timestamp)
- freshness (minuti dal piu' recente)
- conteggio per finestra temporale (ultima ora, 24h, 7d)
- status (OK / STALE / NO_DATA) basato su soglie configurabili

Indice Elastic dedicato:
    metrics-iride-catalog-freshness.monitoring-default
"""

import sys
import os
import json
import socket
import time
import logging
import configparser
from datetime import datetime, timezone, timedelta

import requests

try:
    from pymongo import MongoClient
    from pymongo.errors import (
        ServerSelectionTimeoutError, OperationFailure, ConnectionFailure,
    )
except ImportError:
    print("ERRORE: pip install pymongo")
    sys.exit(1)


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


# ============================================================
# Config
# ============================================================
base_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_dir, 'mongo_catalog_freshness_iride_elastic.ini')
config = configparser.ConfigParser()
config.read(config_path)

# Elastic
ELASTIC_ENABLED = config.getboolean('CONFIG', 'ELASTIC_ENABLED', fallback=False)
MONITORING_URL = config.get('CONFIG', 'MONITORING_URL', fallback='')
MONITORING_APIKEY = config.get('CONFIG', 'MONITORING_APIKEY', fallback='')
MONITORING_VERIFY_CERTS = config.getboolean(
    'CONFIG', 'MONITORING_VERIFY_CERTS', fallback=True)
FRESHNESS_INDEX = config.get(
    'CONFIG', 'FRESHNESS_INDEX',
    fallback='metrics-iride-catalog-freshness.monitoring-default')

# MongoDB
MONGO_HOST = config.get('MONGODB', 'HOST')
MONGO_PORT = config.getint('MONGODB', 'PORT', fallback=27017)
MONGO_USER = config.get('MONGODB', 'USERNAME')
MONGO_PASS = config.get('MONGODB', 'PASSWORD')
MONGO_AUTH_DB = config.get('MONGODB', 'AUTH_DB', fallback='admin')
MONGO_TIMEOUT_MS = config.getint('MONGODB', 'TIMEOUT_MS', fallback=10000)

# Catalog target
TARGET_DB = config.get('CATALOG_FRESHNESS', 'DATABASE', fallback='catalog')
COLLECTIONS = [c.strip() for c in
               config.get('CATALOG_FRESHNESS', 'COLLECTIONS',
                          fallback='').split(',') if c.strip()]
# Lasciato vuoto = autodiscovery
TIMESTAMP_FIELD_CANDIDATES = [
    c.strip() for c in
    config.get('CATALOG_FRESHNESS', 'TIMESTAMP_FIELD_CANDIDATES',
               fallback='createdAt,created_at,creation_date,timestamp,'
                        'date,inserted_at,ingestion_time,publishedAt').split(',')
]
THRESHOLD_OK_MINUTES = config.getint(
    'CATALOG_FRESHNESS', 'THRESHOLD_OK_MINUTES', fallback=60)
THRESHOLD_STALE_MINUTES = config.getint(
    'CATALOG_FRESHNESS', 'THRESHOLD_STALE_MINUTES', fallback=720)


logger.info(f"[AVVIO] {script_name} — hostname={hostname}")
logger.info(f"[CONFIG] Mongo target: mongodb://{MONGO_USER}@"
            f"{MONGO_HOST}:{MONGO_PORT}/{TARGET_DB}")
logger.info(f"[CONFIG] Collections: {COLLECTIONS or '(autodiscovery)'}")
logger.info(f"[CONFIG] Thresholds: OK<{THRESHOLD_OK_MINUTES}min, "
            f"STALE<{THRESHOLD_STALE_MINUTES}min")
if not ELASTIC_ENABLED:
    logger.info(f"[CONFIG] >>> DRY-RUN MODE (no Elastic) <<<")
else:
    logger.info(f"[CONFIG] Elastic ENABLED → {FRESHNESS_INDEX}")
logger.info("")


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
        url = f"{MONITORING_URL.rstrip('/')}/{FRESHNESS_INDEX}/_doc"
        headers = {'Authorization': f'ApiKey {MONITORING_APIKEY}',
                   'Content-Type': 'application/json'}
        r = requests.post(url, headers=headers, json=entry,
                          verify=MONITORING_VERIFY_CERTS, timeout=30)
        if r.status_code not in (200, 201):
            logger.error(f"[ELASTIC] HTTP {r.status_code}: {r.text[:200]}")
    except Exception as e:
        logger.error(f"[ELASTIC] {type(e).__name__}: {e}")


def detect_timestamp_field(coll, sample_size=5):
    """Sonda i primi N documenti per scoprire qual e' il campo timestamp."""
    for field in TIMESTAMP_FIELD_CANDIDATES:
        # Verifica se esiste e ha valori non-null in almeno 1 documento
        count = coll.count_documents({field: {"$exists": True, "$ne": None}},
                                     limit=sample_size)
        if count > 0:
            return field
    return None


def get_freshness_metrics(coll, ts_field):
    """Estrae metriche di freshness per una collection."""
    now = datetime.now(timezone.utc)
    one_hour_ago = now - timedelta(hours=1)
    one_day_ago = now - timedelta(hours=24)
    seven_days_ago = now - timedelta(days=7)

    # Conteggio totale (stima veloce su collection grandi)
    total_count = coll.estimated_document_count()

    # Ultimo documento per timestamp DESC
    latest = coll.find_one(sort=[(ts_field, -1)])
    latest_ts = None
    freshness_min = None
    if latest and ts_field in latest:
        latest_ts_raw = latest[ts_field]
        # Normalizza: MongoDB datetime, ISO string, epoch
        if isinstance(latest_ts_raw, datetime):
            if latest_ts_raw.tzinfo is None:
                latest_ts_raw = latest_ts_raw.replace(tzinfo=timezone.utc)
            latest_ts = latest_ts_raw
        elif isinstance(latest_ts_raw, str):
            try:
                latest_ts = datetime.fromisoformat(
                    latest_ts_raw.replace("Z", "+00:00"))
            except Exception:
                pass
        elif isinstance(latest_ts_raw, (int, float)):
            # Epoch in ms o sec
            if latest_ts_raw > 1e12:  # ms
                latest_ts = datetime.fromtimestamp(latest_ts_raw / 1000,
                                                    tz=timezone.utc)
            else:
                latest_ts = datetime.fromtimestamp(latest_ts_raw,
                                                    tz=timezone.utc)
        if latest_ts:
            freshness_min = (now - latest_ts).total_seconds() / 60

    # Conteggi per finestra
    try:
        count_last_hour = coll.count_documents(
            {ts_field: {"$gte": one_hour_ago}})
    except Exception:
        count_last_hour = None
    try:
        count_last_24h = coll.count_documents(
            {ts_field: {"$gte": one_day_ago}})
    except Exception:
        count_last_24h = None
    try:
        count_last_7d = coll.count_documents(
            {ts_field: {"$gte": seven_days_ago}})
    except Exception:
        count_last_7d = None

    return {
        "total_count": total_count,
        "latest_timestamp": latest_ts.isoformat() if latest_ts else None,
        "freshness_minutes": (round(freshness_min, 2)
                              if freshness_min is not None else None),
        "count_last_hour": count_last_hour,
        "count_last_24h": count_last_24h,
        "count_last_7d": count_last_7d,
    }


# ============================================================
# Main
# ============================================================
def main():
    # 1) Connessione MongoDB
    uri = (f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/"
           f"?authSource={MONGO_AUTH_DB}")
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=MONGO_TIMEOUT_MS)
        client.admin.command("ping")
        logger.info("[MONGO] Connessione OK")
    except (ServerSelectionTimeoutError, ConnectionFailure) as e:
        logger.error(f"[MONGO] Connessione fallita: {e}")
        return 1
    except OperationFailure as e:
        logger.error(f"[MONGO] Auth fallita: {e}")
        return 1

    db = client[TARGET_DB]

    # 2) Lista collection (autodiscovery se vuota)
    collections_to_check = COLLECTIONS or db.list_collection_names()
    logger.info(f"[CATALOG] {len(collections_to_check)} collection da processare")

    # 3) Per ciascuna collection
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    fresh_ok = 0
    fresh_stale = 0
    fresh_no_data = 0

    for coll_name in collections_to_check:
        try:
            coll = db[coll_name]

            # Detect timestamp field
            ts_field = detect_timestamp_field(coll)
            if ts_field is None:
                logger.warning(f"  {coll_name}: nessun campo timestamp riconosciuto")
                entry = {
                    "@timestamp": now_iso,
                    "event_timestamp": now_iso,
                    "platform": "iride",
                    "service_provider_log": "iride_catalog_mongo",
                    "event_type": "catalog_freshness",
                    "response_status": "NO_TIMESTAMP_FIELD",
                    "hostname": hostname,
                    "mongo_host": MONGO_HOST,
                    "database": TARGET_DB,
                    "collection": coll_name,
                    "total_count": coll.estimated_document_count(),
                    "ts_field": None,
                }
                write_log(entry)
                ship_to_elastic(entry)
                continue

            metrics = get_freshness_metrics(coll, ts_field)

            # Status semaforo
            if metrics["latest_timestamp"] is None or metrics["total_count"] == 0:
                status = "NO_DATA"
                fresh_no_data += 1
            elif metrics["freshness_minutes"] is None:
                status = "UNKNOWN"
            elif metrics["freshness_minutes"] < THRESHOLD_OK_MINUTES:
                status = "OK"
                fresh_ok += 1
            elif metrics["freshness_minutes"] < THRESHOLD_STALE_MINUTES:
                status = "STALE"
                fresh_stale += 1
            else:
                status = "NO_DATA"
                fresh_no_data += 1

            entry = {
                "@timestamp": now_iso,
                "event_timestamp": now_iso,
                "platform": "iride",
                "service_provider_log": "iride_catalog_mongo",
                "event_type": "catalog_freshness",
                "response_status": status,
                "hostname": hostname,
                "mongo_host": MONGO_HOST,
                "database": TARGET_DB,
                "collection": coll_name,
                "ts_field": ts_field,
                **metrics,
                "threshold_ok_minutes": THRESHOLD_OK_MINUTES,
                "threshold_stale_minutes": THRESHOLD_STALE_MINUTES,
            }
            write_log(entry)
            ship_to_elastic(entry)

            fresh_str = (f"{metrics['freshness_minutes']:.1f}min"
                         if metrics['freshness_minutes'] is not None else "?")
            logger.info(f"  {coll_name[:30]:30s} status={status:8s} "
                        f"total={metrics['total_count']:>8} "
                        f"latest_age={fresh_str:>12} "
                        f"24h={metrics['count_last_24h']}")

        except OperationFailure as e:
            logger.warning(f"  {coll_name}: access denied: {e}")
        except Exception as e:
            logger.error(f"  {coll_name}: {type(e).__name__}: {e}")

    logger.info("")
    logger.info(f"[REPORT] OK={fresh_ok}  STALE={fresh_stale}  "
                f"NO_DATA={fresh_no_data}  /  totali={len(collections_to_check)}")
    logger.info("[FINE]")
    client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

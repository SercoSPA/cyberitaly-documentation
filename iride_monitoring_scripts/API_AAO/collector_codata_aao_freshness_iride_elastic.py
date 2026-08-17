"""
collector_codata_aao_freshness_iride_elastic.py
================================================
Per ciascuna stazione monitorata (e per ciascun tipo di misura di interesse)
verifica quanto recente è l'ultima osservazione disponibile sul CoDataService
AMICO Alpi Orientali.

Emette un evento JSON per ogni stazione, in stile DESP.

MODALITA' AUTO-DISCOVERY:
  Se nella config STATION_IDS è vuoto/non specificato, lo script scarica
  TUTTE le stazioni del network configurato. Utile per il primo run, dopo
  raffini la lista in base alle DT che ti interessano.

MODALITA' DRY-RUN ELASTIC:
  Quando ELASTIC_ENABLED=False, scrive solo su file locale (jsonl).

NOTA: lo script NON scrive nulla sul server CoDataService. Tutte chiamate GET.

Cadenza suggerita: ogni 1-3 ore (cron).
"""

import requests
import os
import json
import time
import configparser
import logging
import socket
from datetime import datetime, timezone, timedelta
from dateutil import parser as dtparser  # pip install python-dateutil

# ============================================================
# Logging
# ============================================================
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ============================================================
# Config
# ============================================================
base_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_dir, 'codata_aao_freshness_iride_elastic.ini')
config = configparser.ConfigParser()
config.read(config_path)

hostname = os.environ.get("HOSTNAME", socket.gethostname())

LOG_FILE_NAME = 'collector_codata_aao_freshness_iride_elastic.log'
log_file = os.path.join(base_dir, LOG_FILE_NAME)

ELASTIC_ENABLED = config['CONFIG'].getboolean('ELASTIC_ENABLED', fallback=False)

es = None
MONITORING_INDEX = None
if ELASTIC_ENABLED:
    from elasticsearch import Elasticsearch
    MONITORING_URL = config['CONFIG']['MONITORING_URL']
    MONITORING_APIKEY = config['CONFIG']['MONITORING_APIKEY']
    MONITORING_VERIFY_CERTS = config['CONFIG'].getboolean('MONITORING_VERIFY_CERTS', fallback=True)
    MONITORING_INDEX = config['CONFIG'].get('FRESHNESS_INDEX',
                                             fallback='metrics-iride-codata-aao-freshness.monitoring-default')
    es = Elasticsearch(
        [MONITORING_URL],
        headers={"Authorization": "ApiKey " + MONITORING_APIKEY},
        verify_certs=MONITORING_VERIFY_CERTS
    )
    logger.info(f"[CONFIG] Elasticsearch ABILITATO: {MONITORING_URL}")
else:
    logger.info("[CONFIG] Elasticsearch DISABILITATO (dry-run). Output solo su file locale.")

# Credenziali e endpoint
AAO_BASE_URL = config['CODATA_AAO']['BASE_URL']
AAO_USERNAME = config['CODATA_AAO']['USERNAME']
AAO_PASSWORD = config['CODATA_AAO']['PASSWORD']

# Parametri di freshness check
FRESHNESS_CFG = config['CODATA_AAO_FRESHNESS'] if 'CODATA_AAO_FRESHNESS' in config else {}
# Tipi di misura da controllare. Lista di id separati da virgola (es. "1,2,3").
# Se non specificato, dopo l'autodiscovery viene fatto check con measureTypeId=1
MEASURE_TYPE_IDS = [
    int(x.strip())
    for x in FRESHNESS_CFG.get('MEASURE_TYPE_IDS', '1').split(',')
    if x.strip()
]
# ID delle stazioni da monitorare. Vuoto = autodiscovery di tutte
STATION_IDS_CFG = FRESHNESS_CFG.get('STATION_IDS', '').strip()
STATION_IDS = [
    int(x.strip()) for x in STATION_IDS_CFG.split(',') if x.strip()
] if STATION_IDS_CFG else []
# Soglie per classificare la freshness
THRESHOLD_OK_MIN = int(FRESHNESS_CFG.get('THRESHOLD_OK_MINUTES', '60'))     # < 1h
THRESHOLD_STALE_MIN = int(FRESHNESS_CFG.get('THRESHOLD_STALE_MINUTES', '360'))  # < 6h
# Finestra di lookback per cercare misure (default: ultime 24h)
LOOKBACK_HOURS = int(FRESHNESS_CFG.get('LOOKBACK_HOURS', '24'))
# Quante stazioni al massimo controllare per run (safety in autodiscovery)
MAX_STATIONS = int(FRESHNESS_CFG.get('MAX_STATIONS', '100'))

HTTP_TIMEOUT = 30


# ============================================================
# Helpers
# ============================================================
def make_log_entry(station_id, measure_type_id, status,
                   latest_timestamp=None, freshness_seconds=None,
                   measurement_count=None, latency_ms=None, error=None):
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    entry = {
        "@timestamp": timestamp,
        "event_timestamp": timestamp,
        "platform": "iride",
        "service_provider_log": "iride_codata_aao",
        "event_type": "station_freshness_check",
        "response_status": status,  # OK | STALE | NO_DATA | FAIL
        "hostname": hostname,
        "endpoint": AAO_BASE_URL,
        "station_id": int(station_id),
        "measure_type_id": int(measure_type_id),
    }
    if latest_timestamp:
        entry["latest_measurement_timestamp"] = latest_timestamp
    if freshness_seconds is not None:
        entry["freshness_seconds"] = round(freshness_seconds, 1)
    if measurement_count is not None:
        entry["measurement_count"] = int(measurement_count)
    if latency_ms is not None:
        entry["query_latency_ms"] = round(latency_ms, 2)
    if error:
        entry["error_message"] = str(error)[:500]
    return entry


def write_log(entry):
    line = json.dumps(entry, ensure_ascii=False, separators=(',', ':'))
    with open(log_file, 'a') as f:
        f.write(line + '\n')
    if ELASTIC_ENABLED and es is not None:
        try:
            es.index(index=MONITORING_INDEX, body=entry)
        except Exception as e:
            logger.warning(f"[ES_FAIL] {type(e).__name__}: {e}")


def authenticate(session):
    """Login e ritorna il bearer token, oppure None."""
    try:
        r = session.post(
            f"{AAO_BASE_URL}/api/Auth/login",
            data={"userName": AAO_USERNAME, "password": AAO_PASSWORD},
            timeout=HTTP_TIMEOUT,
        )
        if r.status_code != 200:
            logger.error(f"[AUTH] HTTP {r.status_code}: {r.text[:200]}")
            return None
        raw_token = r.text.strip('"').strip()
        # Il server ritorna gia' "Bearer eyJ..." con il prefisso incluso.
        # Normalizziamo togliendolo, lo aggiungeremo noi nell'header.
        if raw_token.startswith("Bearer "):
            token = raw_token[len("Bearer "):]
        else:
            token = raw_token
        if not token.startswith("ey"):
            logger.error(f"[AUTH] Token formato inatteso")
            return None
        return token
    except Exception as e:
        logger.error(f"[AUTH] {type(e).__name__}: {e}")
        return None


def discover_stations(session, headers):
    """Auto-discovery: scarica tutte le stazioni disponibili."""
    try:
        r = session.get(f"{AAO_BASE_URL}/api/stations",
                        headers=headers, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            logger.error(f"[DISCOVER] HTTP {r.status_code}")
            return []
        stations = r.json()
        ids = [int(s.get("id")) for s in stations if s.get("id") is not None]
        logger.info(f"[DISCOVER] {len(ids)} stazioni trovate via autodiscovery")
        return ids[:MAX_STATIONS]
    except Exception as e:
        logger.error(f"[DISCOVER] {type(e).__name__}: {e}")
        return []


def query_measures_bulk(session, headers, measure_type_id, station_ids):
    """
    Una sola chiamata per scaricare TUTTE le osservazioni di un tipo di misura
    per TUTTE le stazioni nell'ultimo lookback window. Poi raggruppa per
    idstazione. Molto piu' efficiente che 20 chiamate per stazione.
    """
    # AAO interpreta startDate/endDate nel suo fuso UTC+1 fisso.
    # Per essere coerenti, costruiamo le bound in quel fuso e mandiamo
    # la stringa senza timezone (cosi' AAO la prende come UTC+1).
    AAO_TZ = timezone(timedelta(hours=1))
    now_utc = datetime.now(timezone.utc)
    now_aao = now_utc.astimezone(AAO_TZ)
    start = (now_aao - timedelta(hours=LOOKBACK_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
    end = now_aao.strftime("%Y-%m-%d %H:%M:%S")

    station_ids_str = ",".join(str(s) for s in station_ids)
    params = {
        "stationIDs": station_ids_str,
        "startDate": start,
        "endDate": end,
        "format": "JSON",
    }

    t0 = time.perf_counter()
    try:
        r = session.get(
            f"{AAO_BASE_URL}/api/measures/{measure_type_id}",
            headers=headers, params=params, timeout=HTTP_TIMEOUT,
        )
        latency_ms = (time.perf_counter() - t0) * 1000

        if r.status_code != 200:
            logger.error(f"[QUERY] measure={measure_type_id} HTTP {r.status_code}")
            return None, latency_ms, f"HTTP {r.status_code}: {r.text[:200]}"

        data = r.json()
        logger.info(f"[QUERY] measure={measure_type_id}: "
                    f"{len(data) if isinstance(data, list) else '?'} items in {latency_ms:.0f} ms")
        return data, latency_ms, None
    except Exception as e:
        latency_ms = (time.perf_counter() - t0) * 1000
        logger.error(f"[QUERY] measure={measure_type_id} {type(e).__name__}: {e}")
        return None, latency_ms, f"{type(e).__name__}: {e}"


def group_observations_by_station(data):
    """
    Dato il payload bulk (lista di osservazioni con 'idstazione'),
    raggruppa per stazione e ritorna {station_id: [(ts_utc, valore), ...]}.

    NOTA TIMEZONE (verificata empiricamente il 15/06/2026 via diag script):
    AAO restituisce 'dataora' in un fuso fisso UTC+1 — NON applica ora legale.
    Quindi anche d'estate i timestamp sono offset di +1h rispetto a UTC,
    non +2h come sarebbe Europe/Rome.
    """
    grouped = {}
    if not isinstance(data, list):
        return grouped

    # AAO usa UTC+1 fisso (no DST), confermato da test diagnostico.
    AAO_TZ = timezone(timedelta(hours=1))

    for obs in data:
        if not isinstance(obs, dict):
            continue
        sid = obs.get("idstazione")
        ts_str = obs.get("dataora")
        valore = obs.get("valore")
        if sid is None or not isinstance(ts_str, str):
            continue
        try:
            ts = dtparser.parse(ts_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=AAO_TZ)
            ts_utc = ts.astimezone(timezone.utc)
            grouped.setdefault(int(sid), []).append((ts_utc, valore))
        except Exception:
            continue
    return grouped


def emit_station_freshness(station_id, measure_type_id, observations,
                            now_utc, query_latency_ms):
    """
    Dato l'elenco di osservazioni di una stazione (gia' raggruppate),
    calcola lo stato di freshness e scrive l'evento Elastic.
    """
    if not observations:
        entry = make_log_entry(
            station_id, measure_type_id, "NO_DATA",
            measurement_count=0, latency_ms=query_latency_ms,
        )
        write_log(entry)
        logger.info(f"  station={station_id} measure={measure_type_id}: "
                    f"NO_DATA (no measurements in last {LOOKBACK_HOURS}h)")
        return

    timestamps = [ts for ts, _ in observations]
    latest = max(timestamps)
    freshness_seconds = (now_utc - latest).total_seconds()
    freshness_minutes = freshness_seconds / 60

    if freshness_minutes < THRESHOLD_OK_MIN:
        status = "OK"
    elif freshness_minutes < THRESHOLD_STALE_MIN:
        status = "STALE"
    else:
        status = "NO_DATA"

    entry = make_log_entry(
        station_id, measure_type_id, status,
        latest_timestamp=latest.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        freshness_seconds=freshness_seconds,
        measurement_count=len(observations),
        latency_ms=query_latency_ms,
    )
    write_log(entry)
    logger.info(f"  station={station_id} measure={measure_type_id}: "
                f"{status} ({len(observations)} misure, "
                f"latest {freshness_minutes:.1f} min fa)")


def check_all_stations_for_measure(session, headers, measure_type_id, station_ids):
    """
    Per un singolo measure_type_id, esegue 1 query bulk e poi emette
    un evento per ciascuna stazione richiesta.
    """
    now_utc = datetime.now(timezone.utc)

    data, query_latency_ms, error = query_measures_bulk(
        session, headers, measure_type_id, station_ids
    )

    if data is None:
        # Query fallita: emetti un evento FAIL per ogni stazione
        for sid in station_ids:
            entry = make_log_entry(
                sid, measure_type_id, "FAIL",
                latency_ms=query_latency_ms, error=error
            )
            write_log(entry)
            logger.warning(f"  station={sid} measure={measure_type_id}: FAIL ({error[:80]})")
        return

    # Raggruppa le osservazioni per idstazione
    grouped = group_observations_by_station(data)
    # Latenza divisa equamente tra le stazioni richieste (per dashboard)
    per_station_latency_ms = query_latency_ms / max(len(station_ids), 1)

    for sid in station_ids:
        observations = grouped.get(int(sid), [])
        emit_station_freshness(sid, measure_type_id, observations,
                                now_utc, per_station_latency_ms)


def _extract_timestamps(data):
    """
    Estrazione difensiva dei timestamp dalle osservazioni AAO.
    (Funzione legacy: il flusso principale usa group_observations_by_station,
    questa resta per safety/refactoring futuro.)

    Lo schema confermato del payload CoDataService e':
        {"idstazione": 10034, "dataora": "2026-06-15T07:42:00",
         "valore": 0, "affid": 10}

    NOTA TIMEZONE (verificata empiricamente il 15/06/2026):
    Il campo 'dataora' e' in UTC+1 fisso (NO ora legale). Anche d'estate
    AAO non applica DST. Convertiamo a UTC per il confronto con
    datetime.now(timezone.utc).
    """
    # AAO usa UTC+1 fisso (no DST)
    AAO_TZ = timezone(timedelta(hours=1))

    timestamps = []

    def walk(obj):
        if isinstance(obj, dict):
            # Chiavi candidate per i timestamp delle osservazioni.
            # 'dataora' e' lo standard CoDataService AAO; il resto e' difensivo.
            for k in ('dataora', 'data_ora', 'data',
                      'timestamp', 'date', 'datetime', 'time',
                      'observedAt', 'phenomenonTime', 'dateTime',
                      'measurementTime', 'observationTime'):
                v = obj.get(k)
                if isinstance(v, str):
                    try:
                        ts = dtparser.parse(v)
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=AAO_TZ)
                        ts_utc = ts.astimezone(timezone.utc)
                        timestamps.append(ts_utc)
                    except Exception:
                        pass
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for item in obj:
                walk(item)

    walk(data)
    return timestamps


# ============================================================
# Main
# ============================================================
def main():
    logger.info(f"[AVVIO] collector_codata_aao_freshness — hostname={hostname}")
    logger.info(f"[CONFIG] Endpoint: {AAO_BASE_URL}")
    logger.info(f"[CONFIG] Lookback window: {LOOKBACK_HOURS}h")
    logger.info(f"[CONFIG] Thresholds: OK<{THRESHOLD_OK_MIN}min, STALE<{THRESHOLD_STALE_MIN}min")
    logger.info(f"[CONFIG] Measure type IDs: {MEASURE_TYPE_IDS}")
    if not ELASTIC_ENABLED:
        logger.info("[CONFIG] >>> DRY-RUN MODE (no Elastic) <<<")
    logger.info("")

    session = requests.Session()
    token = authenticate(session)
    if not token:
        logger.error("[FINE] Authentication failed, abort.")
        return

    headers = {"Authorization": f"Bearer {token}"}

    # Determina la lista delle stazioni da controllare
    stations_to_check = STATION_IDS
    if not stations_to_check:
        logger.info("[CONFIG] STATION_IDS non specificato — auto-discovery")
        stations_to_check = discover_stations(session, headers)
        if not stations_to_check:
            logger.error("[FINE] Nessuna stazione da controllare, abort.")
            return

    logger.info(f"[INFO] Controllo {len(stations_to_check)} stazioni × "
                f"{len(MEASURE_TYPE_IDS)} tipi di misura "
                f"= {len(stations_to_check) * len(MEASURE_TYPE_IDS)} eventi totali "
                f"(con {len(MEASURE_TYPE_IDS)} chiamate API bulk)")

    # Esegui i check: una sola chiamata API per ciascun tipo di misura,
    # poi raggruppa le osservazioni per stazione (molto piu' efficiente
    # che 1 chiamata per stazione).
    for measure_type_id in MEASURE_TYPE_IDS:
        check_all_stations_for_measure(session, headers,
                                        measure_type_id, stations_to_check)

    logger.info("[FINE]")


if __name__ == "__main__":
    main()

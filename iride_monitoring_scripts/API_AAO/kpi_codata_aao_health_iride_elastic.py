"""
kpi_codata_aao_health_iride_elastic.py
======================================
Synthetic health probe COMPREHENSIVE sull'API CoDataService AMICO Alpi
Orientali (network DAO, sensori CAE).

Testa 10 endpoint critici per il monitoring IRIDE in un singolo run:
  1. POST   /api/Auth/login                       (auth)
  2. GET    /api/Auth/token/valid                 (token valid)
  3. GET    /api/networks                         (list networks)
  4. GET    /api/networks/{id}/stations           (stations in network)
  5. GET    /api/networks/{id}/sensors            (sensors in network)
  6. GET    /api/stations                         (list all stations)
  7. GET    /api/stations/{id}                    (station detail)
  8. GET    /api/stations/{id}/sensors            (station sensors)
  9. GET    /api/measures                         (list measure types)
  10. GET   /api/measures/{id}?stationIDs=...     (read measures)

Emette UN SOLO documento JSON per esecuzione, in stile DESP, con il
dettaglio per endpoint.

MODALITA' DRY-RUN ELASTIC:
  Quando ELASTIC_ENABLED=False nel .ini, lo script scrive SOLO su file
  locale (jsonl) e NON tenta connessioni a Elasticsearch.

NOTA: lo script NON scrive nulla sul server CoDataService — fa solo
GET su endpoint pubblici (con auth) + 1 POST per login. Zero invasivita'.

Cadenza suggerita: ogni 30-60 minuti (cron).
"""

import requests
import os
import json
import time
import configparser
import logging
import socket
from datetime import datetime, timezone

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
config_path = os.path.join(base_dir, 'codata_aao_health_iride_elastic.ini')
config = configparser.ConfigParser()
config.read(config_path)

hostname = os.environ.get("HOSTNAME", socket.gethostname())

LOG_FILE_NAME = 'kpi_codata_aao_health_iride_elastic.log'
log_file = os.path.join(base_dir, LOG_FILE_NAME)

ELASTIC_ENABLED = config['CONFIG'].getboolean('ELASTIC_ENABLED', fallback=False)

es = None
MONITORING_INDEX = None
if ELASTIC_ENABLED:
    from elasticsearch import Elasticsearch
    MONITORING_URL = config['CONFIG']['MONITORING_URL']
    MONITORING_APIKEY = config['CONFIG']['MONITORING_APIKEY']
    MONITORING_VERIFY_CERTS = config['CONFIG'].getboolean('MONITORING_VERIFY_CERTS', fallback=True)
    MONITORING_INDEX = config['CONFIG'].get('SYNTHETIC_INDEX',
                                             fallback='kpi-iride-codata-aao.monitoring-default')
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

# Parametri per i probe: quale network, stazione e misura usare come "sample"
HEALTH_CFG = config['CODATA_AAO_HEALTH'] if 'CODATA_AAO_HEALTH' in config else {}
SAMPLE_NETWORK_ID = int(HEALTH_CFG.get('SAMPLE_NETWORK_ID', '2'))   # DAO
SAMPLE_STATION_ID = int(HEALTH_CFG.get('SAMPLE_STATION_ID', '10034'))  # Alleghe - Col dei Baldi
SAMPLE_MEASURE_ID = int(HEALTH_CFG.get('SAMPLE_MEASURE_ID', '2'))   # pioggia

# Soglie globali per la classificazione finale (millisecondi totali)
TOTAL_LATENCY_OK_MS = int(HEALTH_CFG.get('TOTAL_LATENCY_OK_MS', '5000'))
TOTAL_LATENCY_SLOW_MS = int(HEALTH_CFG.get('TOTAL_LATENCY_SLOW_MS', '15000'))

HTTP_TIMEOUT = 30  # secondi per singola chiamata


# ============================================================
# Helpers
# ============================================================
def probe_endpoint(session, name, method, url, **kwargs):
    """
    Esegue una singola chiamata HTTP e ritorna un dict con il risultato.
    Cattura tutte le eccezioni — uno step non interrompe il flusso.
    """
    result = {
        "name": name,
        "method": method,
        "url_path": url.replace(AAO_BASE_URL, ""),
        "latency_ms": None,
        "http_status": None,
        "ok": False,
        "error": None,
    }
    t0 = time.perf_counter()
    try:
        kwargs.setdefault("timeout", HTTP_TIMEOUT)
        if method == "GET":
            r = session.get(url, **kwargs)
        elif method == "POST":
            r = session.post(url, **kwargs)
        else:
            raise ValueError(f"Method {method} not supported")
        result["latency_ms"] = round((time.perf_counter() - t0) * 1000, 2)
        result["http_status"] = r.status_code
        result["ok"] = (200 <= r.status_code < 300)
        return result, r
    except Exception as e:
        result["latency_ms"] = round((time.perf_counter() - t0) * 1000, 2)
        result["error"] = f"{type(e).__name__}: {str(e)[:200]}"
        return result, None


def write_log(entry):
    """Scrive su file locale jsonl e, se abilitato, indicizza su Elastic."""
    line = json.dumps(entry, ensure_ascii=False, separators=(',', ':'))
    with open(log_file, 'a') as f:
        f.write(line + '\n')

    status = entry.get('response_status', '?')
    total = entry.get('total_latency_ms', '?')
    ok_count = entry.get('endpoints_ok', '?')
    tot_count = entry.get('endpoints_tested', '?')
    logger.info(f"[SALVATO] status={status} ok={ok_count}/{tot_count} "
                f"total={total}ms")

    if ELASTIC_ENABLED and es is not None:
        try:
            es.index(index=MONITORING_INDEX, body=entry)
        except Exception as e:
            logger.warning(f"[ES_FAIL] {type(e).__name__}: {e}")


# ============================================================
# Probe sequence
# ============================================================
def run_probes():
    overall_start = time.perf_counter()
    results = []
    session = requests.Session()
    token = None
    headers = {}

    # --- 1. Login ---
    res, r = probe_endpoint(
        session, "login", "POST",
        f"{AAO_BASE_URL}/api/Auth/login",
        data={"userName": AAO_USERNAME, "password": AAO_PASSWORD}
    )
    if res["ok"] and r is not None:
        raw_token = r.text.strip('"').strip()
        if raw_token.startswith("Bearer "):
            token = raw_token[len("Bearer "):]
        else:
            token = raw_token
        if not token.startswith("ey"):
            res["ok"] = False
            res["error"] = "Token format unexpected"
            token = None
    results.append(res)
    logger.info(f"  [{1:2d}/10] {'✓' if res['ok'] else '✗'} login                  "
                f"{res['latency_ms']:>8.2f} ms  (HTTP {res['http_status']})")

    if token is None:
        # Senza token, gli altri probe non possono andare.
        # Marca i successivi come "skipped" (ok=False, http_status=None, error="no auth")
        for name in ["token_valid", "list_networks", "network_stations",
                     "network_sensors", "list_stations", "station_detail",
                     "station_sensors", "list_measures", "read_measure"]:
            results.append({
                "name": name, "method": "GET", "url_path": None,
                "latency_ms": None, "http_status": None, "ok": False,
                "error": "skipped (no auth)",
            })
        return results, time.perf_counter() - overall_start

    headers["Authorization"] = f"Bearer {token}"

    # --- 2. Token valid ---
    res, _ = probe_endpoint(
        session, "token_valid", "GET",
        f"{AAO_BASE_URL}/api/Auth/token/valid",
        headers=headers
    )
    results.append(res)
    logger.info(f"  [{2:2d}/10] {'✓' if res['ok'] else '✗'} token_valid            "
                f"{res['latency_ms']:>8.2f} ms  (HTTP {res['http_status']})")

    # --- 3. List networks ---
    res, _ = probe_endpoint(
        session, "list_networks", "GET",
        f"{AAO_BASE_URL}/api/networks",
        headers=headers
    )
    results.append(res)
    logger.info(f"  [{3:2d}/10] {'✓' if res['ok'] else '✗'} list_networks          "
                f"{res['latency_ms']:>8.2f} ms  (HTTP {res['http_status']})")

    # --- 4. Network stations ---
    res, _ = probe_endpoint(
        session, "network_stations", "GET",
        f"{AAO_BASE_URL}/api/networks/{SAMPLE_NETWORK_ID}/stations",
        headers=headers
    )
    results.append(res)
    logger.info(f"  [{4:2d}/10] {'✓' if res['ok'] else '✗'} network_stations       "
                f"{res['latency_ms']:>8.2f} ms  (HTTP {res['http_status']})")

    # --- 5. Network sensors ---
    res, _ = probe_endpoint(
        session, "network_sensors", "GET",
        f"{AAO_BASE_URL}/api/networks/{SAMPLE_NETWORK_ID}/sensors",
        headers=headers
    )
    results.append(res)
    logger.info(f"  [{5:2d}/10] {'✓' if res['ok'] else '✗'} network_sensors        "
                f"{res['latency_ms']:>8.2f} ms  (HTTP {res['http_status']})")

    # --- 6. List stations ---
    res, _ = probe_endpoint(
        session, "list_stations", "GET",
        f"{AAO_BASE_URL}/api/stations",
        headers=headers
    )
    results.append(res)
    logger.info(f"  [{6:2d}/10] {'✓' if res['ok'] else '✗'} list_stations          "
                f"{res['latency_ms']:>8.2f} ms  (HTTP {res['http_status']})")

    # --- 7. Station detail ---
    res, _ = probe_endpoint(
        session, "station_detail", "GET",
        f"{AAO_BASE_URL}/api/stations/{SAMPLE_STATION_ID}",
        headers=headers
    )
    results.append(res)
    logger.info(f"  [{7:2d}/10] {'✓' if res['ok'] else '✗'} station_detail         "
                f"{res['latency_ms']:>8.2f} ms  (HTTP {res['http_status']})")

    # --- 8. Station sensors ---
    res, _ = probe_endpoint(
        session, "station_sensors", "GET",
        f"{AAO_BASE_URL}/api/stations/{SAMPLE_STATION_ID}/sensors",
        headers=headers
    )
    results.append(res)
    logger.info(f"  [{8:2d}/10] {'✓' if res['ok'] else '✗'} station_sensors        "
                f"{res['latency_ms']:>8.2f} ms  (HTTP {res['http_status']})")

    # --- 9. List measures ---
    res, _ = probe_endpoint(
        session, "list_measures", "GET",
        f"{AAO_BASE_URL}/api/measures",
        headers=headers
    )
    results.append(res)
    logger.info(f"  [{9:2d}/10] {'✓' if res['ok'] else '✗'} list_measures          "
                f"{res['latency_ms']:>8.2f} ms  (HTTP {res['http_status']})")

    # --- 10. Read measures (con bound nel fuso AAO UTC+1) ---
    from datetime import timedelta
    AAO_TZ = timezone(timedelta(hours=1))
    now_aao = datetime.now(timezone.utc).astimezone(AAO_TZ)
    start = (now_aao - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
    end = now_aao.strftime("%Y-%m-%d %H:%M:%S")
    res, _ = probe_endpoint(
        session, "read_measure", "GET",
        f"{AAO_BASE_URL}/api/measures/{SAMPLE_MEASURE_ID}",
        headers=headers,
        params={
            "stationIDs": str(SAMPLE_STATION_ID),
            "startDate": start,
            "endDate": end,
            "format": "JSON",
        }
    )
    results.append(res)
    logger.info(f"  [{10:2d}/10] {'✓' if res['ok'] else '✗'} read_measure           "
                f"{res['latency_ms']:>8.2f} ms  (HTTP {res['http_status']})")

    return results, time.perf_counter() - overall_start


# ============================================================
# Main
# ============================================================
def main():
    logger.info(f"[AVVIO] kpi_codata_aao_health (comprehensive) — hostname={hostname}")
    logger.info(f"[CONFIG] Endpoint: {AAO_BASE_URL}")
    logger.info(f"[CONFIG] User: {AAO_USERNAME}")
    logger.info(f"[CONFIG] Sample network={SAMPLE_NETWORK_ID}, "
                f"station={SAMPLE_STATION_ID}, measure={SAMPLE_MEASURE_ID}")
    if not ELASTIC_ENABLED:
        logger.info("[CONFIG] >>> DRY-RUN MODE (no Elastic) <<<")
    logger.info("")

    results, total_seconds = run_probes()
    total_ms = round(total_seconds * 1000, 2)

    ok_count = sum(1 for r in results if r["ok"])
    failed_count = len(results) - ok_count

    # Classificazione globale
    if failed_count > 0:
        status = "FAIL"
    elif total_ms < TOTAL_LATENCY_OK_MS:
        status = "OK"
    elif total_ms < TOTAL_LATENCY_SLOW_MS:
        status = "SLOW"
    else:
        status = "FAIL"

    # Documento JSON in stile DESP
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    entry = {
        "@timestamp": timestamp,
        "event_timestamp": timestamp,
        "platform": "iride",
        "service_provider_log": "iride_codata_aao",
        "event_type": "kpi_codata_aao_health_probe",
        "response_status": status,
        "hostname": hostname,
        "endpoint": AAO_BASE_URL,
        "endpoints_tested": len(results),
        "endpoints_ok": ok_count,
        "endpoints_failed": failed_count,
        "total_latency_ms": total_ms,
        "sample_network_id": SAMPLE_NETWORK_ID,
        "sample_station_id": SAMPLE_STATION_ID,
        "sample_measure_id": SAMPLE_MEASURE_ID,
        "endpoint_results": results,
    }
    write_log(entry)

    logger.info("")
    logger.info(f"  [TOTAL]    {status} {total_ms:.2f} ms — {ok_count}/{len(results)} endpoint OK")
    logger.info("[FINE]")


if __name__ == "__main__":
    main()

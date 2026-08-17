"""
collector_codata_aao_value_anomaly_iride_elastic.py
===================================================
Quality check sui VALORI delle misure (non solo sulla freshness).

Per ciascuna stazione * tipo di misura, scarica le ultime N ore di
osservazioni e valuta:
  - Stuck value: lo stesso identico valore ripetuto > N misure di fila
    (sensore bloccato fisicamente o elettricamente)
  - Out of range: valore fuori dai limiti fisici plausibili
    (es. pioggia negativa, livello > 50m, ecc.)
  - Affidabilita' (campo affid): non tutte le misure hanno affid==10
  - Costante: solo 1 valore unico in tutta la finestra (sensore "morto")
  - Statistiche descrittive: min, max, mean, count

A cosa serve: il check di freshness ti dice solo "i dati arrivano". Questo
script controlla che i dati abbiano SENSO. Un sensore puo' continuare a
trasmettere essendo guasto (sempre 0, sempre lo stesso numero, ecc.).

Cadenza suggerita: ogni 1-3 ore (cron).

MODALITA' DRY-RUN ELASTIC: ELASTIC_ENABLED=False -> file locale jsonl.

NOTA: lo script NON scrive nulla sul server CoDataService. Tutte chiamate GET.
"""

import requests
import os
import json
import time
import configparser
import logging
import socket
from datetime import datetime, timezone, timedelta
from dateutil import parser as dtparser
from collections import Counter

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
config_path = os.path.join(base_dir, 'codata_aao_value_anomaly_iride_elastic.ini')
config = configparser.ConfigParser()
config.read(config_path)

hostname = os.environ.get("HOSTNAME", socket.gethostname())

LOG_FILE_NAME = 'collector_codata_aao_value_anomaly_iride_elastic.log'
log_file = os.path.join(base_dir, LOG_FILE_NAME)

ELASTIC_ENABLED = config['CONFIG'].getboolean('ELASTIC_ENABLED', fallback=False)

es = None
MONITORING_INDEX = None
if ELASTIC_ENABLED:
    from elasticsearch import Elasticsearch
    MONITORING_URL = config['CONFIG']['MONITORING_URL']
    MONITORING_APIKEY = config['CONFIG']['MONITORING_APIKEY']
    MONITORING_VERIFY_CERTS = config['CONFIG'].getboolean('MONITORING_VERIFY_CERTS', fallback=True)
    MONITORING_INDEX = config['CONFIG'].get('VALUE_ANOMALY_INDEX',
                                             fallback='metrics-iride-codata-aao-value-anomaly.monitoring-default')
    es = Elasticsearch(
        [MONITORING_URL],
        headers={"Authorization": "ApiKey " + MONITORING_APIKEY},
        verify_certs=MONITORING_VERIFY_CERTS
    )
    logger.info(f"[CONFIG] Elasticsearch ABILITATO: {MONITORING_URL}")
else:
    logger.info("[CONFIG] Elasticsearch DISABILITATO (dry-run). Output solo su file locale.")

AAO_BASE_URL = config['CODATA_AAO']['BASE_URL']
AAO_USERNAME = config['CODATA_AAO']['USERNAME']
AAO_PASSWORD = config['CODATA_AAO']['PASSWORD']

ANOMALY_CFG = config['CODATA_AAO_ANOMALY'] if 'CODATA_AAO_ANOMALY' in config else {}
MEASURE_TYPE_IDS = [
    int(x.strip())
    for x in ANOMALY_CFG.get('MEASURE_TYPE_IDS', '2,6').split(',')
    if x.strip()
]
STATION_IDS_CFG = ANOMALY_CFG.get('STATION_IDS', '').strip()
STATION_IDS = [
    int(x.strip()) for x in STATION_IDS_CFG.split(',') if x.strip()
] if STATION_IDS_CFG else []
LOOKBACK_HOURS = int(ANOMALY_CFG.get('LOOKBACK_HOURS', '6'))

# Soglie stuck detection: lo stesso identico valore ripetuto piu' volte
STUCK_THRESHOLD = int(ANOMALY_CFG.get('STUCK_THRESHOLD', '60'))

# Soglia "alto numero di osservazioni con affid != 10"
AFFID_OK_VALUE = int(ANOMALY_CFG.get('AFFID_OK_VALUE', '10'))
LOW_AFFID_RATIO_THRESHOLD = float(ANOMALY_CFG.get('LOW_AFFID_RATIO_THRESHOLD', '0.1'))

MAX_STATIONS = int(ANOMALY_CFG.get('MAX_STATIONS', '100'))

HTTP_TIMEOUT = 30

# ============================================================
# Range fisici plausibili per misura (per out-of-range detection)
# Conservativi: usati solo per flaggare valori palesemente impossibili.
# ============================================================
PHYSICAL_RANGES = {
    1:   (-30, 50),       # temperatura (C)
    2:   (0, 200),        # pioggia (mm per intervallo di misura)
    3:   (0, 100),        # umidita (%)
    4:   (800, 1100),     # pressione (hPa)
    5:   (0, 200),        # vento (m/s o km/h, conservativo)
    6:   (-2, 50),        # livmis (m)
    7:   (0, 10000),      # portmis (m3/s, larga per fiumi importanti)
    9:   (0, 500),        # neve (cm)
    10:  (0, 1500),       # radiazione (W/m2)
    15:  (-2, 5),         # livmare (m)
    22:  (-2, 100),       # livello_monte (m, dighe)
    23:  (-2, 100),       # livello_valle (m, dighe)
    101: (0, 500),        # v_pioggia_aggregata (mm)
    361: (0, 1000),       # pioggia_cum (mm)
}

# Misure "additive/eventuali": il valore 0 e' atteso e significa
# "non sta succedendo nulla", non un sensore guasto.
# Pioggia tutti 0 = non sta piovendo = OK, non CONSTANT.
ADDITIVE_MEASURE_IDS = {
    2,    # pioggia
    9,    # neve
    24,   # dens_neve
    101,  # v_pioggia_aggregata
    361,  # pioggia_cum
}

# Misure "di stato": livello, temperatura, ecc. dovrebbero variare in
# condizioni normali. Costanti per ore sono sospette.
# (Tutte le altre misure non in ADDITIVE_MEASURE_IDS rientrano qui per default.)


# ============================================================
# Helpers
# ============================================================
def make_log_entry(station_id, measure_type_id, status, **kwargs):
    """Documento JSON in stile DESP."""
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    entry = {
        "@timestamp": timestamp,
        "event_timestamp": timestamp,
        "platform": "iride",
        "service_provider_log": "iride_codata_aao",
        "event_type": "station_value_quality",
        "response_status": status,
        "hostname": hostname,
        "endpoint": AAO_BASE_URL,
        "station_id": int(station_id),
        "measure_type_id": int(measure_type_id),
    }
    for k, v in kwargs.items():
        if v is not None:
            entry[k] = v
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
    """Login -> bearer token."""
    try:
        r = session.post(
            f"{AAO_BASE_URL}/api/Auth/login",
            data={"userName": AAO_USERNAME, "password": AAO_PASSWORD},
            timeout=HTTP_TIMEOUT,
        )
        if r.status_code != 200:
            logger.error(f"[AUTH] HTTP {r.status_code}: {r.text[:200]}")
            return None
        raw = r.text.strip('"').strip()
        if raw.startswith("Bearer "):
            raw = raw[len("Bearer "):]
        if not raw.startswith("ey"):
            logger.error("[AUTH] Token formato inatteso")
            return None
        return raw
    except Exception as e:
        logger.error(f"[AUTH] {type(e).__name__}: {e}")
        return None


def discover_stations(session, headers):
    """Auto-discovery di tutte le stazioni del network DAO."""
    try:
        r = session.get(f"{AAO_BASE_URL}/api/stations",
                        headers=headers, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            logger.error(f"[DISCOVER] HTTP {r.status_code}")
            return []
        stations = r.json()
        ids = [int(s.get("id")) for s in stations if s.get("id") is not None]
        logger.info(f"[DISCOVER] {len(ids)} stazioni trovate")
        return ids[:MAX_STATIONS]
    except Exception as e:
        logger.error(f"[DISCOVER] {type(e).__name__}: {e}")
        return []


def query_measures_bulk(session, headers, measure_type_id, station_ids):
    """Una sola chiamata per scaricare tutte le osservazioni del tipo
    di misura, per tutte le stazioni nella finestra di lookback."""
    AAO_TZ = timezone(timedelta(hours=1))  # AAO usa UTC+1 fisso (no DST)
    now_aao = datetime.now(timezone.utc).astimezone(AAO_TZ)
    start = (now_aao - timedelta(hours=LOOKBACK_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
    end = now_aao.strftime("%Y-%m-%d %H:%M:%S")

    params = {
        "stationIDs": ",".join(str(s) for s in station_ids),
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
            return None, latency_ms, f"HTTP {r.status_code}"
        data = r.json()
        logger.info(f"[QUERY] measure={measure_type_id}: "
                    f"{len(data) if isinstance(data, list) else '?'} items in {latency_ms:.0f} ms")
        return data, latency_ms, None
    except Exception as e:
        latency_ms = (time.perf_counter() - t0) * 1000
        logger.error(f"[QUERY] measure={measure_type_id} {type(e).__name__}: {e}")
        return None, latency_ms, f"{type(e).__name__}: {e}"


def group_by_station(data):
    """Raggruppa le osservazioni per idstazione e ordina per dataora."""
    AAO_TZ = timezone(timedelta(hours=1))
    grouped = {}
    if not isinstance(data, list):
        return grouped
    for obs in data:
        if not isinstance(obs, dict):
            continue
        sid = obs.get("idstazione")
        ts_str = obs.get("dataora")
        valore = obs.get("valore")
        affid = obs.get("affid")
        if sid is None or not isinstance(ts_str, str):
            continue
        try:
            ts = dtparser.parse(ts_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=AAO_TZ)
            ts_utc = ts.astimezone(timezone.utc)
            grouped.setdefault(int(sid), []).append({
                "ts": ts_utc, "valore": valore, "affid": affid
            })
        except Exception:
            continue
    # Ordina cronologicamente
    for sid in grouped:
        grouped[sid].sort(key=lambda o: o["ts"])
    return grouped


def analyze_station_values(station_id, measure_type_id, observations):
    """
    Analisi statistica + anomaly detection sulle osservazioni di una stazione.
    Ritorna un dict con tutte le metriche e flag, e lo status complessivo.
    """
    if not observations:
        return None, "NO_DATA"

    values = [o["valore"] for o in observations if isinstance(o["valore"], (int, float))]
    affids = [o["affid"] for o in observations if o["affid"] is not None]

    if not values:
        return None, "NO_NUMERIC_VALUES"

    # Statistiche base
    v_min = min(values)
    v_max = max(values)
    v_mean = sum(values) / len(values)
    v_unique = len(set(values))

    # --- Stuck detection: lo stesso identico valore ripetuto >= STUCK_THRESHOLD volte di fila ---
    stuck_detected = False
    stuck_value = None
    stuck_streak = 0
    current_streak = 1
    last_value = values[0]
    longest_streak = 1
    longest_value = last_value
    for v in values[1:]:
        if v == last_value:
            current_streak += 1
            if current_streak > longest_streak:
                longest_streak = current_streak
                longest_value = v
        else:
            current_streak = 1
            last_value = v
    if longest_streak >= STUCK_THRESHOLD:
        stuck_detected = True
        stuck_value = longest_value
        stuck_streak = longest_streak

    # --- Out of range detection ---
    out_of_range_count = 0
    out_of_range_min = out_of_range_max = None
    if measure_type_id in PHYSICAL_RANGES:
        lo, hi = PHYSICAL_RANGES[measure_type_id]
        out_of_range_min, out_of_range_max = lo, hi
        out_of_range_count = sum(1 for v in values if v < lo or v > hi)

    # --- Affid analysis ---
    affid_distribution = dict(Counter(affids)) if affids else {}
    low_affid_count = sum(1 for a in affids if a != AFFID_OK_VALUE)
    low_affid_ratio = (low_affid_count / len(affids)) if affids else 0.0

    # --- Costante / quasi-costante ---
    is_constant = (v_unique == 1)
    is_quasi_constant = (v_unique <= 3 and len(values) > 50)

    # --- Determina status complessivo (con logica context-aware) ---
    is_additive_measure = (measure_type_id in ADDITIVE_MEASURE_IDS)
    all_zero = (v_min == 0 and v_max == 0)

    if is_additive_measure and all_zero:
        # Pioggia/neve tutti zeri = "non sta piovendo/nevicando" = OK normale.
        # NON e' un sensore stuck — e' la natura del fenomeno.
        status = "OK_NO_EVENT"
    elif is_additive_measure and is_constant and v_min != 0:
        # Misura additiva costante a valore != 0 (es. pioggia bloccata
        # a 0.3 mm per ore): vero stuck, e' una anomalia.
        status = "STUCK"
    elif is_constant and not is_additive_measure:
        # Misura "di stato" (livmis, temp, ecc.) costante: sospetto,
        # ma puo' essere magra prolungata. Warning, non fail.
        status = "CONSTANT"
    elif stuck_detected and not is_additive_measure:
        # Stuck su misura di stato (es. livmis bloccato a un valore != 0
        # per ore consecutive). Sospetto.
        status = "STUCK"
    elif stuck_detected and is_additive_measure and longest_value != 0:
        # Stuck su pioggia/neve a un valore non-zero (raro ma significativo)
        status = "STUCK"
    elif out_of_range_count > 0:
        status = "OUT_OF_RANGE"
    elif low_affid_ratio >= LOW_AFFID_RATIO_THRESHOLD:
        status = "LOW_AFFID"
    elif is_quasi_constant and not is_additive_measure:
        status = "QUASI_CONSTANT"
    else:
        status = "OK"

    metrics = {
        "values_count": len(values),
        "unique_values_count": v_unique,
        "value_min": round(v_min, 4),
        "value_max": round(v_max, 4),
        "value_mean": round(v_mean, 4),
        "latest_value": values[-1] if values else None,
        "latest_timestamp": observations[-1]["ts"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "is_additive_measure": is_additive_measure,
        "all_zero": all_zero,
        "stuck_detected": stuck_detected,
        "longest_stuck_streak": longest_streak,
        "longest_stuck_value": longest_value,
        "stuck_threshold": STUCK_THRESHOLD,
        "is_constant": is_constant,
        "is_quasi_constant": is_quasi_constant,
        "out_of_range_count": out_of_range_count,
        "physical_range_min": out_of_range_min,
        "physical_range_max": out_of_range_max,
        "affid_distribution": affid_distribution,
        "low_affid_count": low_affid_count,
        "low_affid_ratio": round(low_affid_ratio, 4),
    }
    if stuck_detected:
        metrics["stuck_value"] = stuck_value
        metrics["stuck_streak"] = stuck_streak

    return metrics, status


def check_all_stations_for_measure(session, headers, measure_type_id, station_ids):
    """Una query bulk, poi analisi anomaly per ciascuna stazione."""
    data, query_latency_ms, error = query_measures_bulk(
        session, headers, measure_type_id, station_ids
    )

    if data is None:
        for sid in station_ids:
            entry = make_log_entry(
                sid, measure_type_id, "FAIL",
                query_latency_ms=round(query_latency_ms, 2),
                error_message=error,
            )
            write_log(entry)
        return

    grouped = group_by_station(data)
    per_station_latency_ms = query_latency_ms / max(len(station_ids), 1)

    for sid in station_ids:
        obs = grouped.get(int(sid), [])
        metrics, status = analyze_station_values(sid, measure_type_id, obs)

        if metrics is None:
            entry = make_log_entry(
                sid, measure_type_id, status,
                values_count=0,
                query_latency_ms=round(per_station_latency_ms, 2),
            )
        else:
            entry = make_log_entry(
                sid, measure_type_id, status,
                query_latency_ms=round(per_station_latency_ms, 2),
                **metrics
            )
        write_log(entry)

        if status == "OK":
            logger.info(f"  station={sid:>6} measure={measure_type_id}: OK "
                        f"({metrics['values_count']} val, "
                        f"range {metrics['value_min']}..{metrics['value_max']})")
        elif status == "OK_NO_EVENT":
            logger.info(f"  station={sid:>6} measure={measure_type_id}: OK_NO_EVENT "
                        f"({metrics['values_count']} val tutti 0 — atteso per misura additiva)")
        elif status in ("STUCK", "CONSTANT", "QUASI_CONSTANT"):
            logger.warning(f"  station={sid:>6} measure={measure_type_id}: {status} "
                           f"(unique={metrics['unique_values_count']}, "
                           f"longest_streak={metrics['longest_stuck_streak']}, "
                           f"val={metrics['value_min']}..{metrics['value_max']})")
        elif status == "OUT_OF_RANGE":
            logger.warning(f"  station={sid:>6} measure={measure_type_id}: OUT_OF_RANGE "
                           f"({metrics['out_of_range_count']} values outside "
                           f"[{metrics['physical_range_min']}, {metrics['physical_range_max']}])")
        elif status == "LOW_AFFID":
            logger.warning(f"  station={sid:>6} measure={measure_type_id}: LOW_AFFID "
                           f"(ratio={metrics['low_affid_ratio']:.1%}, "
                           f"distribution={metrics['affid_distribution']})")
        elif status == "NO_DATA":
            logger.info(f"  station={sid:>6} measure={measure_type_id}: NO_DATA")
        else:
            logger.info(f"  station={sid:>6} measure={measure_type_id}: {status}")


# ============================================================
# Main
# ============================================================
def main():
    logger.info(f"[AVVIO] collector_codata_aao_value_anomaly — hostname={hostname}")
    logger.info(f"[CONFIG] Endpoint: {AAO_BASE_URL}")
    logger.info(f"[CONFIG] Lookback: {LOOKBACK_HOURS}h")
    logger.info(f"[CONFIG] Stuck threshold: {STUCK_THRESHOLD} valori uguali consecutivi")
    logger.info(f"[CONFIG] Low affid threshold: {LOW_AFFID_RATIO_THRESHOLD:.0%}")
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

    stations_to_check = STATION_IDS
    if not stations_to_check:
        logger.info("[CONFIG] STATION_IDS non specificato — auto-discovery")
        stations_to_check = discover_stations(session, headers)
        if not stations_to_check:
            logger.error("[FINE] Nessuna stazione disponibile, abort.")
            return

    logger.info(f"[INFO] Analisi {len(stations_to_check)} stazioni × "
                f"{len(MEASURE_TYPE_IDS)} tipi di misura "
                f"({len(MEASURE_TYPE_IDS)} chiamate API bulk)")
    logger.info("")

    for measure_type_id in MEASURE_TYPE_IDS:
        check_all_stations_for_measure(session, headers,
                                        measure_type_id, stations_to_check)

    logger.info("")
    logger.info("[FINE]")


if __name__ == "__main__":
    main()
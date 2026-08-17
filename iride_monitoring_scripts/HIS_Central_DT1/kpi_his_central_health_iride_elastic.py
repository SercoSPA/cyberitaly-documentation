"""
kpi_his_central_health_iride_elastic.py
========================================
Synthetic health + freshness probe per HIS-Central (GeoDAB / MEEO).

Contesto:
- HIS-Central e' fonte dati aggregati ISPRA (livelli idrometrici
  Lombardia/Piemonte/Emilia-Romagna) usata da CIMS-49.
- Il token nell'URL e' personale dell'utenza CyberItaly, non scade.
- Endpoint stabile: /om-api/observations?limit=N

DUE LIVELLI DI MONITORAGGIO:
  Livello 2 (reachability + latenza): HTTP status + response time
  Livello 4 (freshness dati): timestamp piu' recente estratto dalla
                              risposta, confrontato con l'ora corrente

RATIONALE del limit=50 invece di 1:
  Con limit=1 HIS Central non garantisce ordinamento cronologico, quindi
  il timestamp restituito puo' NON essere l'ultimo disponibile. Scanniamo
  50 observation e calcoliamo max(phenomenonTime.end), che e' il timestamp
  del dato piu' recente cross-stazione.

Output: 1 evento JSON in stile DESP per esecuzione, con:
  - reachability (http_code, latency_ms, response_size_bytes)
  - freshness (data_last_timestamp, data_freshness_minutes,
               data_freshness_status)
  - metadata (station sample, sources sample)

Su file locale (sempre) + Elastic (se ELASTIC_ENABLED=True).
Index Elastic: logs-iride-his-central-health.monitoring-default
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

# Silenzia librerie noisy (POST 201 verbosi, TLS warning)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)

# ============================================================
# Config
# ============================================================
base_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_dir, 'his_central_health_iride_elastic.ini')
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
    fallback='logs-iride-his-central-health.monitoring-default')

# HIS-Central endpoint
HIS_BASE_URL = config.get('HIS_CENTRAL', 'BASE_URL')
HIS_PROBE_PATH = config.get('HIS_CENTRAL', 'PROBE_PATH',
                            fallback='/observations?limit=50')

# Soglie reachability
LATENCY_OK_MS = config.getint('HIS_CENTRAL_HEALTH', 'LATENCY_OK_MS',
                              fallback=3000)
LATENCY_SLOW_MS = config.getint('HIS_CENTRAL_HEALTH', 'LATENCY_SLOW_MS',
                                fallback=10000)
HTTP_TIMEOUT = config.getint('HIS_CENTRAL_HEALTH', 'HTTP_TIMEOUT', fallback=30)

# Soglie freshness (nuove)
FRESH_MAX_MINUTES = config.getint(
    'HIS_CENTRAL_FRESHNESS', 'FRESH_MAX_MINUTES', fallback=180)
STALE_MAX_MINUTES = config.getint(
    'HIS_CENTRAL_FRESHNESS', 'STALE_MAX_MINUTES', fallback=1440)
SAMPLE_SIZE = config.getint(
    'HIS_CENTRAL_FRESHNESS', 'SAMPLE_SIZE', fallback=10)

# Cap "archivio storico": record con phenomenonTime.end piu' vecchi di questa
# soglia sono considerati serie storiche (non problema di ingest) e vengono
# ESCLUSI dai calcoli percentili, stale count e per-source.
# Default 30 giorni (43200 min): se HIS Central serve dati di anni fa e' catalogo
# storico, non un buco di ingest.
# Il worst raw (senza cap) viene comunque preservato in data_worst_raw_minutes
# per debug/audit.
ARCHIVE_CAP_MINUTES = config.getint(
    'HIS_CENTRAL_FRESHNESS', 'ARCHIVE_CAP_MINUTES', fallback=43200)

logger.info(f"[AVVIO] {script_name} — hostname={hostname}")
logger.info(f"[CONFIG] Endpoint: {HIS_BASE_URL}")
logger.info(f"[CONFIG] Probe path: {HIS_PROBE_PATH}")
logger.info(f"[CONFIG] Latency thresholds: OK<{LATENCY_OK_MS}ms, "
            f"SLOW<{LATENCY_SLOW_MS}ms")
logger.info(f"[CONFIG] Freshness thresholds: FRESH<{FRESH_MAX_MINUTES}min, "
            f"STALE<{STALE_MAX_MINUTES}min, "
            f"ARCHIVE_CAP>{ARCHIVE_CAP_MINUTES}min "
            f"(~{ARCHIVE_CAP_MINUTES/1440:.0f}gg)")
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


def parse_iso_timestamp(ts_str):
    """Parsa timestamp ISO in datetime aware UTC. None se non parsabile."""
    if not ts_str:
        return None
    try:
        # Rimuovo suffisso Z e uso fromisoformat (Python 3.7+)
        clean = ts_str.replace('Z', '+00:00')
        dt = datetime.fromisoformat(clean)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, AttributeError):
        return None


def extract_freshness_data(response_json):
    """
    Estrae dai member il timestamp piu' recente + statistiche.
    Ritorna dict con:
        - last_timestamp_iso: str o None
        - observations_scanned: int
        - stations_seen: int (distinct featureOfInterest.title)
        - sample_stations: list[str] (fino a SAMPLE_SIZE)
        - sample_sources: list[str] (distinct source)

    Campi extra (Livello 4+ patch 14/07/2026):
        - per_station: dict {station_title: {last_dt, records}}
        - per_source:  dict {source_name:   {last_dt, records, stations:set}}
    """
    result = {
        "last_timestamp_iso": None,
        "observations_scanned": 0,
        "stations_seen": 0,
        "sample_stations": [],
        "sample_sources": [],
        "per_station": {},   # {title: {"last_dt": datetime, "records": int}}
        "per_source": {},    # {source: {"last_dt": datetime, "records": int, "stations": set}}
    }

    if not isinstance(response_json, dict):
        return result

    members = response_json.get('member') or []
    if not isinstance(members, list):
        return result

    result["observations_scanned"] = len(members)

    max_ts = None
    stations = set()
    sources = set()

    for m in members:
        if not isinstance(m, dict):
            continue

        # Timestamp: phenomenonTime.end
        pt = m.get('phenomenonTime') or {}
        end_ts_str = pt.get('end')
        end_dt = parse_iso_timestamp(end_ts_str)
        if end_dt and (max_ts is None or end_dt > max_ts):
            max_ts = end_dt

        # Stazione
        foi = m.get('featureOfInterest') or {}
        title = foi.get('title')
        if title:
            stations.add(title)

        # Source (provider ISPRA)
        source = None
        for p in (m.get('parameter') or []):
            if isinstance(p, dict) and p.get('name') == 'source':
                source = p.get('value')
                if source:
                    sources.add(source)
                    break

        # Aggregazione per_station: teniamo il piu' recente per stazione
        if title and end_dt:
            st_entry = result["per_station"].get(title)
            if st_entry is None:
                result["per_station"][title] = {
                    "last_dt": end_dt, "records": 1
                }
            else:
                st_entry["records"] += 1
                if end_dt > st_entry["last_dt"]:
                    st_entry["last_dt"] = end_dt

        # Aggregazione per_source: piu' recente, records count, set stazioni
        if source and end_dt:
            src_entry = result["per_source"].get(source)
            if src_entry is None:
                result["per_source"][source] = {
                    "last_dt": end_dt,
                    "records": 1,
                    "stations": {title} if title else set(),
                }
            else:
                src_entry["records"] += 1
                if end_dt > src_entry["last_dt"]:
                    src_entry["last_dt"] = end_dt
                if title:
                    src_entry["stations"].add(title)

    if max_ts:
        result["last_timestamp_iso"] = max_ts.strftime(
            "%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    result["stations_seen"] = len(stations)
    result["sample_stations"] = sorted(stations)[:SAMPLE_SIZE]
    result["sample_sources"] = sorted(sources)

    return result


def compute_freshness_status(last_ts_iso, now_utc):
    """
    Ritorna (freshness_minutes, freshness_status).
    Status: FRESH / STALE / VERY_STALE / UNKNOWN.

    Clock skew handling: differenze negative fino a -5 minuti sono
    normalizzate a 0 (tolleranza per timezone offset e drift NTP tra
    la VM di probe e il server HIS-Central). Sotto -5 min si tiene
    il valore reale ma classificato FRESH (dato futuro comunque OK).
    """
    if not last_ts_iso:
        return None, "UNKNOWN"

    last_dt = parse_iso_timestamp(last_ts_iso)
    if not last_dt:
        return None, "UNKNOWN"

    delta_min = (now_utc - last_dt).total_seconds() / 60.0

    # Clock skew tollerabile: -5 min ≤ delta < 0 → 0.0 pulito
    if -5.0 <= delta_min < 0:
        return 0.0, "FRESH"

    if delta_min < 0:
        # Anomalia server clock significativa (>5 min nel futuro)
        return round(delta_min, 1), "FRESH"
    elif delta_min <= FRESH_MAX_MINUTES:
        return round(delta_min, 1), "FRESH"
    elif delta_min <= STALE_MAX_MINUTES:
        return round(delta_min, 1), "STALE"
    else:
        return round(delta_min, 1), "VERY_STALE"


def compute_freshness_analytics(freshness_data, now_utc):
    """
    Calcola dai per_station / per_source raccolti nel payload:
      - percentili freshness (p50, p95, worst, best) su stazioni RECENTI
      - stale stations: count, pct, list
      - freshness per source: dict {source: {last_ts, minutes_ago,
                                             records, stations, archive_only}}
      - stale sources: count, list (sopra soglia FRESH)

    ARCHIVE CAP (patch 14/07/2026):
      HIS Central serve talvolta record di serie storiche (dati di anni fa)
      insieme a dati recenti. Questi record ROVINANO percentili e stale count
      con valori assurdi (worst = 20 anni fa). Vengono ESCLUSI dal calcolo
      se piu' vecchi di ARCHIVE_CAP_MINUTES.

      Il worst raw pre-cap viene comunque conservato in data_worst_raw_minutes
      per debug/audit. Il count di record archivio esclusi va in
      data_archive_records_excluded.

      Per source, se TUTTI i record sono archivio → archive_only=True nel doc.

    Ritorna dict Elastic-ready.
    """
    analytics = {
        "data_freshness_p50_minutes": None,
        "data_freshness_p95_minutes": None,
        "data_freshness_worst_minutes": None,
        "data_freshness_best_minutes": None,
        "data_worst_raw_minutes": None,
        "data_archive_records_excluded": 0,
        "data_archive_pct": 0.0,
        "data_stale_stations_count": 0,
        "data_stale_stations_pct": 0.0,
        "data_stale_stations_list": [],
        "data_stale_sources_count": 0,
        "data_stale_sources_list": [],
        "data_freshness_per_source": {},
    }

    per_station = freshness_data.get("per_station") or {}
    per_source = freshness_data.get("per_source") or {}

    # ---- Percentili + stale stations ----
    if per_station:
        deltas_all = []          # tutti i delta, per worst raw
        deltas_recent = []       # esclusi gli archivio, per percentili puliti
        stale_list = []
        archive_excluded = 0

        for title, entry in per_station.items():
            dt_last = entry.get("last_dt")
            if not dt_last:
                continue
            delta_min = (now_utc - dt_last).total_seconds() / 60.0
            if -5.0 <= delta_min < 0:
                delta_min = 0.0
            deltas_all.append(delta_min)

            # Applica cap archivio
            if delta_min > ARCHIVE_CAP_MINUTES:
                archive_excluded += 1
                continue  # esclude da percentili + stale count

            deltas_recent.append((delta_min, title))
            if delta_min > FRESH_MAX_MINUTES:
                stale_list.append(title)

        # Worst raw sempre disponibile (incluso archivio)
        if deltas_all:
            analytics["data_worst_raw_minutes"] = round(max(deltas_all), 1)
            analytics["data_archive_records_excluded"] = archive_excluded
            analytics["data_archive_pct"] = round(
                100.0 * archive_excluded / len(deltas_all), 1)

        # Percentili solo sui dati RECENTI (post-cap)
        if deltas_recent:
            values = sorted(d for d, _ in deltas_recent)
            n = len(values)

            def pct(p):
                if n == 1:
                    return values[0]
                idx = (p / 100.0) * (n - 1)
                lo = int(idx)
                hi = min(lo + 1, n - 1)
                frac = idx - lo
                return values[lo] * (1 - frac) + values[hi] * frac

            analytics["data_freshness_p50_minutes"] = round(pct(50), 1)
            analytics["data_freshness_p95_minutes"] = round(pct(95), 1)
            analytics["data_freshness_worst_minutes"] = round(values[-1], 1)
            analytics["data_freshness_best_minutes"] = round(values[0], 1)

            analytics["data_stale_stations_count"] = len(stale_list)
            analytics["data_stale_stations_pct"] = round(
                100.0 * len(stale_list) / n, 1)
            analytics["data_stale_stations_list"] = sorted(stale_list)[:SAMPLE_SIZE]

    # ---- Freshness per source (provider ISPRA) ----
    if per_source:
        stale_sources = []
        for source_name, entry in per_source.items():
            dt_last = entry.get("last_dt")
            if not dt_last:
                continue
            delta_min = (now_utc - dt_last).total_seconds() / 60.0
            if -5.0 <= delta_min < 0:
                delta_min = 0.0

            last_ts_iso = dt_last.strftime(
                "%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

            # Per source: se il MASSIMO delta e' oltre il cap = tutto archivio
            archive_only = delta_min > ARCHIVE_CAP_MINUTES

            analytics["data_freshness_per_source"][source_name] = {
                "last_ts": last_ts_iso,
                "minutes_ago": round(delta_min, 1),
                "records": entry.get("records", 0),
                "stations": len(entry.get("stations") or []),
                "archive_only": archive_only,
            }

            # Stale sources: sopra FRESH ma NON archive (l'archivio non e'
            # un problema di ingestion, e' catalogo storico)
            if delta_min > FRESH_MAX_MINUTES and not archive_only:
                stale_sources.append(source_name)

        analytics["data_stale_sources_count"] = len(stale_sources)
        analytics["data_stale_sources_list"] = sorted(stale_sources)

    return analytics


# ============================================================
# Main probe
# ============================================================
def main():
    probe_url = HIS_BASE_URL.rstrip('/') + HIS_PROBE_PATH

    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    status = "OK"
    http_code = None
    response_size = 0
    error_message = None
    response_snippet = None
    latency_ms = None
    response_json = None

    # ---- HTTP probe ----
    t0 = time.perf_counter()
    try:
        r = requests.get(probe_url, timeout=HTTP_TIMEOUT)
        latency_ms = (time.perf_counter() - t0) * 1000
        http_code = r.status_code
        response_size = len(r.content)
        try:
            response_snippet = r.text[:200] if r.text else None
        except Exception:
            response_snippet = None

        if r.status_code != 200:
            status = "FAIL"
            error_message = f"HTTP {r.status_code}"
        else:
            try:
                response_json = r.json()
            except ValueError:
                status = "FAIL"
                error_message = "Response not JSON"

    except requests.exceptions.Timeout:
        latency_ms = (time.perf_counter() - t0) * 1000
        status = "FAIL"
        error_message = f"Timeout after {HTTP_TIMEOUT}s"
    except requests.exceptions.ConnectionError as e:
        latency_ms = (time.perf_counter() - t0) * 1000
        status = "FAIL"
        error_message = f"ConnectionError: {str(e)[:150]}"
    except Exception as e:
        latency_ms = (time.perf_counter() - t0) * 1000
        status = "FAIL"
        error_message = f"{type(e).__name__}: {str(e)[:150]}"

    # ---- Latency check (solo se HTTP e' andato) ----
    if status == "OK" and latency_ms is not None:
        if latency_ms > LATENCY_SLOW_MS:
            status = "FAIL"
            error_message = f"Latency {latency_ms:.0f}ms > {LATENCY_SLOW_MS}ms"
        elif latency_ms > LATENCY_OK_MS:
            status = "SLOW"

    # ---- Freshness extraction (solo se HTTP OK e JSON parsato) ----
    freshness_data = {
        "last_timestamp_iso": None,
        "observations_scanned": 0,
        "stations_seen": 0,
        "sample_stations": [],
        "sample_sources": [],
    }
    freshness_minutes = None
    freshness_status = "UNKNOWN"

    if response_json is not None:
        freshness_data = extract_freshness_data(response_json)
        freshness_minutes, freshness_status = compute_freshness_status(
            freshness_data["last_timestamp_iso"], now)

        # Combina freshness in response_status:
        # VERY_STALE promuove a FAIL, STALE promuove a SLOW.
        # Un FAIL/SLOW gia' presente NON viene degradato ulteriormente.
        if status == "OK":
            if freshness_status == "VERY_STALE":
                status = "FAIL"
                error_message = (f"Data very stale: {freshness_minutes:.0f}min "
                                 f"since last observation")
            elif freshness_status == "STALE":
                status = "SLOW"
                error_message = (f"Data stale: {freshness_minutes:.0f}min "
                                 f"since last observation")

    # ---- Analytics avanzati (percentili + stale count + per-source) ----
    analytics = compute_freshness_analytics(freshness_data, now) \
        if response_json is not None else {}

    # ---- Costruzione documento ----
    entry = {
        "@timestamp": timestamp,
        "event_timestamp": timestamp,
        "platform": "iride",
        "service_provider_log": "iride_his_central",
        "event_type": "his_central_health_probe",
        "response_status": status,
        "hostname": hostname,
        "endpoint": HIS_BASE_URL,
        "probe_url": probe_url,

        # Reachability
        "http_code": http_code,
        "latency_ms": round(latency_ms, 2) if latency_ms is not None else None,
        "response_size_bytes": response_size,
        "response_snippet": response_snippet,
        "error_message": error_message,
        "latency_threshold_ok_ms": LATENCY_OK_MS,
        "latency_threshold_slow_ms": LATENCY_SLOW_MS,

        # Freshness
        "data_last_timestamp": freshness_data["last_timestamp_iso"],
        "data_freshness_minutes": freshness_minutes,
        "data_freshness_hours": (round(freshness_minutes / 60.0, 2)
                                 if freshness_minutes is not None else None),
        "data_freshness_status": freshness_status,
        "data_observations_scanned": freshness_data["observations_scanned"],
        "data_stations_seen": freshness_data["stations_seen"],
        "data_sample_stations": freshness_data["sample_stations"],
        "data_sample_sources": freshness_data["sample_sources"],
        "freshness_threshold_fresh_min": FRESH_MAX_MINUTES,
        "freshness_threshold_stale_min": STALE_MAX_MINUTES,

        # Analytics avanzati (patch 14/07/2026):
        # percentili di freshness tra tutte le stazioni, count/pct stale,
        # freshness per source ISPRA. Utili per dashboard granulari e alert.
        "data_freshness_p50_minutes": analytics.get("data_freshness_p50_minutes"),
        "data_freshness_p95_minutes": analytics.get("data_freshness_p95_minutes"),
        "data_freshness_worst_minutes": analytics.get("data_freshness_worst_minutes"),
        "data_freshness_best_minutes": analytics.get("data_freshness_best_minutes"),
        # Archive cap (patch 14/07/2026): esclude serie storiche dai calcoli
        "data_worst_raw_minutes": analytics.get("data_worst_raw_minutes"),
        "data_archive_records_excluded": analytics.get("data_archive_records_excluded", 0),
        "data_archive_pct": analytics.get("data_archive_pct", 0.0),
        "archive_cap_minutes": ARCHIVE_CAP_MINUTES,
        "data_stale_stations_count": analytics.get("data_stale_stations_count", 0),
        "data_stale_stations_pct": analytics.get("data_stale_stations_pct", 0.0),
        "data_stale_stations_list": analytics.get("data_stale_stations_list", []),
        "data_stale_sources_count": analytics.get("data_stale_sources_count", 0),
        "data_stale_sources_list": analytics.get("data_stale_sources_list", []),
        "data_freshness_per_source": analytics.get("data_freshness_per_source", {}),
    }

    write_log(entry)

    # ---- Console output ----
    logger.info(f"[HTTP] {http_code} in {latency_ms:.0f}ms "
                f"({response_size} bytes)")
    if response_json is not None:
        logger.info(f"[DATA] observations scanned: "
                    f"{freshness_data['observations_scanned']}, "
                    f"stations seen: {freshness_data['stations_seen']}, "
                    f"sources: {freshness_data['sample_sources']}")
        if freshness_data["last_timestamp_iso"]:
            logger.info(f"[DATA] last timestamp: "
                        f"{freshness_data['last_timestamp_iso']} "
                        f"→ freshness: {freshness_minutes:.0f} min "
                        f"({freshness_minutes/60:.1f} h) "
                        f"= {freshness_status}")
        else:
            logger.warning("[DATA] no timestamp extractable from response")

        # Log analytics avanzati (patch 14/07/2026)
        p50 = analytics.get("data_freshness_p50_minutes")
        p95 = analytics.get("data_freshness_p95_minutes")
        worst = analytics.get("data_freshness_worst_minutes")
        best = analytics.get("data_freshness_best_minutes")
        worst_raw = analytics.get("data_worst_raw_minutes")
        arch_excl = analytics.get("data_archive_records_excluded", 0)
        arch_pct = analytics.get("data_archive_pct", 0.0)

        if arch_excl > 0:
            logger.info(f"[ARCHIVE] {arch_excl} stazioni escluse "
                        f"({arch_pct:.1f}%) — dati > "
                        f"{ARCHIVE_CAP_MINUTES/1440:.0f}gg (serie storiche) | "
                        f"worst_raw={worst_raw:.0f}min "
                        f"(~{worst_raw/1440:.0f}gg)")

        if p50 is not None:
            logger.info(f"[STATS] freshness distribution (post-cap): "
                        f"best={best:.0f}min | p50={p50:.0f}min | "
                        f"p95={p95:.0f}min | worst={worst:.0f}min")
        else:
            logger.warning("[STATS] tutte le stazioni sono archivio, "
                           "nessuna metrica recente disponibile")

        stale_st_count = analytics.get("data_stale_stations_count", 0)
        stale_st_pct = analytics.get("data_stale_stations_pct", 0.0)
        stale_st_list = analytics.get("data_stale_stations_list", [])
        if stale_st_count > 0:
            logger.warning(f"[STALE] {stale_st_count} stazioni stale "
                           f"({stale_st_pct:.1f}%): {stale_st_list}")
        elif p50 is not None:
            logger.info(f"[STALE] tutte le stazioni recenti entro soglia "
                        f"FRESH ({FRESH_MAX_MINUTES}min)")

        for src, info in analytics.get("data_freshness_per_source", {}).items():
            if info.get("archive_only"):
                marker = "📁"    # tag archivio
                extra = " [ARCHIVE ONLY]"
            elif info["minutes_ago"] > FRESH_MAX_MINUTES:
                marker = "⚠"
                extra = ""
            else:
                marker = "✓"
                extra = ""
            logger.info(f"[SOURCE] {marker} {src}: "
                        f"{info['minutes_ago']:.0f}min ago | "
                        f"{info['records']} records | "
                        f"{info['stations']} stations{extra}")

        stale_src_list = analytics.get("data_stale_sources_list", [])
        if stale_src_list:
            logger.warning(f"[STALE-SOURCE] provider stale (non archive): "
                           f"{stale_src_list}")

    if status == "OK":
        logger.info(f"[RESULT] OK — reachability + freshness both healthy")
    elif status == "SLOW":
        logger.warning(f"[RESULT] SLOW — {error_message}")
    else:
        logger.error(f"[RESULT] FAIL — {error_message}")
        if response_snippet:
            logger.error(f"[RESULT] response snippet: {response_snippet}")

    ship_to_elastic(entry)

    logger.info("[FINE]")
    return 0 if status == "OK" else 1


if __name__ == "__main__":
    sys.exit(main())


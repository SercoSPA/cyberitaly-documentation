"""
collector_codata_aao_station_sensors_iride_elastic.py
=====================================================
Inventory dei sensori configurati per ciascuna stazione del network DAO
(CoDataService AAO).

Per ciascuna stazione emette un documento JSON che descrive:
  - Dettagli stazione (codice, descr, lat/lon, quotaslm, zeroidrometrico)
  - Lista dei sensori configurati (con il loro measure_type_id)
  - Conteggio dei sensori totali

A cosa serve: avere una baseline "configurazionale" che permette di
distinguere il NO_DATA atteso (stazione che non ha quel tipo di sensore)
dal NO_DATA inatteso (sensore che ha smesso di pubblicare).

Combinato col collector_freshness, abilita alert tipo:
  "Stazione 10034 normalmente ha sensore pioggia ma non e' piu' nell'inventory"

Cadenza suggerita: ogni 24h (la configurazione delle stazioni cambia raramente).

MODALITA' DRY-RUN ELASTIC:
  ELASTIC_ENABLED=False -> output solo su file locale jsonl.

NOTA: lo script NON scrive nulla sul server CoDataService. Tutte chiamate GET.
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
config_path = os.path.join(base_dir, 'codata_aao_station_sensors_iride_elastic.ini')
config = configparser.ConfigParser()
config.read(config_path)

hostname = os.environ.get("HOSTNAME", socket.gethostname())

LOG_FILE_NAME = 'collector_codata_aao_station_sensors_iride_elastic.log'
log_file = os.path.join(base_dir, LOG_FILE_NAME)

ELASTIC_ENABLED = config['CONFIG'].getboolean('ELASTIC_ENABLED', fallback=False)

es = None
MONITORING_INDEX = None
if ELASTIC_ENABLED:
    from elasticsearch import Elasticsearch
    MONITORING_URL = config['CONFIG']['MONITORING_URL']
    MONITORING_APIKEY = config['CONFIG']['MONITORING_APIKEY']
    MONITORING_VERIFY_CERTS = config['CONFIG'].getboolean('MONITORING_VERIFY_CERTS', fallback=True)
    MONITORING_INDEX = config['CONFIG'].get('STATION_SENSORS_INDEX',
                                             fallback='metrics-iride-codata-aao-station-sensors.monitoring-default')
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

INVENTORY_CFG = config['CODATA_AAO_INVENTORY'] if 'CODATA_AAO_INVENTORY' in config else {}
# Solo le stazioni nel network DAO (id=2 di default)
TARGET_NETWORK_ID = int(INVENTORY_CFG.get('TARGET_NETWORK_ID', '2'))

HTTP_TIMEOUT = 30


# ============================================================
# Helpers
# ============================================================
def make_log_entry(station, sensors, measures_lookup):
    """Documento JSON in stile DESP per una stazione."""
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    # Estrai info stazione (campi noti di CoDataService AAO)
    station_id = station.get("id")
    station_codice = station.get("codice")
    station_descr = station.get("descr")
    station_lat = station.get("lat")
    station_lon = station.get("lon")

    # Costruisci la lista sensori arricchita con il nome del measure type.
    # CoDataService AAO usa il naming italiano:
    #   {"idStazione": 10034, "idSensore": 308, "idTipoDato": 3}
    # dove idTipoDato e' l'ID del tipo di misura (verificato 15/06/2026).
    sensors_list = []
    measure_type_ids = []
    for s in sensors or []:
        if not isinstance(s, dict):
            continue

        # measure_type_id: il campo confermato di AAO e' 'idTipoDato'.
        # Fallback su altri nomi per robustezza in caso di evoluzioni API.
        mid = None
        for k in ('idTipoDato', 'idtipodato', 'idmisura', 'idMisura',
                  'measureId', 'idMeasure', 'measure_type_id',
                  'measureTypeId', 'tipo', 'tipomisura'):
            if k in s and s[k] is not None:
                mid = s[k]
                break

        # sensor_id: il campo confermato di AAO e' 'idSensore'.
        sid = None
        for k in ('idSensore', 'idsensore', 'sensorId', 'sensor_id', 'id',
                  'codice'):
            if k in s and s[k] is not None:
                sid = s[k]
                break

        sensor_entry = {
            "sensor_id": sid,
            "measure_type_id": mid,
            "measure_name": measures_lookup.get(int(mid), "unknown")
                            if mid is not None else "unknown",
        }
        # Aggiungo eventuali altri campi utili se presenti
        for extra_key in ('descr', 'description', 'codice', 'name', 'denominazione'):
            if extra_key in s and s[extra_key] is not None:
                sensor_entry[f"sensor_{extra_key}"] = s[extra_key]

        sensors_list.append(sensor_entry)
        if mid is not None:
            measure_type_ids.append(int(mid))

    entry = {
        "@timestamp": timestamp,
        "event_timestamp": timestamp,
        "platform": "iride",
        "service_provider_log": "iride_codata_aao",
        "event_type": "station_sensors_inventory",
        "response_status": "OK" if sensors_list else "EMPTY_SENSORS",
        "hostname": hostname,
        "endpoint": AAO_BASE_URL,
        "network_id": TARGET_NETWORK_ID,
        "station_id": int(station_id) if station_id is not None else None,
        "station_codice": station_codice,
        "station_descr": station_descr,
        "station_lat": station_lat,
        "station_lon": station_lon,
        "configured_sensors_count": len(sensors_list),
        "configured_measure_type_ids": sorted(set(measure_type_ids)),
        "configured_sensors": sensors_list,
    }

    # Aggiungo location come geo_point per Kibana maps (se lat/lon valide)
    if isinstance(station_lat, (int, float)) and isinstance(station_lon, (int, float)):
        if -90 <= station_lat <= 90 and -180 <= station_lon <= 180:
            entry["location"] = {"lat": station_lat, "lon": station_lon}

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
    """Login -> bearer token (senza prefisso 'Bearer ')."""
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


def load_measures_lookup(session, headers):
    """Carica dictionary {measure_type_id: measure_name} via /api/measures."""
    try:
        r = session.get(f"{AAO_BASE_URL}/api/measures",
                        headers=headers, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            logger.warning(f"[MEASURES] HTTP {r.status_code} — lookup vuoto")
            return {}
        data = r.json()
        lookup = {}
        for m in data:
            mid = m.get("id")
            mname = m.get("description") or m.get("descr") or m.get("name")
            if mid is not None and mname:
                lookup[int(mid)] = mname
        logger.info(f"[MEASURES] caricati {len(lookup)} tipi di misura")
        return lookup
    except Exception as e:
        logger.warning(f"[MEASURES] {type(e).__name__}: {e}")
        return {}


def fetch_network_stations(session, headers, network_id):
    """
    Lista delle stazioni. Usa /api/stations (validato via smoke test) e
    poi filtra per network se possibile. Visto che guest01 vede solo il
    network DAO (id=2), in pratica restituisce tutte le 20 stazioni.

    NOTA: /api/networks/{id}/stations esiste come endpoint ma ritorna
    payload con chiavi diverse (id=None), quindi inutilizzabile per ora.
    """
    try:
        r = session.get(
            f"{AAO_BASE_URL}/api/stations",
            headers=headers, timeout=HTTP_TIMEOUT
        )
        if r.status_code != 200:
            logger.error(f"[STATIONS] HTTP {r.status_code}")
            return []
        all_stations = r.json()
        # Filtro per network se le stazioni hanno il campo. Difensivo:
        # se nessuna lo ha, restituisce tutte (caso guest01).
        filtered = []
        for s in all_stations:
            sid_network = (s.get("networkId") or s.get("networkID")
                           or s.get("network") or s.get("idNetwork")
                           or s.get("idnetwork"))
            if sid_network is None or int(sid_network) == int(network_id):
                filtered.append(s)
        if not filtered:
            # Se filtering era troppo restrittivo, fallback a tutte
            logger.warning(f"[STATIONS] filtro network={network_id} vuoto, "
                           f"fallback a tutte le {len(all_stations)} stazioni")
            return all_stations
        return filtered
    except Exception as e:
        logger.error(f"[STATIONS] {type(e).__name__}: {e}")
        return []


def fetch_station_sensors(session, headers, station_id):
    """Lista dei sensori configurati per una stazione."""
    try:
        r = session.get(
            f"{AAO_BASE_URL}/api/stations/{station_id}/sensors",
            headers=headers, timeout=HTTP_TIMEOUT
        )
        if r.status_code != 200:
            return None
        return r.json()
    except Exception as e:
        logger.warning(f"[SENSORS] station={station_id}: {type(e).__name__}: {e}")
        return None


# ============================================================
# Main
# ============================================================
def main():
    logger.info(f"[AVVIO] collector_codata_aao_station_sensors — hostname={hostname}")
    logger.info(f"[CONFIG] Endpoint: {AAO_BASE_URL}")
    logger.info(f"[CONFIG] Target network: {TARGET_NETWORK_ID}")
    if not ELASTIC_ENABLED:
        logger.info("[CONFIG] >>> DRY-RUN MODE (no Elastic) <<<")
    logger.info("")

    session = requests.Session()
    token = authenticate(session)
    if not token:
        logger.error("[FINE] Authentication failed, abort.")
        return

    headers = {"Authorization": f"Bearer {token}"}

    # 1) Carica lookup measures (id -> nome) per arricchire l'output
    measures_lookup = load_measures_lookup(session, headers)

    # 2) Scarica stazioni del network target
    stations = fetch_network_stations(session, headers, TARGET_NETWORK_ID)
    if not stations:
        logger.error(f"[FINE] Nessuna stazione trovata nel network {TARGET_NETWORK_ID}")
        return

    logger.info(f"[INFO] {len(stations)} stazioni nel network {TARGET_NETWORK_ID}")
    logger.info("")

    # 3) Per ciascuna stazione, scarica i sensori e emetti un evento
    stations_with_sensors = 0
    stations_empty = 0
    stations_skipped = 0

    # Stampa il payload del primo elemento stazione per debug — utile se domani
    # AAO cambia struttura JSON e dobbiamo capire al volo cosa è successo.
    if stations:
        logger.info(f"[DEBUG] Primo elemento stazione: "
                    f"{json.dumps(stations[0], ensure_ascii=False)[:300]}")

    for st in stations:
        # Cerca l'ID stazione in modo difensivo (l'API ha naming non sempre uniforme)
        sid = (st.get("id") or st.get("idStazione") or st.get("idstazione")
               or st.get("stationId") or st.get("stationID")
               or st.get("idstation"))
        if sid is None:
            stations_skipped += 1
            logger.warning(f"[SKIP] Stazione senza id riconoscibile: "
                           f"{list(st.keys())[:10]}")
            continue

        sensors = fetch_station_sensors(session, headers, sid)
        entry = make_log_entry(st, sensors or [], measures_lookup)
        write_log(entry)

        s_count = entry["configured_sensors_count"]
        s_descr = entry["station_descr"] or "?"
        measure_ids = entry["configured_measure_type_ids"]
        # Aggiungi i nomi delle misure per leggibilita' del log
        measure_names = [measures_lookup.get(mid, f"id{mid}")
                         for mid in measure_ids]
        if s_count > 0:
            stations_with_sensors += 1
            logger.info(f"  station={sid:>6} ({s_descr[:40]:40s}) "
                        f"sensors={s_count}  measures={measure_names}")
        else:
            stations_empty += 1
            logger.info(f"  station={sid:>6} ({s_descr[:40]:40s}) "
                        f"sensors=0  (EMPTY)")

    logger.info("")
    logger.info(f"[REPORT] {stations_with_sensors} con sensori configurati / "
                f"{stations_empty} vuote / {stations_skipped} skipped / "
                f"{len(stations)} totali")
    logger.info("[FINE]")


if __name__ == "__main__":
    main()

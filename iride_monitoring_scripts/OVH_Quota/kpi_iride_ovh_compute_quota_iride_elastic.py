"""
kpi_iride_ovh_compute_quota_iride_elastic.py
=============================================
Collector della quota compute (instance, volume, network, loadbalancer)
del tenant OVH CYIT-01 (CyberItaly-01) che ospita IRIDE.

Chiama OVH API:
    GET /cloud/project/{service_name}/region/{REGION}/quota

Emette UN documento JSON DESP-style in stile Elastic con lo snapshot
completo della quota: numero di VM usate vs max, RAM, volumi, network,
load balancer. Conversioni MB/GB -> bytes per coerenza con altri indici.

Pattern di riferimento: quota_ovh.py (DESP).
Adattamenti IRIDE:
  - Indice Elastic dedicato (metrics-iride-ovh-compute-quota.*)
  - Toggle ELASTIC_ENABLED per dry-run
  - Stato OK/ERROR per ciascun documento
  - Read-only: usa solo GET, nessuna scrittura sul tenant
  - Logging strutturato su file + console
  - Hostname reale (non hardcoded "ovh_api")
  - Compat client elasticsearch v7/v8
  - [NUOVO] Campi *_usage_pct additivi per alerting su soglia

Prerequisiti:
    pip install ovh elasticsearch

Uso:
    python kpi_iride_ovh_compute_quota_iride_elastic.py
"""

import json
import os
import socket
import logging
import configparser
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    import ovh
except ImportError:
    print("ERRORE: pip install ovh")
    raise

try:
    from elasticsearch import Elasticsearch
except ImportError:
    print("ERRORE: pip install elasticsearch")
    raise


# ============================================================
# Setup base
# ============================================================
base_dir = os.path.dirname(os.path.abspath(__file__))
script_name = os.path.splitext(os.path.basename(__file__))[0]

config_path = os.path.join(base_dir, 'kpi_iride_ovh_compute_quota_iride_elastic.ini')
config = configparser.ConfigParser()
config.read(config_path)

hostname = socket.gethostname()


# ============================================================
# Config — Elasticsearch
# ============================================================
ELASTIC_ENABLED = config.getboolean('CONFIG', 'ELASTIC_ENABLED', fallback=False)
MONITORING_URL = config.get('CONFIG', 'MONITORING_URL', fallback='')
MONITORING_APIKEY = config.get('CONFIG', 'MONITORING_APIKEY', fallback='')
MONITORING_VERIFY_CERTS = config.getboolean(
    'CONFIG', 'MONITORING_VERIFY_CERTS', fallback=True)
MONITORING_INDEX = config.get(
    'CONFIG', 'MONITORING_INDEX',
    fallback='metrics-iride-ovh-compute-quota.monitoring-default')

LOG_FILE_NAME = config.get(
    'CONFIG', 'LOG_FILE_NAME',
    fallback=f"{script_name}.log")

# File NDJSON: contiene SOLO i documenti JSON, una riga per documento.
# Formato pronto per ingestion via Filebeat verso Elastic.
NDJSON_FILE_NAME = config.get(
    'CONFIG', 'NDJSON_FILE_NAME',
    fallback=f"{script_name}.log")


# ============================================================
# Config — Logging
# ============================================================
log_file_path = os.path.join(base_dir, LOG_FILE_NAME)
ndjson_file_path = os.path.join(base_dir, NDJSON_FILE_NAME)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(script_name)


# ============================================================
# Config — OVH
# ============================================================
OVH_ENDPOINT = config.get('OVH', 'ENDPOINT', fallback='ovh-eu')
OVH_APPLICATION_KEY = config.get('OVH', 'APPLICATION_KEY')
OVH_APPLICATION_SECRET = config.get('OVH', 'APPLICATION_SECRET')
OVH_CONSUMER_KEY = config.get('OVH', 'CONSUMER_KEY')
OVH_PROJECT_ID = config.get('OVH', 'PROJECT_ID')

# SERVICE_NAME: UUID usato nel path API /cloud/project/{SERVICE_NAME}/...
# Negli script DESP era diverso dal PROJECT_ID. Per IRIDE assumiamo lo
# stesso UUID (CYIT-01) ma teniamo il campo separato per flessibilita'.
# Default = PROJECT_ID se non specificato.
OVH_SERVICE_NAME = config.get('OVH', 'SERVICE_NAME', fallback=OVH_PROJECT_ID)
OVH_REGION = config.get('OVH', 'REGION', fallback='GRA')

SERVICE_NAME = config.get('IRIDE', 'SERVICE_NAME', fallback='iride-cyberitaly')


# ============================================================
# Validazione config
# ============================================================
def _validate_config() -> bool:
    """Verifica che i parametri critici siano valorizzati e non placeholder."""
    placeholders = ('CHANGE_ME', '<TODO>', '')
    required = {
        'OVH.APPLICATION_KEY': OVH_APPLICATION_KEY,
        'OVH.APPLICATION_SECRET': OVH_APPLICATION_SECRET,
        'OVH.CONSUMER_KEY': OVH_CONSUMER_KEY,
        'OVH.PROJECT_ID': OVH_PROJECT_ID,
        'OVH.SERVICE_NAME': OVH_SERVICE_NAME,
        'OVH.REGION': OVH_REGION,
    }
    missing = [k for k, v in required.items()
               if not v or any(p in str(v) for p in placeholders if p)]
    if missing:
        logger.error(
            "Config incompleta: i seguenti campi sono mancanti o placeholder: %s",
            missing)
        return False
    return True


# ============================================================
# Elastic client (creato solo se ELASTIC_ENABLED=True)
# ============================================================
es: Optional[Elasticsearch] = None
if ELASTIC_ENABLED:
    es = Elasticsearch(
        [MONITORING_URL],
        headers={"Authorization": f"ApiKey {MONITORING_APIKEY}"},
        verify_certs=MONITORING_VERIFY_CERTS,
    )


# ============================================================
# Helpers
# ============================================================
def now_iso() -> str:
    """Timestamp UTC con millisecondi e suffisso Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def mb_to_bytes(mb: float) -> int:
    return int(mb * 1024 * 1024)


def gb_to_bytes(gb: float) -> int:
    return int(gb * 1024 * 1024 * 1024)


def _pct(used: Any, mx: Any) -> Optional[float]:
    """
    Percentuale di utilizzo used/max, arrotondata a 2 decimali.
    Ritorna None se i dati mancano o max e' 0/None (stessa filosofia
    difensiva dei .get() in enrich_with_bytes: mai crashare se l'API
    OVH omette un valore).
    """
    try:
        if used is None or not mx:
            return None
        return round(used / mx * 100.0, 2)
    except (TypeError, ZeroDivisionError):
        return None


def enrich_with_bytes(quota_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Aggiunge campi *_bytes alle metriche in MB/GB cosi' Kibana puo' fare
    aggregazioni omogenee senza dover convertire le unita' al volo.

    Pattern identico a quota_ovh.py DESP, ma resiliente: usa .get() invece
    di accessi diretti per non rompersi se l'API OVH dovesse omettere
    una sezione (es. niente loadbalancer su un progetto base).
    """
    out = json.loads(json.dumps(quota_data))  # deep copy semplice

    # ---- RAM (MB -> bytes) ----
    inst = out.get("instance") or {}
    if "usedRAM" in inst:
        inst["usedRAM_bytes"] = mb_to_bytes(inst["usedRAM"])
    if "maxRam" in inst:
        inst["maxRam_bytes"] = mb_to_bytes(inst["maxRam"])

    # ---- Volume (GB -> bytes) ----
    vol = out.get("volume") or {}
    for src_key in ("usedGigabytes", "maxGigabytes",
                    "maxBackupGigabytes", "usedBackupGigabytes"):
        if src_key in vol and vol[src_key] is not None:
            vol[f"{src_key}_bytes"] = gb_to_bytes(vol[src_key])

    return out


def build_payload(quota_data: Dict[str, Any],
                  status: str = "OK",
                  message: str = "OVH quota statistics fetched successfully"
                  ) -> Dict[str, Any]:
    """Costruisce il documento Elastic finale."""
    timestamp = now_iso()
    enriched = enrich_with_bytes(quota_data) if quota_data else {}

    # --- NUOVO: percentuali di utilizzo quota (additive, tenant-level) ---
    # Servono per l'alerting su soglia (es. cores_usage_pct > 85). Non
    # sostituiscono nessun campo esistente: si affiancano ai used/max grezzi.
    _inst = enriched.get("instance", {}) or {}
    _vol = enriched.get("volume", {}) or {}

    return {
        "@timestamp": timestamp,
        "event_timestamp": timestamp,
        "platform": "iride",
        "service_provider_log": "iride_ovh_compute",
        "service_name": SERVICE_NAME,
        "event_type": "ovh_compute_quota",
        "response_status": status,
        "hostname": hostname,
        "ovh_endpoint": OVH_ENDPOINT,
        "ovh_project_id": OVH_PROJECT_ID,
        "ovh_service_name": OVH_SERVICE_NAME,
        "ovh_region": OVH_REGION,
        "instance": enriched.get("instance", {}),
        "loadbalancer": enriched.get("loadbalancer", {}),
        "network": enriched.get("network", {}),
        "volume": enriched.get("volume", {}),
        "keymanager": enriched.get("keymanager", {}),

        # --- NUOVO: percentuali quota per alerting ---
        "cores_usage_pct": _pct(_inst.get("usedCores"), _inst.get("maxCores")),
        "ram_usage_pct": _pct(_inst.get("usedRAM"), _inst.get("maxRam")),
        "instances_usage_pct": _pct(_inst.get("usedInstances"),
                                    _inst.get("maxInstances")),
        "volume_usage_pct": _pct(_vol.get("usedGigabytes"),
                                 _vol.get("maxGigabytes")),

        "message": message,
    }


def write_log(entry: Dict[str, Any]) -> None:
    """
    Scrive il documento sia su file NDJSON (sempre) sia su Elastic (se abilitato).
    Il file di log "umano" riceve solo un messaggio di conferma sintetico.
    """
    line = json.dumps(entry, ensure_ascii=False, separators=(',', ':'))

    # 1) File NDJSON: una riga per documento, pronto per Filebeat -> Elastic.
    #    NIENTE prefissi timestamp, NIENTE prosa: solo il JSON puro.
    try:
        with open(ndjson_file_path, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except Exception as e:
        logger.error("[NDJSON] errore scrittura file: %s: %s",
                     type(e).__name__, e)

    # 2) Log umano: messaggio sintetico, non il JSON intero.
    inst = entry.get("instance", {}) or {}
    vol = entry.get("volume", {}) or {}
    logger.info(
        "[SALVATO] quota snapshot status=%s — VM %s/%s, RAM %s/%s MB, vol %s/%s GB",
        entry.get("response_status", "?"),
        inst.get("usedInstances", "?"), inst.get("maxInstances", "?"),
        inst.get("usedRAM", "?"), inst.get("maxRam", "?"),
        vol.get("usedGigabytes", "?"), vol.get("maxGigabytes", "?"),
    )

    # 3) Indicizzazione diretta su Elastic (se abilitato).
    if not ELASTIC_ENABLED:
        return

    try:
        # Compat sia v7 (body=) che v8 (document=)
        try:
            es.index(index=MONITORING_INDEX, document=entry)
        except TypeError:
            es.index(index=MONITORING_INDEX, body=entry)
    except Exception as e:
        logger.error("[ELASTIC] errore indicizzazione: %s: %s",
                     type(e).__name__, e)


# ============================================================
# OVH API call
# ============================================================
def fetch_quota(client: "ovh.Client", service_name: str,
                region: str) -> Dict[str, Any]:
    """
    GET /cloud/project/{service_name}/region/{region}/quota

    Ritorna il payload OVH (dict con sezioni instance/volume/network/...).
    Pattern identico a quota_ovh.py DESP.
    """
    path = f"/cloud/project/{service_name}/region/{region}/quota"
    logger.info("[OVH] GET %s", path)
    result = client.get(path)
    return result or {}


# ============================================================
# Main
# ============================================================
def main() -> int:
    logger.info("=" * 70)
    logger.info("[AVVIO] %s — hostname=%s", script_name, hostname)
    logger.info("[CONFIG] OVH endpoint: %s", OVH_ENDPOINT)
    logger.info("[CONFIG] OVH project_id: %s", OVH_PROJECT_ID)
    logger.info("[CONFIG] OVH service_name (path API): %s", OVH_SERVICE_NAME)
    logger.info("[CONFIG] OVH region: %s", OVH_REGION)
    logger.info("[CONFIG] Service name (Elastic field): %s", SERVICE_NAME)
    logger.info("[CONFIG] NDJSON file (per Elastic ingestion): %s",
                ndjson_file_path)

    if ELASTIC_ENABLED:
        logger.info("[CONFIG] Elastic ENABLED -> %s (index=%s)",
                    MONITORING_URL, MONITORING_INDEX)
    else:
        logger.info("[CONFIG] >>> DRY-RUN MODE (no Elastic) <<<")
    logger.info("=" * 70)

    if not _validate_config():
        logger.error("[FINE] Config non valida, abort.")
        return 1

    # Crea client OVH
    try:
        client = ovh.Client(
            endpoint=OVH_ENDPOINT,
            application_key=OVH_APPLICATION_KEY,
            application_secret=OVH_APPLICATION_SECRET,
            consumer_key=OVH_CONSUMER_KEY,
        )
    except Exception as e:
        logger.error("[OVH] errore creazione client: %s: %s", type(e).__name__, e)
        return 2

    # Chiama l'API
    try:
        quota = fetch_quota(client, OVH_SERVICE_NAME, OVH_REGION)
    except ovh.exceptions.APIError as e:
        logger.error("[OVH] API error: %s", e)
        write_log(build_payload({}, status="ERROR",
                                message=f"OVH API error: {e}"))
        return 3
    except Exception as e:
        logger.error("[OVH] errore generico: %s: %s", type(e).__name__, e)
        write_log(build_payload({}, status="ERROR",
                                message=f"{type(e).__name__}: {e}"))
        return 4

    if not quota:
        logger.warning("[OVH] Quota payload vuoto.")
        write_log(build_payload({}, status="OK",
                                message="Empty quota payload"))
        logger.info("[FINE]")
        return 0

    # Emette un documento unico con tutto lo snapshot quota
    payload = build_payload(quota, status="OK")
    write_log(payload)

    # Report sintetico in log per leggibilita'
    inst = payload.get("instance", {}) or {}
    vol = payload.get("volume", {}) or {}
    net = payload.get("network", {}) or {}
    lb = payload.get("loadbalancer", {}) or {}

    logger.info(
        "[REPORT] instances: %s/%s VM, RAM %s/%s MB, cores %s/%s",
        inst.get("usedInstances", "?"), inst.get("maxInstances", "?"),
        inst.get("usedRAM", "?"), inst.get("maxRam", "?"),
        inst.get("usedCores", "?"), inst.get("maxCores", "?"),
    )
    logger.info(
        "[REPORT] volumes: %s/%s GB used, %s/%s volumes count",
        vol.get("usedGigabytes", "?"), vol.get("maxGigabytes", "?"),
        vol.get("volumeCount", "?"), vol.get("maxVolumeCount", "?"),
    )
    if net:
        logger.info(
            "[REPORT] network: %s/%s networks, %s/%s subnets",
            net.get("usedNetworks", "?"), net.get("maxNetworks", "?"),
            net.get("usedSubnets", "?"), net.get("maxSubnets", "?"),
        )
    if lb:
        logger.info(
            "[REPORT] loadbalancer: %s/%s",
            lb.get("usedLoadbalancers", "?"), lb.get("maxLoadbalancers", "?"),
        )

    logger.info("[FINE]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

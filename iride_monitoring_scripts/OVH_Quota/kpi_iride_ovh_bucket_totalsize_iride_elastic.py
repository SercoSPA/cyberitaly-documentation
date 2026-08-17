"""
kpi_iride_ovh_bucket_totalsize_iride_elastic.py
================================================
Collector della dimensione totale dei bucket Object Storage del tenant
OVH IRIDE CyberItaly.

Chiama OVH API:
    GET /cloud/project/{project_id}/region/{REGION}/storage

Per ciascun bucket trovato emette un documento JSON DESP-style in stile
Elastic, con dimensione in byte/MB/GB e numero di oggetti.

Pattern di riferimento: service_ovhbucket_totalsize_elastic.py (DESP).
Adattamenti IRIDE:
  - Indice Elastic dedicato (metrics-iride-ovh-bucket-totalsize.*)
  - Toggle ELASTIC_ENABLED per dry-run
  - Stato OK/ERROR per ciascun documento
  - Read-only: usa solo GET, nessuna scrittura sul tenant
  - Logging strutturato + log file

Prerequisiti:
    pip install ovh elasticsearch

Uso:
    python kpi_iride_ovh_bucket_totalsize_iride_elastic.py
"""

import json
import os
import socket
import logging
import configparser
import time
import urllib3
import warnings
from datetime import datetime, timezone
from typing import Dict, List, Optional

# Silenzia rumore cosmetico: warning TLS self-signed di Elastic + deprecation
# warnings di client library. Errori veri restano visibili.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

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

config_path = os.path.join(base_dir, 'kpi_iride_ovh_bucket_totalsize_iride_elastic.ini')
config = configparser.ConfigParser()
config.read(config_path)

# Hostname (utile per filtrare in Kibana se piu' VM scrivono sullo stesso indice)
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
    fallback='metrics-iride-ovh-bucket-totalsize.monitoring-default')

LOG_FILE_NAME = config.get(
    'CONFIG', 'LOG_FILE_NAME',
    fallback=f"{script_name}.log")

# File NDJSON: contiene SOLO i documenti JSON, una riga per documento.
# Formato pronto per ingestion via Filebeat verso Elastic.
# Separato dal LOG_FILE_NAME (che contiene la prosa umana).
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

# Silenzia il rumore verbose dei client library (POST 201 per ogni doc,
# handshake dettagli boto3/OVH SDK, deprecation TLS). Warning/error reali restano.
logging.getLogger('elasticsearch').setLevel(logging.WARNING)
logging.getLogger('elastic_transport').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('ovh').setLevel(logging.WARNING)


# ============================================================
# Config — OVH
# ============================================================
OVH_ENDPOINT = config.get('OVH', 'ENDPOINT', fallback='ovh-eu')
OVH_APPLICATION_KEY = config.get('OVH', 'APPLICATION_KEY')
OVH_APPLICATION_SECRET = config.get('OVH', 'APPLICATION_SECRET')
OVH_CONSUMER_KEY = config.get('OVH', 'CONSUMER_KEY')
OVH_PROJECT_ID = config.get('OVH', 'PROJECT_ID')
OVH_REGION = config.get('OVH', 'REGION', fallback='GRA')

# Nome simbolico del servizio (per il campo service_name nel doc Elastic).
# Permette di distinguere in Kibana se in futuro monitoriamo piu' tenant.
SERVICE_NAME = config.get('IRIDE', 'SERVICE_NAME', fallback='iride-cyberitaly')

# Cadenza interna tra una scrittura Elastic e la successiva, in secondi.
# Mantiene il pattern DESP di "non bombardare Elastic".
INTER_DOC_SLEEP_SEC = config.getfloat(
    'IRIDE', 'INTER_DOC_SLEEP_SEC', fallback=0.5)


# ============================================================
# Validazione config
# ============================================================
def _validate_config():
    """Verifica che i parametri critici siano valorizzati e non placeholder."""
    placeholders = ('CHANGE_ME', 'CHANGE_ME_AFTER', '<TODO>', '')
    required = {
        'OVH.APPLICATION_KEY': OVH_APPLICATION_KEY,
        'OVH.APPLICATION_SECRET': OVH_APPLICATION_SECRET,
        'OVH.CONSUMER_KEY': OVH_CONSUMER_KEY,
        'OVH.PROJECT_ID': OVH_PROJECT_ID,
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
    """Timestamp UTC con millisecondi e suffisso Z (formato DESP standard)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def create_log_entry(
    bucket_name: str,
    size_bytes: int,
    object_count: int,
    status: str = "OK",
    message: str = "Service Bucket Size Success",
) -> Dict:
    """Crea un documento JSON DESP-style per Elastic."""
    timestamp = now_iso()
    size_mb = round(size_bytes / (1024 ** 2), 3) if size_bytes else 0.0
    size_gb = round(size_bytes / (1024 ** 3), 4) if size_bytes else 0.0
    return {
        "@timestamp": timestamp,
        "event_timestamp": timestamp,
        "platform": "iride",
        "service_provider_log": "iride_ovh_storage",
        "service_name": SERVICE_NAME,
        "event_type": "ovh_bucket_totalsize",
        "response_status": status,
        "hostname": hostname,
        "ovh_endpoint": OVH_ENDPOINT,
        "ovh_project_id": OVH_PROJECT_ID,
        "ovh_region": OVH_REGION,
        "bucket_name": bucket_name,
        "size": int(size_bytes),
        "size_mb": size_mb,
        "size_gb": size_gb,
        "objects": int(object_count),
        "message": message,
    }


def write_log(entry: Dict) -> None:
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

    # 2) Log umano: solo conferma sintetica leggibile (no JSON dump completo).
    logger.info("[SALVATO] bucket=%s size=%d bytes objects=%d status=%s",
                entry.get("bucket_name", "?"),
                entry.get("size", 0),
                entry.get("objects", 0),
                entry.get("response_status", "?"))

    # 3) Indicizzazione diretta su Elastic (se abilitato).
    if not ELASTIC_ENABLED:
        return

    try:
        # Compat con sia versione vecchia che nuova del client elasticsearch:
        # versioni >=8 vogliono 'document=', <8 vogliono 'body='. Provo prima
        # 'document=' e in caso di TypeError ripiego su 'body='.
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
def get_bucket_sizes(client: "ovh.Client", project_id: str,
                     region: str) -> List[Dict[str, object]]:
    """
    GET /cloud/project/{project_id}/region/{region}/storage

    Ritorna lista di dict con almeno: name, objectsSize, objectsCount.
    Pattern identico a service_ovhbucket_totalsize_elastic.py (DESP).
    """
    path = f"/cloud/project/{project_id}/region/{region}/storage"
    logger.info("[OVH] GET %s", path)
    result = client.get(path)

    buckets = []
    for storage_object in result or []:
        buckets.append({
            "name": storage_object.get('name', 'unknown_bucket'),
            "size": int(storage_object.get('objectsSize', 0)),
            "count": int(storage_object.get('objectsCount', 0)),
        })
    return buckets


# ============================================================
# Main
# ============================================================
def main() -> int:
    logger.info("=" * 70)
    logger.info("[AVVIO] %s — hostname=%s", script_name, hostname)
    logger.info("[CONFIG] OVH endpoint: %s", OVH_ENDPOINT)
    logger.info("[CONFIG] OVH project_id: %s", OVH_PROJECT_ID)
    logger.info("[CONFIG] OVH region: %s", OVH_REGION)
    logger.info("[CONFIG] Service name: %s", SERVICE_NAME)
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
        buckets = get_bucket_sizes(client, OVH_PROJECT_ID, OVH_REGION)
    except ovh.exceptions.APIError as e:
        logger.error("[OVH] API error: %s", e)
        # Emette comunque un documento di stato ERROR cosi' in Kibana
        # si vede l'anomalia (utile per alert su 'response_status: ERROR')
        error_entry = create_log_entry(
            bucket_name="N/A",
            size_bytes=0,
            object_count=0,
            status="ERROR",
            message=f"OVH API error: {e}",
        )
        write_log(error_entry)
        return 3
    except Exception as e:
        logger.error("[OVH] errore generico: %s: %s", type(e).__name__, e)
        error_entry = create_log_entry(
            bucket_name="N/A",
            size_bytes=0,
            object_count=0,
            status="ERROR",
            message=f"{type(e).__name__}: {e}",
        )
        write_log(error_entry)
        return 4

    if not buckets:
        logger.warning("[OVH] Nessun bucket trovato nel project %s region %s.",
                       OVH_PROJECT_ID, OVH_REGION)
        empty_entry = create_log_entry(
            bucket_name="N/A",
            size_bytes=0,
            object_count=0,
            status="OK",
            message="No buckets found",
        )
        write_log(empty_entry)
        logger.info("[FINE]")
        return 0

    # Emette 1 documento per ciascun bucket
    logger.info("[OVH] %d bucket trovati nel project %s region %s.",
                len(buckets), OVH_PROJECT_ID, OVH_REGION)

    total_size = 0
    for b in buckets:
        entry = create_log_entry(
            bucket_name=b["name"],
            size_bytes=b["size"],
            object_count=b["count"],
            status="OK",
        )
        write_log(entry)
        total_size += b["size"]
        if INTER_DOC_SLEEP_SEC > 0:
            time.sleep(INTER_DOC_SLEEP_SEC)

    total_gb = total_size / (1024 ** 3)
    logger.info("[REPORT] %d bucket processati. "
                "Totale storage: %d bytes (%.3f GB).",
                len(buckets), total_size, total_gb)
    logger.info("[FINE]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

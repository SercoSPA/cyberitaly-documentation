"""
kpi_iride_ovh_k8s_nodepools_iride_elastic.py
=============================================
Monitoring dei cluster K8s e relativi nodepool sul tenant OVH IRIDE CyberItaly.

Sviluppato 26/06/2026 dopo feedback Stefano: lo script kpi_iride_ovh_compute_quota
mostrava la quota globale del tenant (VM, RAM, vol aggregati), ma per il
monitoring outcome serve granularita' per nodepool, cosi' si vedono le
risorse consumate per ciascuna componente (mgmt, proc, dingest-core,
dingest-dret, tools, models, etc).

Usa l'API OVH /cloud/project/{id}/kube/* (validata 26/06/2026):
  GET /cloud/project/{id}/kube                          → lista cluster
  GET /cloud/project/{id}/kube/{kubeId}                 → info cluster
  GET /cloud/project/{id}/kube/{kubeId}/nodepool        → lista nodepool

Output: 1 documento JSON per nodepool con metadati cluster + dettagli pool +
campi derivati (vcpu/ram aggregate calcolate da flavor × nodi).

Indice Elastic target (suggerito):
  metrics-iride-ovh-k8s-nodepool.monitoring-default

Pattern d'uso tipico:
  Ogni 10 min in cron. In Kibana filtra per cluster_name="K8S-CYBERITALY-PROD"
  per vedere solo il cluster prod, oppure cross-cluster per overview tenant.
"""

import configparser
import datetime as dt
import json
import logging
import socket
import sys
import urllib3
import warnings
from pathlib import Path

# Silenzia rumore cosmetico: warning TLS self-signed Elastic + deprecation.
# Errori veri restano visibili.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

try:
    import ovh
except ImportError:
    print("FATAL: lib 'ovh' non installata. pip install ovh", file=sys.stderr)
    sys.exit(1)

# ============================================================
# CONFIG
# ============================================================
SCRIPT_DIR = Path(__file__).resolve().parent
INI_PATH = SCRIPT_DIR / "kpi_iride_ovh_k8s_nodepools_iride_elastic.ini"

config = configparser.ConfigParser()
if not INI_PATH.exists():
    print(f"FATAL: INI non trovato a {INI_PATH}", file=sys.stderr)
    sys.exit(1)
config.read(INI_PATH)

# Elastic (per ora disabilitato finche' non abbiamo endpoint)
ELASTIC_ENABLED = config.getboolean('CONFIG', 'ELASTIC_ENABLED',
                                    fallback=False)
MONITORING_URL = config.get('CONFIG', 'MONITORING_URL', fallback='')
MONITORING_APIKEY = config.get('CONFIG', 'MONITORING_APIKEY', fallback='')
MONITORING_VERIFY_CERTS = config.getboolean('CONFIG',
                                            'MONITORING_VERIFY_CERTS',
                                            fallback=True)
INVENTORY_INDEX = config.get(
    'CONFIG', 'INVENTORY_INDEX',
    fallback='metrics-iride-ovh-k8s-nodepool.monitoring-default')

LOG_FILE_NAME = config.get(
    'CONFIG', 'LOG_FILE_NAME',
    fallback='kpi_iride_ovh_k8s_nodepools_iride_elastic.log')

SERVICE_NAME_FIELD = config.get('CONFIG', 'SERVICE_NAME_FIELD',
                                fallback='iride-cyberitaly')

# OVH credentials
OVH_ENDPOINT = config.get('OVH', 'ENDPOINT', fallback='ovh-eu')
OVH_APPLICATION_KEY = config.get('OVH', 'APPLICATION_KEY')
OVH_APPLICATION_SECRET = config.get('OVH', 'APPLICATION_SECRET')
OVH_CONSUMER_KEY = config.get('OVH', 'CONSUMER_KEY')
OVH_PROJECT_ID = config.get('OVH', 'PROJECT_ID')

# ============================================================
# Flavor lookup table (OVH Public Cloud).
# Discovery 26/06/2026: flavor incontrati sul tenant CYIT-01 sono:
# b3-16, b3-32, b3-128, r3-64, r3-256.
# Specifiche da OVH catalog (vcpu, ram, disk_local).
# ============================================================
FLAVOR_SPECS = {
    # Bx-* = General purpose (balanced CPU/RAM)
    "b3-8":    {"vcpus": 2,  "ram_gb": 8,   "disk_gb": 100},
    "b3-16":   {"vcpus": 4,  "ram_gb": 16,  "disk_gb": 100},
    "b3-32":   {"vcpus": 8,  "ram_gb": 32,  "disk_gb": 200},
    "b3-64":   {"vcpus": 16, "ram_gb": 64,  "disk_gb": 400},
    "b3-128":  {"vcpus": 32, "ram_gb": 128, "disk_gb": 400},
    "b3-256":  {"vcpus": 64, "ram_gb": 256, "disk_gb": 400},
    # Rx-* = RAM optimized
    "r3-64":   {"vcpus": 8,  "ram_gb": 64,  "disk_gb": 200},
    "r3-128":  {"vcpus": 16, "ram_gb": 128, "disk_gb": 400},
    "r3-256":  {"vcpus": 32, "ram_gb": 256, "disk_gb": 400},
    "r3-512":  {"vcpus": 64, "ram_gb": 512, "disk_gb": 400},
    # Cx-* = CPU optimized (per completezza, eventualmente)
    "c3-8":    {"vcpus": 4,  "ram_gb": 8,   "disk_gb": 100},
    "c3-16":   {"vcpus": 8,  "ram_gb": 16,  "disk_gb": 200},
}

# ============================================================
# LOGGING (console = umano, file = JSON puro)
# ============================================================
script_name = Path(__file__).stem
hostname = socket.gethostname()

logger = logging.getLogger(script_name)
logger.setLevel(logging.INFO)
logger.handlers.clear()

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s'))
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

# Silenzia il rumore verbose dei client library. WARNING/ERROR reali restano.
logging.getLogger('elasticsearch').setLevel(logging.WARNING)
logging.getLogger('elastic_transport').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('ovh').setLevel(logging.WARNING)


def write_log(doc):
    """Scrive 1 documento JSON sul .log file (1 riga per documento)."""
    json_logger.info(json.dumps(doc, default=str))


def ship_to_elastic(doc):
    """Placeholder per Elastic ingestion. Abilita ELASTIC_ENABLED quando pronto."""
    if not ELASTIC_ENABLED:
        return
    try:
        import urllib.request
        url = f"{MONITORING_URL.rstrip('/')}/{INVENTORY_INDEX}/_doc"
        body = json.dumps(doc).encode('utf-8')
        req = urllib.request.Request(
            url, data=body,
            headers={
                'Authorization': f'ApiKey {MONITORING_APIKEY}',
                'Content-Type': 'application/json',
            },
            method='POST')
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
# BUILD NODEPOOL DOCUMENT
# ============================================================
def build_nodepool_doc(timestamp, cluster_info, nodepool):
    """
    Costruisce il documento JSON Elastic per un singolo nodepool.
    Include metadati cluster, dettagli pool, e campi derivati (vcpu/ram
    aggregate calcolate da flavor × nodi attivi/max).
    """
    flavor = nodepool.get("flavor", "unknown")
    specs = FLAVOR_SPECS.get(flavor, {})

    current_nodes = nodepool.get("currentNodes", 0)
    desired_nodes = nodepool.get("desiredNodes", 0)
    min_nodes = nodepool.get("minNodes", 0)
    max_nodes = nodepool.get("maxNodes", 0)
    available_nodes = nodepool.get("availableNodes", 0)

    vcpus_per_node = specs.get("vcpus")
    ram_gb_per_node = specs.get("ram_gb")
    disk_gb_per_node = specs.get("disk_gb")

    # Aggregati: solo se conosciamo il flavor
    current_vcpus = (current_nodes * vcpus_per_node) if vcpus_per_node else None
    current_ram_gb = (current_nodes * ram_gb_per_node) if ram_gb_per_node else None
    current_disk_gb = (current_nodes * disk_gb_per_node) if disk_gb_per_node else None
    max_vcpus = (max_nodes * vcpus_per_node) if vcpus_per_node else None
    max_ram_gb = (max_nodes * ram_gb_per_node) if ram_gb_per_node else None

    # Utilizzazione attuale rispetto al massimo (per Kibana gauge)
    utilization_pct = None
    if max_nodes > 0:
        utilization_pct = round(current_nodes / max_nodes * 100.0, 2)

    # Health status semplice del nodepool
    np_status = nodepool.get("status", "Unknown")
    size_status = nodepool.get("sizeStatus", "Unknown")
    if np_status == "READY" and size_status == "CAPACITY_OK":
        health_status = "HEALTHY"
    elif np_status == "READY":
        health_status = "DEGRADED"  # READY ma capacity non OK
    else:
        health_status = "DOWN"

    return {
        # DESP-style standard
        "@timestamp": timestamp,
        "event_timestamp": timestamp,
        "platform": "iride",
        "service_provider_log": "iride_ovh_k8s",
        "service_name": SERVICE_NAME_FIELD,
        "event_type": "ovh_k8s_nodepool_status",
        "response_status": "OK",
        "hostname": hostname,

        # Cluster context
        "cluster_id": cluster_info["id"],
        "cluster_name": cluster_info["name"],
        "cluster_region": cluster_info["region"],
        "cluster_k8s_version": cluster_info["version"],
        "cluster_status": cluster_info["status"],

        # Nodepool identification
        "nodepool_id": nodepool.get("id"),
        "nodepool_name": nodepool.get("name"),
        "flavor": flavor,

        # Nodepool status
        "status": np_status,
        "size_status": size_status,
        "health_status": health_status,

        # Node counts
        "current_nodes": current_nodes,
        "desired_nodes": desired_nodes,
        "min_nodes": min_nodes,
        "max_nodes": max_nodes,
        "available_nodes": available_nodes,
        "up_to_date_nodes": nodepool.get("upToDateNodes", 0),
        "utilization_pct": utilization_pct,

        # Autoscaling config
        "autoscale_enabled": nodepool.get("autoscale", False),
        "anti_affinity": nodepool.get("antiAffinity", False),
        "monthly_billed": nodepool.get("monthlyBilled", False),

        # Flavor-derived specs (per-node)
        "flavor_vcpus": vcpus_per_node,
        "flavor_ram_gb": ram_gb_per_node,
        "flavor_disk_gb": disk_gb_per_node,
        "flavor_known": bool(specs),

        # Aggregati (current_nodes × flavor specs)
        "current_vcpus": current_vcpus,
        "current_ram_gb": current_ram_gb,
        "current_disk_gb": current_disk_gb,
        "max_vcpus": max_vcpus,
        "max_ram_gb": max_ram_gb,

        # Timestamps OVH
        "nodepool_created_at": nodepool.get("createdAt"),
        "nodepool_updated_at": nodepool.get("updatedAt"),
    }


# ============================================================
# MAIN
# ============================================================
def main():
    now = dt.datetime.now(dt.timezone.utc)
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%S.") + \
        f"{now.microsecond // 1000:03d}Z"

    logger.info("=" * 70)
    logger.info(f"[AVVIO] {script_name} — hostname={hostname}")
    logger.info(f"[CONFIG] OVH endpoint: {OVH_ENDPOINT}")
    logger.info(f"[CONFIG] OVH project_id: {OVH_PROJECT_ID}")
    logger.info(f"[CONFIG] Service name (Elastic field): {SERVICE_NAME_FIELD}")
    logger.info(f"[CONFIG] Log file (JSON puro): {log_file_path}")
    if ELASTIC_ENABLED:
        logger.info(f"[CONFIG] Elastic: {MONITORING_URL} → {INVENTORY_INDEX}")
    else:
        logger.info("[CONFIG] >>> DRY-RUN (no Elastic) <<<")
    logger.info("=" * 70)

    try:
        client = ovh.Client(
            endpoint=OVH_ENDPOINT,
            application_key=OVH_APPLICATION_KEY,
            application_secret=OVH_APPLICATION_SECRET,
            consumer_key=OVH_CONSUMER_KEY,
        )
    except Exception as e:
        logger.error(f"[OVH] init client error: {type(e).__name__}: {e}")
        return 1

    # 1. Lista cluster K8s del tenant
    logger.info(f"[OVH] GET /cloud/project/{OVH_PROJECT_ID}/kube")
    try:
        cluster_ids = client.get(f"/cloud/project/{OVH_PROJECT_ID}/kube")
    except Exception as e:
        logger.error(f"[OVH] list cluster error: {type(e).__name__}: {e}")
        err_doc = {
            "@timestamp": timestamp,
            "event_timestamp": timestamp,
            "platform": "iride",
            "service_provider_log": "iride_ovh_k8s",
            "service_name": SERVICE_NAME_FIELD,
            "event_type": "ovh_k8s_probe_error",
            "response_status": "ERROR",
            "hostname": hostname,
            "error_detail": str(e)[:200],
        }
        write_log(err_doc)
        ship_to_elastic(err_doc)
        return 1

    logger.info(f"[OVH] trovati {len(cluster_ids)} cluster K8s sul tenant")

    total_nodepools = 0
    healthy_nodepools = 0
    degraded_nodepools = 0
    down_nodepools = 0
    unknown_flavor_count = 0

    for kube_id in cluster_ids:
        # 2. Info cluster
        try:
            cluster_raw = client.get(
                f"/cloud/project/{OVH_PROJECT_ID}/kube/{kube_id}")
        except Exception as e:
            logger.warning(f"[OVH] cluster {kube_id[:8]} info error: {e}")
            continue

        cluster_info = {
            "id": kube_id,
            "name": cluster_raw.get("name", "unknown"),
            "region": cluster_raw.get("region", "unknown"),
            "version": cluster_raw.get("version", "unknown"),
            "status": cluster_raw.get("status", "unknown"),
        }
        logger.info(f"")
        logger.info(f"--- cluster: {cluster_info['name']} "
                    f"({kube_id[:8]}, region {cluster_info['region']}, "
                    f"k8s {cluster_info['version']}) ---")

        # 3. Lista nodepool del cluster
        try:
            nodepools = client.get(
                f"/cloud/project/{OVH_PROJECT_ID}/kube/{kube_id}/nodepool")
        except Exception as e:
            logger.warning(f"[OVH] nodepool list error per {kube_id[:8]}: {e}")
            continue

        logger.info(f"  trovati {len(nodepools)} nodepool")

        for np in nodepools:
            doc = build_nodepool_doc(timestamp, cluster_info, np)
            total_nodepools += 1

            hs = doc["health_status"]
            if hs == "HEALTHY":
                healthy_nodepools += 1
            elif hs == "DEGRADED":
                degraded_nodepools += 1
            else:
                down_nodepools += 1

            if not doc["flavor_known"]:
                unknown_flavor_count += 1
                logger.warning(f"  [NP {doc['nodepool_name']}] "
                               f"flavor sconosciuto: {doc['flavor']} "
                               f"(aggiornare FLAVOR_SPECS nello script)")

            # Log umano sintetico
            ram_str = (f"{doc['current_ram_gb']}GB"
                       if doc['current_ram_gb'] is not None else "?GB")
            vcpu_str = (f"{doc['current_vcpus']}vCPU"
                        if doc['current_vcpus'] is not None else "?vCPU")
            logger.info(
                f"  [{doc['nodepool_name']:30s}] "
                f"flavor={doc['flavor']:10s} "
                f"current={doc['current_nodes']:2d}/{doc['max_nodes']:2d} "
                f"({vcpu_str}, {ram_str}) "
                f"→ {hs}")

            write_log(doc)
            ship_to_elastic(doc)

    # Summary
    logger.info("")
    logger.info("=" * 70)
    logger.info(f"[SUMMARY] cluster K8s probed: {len(cluster_ids)}")
    logger.info(f"[SUMMARY] nodepool totali: {total_nodepools}")
    logger.info(f"[SUMMARY]   HEALTHY:  {healthy_nodepools}")
    logger.info(f"[SUMMARY]   DEGRADED: {degraded_nodepools}")
    logger.info(f"[SUMMARY]   DOWN:     {down_nodepools}")
    if unknown_flavor_count > 0:
        logger.warning(f"[SUMMARY] flavor sconosciuti: {unknown_flavor_count} "
                       f"(aggregati vcpu/ram NON calcolati per quei nodepool)")
    logger.info("[FINE]")
    return 0


if __name__ == "__main__":
    sys.exit(main())

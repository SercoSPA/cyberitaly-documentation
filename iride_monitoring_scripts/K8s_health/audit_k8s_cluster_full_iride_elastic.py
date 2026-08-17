"""
audit_k8s_cluster_full_iride_elastic.py
========================================
Script di audit completo del cluster K8s IRIDE CyberItaly.
Non filtra nulla: prende TUTTI i namespace, TUTTI i pod, senza mappatura
per componente. Utile per:
  - Discovery iniziale del cluster (cosa esiste davvero)
  - Sanity check periodico (regressioni, nuovi namespace non censiti)
  - Identificare pod problematici in namespace non ancora monitorati

Differenza vs collector_k8s_pod_health_iride_elastic.py:
  - Quello ha una lista fissa di namespace nell'INI (adam-*, platform, logging)
  - Questo pesca TUTTI i namespace del cluster e monitora TUTTI i pod

Usa kubectl (gia' installato e configurato sulla VM ci-mon-dash-01) via
subprocess per interrogare il cluster.

Output:
  1 file .log con JSON puro (1 doc per riga, pronto Elastic ingestion).
  Console: prosa umana con breakdown per namespace.

Indice Elastic target (suggerito):
  metrics-iride-k8s-cluster-audit.monitoring-default

Pattern d'uso tipico:
  Ogni ora in cron. Volume documenti superiore rispetto al collector
  filtrato: se un giorno il cluster arriva a 200 pod, ne genera 200 doc/ora.
  Se il volume diventa un problema, si passa a schedulazione ogni 3-6h.
"""

import configparser
import datetime as dt
import json
import logging
import socket
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

# ============================================================
# CONFIG
# ============================================================
SCRIPT_DIR = Path(__file__).resolve().parent
INI_PATH = SCRIPT_DIR / "audit_k8s_cluster_full_iride_elastic.ini"

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
    fallback='metrics-iride-k8s-cluster-audit.monitoring-default')

LOG_FILE_NAME = config.get(
    'CONFIG', 'LOG_FILE_NAME',
    fallback='audit_k8s_cluster_full_iride_elastic.log')

# K8s
KUBECTL_BIN = config.get('K8S', 'KUBECTL_BIN', fallback='/usr/local/bin/kubectl')
KUBECONFIG = config.get('K8S', 'KUBECONFIG', fallback='')
KUBECTL_TIMEOUT = config.getint('K8S', 'KUBECTL_TIMEOUT', fallback=60)

# Namespace da ESCLUDERE (opzionale). Default: nessuno.
# Utile se vuoi escludere namespace K8s "di sistema" che generano rumore.
# Esempio: EXCLUDE_NAMESPACES = kube-node-lease,kube-public
EXCLUDE_NAMESPACES = [
    ns.strip() for ns in
    config.get('K8S', 'EXCLUDE_NAMESPACES', fallback='').split(',')
    if ns.strip()
]

# Soglie health status
RESTART_DEGRADED_THRESHOLD = config.getint('K8S',
                                           'RESTART_DEGRADED_THRESHOLD',
                                           fallback=5)
RESTART_CRITICAL_THRESHOLD = config.getint('K8S',
                                           'RESTART_CRITICAL_THRESHOLD',
                                           fallback=50)

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


def write_log(doc):
    json_logger.info(json.dumps(doc, default=str))


def ship_to_elastic(doc):
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
# KUBECTL WRAPPERS
# ============================================================
def _run_kubectl(args):
    """Esegue kubectl con args (list) e ritorna JSON parsed o None."""
    cmd = [KUBECTL_BIN]
    if KUBECONFIG:
        cmd += ["--kubeconfig", KUBECONFIG]
    cmd += args

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=KUBECTL_TIMEOUT,
            check=False)
    except subprocess.TimeoutExpired:
        logger.error(f"[KUBECTL] timeout dopo {KUBECTL_TIMEOUT}s ({' '.join(args[:3])})")
        return None
    except FileNotFoundError:
        logger.error(f"[KUBECTL] binario non trovato: {KUBECTL_BIN}")
        return None

    if result.returncode != 0:
        logger.error(f"[KUBECTL] exit={result.returncode} "
                     f"cmd={' '.join(args[:3])} "
                     f"stderr={result.stderr.strip()[:200]}")
        return None

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as e:
        logger.error(f"[KUBECTL] JSON parse error: {e}")
        return None


def get_all_namespaces():
    """kubectl get namespaces -o json → lista di dict metadata."""
    result = _run_kubectl(["get", "namespaces", "-o", "json"])
    if not result:
        return []
    return result.get("items", [])


def get_all_pods_cluster_wide():
    """
    kubectl get pods -A -o json → lista TUTTI i pod del cluster in una singola call.
    Piu' efficiente di N call separate (una per namespace).
    """
    result = _run_kubectl(["get", "pods", "-A", "-o", "json"])
    if not result:
        return []
    return result.get("items", [])


# ============================================================
# POD PARSING
# ============================================================
def parse_pod(pod):
    """Estrae campi rilevanti da un pod, ritorna dict Elastic-ready."""
    meta = pod.get("metadata", {}) or {}
    spec = pod.get("spec", {}) or {}
    status = pod.get("status", {}) or {}

    namespace = meta.get("namespace", "unknown")
    pod_name = meta.get("name", "unknown")
    creation_ts = meta.get("creationTimestamp")
    labels = meta.get("labels", {}) or {}
    node_name = spec.get("nodeName")
    phase = status.get("phase", "Unknown")
    pod_ip = status.get("podIP")
    start_time = status.get("startTime")

    container_statuses = status.get("containerStatuses", []) or []
    init_container_statuses = status.get("initContainerStatuses", []) or []

    total_containers = len(container_statuses)
    ready_containers = sum(
        1 for c in container_statuses if c.get("ready"))
    total_restarts = sum(
        c.get("restartCount", 0) for c in container_statuses)

    # Aggregati init container (spesso e' qui che stanno i CrashLoopBackOff
    # tipo cyberitaly-awareness-cyberitaly con 2722 restart in Init:Error)
    init_total_restarts = sum(
        c.get("restartCount", 0) for c in init_container_statuses)
    init_ready = sum(1 for c in init_container_statuses if c.get("ready"))
    init_total = len(init_container_statuses)

    # Ultimo motivo di terminazione (utile per capire perche' crasha)
    last_terminated_reason = None
    last_terminated_exit_code = None
    last_terminated_at = None
    waiting_reason = None

    for c in container_statuses + init_container_statuses:
        cstate = c.get("state", {}) or {}
        if "waiting" in cstate:
            waiting_reason = (cstate.get("waiting", {}).get("reason")
                              or waiting_reason)
        last_state = c.get("lastState", {}) or {}
        last_term = last_state.get("terminated", {}) or {}
        if last_term:
            if not last_terminated_reason:
                last_terminated_reason = last_term.get("reason")
            if last_terminated_exit_code is None:
                last_terminated_exit_code = last_term.get("exitCode")
            if not last_terminated_at:
                last_terminated_at = last_term.get("finishedAt")

    # Uptime
    uptime_minutes = None
    if start_time:
        try:
            dt_start = dt.datetime.fromisoformat(
                start_time.replace("Z", "+00:00"))
            now = dt.datetime.now(dt.timezone.utc)
            uptime_minutes = round(
                (now - dt_start).total_seconds() / 60.0, 2)
        except (ValueError, TypeError):
            pass

    # Health status derivato
    health_status = compute_health_status(
        phase=phase,
        total_containers=total_containers,
        ready_containers=ready_containers,
        total_restarts=total_restarts,
        init_ready=init_ready,
        init_total=init_total)

    return {
        "platform": "iride",
        "service_provider_log": "iride_k8s_audit",
        "event_type": "iride_k8s_cluster_audit_pod",
        "response_status": "OK",
        "hostname": hostname,

        # Identificazione
        "namespace": namespace,
        "pod_name": pod_name,
        "node_name": node_name,
        "pod_ip": pod_ip,

        # Stato
        "phase": phase,
        "health_status": health_status,
        "creation_timestamp": creation_ts,
        "start_time": start_time,
        "uptime_minutes": uptime_minutes,

        # Container regolari
        "container_count": total_containers,
        "container_ready_count": ready_containers,
        "container_all_ready": (total_containers > 0
                                and ready_containers == total_containers),
        "total_restart_count": total_restarts,

        # Init container (importante: cattura CrashLoopBackOff su init)
        "init_container_count": init_total,
        "init_container_ready_count": init_ready,
        "init_total_restart_count": init_total_restarts,

        # Motivi
        "last_terminated_reason": last_terminated_reason,
        "last_terminated_exit_code": last_terminated_exit_code,
        "last_terminated_at": last_terminated_at,
        "waiting_reason": waiting_reason,

        # Labels utili (owner, app, component)
        # Prendo solo quelle piu' comuni per non gonfiare il doc
        "label_app": labels.get("app") or labels.get("app.kubernetes.io/name"),
        "label_component": labels.get("component") or labels.get("app.kubernetes.io/component"),
        "label_instance": labels.get("app.kubernetes.io/instance"),
        "label_managed_by": labels.get("app.kubernetes.io/managed-by"),
    }


def compute_health_status(phase, total_containers, ready_containers,
                          total_restarts, init_ready=0, init_total=0):
    """Logica deterministica per lo status di salute."""
    if phase == "Succeeded":
        return "COMPLETED"
    if phase in ("Failed", "Unknown"):
        return "DOWN"
    if phase == "Pending":
        # Se ha init container non ready, e' probabilmente CrashLoopBackOff
        # sull'init. Down conferma il problema.
        return "DOWN"
    if phase == "Running":
        # Init container: se ce ne sono e non sono tutti ready, degraded
        if init_total > 0 and init_ready < init_total:
            return "DEGRADED"
        if total_containers == 0:
            return "UNKNOWN"
        if ready_containers < total_containers:
            return "DEGRADED"
        if total_restarts >= RESTART_CRITICAL_THRESHOLD:
            return "DEGRADED"
        if total_restarts >= RESTART_DEGRADED_THRESHOLD:
            return "DEGRADED"
        return "HEALTHY"
    return "UNKNOWN"


# ============================================================
# MAIN
# ============================================================
def main():
    now = dt.datetime.now(dt.timezone.utc)
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%S.") + \
        f"{now.microsecond // 1000:03d}Z"

    logger.info("=" * 70)
    logger.info(f"[AVVIO] {script_name} — hostname={hostname}")
    logger.info(f"[CONFIG] kubectl: {KUBECTL_BIN}")
    logger.info(f"[CONFIG] kubeconfig: {KUBECONFIG or '(default $HOME/.kube/config)'}")
    if EXCLUDE_NAMESPACES:
        logger.info(f"[CONFIG] namespace ESCLUSI: {EXCLUDE_NAMESPACES}")
    else:
        logger.info(f"[CONFIG] namespace esclusi: nessuno (audit completo)")
    logger.info(f"[CONFIG] log file (JSON puro): {log_file_path}")
    if ELASTIC_ENABLED:
        logger.info(f"[CONFIG] Elastic: {MONITORING_URL} → {INVENTORY_INDEX}")
    else:
        logger.info("[CONFIG] >>> DRY-RUN (no Elastic) <<<")
    logger.info("=" * 70)

    # ============================================================
    # 1. Discovery namespace
    # ============================================================
    logger.info("")
    logger.info("[STEP 1] Discovery namespace del cluster...")
    namespaces = get_all_namespaces()
    if not namespaces:
        logger.error("[STEP 1] Nessun namespace trovato o kubectl fallito. Abort.")
        return 1

    ns_names = [ns.get("metadata", {}).get("name", "unknown")
                for ns in namespaces]
    ns_names_filtered = [n for n in ns_names if n not in EXCLUDE_NAMESPACES]
    logger.info(f"[STEP 1] Trovati {len(ns_names)} namespace totali "
                f"({len(ns_names_filtered)} dopo esclusioni)")
    for ns in ns_names_filtered:
        logger.info(f"  - {ns}")

    # ============================================================
    # 2. Discovery pod cluster-wide
    # ============================================================
    logger.info("")
    logger.info("[STEP 2] Discovery pod cluster-wide (kubectl get pods -A)...")
    all_pods = get_all_pods_cluster_wide()
    if not all_pods:
        logger.error("[STEP 2] Nessun pod trovato o kubectl fallito. Abort.")
        return 1

    # Filtro namespace esclusi
    if EXCLUDE_NAMESPACES:
        before = len(all_pods)
        all_pods = [
            p for p in all_pods
            if p.get("metadata", {}).get("namespace") not in EXCLUDE_NAMESPACES
        ]
        logger.info(f"[STEP 2] Filtrati {before - len(all_pods)} pod "
                    f"appartenenti a namespace esclusi")

    logger.info(f"[STEP 2] Totale pod da processare: {len(all_pods)}")

    # ============================================================
    # 3. Parsing + emit documenti
    # ============================================================
    logger.info("")
    logger.info("[STEP 3] Parsing pod e generazione documenti Elastic...")

    # Contatori globali
    total = 0
    healthy = 0
    degraded = 0
    down = 0
    completed = 0
    unknown = 0

    # Breakdown per namespace
    ns_breakdown = defaultdict(lambda: Counter())

    # Pod problematici (per report finale)
    problematic_pods = []

    for pod in all_pods:
        doc = parse_pod(pod)
        doc["@timestamp"] = timestamp
        doc["event_timestamp"] = timestamp

        total += 1
        hs = doc["health_status"]
        ns = doc["namespace"]

        if hs == "HEALTHY":
            healthy += 1
        elif hs == "DEGRADED":
            degraded += 1
        elif hs == "DOWN":
            down += 1
        elif hs == "COMPLETED":
            completed += 1
        else:
            unknown += 1

        ns_breakdown[ns][hs] += 1

        # Traccio i problematici per il report
        if hs in ("DOWN", "DEGRADED"):
            problematic_pods.append({
                "namespace": ns,
                "pod_name": doc["pod_name"],
                "phase": doc["phase"],
                "health_status": hs,
                "restarts": doc["total_restart_count"] + doc["init_total_restart_count"],
                "waiting_reason": doc["waiting_reason"],
                "last_terminated_reason": doc["last_terminated_reason"],
            })

        write_log(doc)
        ship_to_elastic(doc)

    # ============================================================
    # 4. Summary console
    # ============================================================
    logger.info("")
    logger.info("=" * 70)
    logger.info(f"[SUMMARY] namespace processati: {len(ns_breakdown)}")
    logger.info(f"[SUMMARY] pod totali: {total}")
    logger.info(f"[SUMMARY]   HEALTHY:   {healthy}")
    logger.info(f"[SUMMARY]   DEGRADED:  {degraded}")
    logger.info(f"[SUMMARY]   DOWN:      {down}")
    logger.info(f"[SUMMARY]   COMPLETED: {completed}")
    if unknown:
        logger.info(f"[SUMMARY]   UNKNOWN:   {unknown}")

    # Breakdown per namespace (ordinato per numero di pod problematici)
    logger.info("")
    logger.info("[BREAKDOWN per namespace]")
    def ns_sort_key(item):
        ns, counter = item
        # Priorita' ai namespace con piu' problemi
        problem_count = counter.get("DOWN", 0) + counter.get("DEGRADED", 0)
        return (-problem_count, -sum(counter.values()), ns)

    for ns, counter in sorted(ns_breakdown.items(), key=ns_sort_key):
        total_ns = sum(counter.values())
        parts = []
        for status in ("HEALTHY", "DEGRADED", "DOWN", "COMPLETED", "UNKNOWN"):
            if counter.get(status, 0) > 0:
                parts.append(f"{counter[status]} {status}")
        logger.info(f"  {ns:<30s} {total_ns:>3d} pods  [{', '.join(parts)}]")

    # Report pod problematici
    if problematic_pods:
        logger.info("")
        logger.info(f"[PROBLEMATIC PODS] {len(problematic_pods)} pod da attenzionare:")
        # Ordina: DOWN prima di DEGRADED, poi per restart count decrescente
        problematic_pods.sort(
            key=lambda p: (0 if p["health_status"] == "DOWN" else 1,
                           -p["restarts"]))
        for p in problematic_pods[:30]:  # cap per non spammare
            reason = p["waiting_reason"] or p["last_terminated_reason"] or "?"
            logger.info(
                f"  {p['health_status']:<10s} "
                f"{p['namespace']:<25s} "
                f"{p['pod_name']:<55s} "
                f"restarts={p['restarts']:<6d} "
                f"reason={reason}")
        if len(problematic_pods) > 30:
            logger.info(f"  ... e altri {len(problematic_pods) - 30} pod problematici "
                        f"(vedi file .log per elenco completo)")

    logger.info("")
    logger.info("[FINE]")
    return 0


if __name__ == "__main__":
    sys.exit(main())


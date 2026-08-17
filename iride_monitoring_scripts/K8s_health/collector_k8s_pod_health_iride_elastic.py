"""
collector_k8s_pod_health_iride_elastic.py
==========================================
Probe K8s pod health per i microservizi ADAM su cluster IRIDE CyberItaly.

Usa kubectl (gia' installato e configurato sulla VM ci-mon-dash-01) via
subprocess per interrogare lo stato dei pod nei namespace target ADAM.
Emette 1 documento JSON per ciascun pod con stato di salute, ready,
restart count, uptime, container details, e metriche CPU/RAM real-time.

Output:
  1 file .log con JSON puro (1 doc per riga, pronto Elastic ingestion)

Indice Elastic target (suggerito):
  metrics-iride-k8s-pod-health.monitoring-default

Pattern di salute (campo derivato 'health_status'):
  HEALTHY   = phase=Running e tutti i container ready
  DEGRADED  = phase=Running ma 1+ container non ready, oppure restart RECENTI
  DOWN      = phase in [Failed, Pending, Unknown]  (SOLO pod long-running)
  COMPLETED = phase=Succeeded (job-like pod terminato OK)
  RUNNING   = pod job-like ancora in esecuzione (transitorio, non guasto)

Pattern d'uso tipico:
  Ogni 5/15 min in cron, copre TUTTI i pod dei namespace ADAM.
  Sblocca CIMS-44/45/48 (esposizione vRack non piu' necessaria,
  direttiva Stefano 25/06/2026).

--- CHANGELOG modifiche (revisione 2026-07-07) ---
  [FIX-1] memory/cpu_percent_effective: la % ora si calcola col LIMIT se
          presente, altrimenti col REQUEST (fallback). Prima usciva None
          per i pod senza limits -> RAM%/CPU% vuoti a macchia di leopardo,
          alert e grafici inaffidabili. I vecchi campi *_percent_of_limit
          restano invariati per retrocompatibilita'.
  [FIX-2] restart_delta: restart avvenuti DALL'ULTIMO RUN (non cumulativi),
          via state file. total_restart_count e' monotono crescente e
          inutile per gli alert; restart_delta becca i crashloop veri.
  [FIX-3] health_status: la classificazione DEGRADED da restart ora usa
          restart_delta (recenti) invece del cumulativo, cosi' un pod
          vecchio e sano non resta marchiato DEGRADED per sempre.

--- CHANGELOG modifiche (revisione 2026-07-20) ---
  [FIX-4] health_status owner-aware: i pod creati da Job/CronJob hanno
          ciclo di vita FINITO. Pending e Running sono stati transitori
          normali (scheduling, image pull, esecuzione), NON guasti. Se il
          collector li campionava a meta' ciclo li marcava DOWN -> falsi
          positivi rossi in dashboard sui CronJob (dataretrivalpipeline,
          ecc.) che girano piu' frequentemente dell'intervallo del cron.
          Ora: pod job-like -> Failed=DOWN, Succeeded=COMPLETED, il resto
          RUNNING. I pod long-running (Deployment/StatefulSet) mantengono
          la logica severa (Pending/Unknown = DOWN). Aggiunti al doc i
          campi owner_kind e is_job_pod per split/filtri lato Grafana.
"""

import configparser
import datetime as dt
import json
import logging
import os
import socket
import subprocess
import sys
from pathlib import Path

# ============================================================
# CONFIG
# ============================================================
SCRIPT_DIR = Path(__file__).resolve().parent
INI_PATH = SCRIPT_DIR / "k8s_pod_health_iride_elastic.ini"

config = configparser.ConfigParser()
if not INI_PATH.exists():
    print(f"FATAL: INI non trovato a {INI_PATH}", file=sys.stderr)
    sys.exit(1)
config.read(INI_PATH)

# Elastic ingestion (per ora disabilitato finche' non abbiamo l'endpoint)
ELASTIC_ENABLED = config.getboolean('CONFIG', 'ELASTIC_ENABLED',
                                    fallback=False)
MONITORING_URL = config.get('CONFIG', 'MONITORING_URL', fallback='')
MONITORING_APIKEY = config.get('CONFIG', 'MONITORING_APIKEY', fallback='')
MONITORING_VERIFY_CERTS = config.getboolean('CONFIG',
                                            'MONITORING_VERIFY_CERTS',
                                            fallback=True)
INVENTORY_INDEX = config.get(
    'CONFIG', 'INVENTORY_INDEX',
    fallback='metrics-iride-k8s-pod-health.monitoring-default')

LOG_FILE_NAME = config.get(
    'CONFIG', 'LOG_FILE_NAME',
    fallback='collector_k8s_pod_health_iride_elastic.log')

# [FIX-2] State file per il calcolo del restart_delta (restart non cumulativo).
# Persiste tra un run cron e l'altro il total_restart_count di ogni pod,
# cosi' al giro successivo possiamo calcolare quanti restart sono avvenuti
# NELL'INTERVALLO invece del totale storico monotono.
STATE_FILE_NAME = config.get(
    'CONFIG', 'STATE_FILE_NAME',
    fallback='collector_k8s_pod_health_iride_elastic.state.json')
STATE_FILE_PATH = SCRIPT_DIR / STATE_FILE_NAME

# K8s
KUBECTL_BIN = config.get('K8S', 'KUBECTL_BIN', fallback='/usr/bin/kubectl')
KUBECONFIG = config.get('K8S', 'KUBECONFIG', fallback='')  # vuoto = default
TARGET_NAMESPACES = [
    ns.strip() for ns in
    config.get('K8S', 'TARGET_NAMESPACES',
               fallback='adam-catalog,adam-dapapi,adam-dret,adam-ftp,'
                        'adam-mongo,adam-wxs').split(',')
    if ns.strip()
]
KUBECTL_TIMEOUT = config.getint('K8S', 'KUBECTL_TIMEOUT', fallback=30)

# Se True, chiama anche `kubectl top pods -n <ns>` per ottenere le metriche
# di consumo real-time (CPU/RAM effettive) via metrics-server.
KUBECTL_TOP_ENABLED = config.getboolean('K8S', 'KUBECTL_TOP_ENABLED',
                                        fallback=True)

# Soglie per il campo derivato health_status.
# [FIX-3] Queste soglie ora si applicano a restart_delta (restart RECENTI
# nell'intervallo di cron), non piu' al totale cumulativo. Valori bassi
# hanno senso: 3-5 restart in un solo intervallo = crashloop nascente.
RESTART_DEGRADED_THRESHOLD = config.getint('K8S',
                                           'RESTART_DEGRADED_THRESHOLD',
                                           fallback=5)
RESTART_CRITICAL_THRESHOLD = config.getint('K8S',
                                           'RESTART_CRITICAL_THRESHOLD',
                                           fallback=50)

# ============================================================
# LOGGING (console = umano, file = JSON puro per Elastic)
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
    """Scrive 1 documento JSON sul .log file (1 riga per documento)."""
    json_logger.info(json.dumps(doc, default=str))


# ============================================================
# [FIX-2] STATE (per restart_delta non cumulativo)
# ============================================================
def load_state(path):
    """Carica lo stato del run precedente. {} se assente o corrotto."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("pods", {}) if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"[STATE] impossibile leggere {path}: "
                       f"{type(e).__name__}: {e} — riparto da stato vuoto")
        return {}


def save_state(path, pods, run_ts):
    """Salva lo stato in modo atomico (write .tmp + rename)."""
    tmp = f"{path}.tmp"
    payload = {"version": 1, "updated_at": run_ts, "pods": pods}
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, default=str)
        os.replace(tmp, path)
    except OSError as e:
        logger.warning(f"[STATE] impossibile scrivere {path}: "
                       f"{type(e).__name__}: {e}")


def compute_restart_delta(key, current_total, current_ts, prev_pods):
    """
    Restart avvenuti DALL'ULTIMO RUN (non cumulativo).
      - pod mai visto  -> delta=0, first_seen=True (baseline)
      - counter reset  -> pod ricreato: delta = current_total
      - caso normale   -> delta = current_total - prev_total
    """
    prev = prev_pods.get(key)
    if prev is None:
        return {"restart_delta": 0,
                "restart_delta_interval_minutes": None,
                "restart_delta_first_seen": True}

    prev_total = prev.get("total_restart_count", 0)
    delta = current_total if current_total < prev_total \
        else current_total - prev_total

    interval_min = None
    prev_ts = prev.get("ts")
    if prev_ts:
        try:
            dt_prev = dt.datetime.fromisoformat(
                str(prev_ts).replace("Z", "+00:00"))
            dt_cur = dt.datetime.fromisoformat(
                str(current_ts).replace("Z", "+00:00"))
            interval_min = round(
                (dt_cur - dt_prev).total_seconds() / 60.0, 2)
        except (ValueError, TypeError):
            pass

    return {"restart_delta": delta,
            "restart_delta_interval_minutes": interval_min,
            "restart_delta_first_seen": False}


def ship_to_elastic(doc):
    """Spedisce documento a Elastic via API key (se ELASTIC_ENABLED)."""
    if not ELASTIC_ENABLED:
        return
    try:
        import urllib.request
        url = (f"{MONITORING_URL.rstrip('/')}/{INVENTORY_INDEX}/_doc")
        body = json.dumps(doc).encode('utf-8')
        req = urllib.request.Request(
            url, data=body,
            headers={'Authorization': f'ApiKey {MONITORING_APIKEY}',
                     'Content-Type': 'application/json'},
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
# KUBECTL WRAPPER
# ============================================================
def run_kubectl_get_pods(namespace):
    """kubectl get pods -n <ns> -o json. Ritorna dict o None."""
    cmd = [KUBECTL_BIN]
    if KUBECONFIG:
        cmd += ["--kubeconfig", KUBECONFIG]
    cmd += ["get", "pods", "-n", namespace, "-o", "json"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True,
                                timeout=KUBECTL_TIMEOUT, check=False)
    except subprocess.TimeoutExpired:
        logger.error(f"[KUBECTL {namespace}] timeout dopo {KUBECTL_TIMEOUT}s")
        return None
    except FileNotFoundError:
        logger.error(f"[KUBECTL] binario non trovato: {KUBECTL_BIN}")
        return None
    if result.returncode != 0:
        logger.error(f"[KUBECTL {namespace}] exit={result.returncode} "
                     f"stderr={result.stderr.strip()[:200]}")
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as e:
        logger.error(f"[KUBECTL {namespace}] JSON parse error: {e}")
        return None


def get_pod_metrics(namespace):
    """
    kubectl top pods -n <ns> --no-headers
    -> {pod_name: (cpu_millicores, memory_bytes)}. {} se metrics-server giu'.
    """
    if not KUBECTL_TOP_ENABLED:
        return {}
    cmd = [KUBECTL_BIN]
    if KUBECONFIG:
        cmd += ["--kubeconfig", KUBECONFIG]
    cmd += ["top", "pods", "-n", namespace, "--no-headers"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True,
                                timeout=KUBECTL_TIMEOUT, check=False)
    except Exception as e:
        logger.warning(f"[TOP {namespace}] {type(e).__name__}: {e}")
        return {}
    if result.returncode != 0:
        stderr = result.stderr.strip()[:150]
        if "metrics" in stderr.lower() or "not found" in stderr.lower():
            logger.debug(f"[TOP {namespace}] metrics-server non disponibile: "
                         f"{stderr}")
        else:
            logger.warning(f"[TOP {namespace}] exit={result.returncode} "
                           f"stderr={stderr}")
        return {}
    metrics = {}
    for line in result.stdout.strip().split("\n"):
        parts = line.split()
        if len(parts) < 3:
            continue
        metrics[parts[0]] = (_parse_cpu_millicores(parts[1]),
                             _parse_memory_bytes(parts[2]))
    return metrics


def _parse_cpu_millicores(s):
    """'45m','1','2.5' -> millicores (int). None se non parsabile."""
    if not s:
        return None
    try:
        s = s.strip()
        if s.endswith("m"):
            return int(float(s[:-1]))
        if s.endswith("n"):
            return int(float(s[:-1]) / 1_000_000)
        return int(float(s) * 1000)
    except (ValueError, TypeError):
        return None


def _parse_memory_bytes(s):
    """'128Mi','1.5Gi','2G' -> bytes (int). None se non parsabile."""
    if not s:
        return None
    try:
        s = s.strip()
        binary_units = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3, "Ti": 1024**4}
        for suffix, mult in binary_units.items():
            if s.endswith(suffix):
                return int(float(s[:-2]) * mult)
        decimal_units = {"K": 1000, "M": 1000**2, "G": 1000**3, "T": 1000**4}
        for suffix, mult in decimal_units.items():
            if s.endswith(suffix):
                return int(float(s[:-1]) * mult)
        return int(float(s))
    except (ValueError, TypeError):
        return None


def _sum_resource_field(containers, section, field):
    """Somma requests/limits di cpu|memory su tutti i container. None se assente."""
    total = 0
    found = False
    for c in containers:
        resources = c.get("resources", {}) or {}
        section_data = resources.get(section, {}) or {}
        val = section_data.get(field)
        if val is None:
            continue
        found = True
        if field == "cpu":
            m = _parse_cpu_millicores(val)
            if m is not None:
                total += m
        else:
            b = _parse_memory_bytes(val)
            if b is not None:
                total += b
    return total if found else None


def _percent_effective(current, limit, request):
    """
    [FIX-1] Calcola la % di utilizzo con fallback:
      1. LIMIT se presente   -> % del limit (il piu' significativo)
      2. REQUEST altrimenti  -> % del request (fallback)
      3. None (best-effort), ma il valore assoluto resta sempre scritto.
    Ritorna (percentuale|None, base: 'limit'|'request'|'none').
    """
    if current is None:
        return None, "none"
    if limit:
        return round(current / limit * 100.0, 2), "limit"
    if request:
        return round(current / request * 100.0, 2), "request"
    return None, "none"


# ============================================================
# POD STATUS PARSING
# ============================================================
def parse_pod(pod, namespace, metrics=None, timestamp=None):
    """Estrae i campi di un pod e ritorna un dict pronto per Elastic."""
    if metrics is None:
        metrics = {}
    if timestamp is None:
        now = dt.datetime.now(dt.timezone.utc)
        timestamp = (now.strftime("%Y-%m-%dT%H:%M:%S.")
                     + f"{now.microsecond // 1000:03d}Z")
    meta = pod.get("metadata", {})
    spec = pod.get("spec", {})
    status = pod.get("status", {})

    pod_name = meta.get("name", "unknown")
    creation_ts = meta.get("creationTimestamp")
    node_name = spec.get("nodeName")
    phase = status.get("phase", "Unknown")
    pod_ip = status.get("podIP")
    start_time = status.get("startTime")

    # [FIX-4] Owner del pod: un pod creato da Job/CronJob ha ciclo di vita
    # FINITO. Per questi Pending/Running sono stati transitori normali,
    # NON guasti. Solo Failed = DOWN. Distinguerli evita i falsi positivi
    # rossi sui CronJob che girano piu' spesso dell'intervallo del collector.
    owner_refs = meta.get("ownerReferences", []) or []
    owner_kind = owner_refs[0].get("kind") if owner_refs else None
    is_job_pod = owner_kind == "Job"

    container_statuses = status.get("containerStatuses", []) or []
    init_container_statuses = status.get("initContainerStatuses", []) or []

    total_containers = len(container_statuses)
    ready_containers = sum(1 for c in container_statuses if c.get("ready"))
    total_restarts = sum(c.get("restartCount", 0)
                         for c in container_statuses)

    container_details = []
    last_terminated_reason = None
    last_terminated_exit_code = None
    last_terminated_at = None
    waiting_reason = None

    for c in container_statuses:
        cname = c.get("name")
        cready = bool(c.get("ready"))
        crestarts = c.get("restartCount", 0)
        cstate = c.get("state", {}) or {}
        if "running" in cstate:
            cstate_kind = "running"
        elif "waiting" in cstate:
            cstate_kind = "waiting"
            waiting_reason = (cstate.get("waiting", {}).get("reason")
                              or waiting_reason)
        elif "terminated" in cstate:
            cstate_kind = "terminated"
            term = cstate.get("terminated", {}) or {}
            last_terminated_reason = term.get("reason") or last_terminated_reason
            last_terminated_exit_code = (term.get("exitCode")
                                         if last_terminated_exit_code is None
                                         else last_terminated_exit_code)
            last_terminated_at = term.get("finishedAt") or last_terminated_at
        else:
            cstate_kind = "unknown"

        last_state = c.get("lastState", {}) or {}
        last_term = last_state.get("terminated", {}) or {}
        if last_term:
            last_terminated_reason = (last_term.get("reason")
                                      or last_terminated_reason)
            if last_terminated_exit_code is None:
                last_terminated_exit_code = last_term.get("exitCode")
            last_terminated_at = (last_term.get("finishedAt")
                                  or last_terminated_at)

        container_details.append({
            "name": cname, "ready": cready, "restart_count": crestarts,
            "state": cstate_kind, "image": c.get("image"),
        })

    uptime_minutes = None
    if start_time:
        try:
            dt_start = dt.datetime.fromisoformat(
                start_time.replace("Z", "+00:00"))
            now = dt.datetime.now(dt.timezone.utc)
            uptime_minutes = round((now - dt_start).total_seconds() / 60.0, 2)
        except (ValueError, TypeError):
            pass

    # Resource requests/limits aggregati
    spec_containers = spec.get("containers", []) or []
    cpu_request_m = _sum_resource_field(spec_containers, "requests", "cpu")
    cpu_limit_m = _sum_resource_field(spec_containers, "limits", "cpu")
    mem_request_b = _sum_resource_field(spec_containers, "requests", "memory")
    mem_limit_b = _sum_resource_field(spec_containers, "limits", "memory")
    resources_requests_defined = (cpu_request_m is not None
                                  or mem_request_b is not None)
    resources_limits_defined = (cpu_limit_m is not None
                                or mem_limit_b is not None)

    # Pod age vs uptime
    pod_age_hours = None
    restarted_since_creation = None
    if creation_ts:
        try:
            dt_creation = dt.datetime.fromisoformat(
                creation_ts.replace("Z", "+00:00"))
            now = dt.datetime.now(dt.timezone.utc)
            pod_age_hours = round(
                (now - dt_creation).total_seconds() / 3600.0, 2)
            if uptime_minutes is not None:
                restarted_since_creation = (
                    pod_age_hours * 60 > uptime_minutes + 5)
        except (ValueError, TypeError):
            pass

    # CPU/RAM real-time via kubectl top
    cpu_current_m = None
    mem_current_b = None
    if pod_name in metrics:
        cpu_current_m, mem_current_b = metrics[pod_name]

    # [FIX-1] Percentuali: vecchio campo (solo limit) invariato per
    # retrocompatibilita', + nuovo campo effective con fallback su request.
    cpu_pct_of_limit = (round(cpu_current_m / cpu_limit_m * 100.0, 2)
                        if cpu_current_m is not None and cpu_limit_m else None)
    mem_pct_of_limit = (round(mem_current_b / mem_limit_b * 100.0, 2)
                        if mem_current_b is not None and mem_limit_b else None)
    cpu_pct_effective, cpu_pct_basis = _percent_effective(
        cpu_current_m, cpu_limit_m, cpu_request_m)
    mem_pct_effective, mem_pct_basis = _percent_effective(
        mem_current_b, mem_limit_b, mem_request_b)

    return {
        "@timestamp": timestamp,
        "event_timestamp": timestamp,

        "platform": "iride",
        "service_provider_log": "iride_k8s",
        "event_type": "iride_k8s_pod_health",
        "response_status": "OK",
        "hostname": hostname,

        "namespace": namespace,
        "pod_name": pod_name,
        "node_name": node_name,
        "pod_ip": pod_ip,

        "phase": phase,
        "owner_kind": owner_kind,  # Job, ReplicaSet, StatefulSet, DaemonSet...
        "is_job_pod": is_job_pod,  # True = ciclo di vita finito (CronJob/Job)
        "health_status": None,  # riempito in main() dopo il calcolo del delta
        "creation_timestamp": creation_ts,
        "start_time": start_time,
        "uptime_minutes": uptime_minutes,

        "pod_age_hours": pod_age_hours,
        "restarted_since_creation": restarted_since_creation,

        "container_count": total_containers,
        "container_ready_count": ready_containers,
        "container_all_ready": (total_containers > 0
                                and ready_containers == total_containers),
        "total_restart_count": total_restarts,
        "init_container_count": len(init_container_statuses),

        "last_terminated_reason": last_terminated_reason,
        "last_terminated_exit_code": last_terminated_exit_code,
        "last_terminated_at": last_terminated_at,
        "waiting_reason": waiting_reason,

        "cpu_request_millicores": cpu_request_m,
        "cpu_limit_millicores": cpu_limit_m,
        "memory_request_bytes": mem_request_b,
        "memory_limit_bytes": mem_limit_b,
        "resources_requests_defined": resources_requests_defined,
        "resources_limits_defined": resources_limits_defined,

        "cpu_current_millicores": cpu_current_m,
        "memory_current_bytes": mem_current_b,
        # vecchi campi (solo limit) — invariati
        "cpu_percent_of_limit": cpu_pct_of_limit,
        "memory_percent_of_limit": mem_pct_of_limit,
        # [FIX-1] nuovi campi effective (limit->request fallback)
        "cpu_percent_effective": cpu_pct_effective,
        "cpu_percent_basis": cpu_pct_basis,
        "memory_percent_effective": mem_pct_effective,
        "memory_percent_basis": mem_pct_basis,

        # helper interni per il calcolo in main (rimossi prima dello ship)
        "_total_restarts": total_restarts,
        "_phase": phase,
        "_total_containers": total_containers,
        "_ready_containers": ready_containers,
        "_is_job_pod": is_job_pod,

        "containers": container_details,
    }


def compute_health_status(phase, total_containers, ready_containers,
                          restart_signal, is_job_pod=False):
    """
    [FIX-3] restart_signal = restart_delta (restart RECENTI), non piu' il
    totale cumulativo. Un pod vecchio con 200 restart storici ma 0 recenti
    non e' piu' marchiato DEGRADED per sempre.

    [FIX-4] owner-aware: i pod job-like (owner Job/CronJob) hanno ciclo di
    vita FINITO. Pending e Running sono stati transitori normali (scheduling,
    image pull, esecuzione), NON guasti. Se il collector li campiona a meta'
    ciclo NON devono risultare DOWN. Solo Failed = DOWN. Questo elimina i
    falsi positivi sui CronJob che girano piu' frequentemente dell'intervallo
    del collector (dataretrivalpipeline & co.).
    """
    # Stati terminali: valgono per tutti i pod
    if phase == "Succeeded":
        return "COMPLETED"
    if phase == "Failed":
        return "DOWN"

    # Pod job-like: Pending/Running/Unknown sono transitori, non guasti.
    # Un job che nasce (Pending), gira (Running) o e' in cleanup post-
    # terminazione (Unknown) non e' un incidente da segnalare in rosso.
    if is_job_pod:
        return "RUNNING"

    # Pod long-running (Deployment/StatefulSet/DaemonSet): qui Pending
    # prolungato e Unknown sono problemi reali.
    if phase in ("Unknown", "Pending"):
        return "DOWN"
    if phase == "Running":
        if total_containers == 0:
            return "UNKNOWN"
        if ready_containers < total_containers:
            return "DEGRADED"
        if restart_signal >= RESTART_CRITICAL_THRESHOLD:
            return "DEGRADED"
        if restart_signal >= RESTART_DEGRADED_THRESHOLD:
            return "DEGRADED"
        return "HEALTHY"
    return "UNKNOWN"


# ============================================================
# MAIN
# ============================================================
def main():
    now0 = dt.datetime.now(dt.timezone.utc)
    timestamp = (now0.strftime("%Y-%m-%dT%H:%M:%S.")
                 + f"{now0.microsecond // 1000:03d}Z")

    logger.info("=" * 70)
    logger.info(f"[AVVIO] {script_name} — hostname={hostname}")
    logger.info(f"[CONFIG] target namespaces: {TARGET_NAMESPACES}")
    logger.info(f"[CONFIG] kubectl top (metrics-server): "
                f"{'enabled' if KUBECTL_TOP_ENABLED else 'disabled'}")
    if ELASTIC_ENABLED:
        logger.info(f"[CONFIG] Elastic: {MONITORING_URL} → {INVENTORY_INDEX}")
    else:
        logger.info("[CONFIG] >>> DRY-RUN (no Elastic shipping) <<<")
    logger.info("=" * 70)

    total_pods = healthy_pods = degraded_pods = down_pods = 0
    running_pods = completed_pods = 0
    namespaces_with_errors = 0
    recent_restart_pods = 0

    prev_pods = load_state(STATE_FILE_PATH)
    new_pods = {}
    logger.info(f"[STATE] pod tracciati dal run precedente: {len(prev_pods)}")

    for ns in TARGET_NAMESPACES:
        logger.info("")
        logger.info(f"--- namespace: {ns} ---")
        result = run_kubectl_get_pods(ns)

        if result is None:
            err_doc = {
                "@timestamp": timestamp, "event_timestamp": timestamp,
                "platform": "iride", "service_provider_log": "iride_k8s",
                "event_type": "iride_k8s_namespace_probe_error",
                "response_status": "ERROR", "hostname": hostname,
                "namespace": ns,
                "error_detail": "kubectl get pods failed (vedi script log)",
            }
            write_log(err_doc)
            ship_to_elastic(err_doc)
            namespaces_with_errors += 1
            continue

        pods = result.get("items", [])
        if not pods:
            logger.info(f"  [NS {ns}] nessun pod presente")
            empty_doc = {
                "@timestamp": timestamp, "event_timestamp": timestamp,
                "platform": "iride", "service_provider_log": "iride_k8s",
                "event_type": "iride_k8s_namespace_empty",
                "response_status": "OK", "hostname": hostname, "namespace": ns,
            }
            write_log(empty_doc)
            ship_to_elastic(empty_doc)
            continue

        pod_metrics = get_pod_metrics(ns)
        if pod_metrics:
            logger.info(f"  [NS {ns}] metrics-server: {len(pod_metrics)} "
                        f"pod con metriche real-time")

        for pod in pods:
            doc = parse_pod(pod, ns, metrics=pod_metrics, timestamp=timestamp)

            # [FIX-2] restart_delta
            pod_key = f"{ns}/{doc['pod_name']}"
            delta_info = compute_restart_delta(
                pod_key, doc["_total_restarts"], timestamp, prev_pods)
            doc.update(delta_info)
            new_pods[pod_key] = {
                "total_restart_count": doc["_total_restarts"], "ts": timestamp}
            if delta_info["restart_delta"] > 0:
                recent_restart_pods += 1

            # [FIX-3]+[FIX-4] health_status: delta per i restart recenti,
            # is_job_pod per non marcare DOWN i job in transito.
            doc["health_status"] = compute_health_status(
                phase=doc["_phase"],
                total_containers=doc["_total_containers"],
                ready_containers=doc["_ready_containers"],
                restart_signal=delta_info["restart_delta"],
                is_job_pod=doc["_is_job_pod"])

            # rimuovo gli helper interni prima di scrivere/spedire
            for k in ("_total_restarts", "_phase", "_total_containers",
                      "_ready_containers", "_is_job_pod"):
                doc.pop(k, None)

            total_pods += 1
            hs = doc["health_status"]
            if hs == "HEALTHY":
                healthy_pods += 1
            elif hs == "DEGRADED":
                degraded_pods += 1
            elif hs == "DOWN":
                down_pods += 1
            elif hs == "RUNNING":
                running_pods += 1
            elif hs == "COMPLETED":
                completed_pods += 1

            logger.info(
                f"  [{doc['pod_name']}] phase={doc['phase']} "
                f"owner={doc['owner_kind']} "
                f"ready={doc['container_ready_count']}/{doc['container_count']} "
                f"restarts={doc['total_restart_count']} "
                f"(Δ{doc['restart_delta']}) "
                f"mem={doc['memory_percent_effective']}%"
                f"({doc['memory_percent_basis']}) → {hs}")

            write_log(doc)
            ship_to_elastic(doc)

    save_state(STATE_FILE_PATH, new_pods, timestamp)

    logger.info("")
    logger.info("=" * 70)
    logger.info(f"[SUMMARY] namespaces probed: {len(TARGET_NAMESPACES)} "
                f"(errors: {namespaces_with_errors})")
    logger.info(f"[SUMMARY] pods totali: {total_pods}")
    logger.info(f"[SUMMARY]   HEALTHY:  {healthy_pods}")
    logger.info(f"[SUMMARY]   DEGRADED: {degraded_pods}")
    logger.info(f"[SUMMARY]   DOWN:     {down_pods}")
    logger.info(f"[SUMMARY]   RUNNING (job in corso): {running_pods}")
    logger.info(f"[SUMMARY]   COMPLETED (job OK):     {completed_pods}")
    logger.info(f"[SUMMARY]   restart recenti (Δ>0): {recent_restart_pods}")
    logger.info(f"[STATE] pod salvati per il prossimo run: {len(new_pods)}")
    logger.info("[FINE]")
    return 0


if __name__ == "__main__":
    sys.exit(main())

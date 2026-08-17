#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
collector_insula_processing_iride_elastic.py
============================================

KPI: "Availability of Processing services" - Insula (IRIDE CyberItaly).
Requisito contrattuale: >= 95% di NWD-NWH.

E' un KPI DISTINTO da "Availability of data catalogue and access services":
sono due requisiti separati, ciascuno con la propria soglia. Per questo il
collector e' separato e le sonde sul processing NON devono piu' concorrere
all'overall del collector catalogo (nell'altro script: PROBE_JOBS = False,
PROBE_COLLECTIONS = False).

ARCHITETTURA A DUE LIVELLI
--------------------------
LIVELLO 1 -- raggiungibilita' del motore (gratuito, sempre attivo)
  jobs_search       GET /jobs/search/parametricFind    il motore risponde
  jobconfigs_list   GET /jobConfigs                    il registro risponde
  service_check     GET /services/{id}                 il processore esiste,
                                                       e' lanciabile e la sua
                                                       immagine non e' cambiata

LIVELLO 2 -- esecuzione reale (tariffato, cadenza configurabile)
  job_launch        POST /jobConfigs/{id}/launch       il motore accetta il job
  job_execution     GET  /jobs/{id}  in polling        il job arriva a COMPLETED
  job_outputs       (dal payload detailedJob)          ha prodotto qualcosa
  cleanup_outputs   DELETE /platformFiles/{id}         pulizia dei file
  cleanup_job       DELETE /jobs/{id}                  pulizia del record

Il livello 1 da' solo "il servizio risponde". Il livello 2 dimostra che il
processing FUNZIONA davvero, ed e' cio' che il requisito chiede. Se il launch
e' disabilitato o non ancora configurato, il KPI ricade sul livello 1 e lo
dichiara esplicitamente nell'evento (kpi_level).

NOTE OPERATIVE RICAVATE DALL'AVANSCOPERTA (recon_insula_processing_iride.py)
---------------------------------------------------------------------------
  - 'sort=id,desc' e' l'UNICO ordinamento affidabile: startDateTime viene
    accettato e ignorato in silenzio, e startTime/endTime/created ordinano i
    valori NULL per primi.
  - NESSUN filtro temporale funziona su parametricFind: startDateTime ed
    endDateTime restituiscono 0 con qualunque data, startTime ed endTime
    restituiscono tutto. Ogni finestra temporale va applicata lato client.
  - La response del launch espone 'id' oltre a 'extId' (il manuale documenta
    solo extId): si usa l'id, con fallback sulla ricerca per extId.
  - Gli output sono gia' nel payload di detailedJob (campo 'outputFiles'):
    l'endpoint dedicato /jobs/{id}/outputFiles risponde 404.
  - DELETE /jobs/{id} FUNZIONA (HTTP 204) benche' non documentato: il record
    del job puo' essere rimosso dopo aver letto l'esito.
  - startTime/endTime arrivano senza suffisso di fuso ma sono UTC (verificato:
    scarto di ~3 s rispetto a 'created', che ha la Z).
  - Il costo dichiarato in costingExpression e' 1 coin, ma un lancio reale ha
    scalato 2 coin dal wallet: il costo va misurato, non assunto.

ORDINE DELLE OPERAZIONI (importante)
------------------------------------
L'esito del job viene registrato PRIMA della cancellazione. Dopo la DELETE
l'unica prova della misurazione sono gli eventi su Elasticsearch e il log
JSON locale: la pulizia non deve mai precedere la registrazione.

Gli esiti della pulizia NON influenzano il KPI: sono manutenzione, non
disponibilita' del servizio. Vengono comunque emessi come sonde per poterli
allertare separatamente.

OUTPUT
------
  - <LOG_DIR>/collector_insula_processing_iride_elastic.log        eventi JSON
  - <LOG_DIR>/collector_insula_processing_iride_elastic.debug.log log operativo
  - Elasticsearch index KPI_INDEX (se ELASTIC_ENABLED = True)

Eventi emessi:
  event_type = "insula_processing_probe"        (uno per sonda)
  event_type = "insula_processing_availability" (uno per run, sempre)
  event_type = "insula_wallet_recharge"         (solo se ricarica eseguita)

DIPENDENZE: solo standard library.

Uso:
  python3 collector_insula_processing_iride_elastic.py
  python3 collector_insula_processing_iride_elastic.py --dry-run -v
  python3 collector_insula_processing_iride_elastic.py --no-launch
  python3 collector_insula_processing_iride_elastic.py --force-launch
  python3 collector_insula_processing_iride_elastic.py --status
"""

from __future__ import annotations

import argparse
import configparser
import json
import logging
import os
import platform
import socket
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone

# ============================================================================
# COSTANTI
# ============================================================================

SCRIPT_VERSION = "1.0.0"
SCRIPT_NAME = "collector_insula_processing_iride_elastic"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG = os.path.join(SCRIPT_DIR, "insula_processing_iride_elastic.ini")

PLATFORM_TAG = "iride"
SERVICE_PROVIDER_LOG = "iride_insula_processing"
KPI_NAME = "availability_processing_services"

EVENT_PROBE = "insula_processing_probe"
EVENT_AVAILABILITY = "insula_processing_availability"
EVENT_RECHARGE = "insula_wallet_recharge"

STATUS_OK = "OK"
STATUS_NOK = "NOK"

CAT_AUTH = "auth"
CAT_REACHABILITY = "reachability"   # livello 1: il motore risponde
CAT_EXECUTION = "execution"         # livello 2: il motore elabora
CAT_CLEANUP = "cleanup"             # manutenzione, fuori dal KPI
CAT_WALLET = "wallet"

# Stati terminali di un job, sezione 2.16 del manuale API v2.0.
JOB_TERMINAL = ("COMPLETED", "ERROR", "CANCELLED")
JOB_RUNNING = ("CREATED", "PENDING", "RUNNING", "WAITING", "CONDITION_WAIT")

BILLING_MARKERS = ("wallet", "coin", "balance", "payment", "quota exceeded",
                   "insufficient", "credit")

OAUTH_HINTS = {
    "invalid_grant":
        "username/password rifiutati, oppure utente disabilitato o con "
        "azioni obbligatorie pendenti. Controllare [KEYCLOAK] USERNAME/PASSWORD.",
    "invalid_client":
        "il client non e' public: serve [KEYCLOAK] CLIENT_SECRET, "
        "oppure CLIENT_ID errato.",
    "unauthorized_client":
        "sul client il flusso Direct Access Grants e' disabilitato.",
    "invalid_scope": "scope richiesto non assegnato al client.",
    "invalid_request": "parametri della richiesta token incompleti.",
    "access_denied": "accesso negato da una policy del realm.",
}

log = logging.getLogger(SCRIPT_NAME)


# ============================================================================
# UTILITY
# ============================================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_ms(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + \
        f"{dt.microsecond // 1000:03d}Z"


try:
    from zoneinfo import ZoneInfo
    _ROME_TZ = ZoneInfo("Europe/Rome")
except Exception:  # pragma: no cover
    _ROME_TZ = None

# Finestra NWD/NWH: lun-ven, 08:00-16:59 ora italiana. Fascia [start, end).
NWH_START_HOUR = 8
NWH_END_HOUR = 17
NWD_DAYS = (1, 2, 3, 4, 5)


def local_rome(dt: datetime):
    dt = dt.astimezone(timezone.utc)
    if _ROME_TZ is not None:
        loc = dt.astimezone(_ROME_TZ)
    else:
        loc = dt.astimezone(timezone(timedelta(hours=1)))
    return loc, loc.isoweekday(), loc.hour


def is_business_window(dt: datetime) -> bool:
    _, wd, hour = local_rome(dt)
    return wd in NWD_DAYS and NWH_START_HOUR <= hour < NWH_END_HOUR


def parse_dt(value):
    """
    I timestamp dei job arrivano senza suffisso di fuso (startTime, endTime)
    oppure con la Z (created, lastUpdated). Verificato in avanscoperta che i
    naive siano comunque UTC: lo scarto mediano fra startTime e created e' di
    circa 3 secondi.
    """
    if not value:
        return None
    raw = str(value).strip()
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        dt = None
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue
        if dt is None:
            return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def as_bool(value, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on", "si", "s")


def as_int(value, default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def as_limit(value, default: int):
    """Vuoto, 0 o negativo = nessun limite. Ritorna None quando disattivato."""
    if value is None:
        return default
    raw = str(value).strip()
    if raw == "":
        return None
    try:
        n = int(raw)
    except ValueError:
        return default
    return None if n <= 0 else n


def as_float(value, default: float) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return default


def redact(text, secrets) -> str:
    if not text:
        return text
    out = str(text)
    for s in secrets:
        if s and len(str(s)) > 3:
            out = out.replace(str(s), "***REDACTED***")
    return out


def body_snippet(res, secrets, limit: int = 400) -> str:
    if res is None or not getattr(res, "body", None):
        return ""
    raw = res.body.decode("utf-8", errors="replace").strip()
    data = res.json()
    if isinstance(data, dict):
        parts = []
        for key in ("error", "error_description", "message", "status", "path"):
            val = data.get(key)
            if val not in (None, ""):
                parts.append(f"{key}={val}")
        if parts:
            raw = " ".join(parts)
    raw = " ".join(raw.split())
    if len(raw) > limit:
        raw = raw[:limit] + "..."
    return redact(raw, secrets)


def classify_error(res, secrets) -> str:
    """
    'entitlement' -> il servizio risponde ma nega per credito/quota. NON e'
                     indisponibilita' della piattaforma.
    'unavailable' -> rete, 5xx, timeout: il servizio non funziona.
    'client'      -> 4xx generico: richiesta o configurazione sbagliata.
    """
    if res is None:
        return "unknown"
    status = res.status
    text = (body_snippet(res, secrets) or "").lower()
    looks_billing = any(m in text for m in BILLING_MARKERS)
    if status == 402 or (status in (403, 429) and looks_billing):
        return "entitlement"
    if status is None or (status and status >= 500):
        return "unavailable"
    if status and 400 <= status < 500:
        return "entitlement" if looks_billing else "client"
    return "unknown"


def http_error_text(res, secrets) -> str:
    base = (res.error if res and res.error else
            (f"HTTP {res.status}" if res and res.status else "errore sconosciuto"))
    snippet = body_snippet(res, secrets)
    return f"{base} | {snippet}" if snippet else base


def human_duration(seconds):
    if seconds is None:
        return "n/d"
    s = int(round(seconds))
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60:02d}s"
    return f"{s // 3600}h {(s % 3600) // 60:02d}m"


class RunLock:
    """
    Lock su file per impedire run sovrapposti.

    Serve con cron a intervalli brevi: se un job resta appeso, il polling puo'
    tenere occupato il processo piu' a lungo dell'intervallo del cron, che nel
    frattempo ne avvia altri. Piu' istanze concorrenti significano file di
    stato corrotto e lanci paralleli non voluti (quindi coin sprecati).

    Implementazione volutamente semplice e portabile: creazione atomica con
    O_CREAT|O_EXCL, che funziona sia su Linux sia su Windows senza fcntl.
    Il lock contiene PID e timestamp, e viene considerato abbandonato oltre
    una soglia: senza questo, un processo ucciso male bloccherebbe il
    monitoraggio per sempre.
    """

    def __init__(self, path: str, stale_minutes: int = 60):
        self.path = path
        self.stale_minutes = stale_minutes
        self.acquired = False

    def _read(self):
        try:
            with open(self.path, encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            return None

    def _is_stale(self, info) -> bool:
        if not info:
            return True
        ts = info.get("at")
        try:
            age_min = (utc_now() - datetime.fromisoformat(
                str(ts).replace("Z", "+00:00"))).total_seconds() / 60.0
        except (TypeError, ValueError):
            return True
        return age_min > self.stale_minutes

    def acquire(self) -> bool:
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            info = self._read()
            if self._is_stale(info):
                log.warning("Lock abbandonato da oltre %d minuti (pid %s): "
                            "lo rimuovo e proseguo.",
                            self.stale_minutes, (info or {}).get("pid"))
                try:
                    os.unlink(self.path)
                except OSError:
                    pass
                return self.acquire()
            log.info("Un altro run e' gia' in corso (pid %s, avviato %s): "
                     "esco senza fare nulla.",
                     (info or {}).get("pid"), (info or {}).get("at"))
            return False
        except OSError as e:
            log.warning("Lock non creabile (%s): proseguo senza.", e)
            return True

        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump({"pid": os.getpid(), "at": iso_ms(utc_now()),
                       "host": platform.node()}, f)
        self.acquired = True
        return True

    def release(self):
        if not self.acquired:
            return
        try:
            os.unlink(self.path)
        except OSError as e:
            log.warning("Lock non rimosso (%s): il prossimo run lo trattera' "
                        "come abbandonato dopo %d minuti.", e, self.stale_minutes)


def setup_logging(log_dir: str, verbose: bool) -> str:
    os.makedirs(log_dir, exist_ok=True)
    debug_path = os.path.join(log_dir, f"{SCRIPT_NAME}.debug.log")

    log.setLevel(logging.DEBUG)
    log.handlers.clear()
    log.propagate = False

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-7s] %(message)s", "%Y-%m-%dT%H:%M:%S")

    fh = logging.FileHandler(debug_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    log.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(fmt)
    log.addHandler(ch)

    for noisy in ("urllib3", "elasticsearch", "elastic_transport",
                  "requests", "charset_normalizer"):
        logging.getLogger(noisy).setLevel(logging.CRITICAL)
        logging.getLogger(noisy).propagate = False

    return debug_path


# ============================================================================
# CONFIGURAZIONE
# ============================================================================

class Config:

    def __init__(self, path: str):
        if not os.path.isfile(path):
            raise FileNotFoundError(f"File di configurazione non trovato: {path}")
        self.path = path
        cp = configparser.ConfigParser(interpolation=None)
        cp.optionxform = str
        cp.read(path, encoding="utf-8")
        self.cp = cp
        g = self._get

        # --- [CONFIG] -------------------------------------------------------
        self.elastic_enabled = as_bool(g("CONFIG", "ELASTIC_ENABLED"), False)
        self.monitoring_url = (g("CONFIG", "MONITORING_URL", "") or "").rstrip("/")
        self.monitoring_apikey = g("CONFIG", "MONITORING_APIKEY", "")
        self.monitoring_verify_certs = as_bool(
            g("CONFIG", "MONITORING_VERIFY_CERTS"), False)
        self.kpi_index = g("CONFIG", "KPI_INDEX",
                           "logs-iride-insula-processing.monitoring-default")
        self.log_dir = (g("CONFIG", "LOG_DIR", "") or "").strip() or SCRIPT_DIR

        # --- [INSULA] / [KEYCLOAK] ------------------------------------------
        self.insula_base = (g("INSULA", "BASE_URL", "") or "").rstrip("/")
        self.kc_url = (g("KEYCLOAK", "URL", "") or "").rstrip("/")
        self.kc_realm = g("KEYCLOAK", "REALM", "")
        self.kc_client_id = g("KEYCLOAK", "CLIENT_ID", "admin-cli")
        self.kc_client_secret = g("KEYCLOAK", "CLIENT_SECRET", "")
        self.kc_username = g("KEYCLOAK", "USERNAME", "")
        self.kc_password = g("KEYCLOAK", "PASSWORD", "")

        # --- [INSULA_PROCESSING] --------------------------------------------
        s = "INSULA_PROCESSING"
        self.probe_jobs = as_bool(g(s, "PROBE_JOBS"), True)
        self.jobs_expect_any = as_bool(g(s, "JOBS_EXPECT_ANY"), False)
        self.probe_jobconfigs = as_bool(g(s, "PROBE_JOBCONFIGS"), True)
        self.probe_service = as_bool(g(s, "PROBE_SERVICE"), True)

        self.launch_enabled = as_bool(g(s, "JOB_LAUNCH_ENABLED"), False)
        self.launch_config_id = (g(s, "JOB_LAUNCH_CONFIG_ID", "") or "").strip()
        self.launch_service_id = (g(s, "JOB_LAUNCH_SERVICE_ID", "") or "").strip()
        self.expected_docker_tag = (g(s, "JOB_EXPECTED_DOCKER_TAG", "") or "").strip()
        self.launch_every_minutes = as_int(g(s, "JOB_LAUNCH_EVERY_MINUTES"), 30)
        self.max_wait_minutes = as_int(g(s, "JOB_MAX_WAIT_MINUTES"), 30)
        self.poll_interval_s = as_int(g(s, "JOB_POLL_INTERVAL_S"), 15)
        self.expect_outputs = as_bool(g(s, "JOB_EXPECT_OUTPUTS"), True)
        self.cleanup_outputs = as_bool(g(s, "JOB_CLEANUP_OUTPUTS"), True)
        self.cleanup_job = as_bool(g(s, "JOB_CLEANUP_JOB"), True)
        self.billing_errors_as_nok = as_bool(g(s, "BILLING_ERRORS_AS_NOK"), False)
        self.state_file = (g(s, "STATE_FILE", "") or "").strip()
        self.lock_file = (g(s, "LOCK_FILE", "") or "").strip()
        self.lock_stale_minutes = as_int(g(s, "LOCK_STALE_MINUTES"), 60)

        self.http_timeout = as_int(g(s, "HTTP_TIMEOUT"), 60)
        self.retries = as_int(g(s, "RETRIES"), 2)
        self.retry_backoff_s = as_float(g(s, "RETRY_BACKOFF_S"), 2.0)
        self.use_system_proxy = as_bool(g(s, "USE_SYSTEM_PROXY"), False)
        self.insula_verify_certs = as_bool(g(s, "INSULA_VERIFY_CERTS"), True)
        self.latency_warn_ms = as_int(g(s, "LATENCY_WARN_MS"), 3000)
        self.latency_crit_ms = as_int(g(s, "LATENCY_CRIT_MS"), 10000)
        self.duration_warn_s = as_int(g(s, "JOB_DURATION_WARN_S"), 300)
        self.emit_individual_probes = as_bool(g(s, "EMIT_INDIVIDUAL_PROBES"), True)

        # --- [INSULA_WALLET] -------------------------------------------------
        w = "INSULA_WALLET"
        self.wallet_monitor = as_bool(g(w, "WALLET_MONITOR"), True)
        self.wallet_autorecharge = as_bool(g(w, "WALLET_AUTORECHARGE"), False)
        self.wallet_id = (g(w, "WALLET_ID", "") or "").strip()
        self.wallet_min_balance = as_int(g(w, "WALLET_MIN_BALANCE"), 5)
        self.wallet_recharge_amount = as_int(g(w, "WALLET_RECHARGE_AMOUNT"), 10)
        self.wallet_max_per_day = as_limit(g(w, "WALLET_MAX_RECHARGES_PER_DAY"), 12)
        self.wallet_max_amount_per_day = as_limit(
            g(w, "WALLET_MAX_AMOUNT_PER_DAY"), 120)
        self.wallet_low_threshold = as_int(g(w, "WALLET_LOW_THRESHOLD"), 10)
        self.wallet_state_file = (g(w, "WALLET_STATE_FILE", "") or "").strip()

        self._validate()

    def _get(self, section, key, default=None):
        if self.cp.has_option(section, key):
            return self.cp.get(section, key)
        return default

    def _validate(self):
        missing = []
        if not self.insula_base:
            missing.append("[INSULA] BASE_URL")
        for k, v in (("URL", self.kc_url), ("REALM", self.kc_realm),
                     ("USERNAME", self.kc_username), ("PASSWORD", self.kc_password)):
            if not v:
                missing.append(f"[KEYCLOAK] {k}")
        if self.elastic_enabled and not self.monitoring_url:
            missing.append("[CONFIG] MONITORING_URL")
        if self.launch_enabled and not self.launch_config_id:
            missing.append("[INSULA_PROCESSING] JOB_LAUNCH_CONFIG_ID "
                           "(obbligatorio con JOB_LAUNCH_ENABLED = True)")
        if missing:
            raise ValueError("Parametri obbligatori mancanti nell'ini: "
                             + ", ".join(missing))

    @property
    def secrets(self):
        return [self.kc_password, self.kc_client_secret, self.monitoring_apikey]


# ============================================================================
# CLIENT HTTP (stdlib)
# ============================================================================

class HttpResult:
    __slots__ = ("status", "body", "headers", "latency_ms", "bytes", "error",
                 "url", "attempts")

    def __init__(self, url):
        self.url = url
        self.status = None
        self.body = b""
        self.headers = {}
        self.latency_ms = None
        self.bytes = 0
        self.error = None
        self.attempts = 0

    @property
    def ok(self) -> bool:
        return self.error is None and self.status is not None and 200 <= self.status < 300

    def json(self):
        if not self.body:
            return None
        try:
            return json.loads(self.body.decode("utf-8", errors="replace"))
        except (ValueError, UnicodeDecodeError):
            return None


class HttpClient:
    """Client minimale su urllib: timeout, retry con backoff, bypass proxy."""

    def __init__(self, timeout, retries, backoff, verify_certs,
                 use_system_proxy, secrets):
        self.timeout = timeout
        self.retries = max(0, retries)
        self.backoff = backoff
        self.secrets = secrets

        ctx = ssl.create_default_context()
        if not verify_certs:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

        handlers = [urllib.request.HTTPSHandler(context=ctx)]
        if not use_system_proxy:
            handlers.append(urllib.request.ProxyHandler({}))
        self.opener = urllib.request.build_opener(*handlers)

    def request(self, method, url, headers=None, data=None,
                accept="application/json") -> HttpResult:
        res = HttpResult(url)
        hdrs = {"Accept": accept, "User-Agent": f"{SCRIPT_NAME}/{SCRIPT_VERSION}"}
        if headers:
            hdrs.update(headers)

        body = data
        if isinstance(data, dict):
            body = urllib.parse.urlencode(data).encode("utf-8")
            hdrs.setdefault("Content-Type", "application/x-www-form-urlencoded")

        attempt = 0
        while attempt <= self.retries:
            attempt += 1
            res.attempts = attempt
            req = urllib.request.Request(url, data=body, headers=hdrs, method=method)
            t0 = time.perf_counter()
            try:
                with self.opener.open(req, timeout=self.timeout) as resp:
                    res.status = resp.getcode()
                    res.headers = {k.lower(): v for k, v in resp.headers.items()}
                    res.body = resp.read()
                res.bytes = len(res.body)
                res.latency_ms = round((time.perf_counter() - t0) * 1000, 2)
                res.error = None
                return res

            except urllib.error.HTTPError as e:
                res.latency_ms = round((time.perf_counter() - t0) * 1000, 2)
                res.status = e.code
                try:
                    res.body = e.read()[:4096]
                except Exception:
                    res.body = b""
                res.bytes = len(res.body)
                res.error = f"HTTP {e.code} {e.reason}"
                if 400 <= e.code < 500 and e.code != 429:
                    return res

            except (urllib.error.URLError, socket.timeout, ssl.SSLError, OSError) as e:
                res.latency_ms = round((time.perf_counter() - t0) * 1000, 2)
                res.status = None
                res.error = f"{type(e).__name__}: {getattr(e, 'reason', e)}"

            if attempt <= self.retries:
                sleep_s = self.backoff * attempt
                log.debug("Retry %d/%d fra %.1fs (%s)", attempt, self.retries,
                          sleep_s, redact(res.error, self.secrets))
                time.sleep(sleep_s)

        return res

    def get(self, url, headers=None, accept="application/json"):
        return self.request("GET", url, headers=headers, accept=accept)

    def post(self, url, headers=None, data=None):
        return self.request("POST", url, headers=headers, data=data)

    def delete(self, url, headers=None):
        return self.request("DELETE", url, headers=headers)


# ============================================================================
# AUTENTICAZIONE KEYCLOAK
# ============================================================================

class KeycloakAuth:
    """Password grant su admin-cli (public client). Token cache con margine."""

    def __init__(self, cfg: Config, http: HttpClient):
        self.cfg = cfg
        self.http = http
        self._token = None
        self._expires_at = 0.0

    @property
    def token_url(self) -> str:
        return (f"{self.cfg.kc_url}/realms/{self.cfg.kc_realm}"
                f"/protocol/openid-connect/token")

    def fetch_token(self, force: bool = False):
        if not force and self._token and time.time() < self._expires_at:
            return self._token, None

        payload = {
            "grant_type": "password",
            "client_id": self.cfg.kc_client_id,
            "username": self.cfg.kc_username,
            "password": self.cfg.kc_password,
        }
        if self.cfg.kc_client_secret:
            payload["client_secret"] = self.cfg.kc_client_secret

        res = self.http.post(self.token_url, data=payload)
        if not res.ok:
            return None, res

        data = res.json() or {}
        token = data.get("access_token")
        if not token:
            res.error = "access_token assente nella risposta Keycloak"
            return None, res

        expires_in = as_int(data.get("expires_in"), 300)
        self._token = token
        self._expires_at = time.time() + max(30, expires_in - 30)
        return token, res

    def auth_header(self):
        return {"Authorization": f"Bearer {self._token}"} if self._token else {}

    def ensure_fresh(self):
        """
        Il polling di un job puo' durare piu' della vita del token (300 s).
        Va rinnovato prima che scada, altrimenti a meta' attesa arrivano 401
        che sembrerebbero indisponibilita' del servizio.
        """
        if time.time() >= self._expires_at - 15:
            self.fetch_token(force=True)


# ============================================================================
# SONDE
# ============================================================================

class Probe:
    """Esito di una singola operazione misurata."""

    def __init__(self, name, category, url, description=""):
        self.name = name
        self.category = category
        self.url = url
        self.description = description
        self.status = STATUS_NOK
        self.http_status = None
        self.latency_ms = None
        self.attempts = 0
        self.bytes = 0
        self.error = None
        self.details = {}
        self.skipped = False

    def mark_ok(self, res=None, **details):
        self.status = STATUS_OK
        self._absorb(res)
        self.details.update(details)
        return self

    def mark_nok(self, error, res=None, **details):
        self.status = STATUS_NOK
        self.error = error
        self._absorb(res)
        self.details.update(details)
        return self

    def mark_skipped(self, reason):
        self.skipped = True
        self.status = STATUS_OK          # non conteggiata come fallimento
        self.details["skip_reason"] = reason
        return self

    def _absorb(self, res):
        if res is None:
            return
        self.http_status = res.status
        self.latency_ms = res.latency_ms
        self.attempts = res.attempts
        self.bytes = res.bytes
        if self.url is None:
            self.url = res.url

    def latency_status(self, warn_ms, crit_ms):
        if self.latency_ms is None:
            return None
        if self.latency_ms >= crit_ms:
            return "CRITICAL"
        if self.latency_ms >= warn_ms:
            return "WARNING"
        return "NORMAL"


class ProcessingKpiCollector:

    def __init__(self, cfg: Config, args):
        self.cfg = cfg
        self.args = args
        self.http = HttpClient(
            timeout=cfg.http_timeout, retries=cfg.retries,
            backoff=cfg.retry_backoff_s, verify_certs=cfg.insula_verify_certs,
            use_system_proxy=cfg.use_system_proxy, secrets=cfg.secrets)
        self.auth = KeycloakAuth(cfg, self.http)
        self.run_id = str(uuid.uuid4())
        self.hostname = platform.node() or socket.gethostname()
        self.probes = []

        self.job_id = None
        self.job_ext_id = None
        self.job_status = None
        self.job_duration_s = None
        self.job_queue_wait_s = None
        self.job_outputs = []
        self.launch_performed = False
        self.launch_skipped_reason = None
        self.cost_declared = None
        self.cost_measured = None
        # 'HOURLY' nella costQuotation suggerisce una tariffazione a scatti
        # orari e non per singolo lancio: spiegherebbe perche' il costo
        # misurato oscilla fra 1 e 2 coin. Va registrato per verificarlo sui
        # dati invece di ipotizzarlo.
        self.cost_recurrence = None

        self.wallet_balance = None
        self.wallet_before = None
        self.wallet_id_resolved = None
        self.wallet_recharge_result = None
        self.wallet_capped = False

        self.docker_tag_seen = None
        self.docker_tag_changed = False
        self.service_status_seen = None

        self.state = self._state_read()

    # -- stato persistente ---------------------------------------------------

    def _state_path(self) -> str:
        if self.cfg.state_file:
            return self.cfg.state_file
        return os.path.join(self.cfg.log_dir, ".insula_processing_state.json")

    def _state_read(self) -> dict:
        try:
            with open(self._state_path(), encoding="utf-8") as f:
                d = json.load(f)
            return d if isinstance(d, dict) else {}
        except (OSError, ValueError):
            return {}

    def _state_write(self):
        try:
            with open(self._state_path(), "w", encoding="utf-8") as f:
                json.dump(self.state, f, ensure_ascii=False, indent=2)
        except OSError as e:
            log.warning("Impossibile scrivere lo stato: %s", e)

    # -- helpers -------------------------------------------------------------

    def _api(self, path, params=None) -> str:
        url = f"{self.cfg.insula_base}{path}"
        if params:
            clean = {k: v for k, v in params.items() if v not in (None, "")}
            if clean:
                url = f"{url}?{urllib.parse.urlencode(clean)}"
        return url

    def _add(self, probe: Probe) -> Probe:
        self.probes.append(probe)
        if probe.skipped:
            icon, note = "-- ", f"saltata: {probe.details.get('skip_reason') or ''}"
        else:
            icon = "OK " if probe.status == STATUS_OK else "NOK"
            note = redact(probe.error or "", self.cfg.secrets)
        log.info("[%s] %-18s %-13s http=%s  %s ms  %s",
                 icon, probe.name, probe.category, probe.http_status,
                 probe.latency_ms, note)
        return probe

    @staticmethod
    def _embedded(data, *keys):
        """
        Il manuale usa nomi incoerenti fra endpoint: si accettano i nomi attesi
        e, in mancanza, la prima lista trovata dentro _embedded.
        """
        if not isinstance(data, dict):
            return []
        emb = data.get("_embedded") or {}
        for k in keys:
            v = emb.get(k)
            if isinstance(v, list):
                return v
        for v in emb.values():
            if isinstance(v, list):
                return v
        return []

    # ========================================================================
    # LIVELLO 0: autenticazione
    # ========================================================================

    def probe_auth(self) -> Probe:
        p = Probe("auth", CAT_AUTH, self.auth.token_url,
                  "Keycloak password grant (prerequisito di tutte le sonde)")
        token, res = self.auth.fetch_token(force=True)
        if token:
            return self._add(p.mark_ok(res, realm=self.cfg.kc_realm,
                                       client_id=self.cfg.kc_client_id))
        data = (res.json() if res else None) or {}
        if not isinstance(data, dict):
            data = {}
        # ATTENZIONE: 'error' non e' sempre una stringa. Se l'endpoint token e'
        # sbagliato si finisce su un altro servizio, che puo' restituire un
        # oggetto annidato: usarlo come chiave di dizionario solleverebbe
        # TypeError e farebbe morire il collector proprio quando deve invece
        # registrare che l'autenticazione non funziona.
        raw_error = data.get("error")
        oauth_error = raw_error if isinstance(raw_error, str) else None
        hint = OAUTH_HINTS.get(oauth_error) if oauth_error else None
        if raw_error is not None and oauth_error is None:
            hint = ("la risposta non ha la forma di un errore OAuth: "
                    "verificare che [KEYCLOAK] URL punti davvero al server "
                    "Keycloak. L'endpoint atteso e' "
                    "{URL}/realms/{REALM}/protocol/openid-connect/token")
        msg = http_error_text(res, self.cfg.secrets) if res else "nessuna risposta"
        if hint:
            msg = f"{msg} -> {hint}"
        desc = data.get("error_description")
        return self._add(p.mark_nok(
            msg, res,
            oauth_error=oauth_error,
            oauth_error_raw=(json.dumps(raw_error, ensure_ascii=False)[:200]
                             if raw_error is not None and oauth_error is None
                             else None),
            oauth_error_description=desc if isinstance(desc, str) else None,
            token_url=self.auth.token_url,
            hint=hint))

    # ========================================================================
    # LIVELLO 1: raggiungibilita' del motore (gratuito)
    # ========================================================================

    def probe_jobs_search(self) -> Probe:
        """
        GET /jobs/search/parametricFind
        Il motore di processing risponde e la lista dei job e' interrogabile.

        NOTA: si ordina per 'id,desc'. In avanscoperta e' emerso che
        'startDateTime,desc' viene accettato e IGNORATO in silenzio (ricade
        sull'ordine naturale, cioe' i job piu' vecchi) e che gli ordinamenti
        sui campi temporali mettono in testa i record con timestamp NULL.
        """
        params = {"projection": "shortJob", "size": 1, "page": 0,
                  "sort": "id,desc"}
        url = self._api("/jobs/search/parametricFind", params)
        p = Probe("jobs_search", CAT_REACHABILITY, url,
                  "Ricerca job: il motore di processing risponde")

        if not self.cfg.probe_jobs:
            return self._add(p.mark_skipped("PROBE_JOBS = False"))

        res = self.http.get(url, headers=self.auth.auth_header())
        if not res.ok:
            return self._add(p.mark_nok(
                http_error_text(res, self.cfg.secrets), res,
                error_class=classify_error(res, self.cfg.secrets),
                response_body=body_snippet(res, self.cfg.secrets)))

        data = res.json()
        if not isinstance(data, dict):
            return self._add(p.mark_nok("risposta non JSON", res,
                                        error_class="client"))

        total = (data.get("page") or {}).get("totalElements")
        jobs = self._embedded(data, "jobs")

        if self.cfg.jobs_expect_any and not total:
            return self._add(p.mark_nok(
                "il servizio risponde ma non risultano job "
                "(JOBS_EXPECT_ANY = True)", res, total_elements=total))

        latest = jobs[0] if jobs else {}
        return self._add(p.mark_ok(
            res, total_elements=total,
            latest_job_id=latest.get("id"),
            latest_job_status=latest.get("status"),
            latest_job_service=latest.get("serviceName")))

    def probe_jobconfigs(self) -> Probe:
        """
        GET /jobConfigs
        Il registro delle configurazioni risponde. E' il prerequisito del
        launch: se non e' interrogabile, non si puo' lanciare nulla.
        """
        params = {"size": 1, "page": 0, "sort": "id,desc"}
        url = self._api("/jobConfigs", params)
        p = Probe("jobconfigs_list", CAT_REACHABILITY, url,
                  "Elenco jobConfig: il registro delle configurazioni risponde")

        if not self.cfg.probe_jobconfigs:
            return self._add(p.mark_skipped("PROBE_JOBCONFIGS = False"))

        res = self.http.get(url, headers=self.auth.auth_header())
        if not res.ok:
            return self._add(p.mark_nok(
                http_error_text(res, self.cfg.secrets), res,
                error_class=classify_error(res, self.cfg.secrets),
                response_body=body_snippet(res, self.cfg.secrets)))

        data = res.json()
        if not isinstance(data, dict):
            return self._add(p.mark_nok("risposta non JSON", res,
                                        error_class="client"))
        total = (data.get("page") or {}).get("totalElements")
        return self._add(p.mark_ok(res, total_elements=total))

    def probe_service(self) -> Probe:
        """
        GET /services/{id}
        Il processore usato dalla sonda esiste, non e' DISABLED, e la sua
        immagine docker non e' cambiata.

        Perche' conta: tutti i service di questa piattaforma sono in stato
        IN_DEVELOPMENT, quindi nessuno garantisce che l'immagine resti quella.
        Se cambia, il comportamento del processore puo' cambiare e il KPI
        misurerebbe una cosa diversa senza che nessuno se ne accorga. Il
        dockerTag atteso e' congelato in JOB_EXPECTED_DOCKER_TAG al momento
        del setup.
        """
        sid = self.cfg.launch_service_id
        url = self._api(f"/services/{sid}") if sid else None
        p = Probe("service_check", CAT_REACHABILITY, url,
                  "Il processore della sonda esiste ed e' invariato")

        if not self.cfg.probe_service or not sid:
            return self._add(p.mark_skipped(
                "PROBE_SERVICE = False o JOB_LAUNCH_SERVICE_ID non valorizzato"))

        res = self.http.get(url, headers=self.auth.auth_header())
        if not res.ok:
            return self._add(p.mark_nok(
                http_error_text(res, self.cfg.secrets), res,
                error_class=classify_error(res, self.cfg.secrets)))

        svc = res.json() or {}
        self.docker_tag_seen = svc.get("dockerTag")
        self.service_status_seen = svc.get("status")
        cost = (svc.get("costingExpression") or {}).get("costExpression")
        self.cost_declared = cost

        details = {"service_id": sid, "service_name": svc.get("name"),
                   "service_status": svc.get("status"),
                   "docker_tag": self.docker_tag_seen,
                   "cost_expression": cost}

        if svc.get("status") == "DISABLED":
            return self._add(p.mark_nok(
                f"il service {sid} e' DISABLED: non e' lanciabile", res, **details))

        if (self.cfg.expected_docker_tag
                and self.docker_tag_seen
                and self.docker_tag_seen != self.cfg.expected_docker_tag):
            self.docker_tag_changed = True
            log.warning("dockerTag cambiato: atteso '%s', trovato '%s'. "
                        "Il processore potrebbe comportarsi diversamente: "
                        "verificare e aggiornare JOB_EXPECTED_DOCKER_TAG.",
                        self.cfg.expected_docker_tag, self.docker_tag_seen)
            # Non e' indisponibilita': il servizio risponde. Si segnala come
            # sonda OK con un flag dedicato, cosi' si puo' allertare a parte
            # senza far crollare il KPI contrattuale.
            details["docker_tag_expected"] = self.cfg.expected_docker_tag
            details["docker_tag_changed"] = True

        return self._add(p.mark_ok(res, **details))

    # ========================================================================
    # WALLET
    # ========================================================================

    WALLET_BALANCE_KEYS = ("balance", "credit", "credits", "amount",
                           "coins", "value", "available", "remaining")

    @classmethod
    def _extract_balance(cls, data):
        if isinstance(data, (int, float)):
            return int(data), "valore scalare"
        if not isinstance(data, dict):
            return None, None
        for k in cls.WALLET_BALANCE_KEYS:
            v = data.get(k)
            if isinstance(v, (int, float)):
                return int(v), k
        for key, sub in data.items():
            if isinstance(sub, dict):
                for k in cls.WALLET_BALANCE_KEYS:
                    v = sub.get(k)
                    if isinstance(v, (int, float)):
                        return int(v), f"{key}.{k}"
        return None, None

    def _wallet_get(self):
        h = self.auth.auth_header()
        paths = ["/wallets/current"]
        if self.cfg.wallet_id:
            paths.append(f"/wallets/{self.cfg.wallet_id}")
        last_err = None
        for path in paths:
            r = self.http.get(self._api(path), headers=h)
            if not r.ok:
                last_err = f"{path}: {http_error_text(r, self.cfg.secrets)}"
                continue
            data = r.json()
            balance, _ = self._extract_balance(data)
            wid = data.get("id") or data.get("walletId") if isinstance(data, dict) else None
            return (str(wid) if wid else self.cfg.wallet_id or None), balance, None
        return None, None, last_err or "nessun endpoint wallet raggiungibile"

    def probe_wallet(self) -> Probe:
        """
        Il saldo e' una metrica monitorabile: consente di allertare PRIMA che
        il KPI si fermi, invece di scoprirlo da un HTTP 402.
        """
        p = Probe("wallet_balance", CAT_WALLET, self._api("/wallets/current"),
                  "Saldo del wallet dell'utenza di monitoraggio")

        if not self.cfg.wallet_monitor:
            return self._add(p.mark_skipped("WALLET_MONITOR = False"))

        wid, balance, err = self._wallet_get()
        if balance is None:
            return self._add(p.mark_skipped(f"saldo non leggibile: {err}"))

        self.wallet_balance = balance
        self.wallet_before = balance
        self.wallet_id_resolved = wid or self.cfg.wallet_id
        low = balance <= self.cfg.wallet_low_threshold

        if low:
            log.warning("Saldo wallet basso: %d coin (soglia %d)",
                        balance, self.cfg.wallet_low_threshold)

        self._add(p.mark_ok(None, wallet_id=self.wallet_id_resolved,
                            balance=balance, balance_low=low,
                            low_threshold=self.cfg.wallet_low_threshold))
        self._maybe_recharge(self.wallet_id_resolved, balance)
        return p

    def _wallet_state_path(self) -> str:
        if self.cfg.wallet_state_file:
            return self.cfg.wallet_state_file
        return os.path.join(self.cfg.log_dir, ".insula_processing_wallet.json")

    def _maybe_recharge(self, wallet_id, balance):
        """Ricarica se abilitata, sotto soglia e entro i tetti giornalieri."""
        if not self.cfg.wallet_autorecharge or balance > self.cfg.wallet_min_balance:
            return
        if not wallet_id:
            log.error("Ricarica impossibile: wallet_id non risolto. "
                      "Valorizzare [INSULA_WALLET] WALLET_ID.")
            return

        today = utc_now().strftime("%Y-%m-%d")
        try:
            with open(self._wallet_state_path(), encoding="utf-8") as f:
                st = json.load(f)
        except (OSError, ValueError):
            st = {}
        if st.get("day") != today:
            st = {"day": today, "recharges": 0, "amount": 0}

        if (self.cfg.wallet_max_per_day is not None
                and st["recharges"] >= self.cfg.wallet_max_per_day):
            log.error("Ricarica NON eseguita: raggiunto il tetto di %d ricariche "
                      "giornaliere. Saldo %d: verificare il consumo reale.",
                      self.cfg.wallet_max_per_day, balance)
            self.wallet_capped = True
            return

        amount = self.cfg.wallet_recharge_amount
        if (self.cfg.wallet_max_amount_per_day is not None
                and st["amount"] + amount > self.cfg.wallet_max_amount_per_day):
            log.error("Ricarica NON eseguita: supererebbe il tetto di %d coin "
                      "giornalieri (gia' ricaricati %d).",
                      self.cfg.wallet_max_amount_per_day, st["amount"])
            self.wallet_capped = True
            return

        headers = dict(self.auth.auth_header())
        headers["Content-Type"] = "application/json"
        res = self.http.request("POST", self._api(f"/wallets/{wallet_id}/credit"),
                                headers=headers,
                                data=json.dumps({"amount": int(amount)}).encode())
        if not res.ok:
            log.error("Ricarica fallita: %s", http_error_text(res, self.cfg.secrets))
            self.wallet_recharge_result = {
                "ok": False, "amount": amount, "wallet_id": wallet_id,
                "error": http_error_text(res, self.cfg.secrets),
                "balance_before": balance, "balance_after": None}
            return

        _, new_balance, _ = self._wallet_get()
        st["recharges"] += 1
        st["amount"] += amount
        try:
            with open(self._wallet_state_path(), "w", encoding="utf-8") as f:
                json.dump(st, f, ensure_ascii=False, indent=2)
        except OSError as e:
            log.warning("Stato wallet non scritto: %s", e)

        log.info("Ricarica eseguita: %d coin. Saldo %s -> %s (%d ricariche oggi).",
                 amount, balance, new_balance, st["recharges"])
        self.wallet_balance = new_balance if new_balance is not None else balance
        self.wallet_before = self.wallet_balance
        self.wallet_recharge_result = {
            "ok": True, "amount": amount, "wallet_id": wallet_id,
            "balance_before": balance, "balance_after": new_balance,
            "recharges_today": st["recharges"], "amount_today": st["amount"]}

    # ========================================================================
    # LIVELLO 2: esecuzione reale
    # ========================================================================

    def _should_launch(self):
        """
        Decide se lanciare in questo run.

        Il cron puo' girare piu' spesso della cadenza di lancio desiderata: le
        sonde di raggiungibilita' sono gratuite e conviene eseguirle spesso,
        mentre il lancio costa. JOB_LAUNCH_EVERY_MINUTES governa la cadenza
        reale indipendentemente da quella del cron.
        """
        if self.args.no_launch:
            return False, "disattivato da --no-launch"
        if not self.cfg.launch_enabled:
            return False, "JOB_LAUNCH_ENABLED = False"
        if not self.cfg.launch_config_id:
            return False, "JOB_LAUNCH_CONFIG_ID non valorizzato"
        if self.args.force_launch:
            return True, None

        last = parse_dt((self.state.get("last_launch") or {}).get("at"))
        if last:
            elapsed = (utc_now() - last).total_seconds() / 60.0
            if elapsed < self.cfg.launch_every_minutes:
                return False, (f"ultimo lancio {elapsed:.0f} minuti fa, cadenza "
                               f"{self.cfg.launch_every_minutes} minuti")
        return True, None

    def resume_pending_job(self):
        """
        Se il run precedente ha lasciato un job non concluso (timeout del
        polling), lo si riprende prima di lanciarne un altro. Evita di
        accumulare job orfani e recupera la misurazione invece di perderla.
        """
        pending = self.state.get("pending_job") or {}
        jid = pending.get("job_id")
        if not jid:
            return False

        log.info("Job pendente dal run precedente: %s. Ne verifico l'esito.", jid)
        r = self.http.get(self._api(f"/jobs/{jid}",
                                    {"projection": "detailedJob"}),
                          headers=self.auth.auth_header())
        if not r.ok:
            log.warning("Job pendente %s non leggibile: %s. Lo abbandono.",
                        jid, http_error_text(r, self.cfg.secrets))
            self.state.pop("pending_job", None)
            self._state_write()
            return False

        job = r.json() or {}
        status = job.get("status")
        if status in JOB_RUNNING:
            log.info("Job %s ancora in %s: nessun nuovo lancio in questo run.",
                     jid, status)
            self.job_id = jid
            self.job_status = status
            return True

        # concluso: si registra l'esito e si pulisce
        self.job_id = jid
        self.job_ext_id = job.get("extId")
        self._record_execution(job, launched_now=False)
        self.state.pop("pending_job", None)
        self._state_write()
        self._cleanup(job)
        return True

    def probe_launch(self) -> Probe:
        """
        POST /jobConfigs/{id}/launch
        E' l'operazione tariffata: il motore accetta il job e lo mette in coda.
        """
        cfg_id = self.cfg.launch_config_id
        url = self._api(f"/jobConfigs/{cfg_id}/launch")
        p = Probe("job_launch", CAT_EXECUTION, url,
                  "Lancio del job di monitoraggio (operazione tariffata)")

        should, reason = self._should_launch()
        if not should:
            self.launch_skipped_reason = reason
            return self._add(p.mark_skipped(reason))

        headers = dict(self.auth.auth_header())
        headers["Content-Type"] = "application/json"
        res = self.http.request("POST", url, headers=headers, data=b"")

        if not res.ok:
            klass = classify_error(res, self.cfg.secrets)
            msg = http_error_text(res, self.cfg.secrets)
            if klass == "entitlement":
                msg = (f"{msg} -> il servizio risponde ma nega il lancio per "
                       f"credito esaurito, non per indisponibilita'. "
                       f"Ricaricare il wallet o alzare i tetti.")
                if not self.cfg.billing_errors_as_nok:
                    # Un wallet vuoto e' un problema amministrativo: non deve
                    # far scendere un KPI contrattuale di disponibilita'.
                    p.error = msg
                    return self._add(p.mark_ok(
                        res, error_class=klass, billing_blocked=True,
                        downgraded_note="BILLING_ERRORS_AS_NOK = False: "
                                        "non conteggiato come indisponibilita'"))
            return self._add(p.mark_nok(msg, res, error_class=klass,
                                        billing_blocked=(klass == "entitlement"),
                                        response_body=body_snippet(res, self.cfg.secrets)))

        data = res.json() or {}
        self.launch_performed = True
        self.job_id = data.get("id")
        self.job_ext_id = data.get("extId")

        # Il manuale documenta solo 'extId'. Sull'API viva arriva anche 'id',
        # ma il fallback resta: se un giorno sparisse, si ritrova il job per
        # extId usando l'unico ordinamento affidabile.
        id_source = "response"
        if not self.job_id and self.job_ext_id:
            id_source = "ricerca per extId"
            rr = self.http.get(
                self._api("/jobs/search/parametricFind",
                          {"projection": "detailedJob", "size": 10, "page": 0,
                           "sort": "id,desc"}),
                headers=self.auth.auth_header())
            for j in self._embedded(rr.json() or {}, "jobs"):
                if j.get("extId") == self.job_ext_id:
                    self.job_id = j.get("id")
                    break

        # La response del launch espone costQuotation, che GET /services/{id}
        # non restituisce (li' costingExpression e' a null). E' la fonte piu'
        # attendibile del costo dichiarato: se il service_check non l'ha
        # trovato, si prende da qui.
        quote = data.get("costQuotation") or {}
        if self.cost_declared is None and quote.get("cost") is not None:
            self.cost_declared = quote.get("cost")
        self.cost_recurrence = quote.get("recurrence")
        self.state["last_launch"] = {"at": iso_ms(utc_now()),
                                     "job_id": self.job_id,
                                     "ext_id": self.job_ext_id,
                                     "run_id": self.run_id}
        if self.job_id:
            self.state["pending_job"] = {"job_id": self.job_id,
                                         "ext_id": self.job_ext_id,
                                         "at": iso_ms(utc_now())}
        self._state_write()

        if not self.job_id:
            return self._add(p.mark_nok(
                "job lanciato ma id non ricavabile: impossibile seguirne l'esito",
                res, ext_id=self.job_ext_id))

        return self._add(p.mark_ok(
            res, job_id=self.job_id, ext_id=self.job_ext_id,
            id_source=id_source,
            initial_status=data.get("status"),
            queue_position=data.get("queuePosition"),
            cost_quotation=quote.get("cost"),
            cost_recurrence=quote.get("recurrence")))

    def probe_execution(self) -> Probe:
        """
        Polling su GET /jobs/{id} fino a uno stato terminale.

        E' la sonda che dimostra davvero la disponibilita' del processing: non
        basta che il motore accetti il job, deve portarlo a COMPLETED.
        """
        p = Probe("job_execution", CAT_EXECUTION,
                  self._api(f"/jobs/{self.job_id}") if self.job_id else None,
                  "Esecuzione del job fino a uno stato terminale")

        if not self.launch_performed:
            return self._add(p.mark_skipped(
                self.launch_skipped_reason or "nessun lancio in questo run"))
        if not self.job_id:
            return self._add(p.mark_nok("job non identificabile"))

        deadline = time.perf_counter() + self.cfg.max_wait_minutes * 60
        t0 = time.perf_counter()
        job, status = {}, None
        polls = 0

        while time.perf_counter() < deadline:
            time.sleep(self.cfg.poll_interval_s)
            polls += 1
            # il polling puo' superare la vita del token (300 s)
            self.auth.ensure_fresh()
            r = self.http.get(self._api(f"/jobs/{self.job_id}",
                                        {"projection": "detailedJob"}),
                              headers=self.auth.auth_header())
            if not r.ok:
                log.debug("Poll %d fallito: %s", polls,
                          http_error_text(r, self.cfg.secrets))
                continue
            job = r.json() or {}
            status = job.get("status")
            log.debug("Poll %d: status=%s phase=%s stage=%s",
                      polls, status, job.get("phase"), job.get("stage"))
            if status in JOB_TERMINAL:
                break

        elapsed = round(time.perf_counter() - t0, 1)

        if status not in JOB_TERMINAL:
            # Il job resta in stato di esecuzione: si lascia in sospeso e lo
            # si riprende al run successivo invece di dichiararlo fallito.
            log.warning("Job %s non concluso entro %d minuti (status=%s): "
                        "ripreso al prossimo run.",
                        self.job_id, self.cfg.max_wait_minutes, status)
            return self._add(p.mark_nok(
                f"timeout: il job e' ancora in '{status}' dopo "
                f"{self.cfg.max_wait_minutes} minuti",
                None, job_id=self.job_id, last_status=status,
                phase=job.get("phase"), stage=job.get("stage"),
                queue_position=job.get("queuePosition"),
                polls=polls, waited_s=elapsed, timed_out=True))

        self._record_execution(job, launched_now=True, polls=polls,
                               waited_s=elapsed)
        return self.probes[-1]

    def _record_execution(self, job, launched_now, polls=None, waited_s=None):
        """
        Registra l'esito dell'esecuzione. Chiamato sia dal polling di questo
        run sia dal recupero di un job rimasto pendente.
        """
        status = job.get("status")
        self.job_status = status
        self.job_outputs = [o for o in (job.get("outputFiles") or [])
                            if isinstance(o, dict)]

        # Il job ha raggiunto uno stato terminale: non e' piu' pendente. Va
        # tolto dallo stato PRIMA della pulizia, altrimenti il run successivo
        # cerca un job che abbiamo appena cancellato e logga un 404 fasullo.
        if self.state.pop("pending_job", None) is not None:
            self._state_write()

        created = parse_dt(job.get("created"))
        start = parse_dt(job.get("startTime"))
        end = parse_dt(job.get("endTime"))
        if start and end:
            self.job_duration_s = round((end - start).total_seconds(), 1)
        if created and start:
            self.job_queue_wait_s = round((start - created).total_seconds(), 1)

        p = Probe("job_execution", CAT_EXECUTION,
                  self._api(f"/jobs/{self.job_id}"),
                  "Esecuzione del job fino a uno stato terminale")
        details = {
            "job_id": self.job_id, "ext_id": job.get("extId"),
            "final_status": status, "phase": job.get("phase"),
            "stage": job.get("stage"),
            "service_name": job.get("serviceName"),
            "duration_s": self.job_duration_s,
            "queue_wait_s": self.job_queue_wait_s,
            "n_outputs": len(self.job_outputs),
            "resumed": not launched_now,
        }
        if polls is not None:
            details["polls"] = polls
        if waited_s is not None:
            details["waited_s"] = waited_s

        if status != "COMPLETED":
            self._add(p.mark_nok(
                f"il job e' terminato in stato '{status}'", None, **details))
            return

        if self.cfg.expect_outputs and not self.job_outputs:
            self._add(p.mark_nok(
                "il job risulta COMPLETED ma non ha prodotto output "
                "(JOB_EXPECT_OUTPUTS = True)", None, **details))
            return

        if (self.job_duration_s is not None
                and self.job_duration_s > self.cfg.duration_warn_s):
            log.warning("Job %s concluso in %s, oltre la soglia di %s: "
                        "possibile degrado del processing.",
                        self.job_id, human_duration(self.job_duration_s),
                        human_duration(self.cfg.duration_warn_s))
            details["duration_over_threshold"] = True

        self._add(p.mark_ok(None, **details))

    # ========================================================================
    # PULIZIA -- manutenzione, NON concorre al KPI
    # ========================================================================

    def _cleanup(self, job=None):
        """
        Cancella prima gli output, poi il record del job.

        ORDINE: si esegue DOPO che l'esito e' stato registrato nelle sonde.
        Dopo la DELETE l'unica prova della misurazione sono gli eventi.

        Gli esiti sono emessi come sonde di categoria 'cleanup', che NON entra
        nel calcolo del KPI: una pulizia fallita e' un problema di manutenzione
        (si accumulano file), non di disponibilita' del servizio.
        """
        outs = self.job_outputs or [o for o in ((job or {}).get("outputFiles") or [])
                                    if isinstance(o, dict)]

        # --- output ----------------------------------------------------------
        p = Probe("cleanup_outputs", CAT_CLEANUP, None,
                  "Cancellazione dei file prodotti dal job")
        if not self.cfg.cleanup_outputs:
            self._add(p.mark_skipped("JOB_CLEANUP_OUTPUTS = False"))
        elif not outs:
            self._add(p.mark_skipped("nessun output da cancellare"))
        else:
            deleted, failed = 0, []
            for o in outs:
                oid = o.get("id")
                if not oid:
                    continue
                r = self.http.delete(self._api(f"/platformFiles/{oid}"),
                                     headers=self.auth.auth_header())
                if r.ok or r.status == 204:
                    deleted += 1
                else:
                    failed.append({"id": oid,
                                   "error": http_error_text(r, self.cfg.secrets)})
            if failed:
                self._add(p.mark_nok(
                    f"{len(failed)} file su {len(outs)} non cancellati: "
                    f"si accumuleranno nella collection di output",
                    None, n_deleted=deleted, n_failed=len(failed),
                    failures=failed[:5]))
            else:
                self._add(p.mark_ok(None, n_deleted=deleted))

        # --- record del job ---------------------------------------------------
        # DELETE /jobs/{id} non e' documentato nel manuale ma funziona (204,
        # verificato in avanscoperta). Senza, ogni lancio lascerebbe un record
        # permanente: con cadenza semioraria sono ~17500 record l'anno.
        p2 = Probe("cleanup_job", CAT_CLEANUP,
                   self._api(f"/jobs/{self.job_id}") if self.job_id else None,
                   "Cancellazione del record del job (endpoint non documentato)")
        if not self.cfg.cleanup_job:
            self._add(p2.mark_skipped("JOB_CLEANUP_JOB = False"))
        elif not self.job_id:
            self._add(p2.mark_skipped("nessun job da cancellare"))
        else:
            r = self.http.delete(self._api(f"/jobs/{self.job_id}"),
                                 headers=self.auth.auth_header())
            if r.ok or r.status == 204:
                self._add(p2.mark_ok(r, job_id=self.job_id))
                log.info("Job %s cancellato: nessun residuo sulla piattaforma.",
                         self.job_id)
            else:
                self._add(p2.mark_nok(
                    f"{http_error_text(r, self.cfg.secrets)} -> il record del "
                    f"job resta sulla piattaforma", r, job_id=self.job_id))

    def _measure_cost(self):
        """
        Costo reale del lancio: differenza del saldo prima/dopo.
        Il campo costingExpression dichiara 1 coin, ma un lancio reale ne ha
        scalati 2: il valore dichiarato non e' affidabile e va misurato.
        """
        if not self.launch_performed or self.wallet_before is None:
            return
        _, after, _ = self._wallet_get()
        if after is None:
            return
        self.wallet_balance = after
        self.cost_measured = self.wallet_before - after
        log.info("Costo misurato del lancio: %s coin (saldo %s -> %s, "
                 "dichiarato: %s)",
                 self.cost_measured, self.wallet_before, after,
                 self.cost_declared)

    # ========================================================================
    # ORCHESTRAZIONE
    # ========================================================================

    def run(self):
        started = utc_now()
        t0 = time.perf_counter()

        log.info("=" * 74)
        log.info("KPI availability processing services  v%s  run_id=%s",
                 SCRIPT_VERSION, self.run_id)
        log.info("Config          : %s", os.path.abspath(self.cfg.path))
        log.info("Endpoint Insula : %s", self.cfg.insula_base)
        log.info("Utenza          : %s (realm %s)",
                 self.cfg.kc_username, self.cfg.kc_realm)
        if self.cfg.launch_enabled:
            log.info("Lancio          : jobConfig %s, service %s, cadenza %d min, "
                     "attesa max %d min",
                     self.cfg.launch_config_id, self.cfg.launch_service_id or "n/d",
                     self.cfg.launch_every_minutes, self.cfg.max_wait_minutes)
            log.info("Pulizia         : output=%s  record job=%s",
                     self.cfg.cleanup_outputs, self.cfg.cleanup_job)
        else:
            log.info("Lancio          : DISATTIVATO, il KPI misura la sola "
                     "raggiungibilita' (livello 1)")
        log.info("=" * 74)

        auth_probe = self.probe_auth()

        if auth_probe.status != STATUS_OK:
            log.error("Autenticazione fallita: le sonde successive sono NOK "
                      "per dipendenza.")
            for name, cat, desc in (
                ("jobs_search", CAT_REACHABILITY, "Ricerca job"),
                ("jobconfigs_list", CAT_REACHABILITY, "Elenco jobConfig"),
                ("service_check", CAT_REACHABILITY, "Verifica del processore"),
                ("job_launch", CAT_EXECUTION, "Lancio del job"),
                ("job_execution", CAT_EXECUTION, "Esecuzione del job"),
            ):
                self._add(Probe(name, cat, None, desc).mark_nok(
                    "non eseguita: autenticazione Keycloak fallita"))
        else:
            # --- livello 1: gratuito, sempre --------------------------------
            self.probe_jobs_search()
            self.probe_jobconfigs()
            self.probe_service()
            self.probe_wallet()

            # --- livello 2: tariffato ---------------------------------------
            resumed = self.resume_pending_job()
            if resumed and self.job_status in JOB_RUNNING:
                # job precedente ancora in corso: non se ne lancia un altro
                self._add(Probe("job_launch", CAT_EXECUTION, None,
                                "Lancio del job").mark_skipped(
                    f"job {self.job_id} del run precedente ancora in "
                    f"'{self.job_status}'"))
                self._add(Probe("job_execution", CAT_EXECUTION, None,
                                "Esecuzione del job").mark_skipped(
                    "in attesa della conclusione del job precedente"))
            elif not resumed:
                launch = self.probe_launch()
                if launch.status == STATUS_OK and not launch.skipped:
                    self.probe_execution()
                    self._cleanup()
                    self._measure_cost()
                else:
                    self._add(Probe("job_execution", CAT_EXECUTION, None,
                                    "Esecuzione del job").mark_skipped(
                        launch.details.get("skip_reason")
                        or "lancio non riuscito"))

        elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
        return self.build_events(started, utc_now(), elapsed_ms)

    # -- costruzione eventi --------------------------------------------------

    def _envelope(self, ts, event_type, started, ended):
        _, weekday, hour_local = local_rome(ts)
        return {
            "@timestamp": iso_ms(ts),
            "event_timestamp": iso_ms(ts),
            "platform": PLATFORM_TAG,
            "service_provider_log": SERVICE_PROVIDER_LOG,
            "event_type": event_type,
            "hostname": self.hostname,
            "endpoint": self.cfg.insula_base,
            "kpi": KPI_NAME,
            "collector_version": SCRIPT_VERSION,
            "run_id": self.run_id,
            "window_start": iso_ms(started),
            "window_end": iso_ms(ended),
            # Contesto temporale per i filtri NWD/NWH in Grafana. La raccolta
            # resta h24: questi campi ETICHETTANO soltanto, non filtrano.
            "weekday": weekday,
            "hour_local": hour_local,
            "hour_utc": ts.astimezone(timezone.utc).hour,
            "is_nwd": weekday in NWD_DAYS,
            "is_nwh": is_business_window(ts),
        }

    def build_events(self, started, ended, elapsed_ms):
        warn, crit = self.cfg.latency_warn_ms, self.cfg.latency_crit_ms
        events = []

        if self.cfg.emit_individual_probes:
            for p in self.probes:
                doc = self._envelope(ended, EVENT_PROBE, started, ended)
                doc.update({
                    "probe": p.name,
                    "probe_category": p.category,
                    "probe_description": p.description,
                    "probe_url": p.url,
                    "status": p.status,
                    "status_code_num": 1 if p.status == STATUS_OK else 0,
                    "skipped": p.skipped,
                    "http_status": p.http_status,
                    "latency_ms": p.latency_ms,
                    "latency_status": p.latency_status(warn, crit),
                    "attempts": p.attempts,
                    "response_bytes": p.bytes,
                    "error": p.error,
                    "details": p.details or None,
                })
                events.append(doc)

        executed = [p for p in self.probes if not p.skipped]
        by_cat = {}
        for p in self.probes:
            by_cat.setdefault(p.category, []).append(p)

        def cat_status(*cats):
            pool = [p for c in cats for p in by_cat.get(c, []) if not p.skipped]
            if not pool:
                return None
            return STATUS_OK if all(p.status == STATUS_OK for p in pool) else STATUS_NOK

        reachability_status = cat_status(CAT_AUTH, CAT_REACHABILITY) or STATUS_NOK
        execution_status = cat_status(CAT_EXECUTION)
        cleanup_status = cat_status(CAT_CLEANUP)

        # --- regola del KPI ---------------------------------------------------
        # Il livello 2 e' la misura piu' fedele del requisito: se e' stato
        # eseguito, comanda lui (insieme alla raggiungibilita'). Se non e'
        # stato eseguito -- lancio disattivato, cadenza non ancora scaduta,
        # job precedente in corso -- il KPI ricade sul livello 1 e lo dichiara,
        # cosi' in Grafana si puo' distinguere una misura forte da una debole.
        # La pulizia NON entra: e' manutenzione, non disponibilita'.
        if execution_status is not None:
            overall = (STATUS_OK if (reachability_status == STATUS_OK
                                     and execution_status == STATUS_OK)
                       else STATUS_NOK)
            kpi_level = "execution"
        else:
            overall = reachability_status
            kpi_level = "reachability"

        n_ok = sum(1 for p in executed if p.status == STATUS_OK)
        failed = [p.name for p in executed if p.status != STATUS_OK]

        def lat(name):
            for p in self.probes:
                if p.name == name:
                    return p.latency_ms
            return None

        def det(name, key):
            for p in self.probes:
                if p.name == name:
                    return p.details.get(key)
            return None

        latencies = [p.latency_ms for p in executed if p.latency_ms is not None]

        agg = self._envelope(ended, EVENT_AVAILABILITY, started, ended)
        agg.update({
            # --- esito del KPI ------------------------------------------------
            "overall_status": overall,
            "overall_available": overall == STATUS_OK,
            "overall_up": 1 if overall == STATUS_OK else 0,
            "availability_pct": 100.0 if overall == STATUS_OK else 0.0,
            "kpi_level": kpi_level,

            "reachability_status": reachability_status,
            "reachability_up": 1 if reachability_status == STATUS_OK else 0,
            "execution_status": execution_status,
            "execution_up": (None if execution_status is None
                             else (1 if execution_status == STATUS_OK else 0)),
            "cleanup_status": cleanup_status,

            # --- livello 1 -----------------------------------------------------
            "jobs_total": det("jobs_search", "total_elements"),
            "jobs_search_latency_ms": lat("jobs_search"),
            "jobconfigs_total": det("jobconfigs_list", "total_elements"),
            "jobconfigs_latency_ms": lat("jobconfigs_list"),
            "auth_latency_ms": lat("auth"),
            "service_id": self.cfg.launch_service_id or None,
            "service_name": det("service_check", "service_name"),
            "service_status": self.service_status_seen,
            "docker_tag": self.docker_tag_seen,
            "docker_tag_expected": self.cfg.expected_docker_tag or None,
            "docker_tag_changed": self.docker_tag_changed,

            # --- livello 2 -----------------------------------------------------
            "launch_enabled": self.cfg.launch_enabled,
            "launch_performed": self.launch_performed,
            "launch_skipped_reason": self.launch_skipped_reason,
            "launch_latency_ms": lat("job_launch"),
            "job_config_id": self.cfg.launch_config_id or None,
            "job_id": self.job_id,
            "job_ext_id": self.job_ext_id,
            "job_status": self.job_status,
            "job_duration_s": self.job_duration_s,
            "job_queue_wait_s": self.job_queue_wait_s,
            "job_timed_out": bool(det("job_execution", "timed_out")),
            "job_resumed": bool(det("job_execution", "resumed")),
            "n_outputs": len(self.job_outputs),

            # --- costi ---------------------------------------------------------
            "cost_declared": self.cost_declared,
            "cost_recurrence": self.cost_recurrence,
            "cost_measured": self.cost_measured,
            "wallet_id": self.wallet_id_resolved,
            "wallet_balance": self.wallet_balance,
            "wallet_balance_low": (None if self.wallet_balance is None
                                   else self.wallet_balance
                                   <= self.cfg.wallet_low_threshold),
            "wallet_recharged": bool(self.wallet_recharge_result
                                     and self.wallet_recharge_result.get("ok")),
            "wallet_cap_reached": self.wallet_capped,
            "billing_blocked": bool(det("job_launch", "billing_blocked")),

            # --- pulizia -------------------------------------------------------
            "outputs_deleted": det("cleanup_outputs", "n_deleted") or 0,
            # 0 e non null: un campo nullo in Grafana non si somma e il
            # pannello sui fallimenti di pulizia resterebbe vuoto anche
            # quando i dati ci sono.
            "outputs_delete_failed": det("cleanup_outputs", "n_failed") or 0,
            "job_record_deleted": (det("cleanup_job", "job_id") is not None
                                   and cat_status(CAT_CLEANUP) == STATUS_OK),

            # --- riepilogo ------------------------------------------------------
            "total_elapsed_ms": elapsed_ms,
            "max_latency_ms": max(latencies) if latencies else None,
            "n_probes": len(executed),
            "n_probes_ok": n_ok,
            "n_probes_nok": len(executed) - n_ok,
            "failed_probes": failed or None,
            "probe_status": {p.name: p.status for p in self.probes},
            "latency_status": ("CRITICAL" if any((p.latency_ms or 0) >= crit
                                                 for p in executed)
                               else "WARNING" if any((p.latency_ms or 0) >= warn
                                                     for p in executed)
                               else "NORMAL"),
        })

        if self.wallet_recharge_result:
            rec = self._envelope(ended, EVENT_RECHARGE, started, ended)
            rec.update(self.wallet_recharge_result)
            rec["wallet_autorecharge"] = True
            events.append(rec)

        events.append(agg)

        log.info("-" * 74)
        log.info("ESITO KPI  overall=%s  (livello: %s)  raggiungibilita'=%s  "
                 "esecuzione=%s  (%d/%d sonde OK)",
                 overall, kpi_level, reachability_status,
                 execution_status or "non misurata", n_ok, len(executed))
        if self.job_id:
            log.info("Job %s: status=%s  durata=%s  coda=%s  output=%d",
                     self.job_id, self.job_status,
                     human_duration(self.job_duration_s),
                     human_duration(self.job_queue_wait_s), len(self.job_outputs))
        if self.cost_measured is not None:
            log.info("Costo del run: misurato %s coin, dichiarato %s "
                     "(ricorrenza %s), saldo residuo %s",
                     self.cost_measured, self.cost_declared,
                     self.cost_recurrence or "n/d", self.wallet_balance)
            if (self.cost_declared is not None
                    and self.cost_measured != self.cost_declared):
                log.warning("Costo misurato (%s) diverso da quello dichiarato "
                            "(%s). Possibili cause: tariffazione a scatti "
                            "'%s', oppure un altro collector ha consumato dal "
                            "medesimo wallet durante questo run.",
                            self.cost_measured, self.cost_declared,
                            self.cost_recurrence or "n/d")
        if self.docker_tag_changed:
            log.warning("ATTENZIONE: il dockerTag del processore e' cambiato.")
        if failed:
            log.warning("Sonde fallite: %s", ", ".join(failed))
        log.info("-" * 74)

        return events

    # -- modalita' diagnostica ------------------------------------------------

    def show_status(self) -> int:
        """Stato corrente senza lanciare nulla: cosa farebbe il prossimo run."""
        print("\n" + "=" * 74)
        print(" STATO DEL COLLECTOR PROCESSING")
        print("=" * 74)

        token, res = self.auth.fetch_token(force=True)
        if not token:
            print(f"\n[FATAL] {http_error_text(res, self.cfg.secrets)}")
            return 2

        print(f"\n  Utenza        : {self.cfg.kc_username}")
        print(f"  jobConfig     : {self.cfg.launch_config_id or 'non configurata'}")
        print(f"  Service       : {self.cfg.launch_service_id or 'n/d'}")
        print(f"  Lancio        : {'attivo' if self.cfg.launch_enabled else 'DISATTIVO'}"
              f"  ogni {self.cfg.launch_every_minutes} min")
        print(f"  Pulizia       : output={self.cfg.cleanup_outputs}  "
              f"job={self.cfg.cleanup_job}")

        _, bal, _ = self._wallet_get()
        print(f"  Wallet        : {bal} coin")

        last = self.state.get("last_launch") or {}
        if last.get("at"):
            dt = parse_dt(last["at"])
            age = (utc_now() - dt).total_seconds() / 60.0 if dt else None
            print(f"  Ultimo lancio : {last['at']}  ({age:.0f} min fa)  "
                  f"job {last.get('job_id')}")
        else:
            print("  Ultimo lancio : mai")

        pending = self.state.get("pending_job") or {}
        if pending.get("job_id"):
            print(f"  Job pendente  : {pending['job_id']} dal {pending.get('at')}")
        else:
            print("  Job pendente  : nessuno")

        should, reason = self._should_launch()
        print(f"\n  Prossimo run  : {'LANCERA un job' if should else 'nessun lancio'}"
              f"{'' if should else f' ({reason})'}")

        if self.cfg.launch_service_id:
            r = self.http.get(self._api(f"/services/{self.cfg.launch_service_id}"),
                              headers=self.auth.auth_header())
            if r.ok:
                svc = r.json() or {}
                tag = svc.get("dockerTag")
                flag = ""
                if self.cfg.expected_docker_tag and tag != self.cfg.expected_docker_tag:
                    flag = f"   <-- CAMBIATO (atteso {self.cfg.expected_docker_tag})"
                print(f"\n  Service       : {svc.get('name')} [{svc.get('status')}]")
                print(f"  dockerTag     : {tag}{flag}")

        print("\n" + "=" * 74 + "\n")
        return 0


# ============================================================================
# OUTPUT
# ============================================================================

def write_json_log(events, log_dir: str) -> str:
    path = os.path.join(log_dir, f"{SCRIPT_NAME}.log")
    with open(path, "a", encoding="utf-8") as f:
        for doc in events:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")
    log.info("Scritti %d eventi in %s", len(events), path)
    return path


def push_to_elastic(events, cfg: Config) -> bool:
    if not cfg.elastic_enabled:
        log.info("ELASTIC_ENABLED = False: nessuna spedizione su Elasticsearch.")
        return True
    if not events:
        return True
    if not cfg.monitoring_apikey or cfg.monitoring_apikey.startswith("<"):
        log.error("MONITORING_APIKEY non valorizzata: spedizione annullata.")
        return False

    lines = []
    for doc in events:
        lines.append(json.dumps({"create": {}}, ensure_ascii=False))
        lines.append(json.dumps(doc, ensure_ascii=False))
    payload = ("\n".join(lines) + "\n").encode("utf-8")

    http = HttpClient(timeout=cfg.http_timeout, retries=cfg.retries,
                      backoff=cfg.retry_backoff_s,
                      verify_certs=cfg.monitoring_verify_certs,
                      use_system_proxy=cfg.use_system_proxy, secrets=cfg.secrets)
    res = http.request(
        "POST", f"{cfg.monitoring_url}/{cfg.kpi_index}/_bulk", data=payload,
        headers={"Content-Type": "application/x-ndjson",
                 "Authorization": f"ApiKey {cfg.monitoring_apikey}"})

    if not res.ok:
        log.error("Bulk Elasticsearch fallito: %s",
                  redact(res.error or f"HTTP {res.status}", cfg.secrets))
        return False

    body = res.json() or {}
    if body.get("errors"):
        n_err = sum(1 for it in body.get("items", [])
                    if (it.get("create") or {}).get("error"))
        first = next((json.dumps((it.get("create") or {}).get("error"))
                      for it in body.get("items", [])
                      if (it.get("create") or {}).get("error")), "n/d")
        log.error("Bulk parzialmente fallito: %d documenti in errore. Primo: %s",
                  n_err, first[:500])
        return False

    log.info("Indicizzati %d documenti su %s (%.0f ms)",
             len(events), cfg.kpi_index, res.latency_ms or 0)
    return True


# ============================================================================
# MAIN
# ============================================================================

def parse_args():
    ap = argparse.ArgumentParser(
        description="KPI Availability of Processing services (Insula IRIDE). "
                    "Sonde di raggiungibilita' gratuite piu' lancio, verifica "
                    "e pulizia di un job di monitoraggio.")
    ap.add_argument("-c", "--config", default=DEFAULT_CONFIG,
                    help=f"Percorso del file ini (default: {DEFAULT_CONFIG})")
    ap.add_argument("-n", "--dry-run", action="store_true",
                    help="Esegue le sonde ma non spedisce su Elasticsearch")
    ap.add_argument("-v", "--verbose", action="store_true",
                    help="Log di debug anche su console")
    ap.add_argument("--print-events", action="store_true",
                    help="Stampa gli eventi JSON su stdout")
    ap.add_argument("--no-launch", action="store_true",
                    help="Esegue solo il livello 1 (raggiungibilita'): nessun "
                         "job lanciato, nessun coin consumato")
    ap.add_argument("--force-launch", action="store_true",
                    help="Lancia ignorando JOB_LAUNCH_EVERY_MINUTES")
    ap.add_argument("--status", action="store_true",
                    help="Mostra lo stato corrente e cosa farebbe il prossimo "
                         "run, senza lanciare nulla")
    ap.add_argument("-V", "--version", action="version",
                    version=f"{SCRIPT_NAME} {SCRIPT_VERSION}")
    return ap.parse_args()


def main() -> int:
    args = parse_args()

    try:
        cfg = Config(args.config)
    except (FileNotFoundError, ValueError) as e:
        print(f"[FATAL] {e}", file=sys.stderr)
        return 2

    setup_logging(cfg.log_dir, args.verbose)

    if args.dry_run:
        cfg.elastic_enabled = False
        log.info("DRY-RUN attivo: nessuna scrittura su Elasticsearch.")

    lock = RunLock(cfg.lock_file or os.path.join(cfg.log_dir,
                                                 ".insula_processing.lock"),
                   cfg.lock_stale_minutes)

    collector = ProcessingKpiCollector(cfg, args)

    if args.status:
        try:
            return collector.show_status()
        except Exception as e:
            log.exception("Stato non recuperabile: %s", redact(str(e), cfg.secrets))
            return 3

    # Con cron a intervalli brevi un run lento puo' accavallarsi al successivo.
    # Uscire con 0 e' corretto: non e' un errore, e' il sistema che funziona.
    if not lock.acquire():
        return 0

    try:
        try:
            events = collector.run()
        except Exception as e:
            # Non deve mai morire in cron senza lasciare traccia.
            log.exception("Errore non gestito: %s", redact(str(e), cfg.secrets))
            return 3

        write_json_log(events, cfg.log_dir)

        if args.print_events:
            for doc in events:
                print(json.dumps(doc, ensure_ascii=False))

        shipped = push_to_elastic(events, cfg)

        agg = events[-1]
        exit_code = 0 if agg.get("overall_status") == STATUS_OK else 1
        if not shipped:
            exit_code = max(exit_code, 4)

        log.info("Uscita con codice %d "
                 "(0=OK, 1=KPI NOK, 4=errore spedizione Elastic)", exit_code)
        return exit_code
    finally:
        lock.release()


if __name__ == "__main__":
    sys.exit(main())

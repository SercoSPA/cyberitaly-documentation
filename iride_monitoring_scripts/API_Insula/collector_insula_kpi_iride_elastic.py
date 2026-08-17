#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
collector_insula_kpi_iride_elastic.py
=====================================

KPI: "Availability of data catalogue and access services" - Insula (IRIDE CyberItaly).

Il collector esegue una sonda sintetica end-to-end sulle API Insula v2.0
(https://cgi-italy.github.io/insula/apis/cyberitaly/) e produce eventi JSON
(un documento per riga) allineati allo schema gia' in uso per
`collector_insula_jobs_iride_elastic.py`, spediti opzionalmente su Elasticsearch
via _bulk.

SONDE ESEGUITE (in ordine, ognuna con esito OK/NOK e latenza in ms)
------------------------------------------------------------------
  1. auth               POST {KEYCLOAK}/realms/{realm}/protocol/openid-connect/token
                        -> prerequisito: se fallisce, tutte le altre sono NOK.
  2. catalogue_params   GET  /search/parameters?resolveAll=false
                        -> "il servizio catalogo risponde" (handshake leggero,
                           non richiede di conoscere il nome di una collection).
  3. catalogue_search   GET  /search?catalogue=...&resultsPerPage=1&page=0
                        -> query reale sul catalogo: e' LA misura di latenza KPI.
  4. file_lookup        GET  /platformFiles/search/parametricFind?...  (solo se
                        DOWNLOAD_PLATFORM_FILE_ID non e' pinnato in configurazione)
                        -> individua un file candidato al download.
  5. file_metadata      GET  /platformFiles/{id}?projection=detailedPlatformFile
                        -> access service: metadati + link di download.
  6. file_download      GET  /platformFiles/{id}/dl
                        -> scaricamento effettivo (troncato a MAX_DOWNLOAD_BYTES:
                           il file canary deve essere di pochi KB).

REGOLA DI STATO (come da specifica KPI)
---------------------------------------
  catalogue_status = OK  <=> tutte le sonde di categoria "catalogue" sono OK
  access_status    = OK  <=> tutte le sonde di categoria "access"    sono OK
  overall_status   = OK  <=> catalogue_status == OK AND access_status == OK
  Una singola operazione fallita => NOK per la relativa categoria e per l'overall.

OUTPUT
------
  - <LOG_DIR>/collector_insula_kpi_iride_elastic.log        eventi JSON (1 per riga)
  - <LOG_DIR>/collector_insula_kpi_iride_elastic.debug.log  log operativo
  - Elasticsearch index KPI_INDEX (se ELASTIC_ENABLED = True)

Eventi emessi:
  event_type = "insula_kpi_probe"        (uno per sonda, se EMIT_INDIVIDUAL_PROBES)
  event_type = "insula_kpi_availability" (uno per run, sempre)

DIPENDENZE: solo standard library (urllib). Nessun pip install sulla VM.

Uso:
  python3 collector_insula_kpi_iride_elastic.py
  python3 collector_insula_kpi_iride_elastic.py --config /path/insula_kpi_iride_elastic.ini
  python3 collector_insula_kpi_iride_elastic.py --dry-run -v     # non scrive su Elastic
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
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

# ============================================================================
# COSTANTI
# ============================================================================

SCRIPT_VERSION = "1.15.0"
SCRIPT_NAME = "collector_insula_kpi_iride_elastic"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG = os.path.join(SCRIPT_DIR, "insula_kpi_iride_elastic.ini")

PLATFORM_TAG = "iride"
SERVICE_PROVIDER_LOG = "iride_insula_kpi"

EVENT_PROBE = "insula_kpi_probe"
EVENT_AVAILABILITY = "insula_kpi_availability"

STATUS_OK = "OK"
STATUS_NOK = "NOK"

CAT_CATALOGUE = "catalogue"
CAT_ACCESS = "access"
CAT_OGC = "ogc"
CAT_AUTH = "auth"
CAT_PROCESSING = "processing"
CAT_COLLECTIONS = "collections"

# Ogni catalogo Insula richiede un parametro aggiuntivo obbligatorio.
# chiave  = valore di CATALOGUE
# value[0] = nome del parametro nella query string
# value[1] = chiave corrispondente nell'ini
REQUIRED_BY_CATALOGUE = {
    "PLATFORM_PRODUCTS": ("collection", "CATALOGUE_COLLECTION"),
    "REF_DATA":          ("refDataCollection", "CATALOGUE_REFDATA_COLLECTION"),
    "ADAM_DATA":         ("adamCollection", "CATALOGUE_ADAM_COLLECTION"),
    "SATELLITE":         ("mission", "CATALOGUE_MISSION"),
}

# Cataloghi che espongono il filtro temporale sui prodotti.
# ATTENZIONE: il manuale PDF documenta productDateStart/productDateEnd, ma
# GET /search/parameters sull'API viva dichiara 'productDate'. Prima di alzare
# CATALOGUE_LOOKBACK_DAYS verificare il formato con:
#     collector_insula_kpi_iride_elastic.py --param productDate
CATALOGUES_WITH_PRODUCT_DATE = {"REF_DATA", "SATELLITE", "PLATFORM_PRODUCTS"}

# Indizi testuali di un rifiuto per credito/quota e non per guasto del servizio.
BILLING_MARKERS = ("wallet", "coin", "balance", "payment", "quota exceeded",
                   "insufficient", "credit")

# Mappa diagnostica: codice OAuth restituito da Keycloak -> causa concreta.
OAUTH_HINTS = {
    "invalid_grant":
        "username/password rifiutati, oppure utente disabilitato o con "
        "azioni obbligatorie pendenti (Required user actions). "
        "Controllare [KEYCLOAK] USERNAME/PASSWORD nell'ini: se c'e' ancora "
        "un placeholder tipo <PASTE_...> e' questo il problema.",
    "invalid_client":
        "il client non e' public: serve [KEYCLOAK] CLIENT_SECRET, "
        "oppure CLIENT_ID errato.",
    "unauthorized_client":
        "sul client il flusso Direct Access Grants e' disabilitato "
        "(Clients -> {client} -> Settings -> Direct access grants).",
    "invalid_scope":
        "scope richiesto non assegnato al client.",
    "invalid_request":
        "parametri della richiesta token incompleti o malformati.",
    "access_denied":
        "accesso negato da una policy del realm.",
}

log = logging.getLogger(SCRIPT_NAME)


# ============================================================================
# UTILITY
# ============================================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_ms(dt: datetime) -> str:
    """Timestamp ISO8601 in millisecondi con suffisso Z (formato Elastic date)."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + \
        f"{dt.microsecond // 1000:03d}Z"


def as_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on", "si", "s")


def as_int(value: str, default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def as_limit(value: str, default: int):
    """
    Tetto di sicurezza: vuoto, 0 o negativo significano "nessun limite".
    Ritorna None quando il limite e' disattivato.
    """
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


def as_float(value: str, default: float) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return default


def redact(text: str, secrets) -> str:
    """Rimuove password/token/api key dai messaggi di log."""
    if not text:
        return text
    out = str(text)
    for s in secrets:
        if s and len(str(s)) > 3:
            out = out.replace(str(s), "***REDACTED***")
    return out


def body_snippet(res, secrets, limit: int = 400) -> str:
    """
    Estrae un estratto leggibile del corpo di risposta in errore.
    Senza questo, un 500 o un 401 arrivano nel log come semplice codice HTTP e
    si perde il messaggio del backend, che e' quasi sempre la vera diagnosi.
    """
    if res is None or not getattr(res, "body", None):
        return ""
    raw = res.body.decode("utf-8", errors="replace").strip()
    data = res.json()
    if isinstance(data, dict):
        # Spring Boot: timestamp/status/error/path ; OAuth: error/error_description
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
    Distingue un rifiuto per credito/quota da un guasto vero.
      'entitlement' -> il servizio risponde ma nega per wallet/quota (HTTP 402,
                       o 403/429 con riferimenti al credito). Non e' indisponibilita'.
      'unavailable' -> errore di rete, 5xx, timeout: il servizio non funziona.
      'client'      -> 4xx generico: richiesta o configurazione sbagliata.
      'unknown'
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
    """Messaggio di errore completo: codice HTTP + corpo della risposta."""
    base = (res.error if res and res.error else
            (f"HTTP {res.status}" if res and res.status else "errore sconosciuto"))
    snippet = body_snippet(res, secrets)
    return f"{base} | {snippet}" if snippet else base


def setup_logging(log_dir: str, verbose: bool) -> str:
    """
    Logging persistente: file operativo + console.
    Silenzia i logger rumorosi di terze parti (pattern gia' in uso sugli altri
    collector IRIDE).
    """
    os.makedirs(log_dir, exist_ok=True)
    debug_path = os.path.join(log_dir, f"{SCRIPT_NAME}.debug.log")

    log.setLevel(logging.DEBUG)
    log.handlers.clear()
    log.propagate = False

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-7s] %(message)s", "%Y-%m-%dT%H:%M:%S"
    )

    fh = logging.FileHandler(debug_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    log.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(fmt)
    log.addHandler(ch)

    for noisy in ("urllib3", "elasticsearch", "elastic_transport", "requests", "charset_normalizer"):
        logging.getLogger(noisy).setLevel(logging.CRITICAL)
        logging.getLogger(noisy).propagate = False

    return debug_path


# ============================================================================
# CONFIGURAZIONE
# ============================================================================

class Config:
    """Wrapper sull'ini: stessa struttura di insula_jobs_iride_elastic.ini."""

    def __init__(self, path: str):
        if not os.path.isfile(path):
            raise FileNotFoundError(f"File di configurazione non trovato: {path}")

        self.path = path
        cp = configparser.ConfigParser(interpolation=None)
        cp.optionxform = str  # preserva il case delle chiavi
        cp.read(path, encoding="utf-8")
        self.cp = cp

        g = self._get

        # --- [CONFIG] : output Elastic -------------------------------------
        self.elastic_enabled = as_bool(g("CONFIG", "ELASTIC_ENABLED"), False)
        self.monitoring_url = g("CONFIG", "MONITORING_URL", "").rstrip("/")
        self.monitoring_apikey = g("CONFIG", "MONITORING_APIKEY", "")
        self.monitoring_verify_certs = as_bool(g("CONFIG", "MONITORING_VERIFY_CERTS"), False)
        self.kpi_index = g("CONFIG", "KPI_INDEX", "logs-iride-insula-kpi.monitoring-default")
        self.log_dir = (g("CONFIG", "LOG_DIR", "") or "").strip() or SCRIPT_DIR

        # --- [INSULA] -------------------------------------------------------
        self.insula_base = g("INSULA", "BASE_URL", "").rstrip("/")

        # --- [KEYCLOAK] -----------------------------------------------------
        self.kc_url = g("KEYCLOAK", "URL", "").rstrip("/")
        self.kc_realm = g("KEYCLOAK", "REALM", "")
        self.kc_client_id = g("KEYCLOAK", "CLIENT_ID", "admin-cli")
        self.kc_client_secret = g("KEYCLOAK", "CLIENT_SECRET", "")
        self.kc_username = g("KEYCLOAK", "USERNAME", "")
        self.kc_password = g("KEYCLOAK", "PASSWORD", "")

        # --- [INSULA_KPI] ---------------------------------------------------
        s = "INSULA_KPI"
        self.catalogue = g(s, "CATALOGUE", "PLATFORM_PRODUCTS")
        self.catalogue_collection = g(s, "CATALOGUE_COLLECTION", "")
        self.catalogue_refdata_collection = g(s, "CATALOGUE_REFDATA_COLLECTION", "")
        self.catalogue_adam_collection = g(s, "CATALOGUE_ADAM_COLLECTION", "")
        self.catalogue_mission = g(s, "CATALOGUE_MISSION", "")
        self.catalogue_extra_params = g(s, "CATALOGUE_EXTRA_PARAMS", "")
        self.catalogue_page_size = as_int(g(s, "CATALOGUE_PAGE_SIZE"), 1)
        self.catalogue_lookback_days = as_int(g(s, "CATALOGUE_LOOKBACK_DAYS"), 0)
        self.catalogue_expect_results = as_bool(g(s, "CATALOGUE_EXPECT_RESULTS"), False)
        self.check_search_parameters = as_bool(g(s, "CHECK_SEARCH_PARAMETERS"), True)

        self.download_file_id = g(s, "DOWNLOAD_PLATFORM_FILE_ID", "").strip()
        self.download_lookup_type = g(s, "DOWNLOAD_LOOKUP_TYPE", "REFERENCE_DATA")
        self.download_lookup_filter = g(s, "DOWNLOAD_LOOKUP_FILTER", "")
        self.download_lookup_collection = g(s, "DOWNLOAD_LOOKUP_COLLECTION", "")
        self.max_download_bytes = as_int(g(s, "MAX_DOWNLOAD_BYTES"), 10485760)
        self.min_download_bytes = as_int(g(s, "MIN_DOWNLOAD_BYTES"), 1)
        self.candidate_pool_size = as_int(g(s, "CANDIDATE_POOL_SIZE"), 25)
        self.candidate_sort = g(s, "CANDIDATE_SORT", "id,desc")
        self.max_size_probes = as_int(g(s, "MAX_SIZE_PROBES"), 25)
        self.size_probe_method = (g(s, "SIZE_PROBE_METHOD", "auto") or "auto").strip().lower()
        if self.size_probe_method not in ("auto", "head", "range", "none"):
            self.size_probe_method = "auto"
        self.entitlement_abort_after = as_int(g(s, "ENTITLEMENT_ABORT_AFTER"), 3)

        # --- [INSULA_OGC] : sonde WMS/WFS (non tariffate) -------------------
        o = "INSULA_OGC"
        self.ogc_enabled = as_bool(g(o, "OGC_PROBE_ENABLED"), True)
        self.ogc_mode = (g(o, "OGC_MODE", "both") or "both").strip().lower()
        if self.ogc_mode not in ("capabilities", "getmap", "both"):
            self.ogc_mode = "both"
        self.ogc_platform_file_id = (g(o, "OGC_PLATFORM_FILE_ID", "") or "").strip()
        self.ogc_lookup_type = g(o, "OGC_LOOKUP_TYPE", "OUTPUT_PRODUCT")
        self.ogc_lookup_collection = g(o, "OGC_LOOKUP_COLLECTION", "")
        self.ogc_candidate_pool = as_int(g(o, "OGC_CANDIDATE_POOL"), 25)
        self.ogc_getmap_size = as_int(g(o, "OGC_GETMAP_SIZE"), 256)
        self.ogc_wms_version = g(o, "OGC_WMS_VERSION", "1.3.0")
        self.ogc_counts_in_kpi = as_bool(g(o, "OGC_COUNTS_IN_KPI"), True)
        self.ogc_cache_file = (g(o, "OGC_CACHE_FILE", "") or "").strip()
        self.ogc_strip_workspace = as_bool(g(o, "OGC_STRIP_WORKSPACE"), False)
        self.ogc_drop_params = tuple(
            x.strip().upper() for x in (g(o, "OGC_DROP_PARAMS", "") or "").split(",")
            if x.strip())

        # --- [INSULA_SERVICES] : sonde su servizi aggiuntivi (non tariffate) --
        sv = "INSULA_SERVICES"
        self.probe_jobs_enabled = as_bool(g(sv, "PROBE_JOBS"), True)
        self.jobs_expect_any = as_bool(g(sv, "JOBS_EXPECT_ANY"), False)
        self.probe_collections_enabled = as_bool(g(sv, "PROBE_COLLECTIONS"), True)
        self.collections_expect_any = as_bool(g(sv, "COLLECTIONS_EXPECT_ANY"), True)

        # --- [INSULA_WALLET] : lettura saldo e ricarica automatica ----------
        w = "INSULA_WALLET"
        self.wallet_monitor = as_bool(g(w, "WALLET_MONITOR"), True)
        self.wallet_autorecharge = as_bool(g(w, "WALLET_AUTORECHARGE"), False)
        self.wallet_id = (g(w, "WALLET_ID", "") or "").strip()
        self.wallet_min_balance = as_int(g(w, "WALLET_MIN_BALANCE"), 2)
        self.wallet_recharge_amount = as_int(g(w, "WALLET_RECHARGE_AMOUNT"), 5)
        self.wallet_max_per_day = as_limit(g(w, "WALLET_MAX_RECHARGES_PER_DAY"), 6)
        self.wallet_max_amount_per_day = as_limit(g(w, "WALLET_MAX_AMOUNT_PER_DAY"), 30)
        self.wallet_state_file = (g(w, "WALLET_STATE_FILE", "") or "").strip()
        self.wallet_low_threshold = as_int(g(w, "WALLET_LOW_THRESHOLD"), 5)
        self.wallet_read_transactions = as_bool(g(w, "WALLET_READ_TRANSACTIONS"), True)
        self.wallet_transactions_size = as_int(g(w, "WALLET_TRANSACTIONS_SIZE"), 20)
        self.skip_redundant_size_probe = as_bool(g(s, "SKIP_REDUNDANT_SIZE_PROBE"), True)
        self.assumed_cron_minutes = as_int(g(s, "ASSUMED_CRON_MINUTES"), 15)
        self.download_mode = (g(s, "DOWNLOAD_MODE", "full") or "full").strip().lower()
        if self.download_mode not in ("full", "ranged", "head"):
            self.download_mode = "full"
        self.download_range_bytes = as_int(g(s, "DOWNLOAD_RANGE_BYTES"), 4096)
        self.billing_errors_as_nok = as_bool(g(s, "BILLING_ERRORS_AS_NOK"), True)
        self.use_selection_cache = as_bool(g(s, "USE_SELECTION_CACHE"), True)
        self.selection_cache_file = (g(s, "SELECTION_CACHE_FILE", "") or "").strip()

        self.http_timeout = as_int(g(s, "HTTP_TIMEOUT"), 30)
        self.retries = as_int(g(s, "RETRIES"), 2)
        self.retry_backoff_s = as_float(g(s, "RETRY_BACKOFF_S"), 2.0)
        self.use_system_proxy = as_bool(g(s, "USE_SYSTEM_PROXY"), False)
        self.insula_verify_certs = as_bool(g(s, "INSULA_VERIFY_CERTS"), True)

        self.latency_warn_ms = as_int(g(s, "LATENCY_WARN_MS"), 3000)
        self.latency_crit_ms = as_int(g(s, "LATENCY_CRIT_MS"), 10000)

        self.emit_individual_probes = as_bool(g(s, "EMIT_INDIVIDUAL_PROBES"), True)

        self._validate()

    def _get(self, section: str, key: str, default: str = None):
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
        if missing:
            raise ValueError("Parametri obbligatori mancanti nell'ini: " + ", ".join(missing))

    @property
    def secrets(self):
        return [self.kc_password, self.kc_client_secret, self.monitoring_apikey]

    def catalogue_required_param(self):
        """
        Restituisce (nome_parametro, chiave_ini, valore_configurato) per il
        parametro obbligatorio richiesto dal catalogo selezionato.
        Ritorna (None, None, None) per cataloghi senza vincoli noti.
        """
        entry = REQUIRED_BY_CATALOGUE.get(self.catalogue)
        if not entry:
            return None, None, None
        param, ini_key = entry
        value = {
            "collection": self.catalogue_collection,
            "refDataCollection": self.catalogue_refdata_collection,
            "adamCollection": self.catalogue_adam_collection,
            "mission": self.catalogue_mission,
        }.get(param, "")
        return param, ini_key, (value or "").strip()

    def parsed_extra_params(self):
        """CATALOGUE_EXTRA_PARAMS = chiave=valore, chiave=valore"""
        out = {}
        for chunk in (self.catalogue_extra_params or "").split(","):
            chunk = chunk.strip()
            if not chunk or "=" not in chunk:
                continue
            k, v = chunk.split("=", 1)
            k, v = k.strip(), v.strip()
            if k and v:
                out[k] = v
        return out


# ============================================================================
# CLIENT HTTP (stdlib)
# ============================================================================

class HttpResult:
    __slots__ = ("status", "body", "headers", "latency_ms", "bytes", "error", "url", "attempts")

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
    """
    Client minimale su urllib: timeout, retry con backoff, bypass proxy
    opzionale, contesto SSL configurabile, lettura troncata per i download.
    """

    def __init__(self, timeout: int, retries: int, backoff: float,
                 verify_certs: bool, use_system_proxy: bool, secrets):
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
            # bypass esplicito dei proxy aziendali: le API sono interne
            handlers.append(urllib.request.ProxyHandler({}))
        self.opener = urllib.request.build_opener(*handlers)

    def request(self, method: str, url: str, headers=None, data=None,
                max_bytes: int = None, accept: str = "application/json") -> HttpResult:
        res = HttpResult(url)
        hdrs = {"Accept": accept, "User-Agent": f"{SCRIPT_NAME}/1.0"}
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
                    if max_bytes is not None:
                        # +1 byte per capire se il file e' piu' grande del cap
                        res.body = resp.read(max_bytes + 1)
                    else:
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
                # 4xx (tranne 429): errore deterministico, inutile ritentare
                if 400 <= e.code < 500 and e.code != 429:
                    return res

            except (urllib.error.URLError, socket.timeout, ssl.SSLError, OSError) as e:
                res.latency_ms = round((time.perf_counter() - t0) * 1000, 2)
                res.status = None
                reason = getattr(e, "reason", e)
                res.error = f"{type(e).__name__}: {reason}"

            if attempt <= self.retries:
                sleep_s = self.backoff * attempt
                log.debug("Retry %d/%d fra %.1fs (%s)", attempt, self.retries,
                          sleep_s, redact(res.error, self.secrets))
                time.sleep(sleep_s)

        return res

    def get(self, url, headers=None, max_bytes=None, accept="application/json"):
        return self.request("GET", url, headers=headers, max_bytes=max_bytes, accept=accept)

    def post(self, url, headers=None, data=None):
        return self.request("POST", url, headers=headers, data=data)


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
        self.last_result = None

    @property
    def token_url(self) -> str:
        return f"{self.cfg.kc_url}/realms/{self.cfg.kc_realm}/protocol/openid-connect/token"

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
        self.last_result = res

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


# ============================================================================
# SONDE KPI
# ============================================================================

class Probe:
    """Esito di una singola operazione misurata."""

    def __init__(self, name: str, category: str, url: str, description: str = ""):
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

    def mark_ok(self, res: HttpResult = None, **details):
        self.status = STATUS_OK
        self._absorb(res)
        self.details.update(details)
        return self

    def mark_nok(self, error: str, res: HttpResult = None, **details):
        self.status = STATUS_NOK
        self.error = error
        self._absorb(res)
        self.details.update(details)
        return self

    def mark_skipped(self, reason: str):
        self.skipped = True
        self.status = STATUS_OK          # non conteggiata come fallimento
        self.details["skip_reason"] = reason
        return self

    def _absorb(self, res: HttpResult):
        if res is None:
            return
        self.http_status = res.status
        self.latency_ms = res.latency_ms
        self.attempts = res.attempts
        self.bytes = res.bytes
        if self.url is None:
            self.url = res.url

    def latency_status(self, warn_ms: int, crit_ms: int):
        if self.latency_ms is None:
            return None
        if self.latency_ms >= crit_ms:
            return "CRITICAL"
        if self.latency_ms >= warn_ms:
            return "WARNING"
        return "NORMAL"


class InsulaKpiCollector:

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.http = HttpClient(
            timeout=cfg.http_timeout,
            retries=cfg.retries,
            backoff=cfg.retry_backoff_s,
            verify_certs=cfg.insula_verify_certs,
            use_system_proxy=cfg.use_system_proxy,
            secrets=cfg.secrets,
        )
        self.auth = KeycloakAuth(cfg, self.http)
        self.run_id = str(uuid.uuid4())
        self.hostname = platform.node() or socket.gethostname()
        self.probes = []
        self.download_file_id = None
        self.download_expected_size = None
        self.content_requests = 0      # richieste a /dl: potenzialmente tariffate
        self.wallet_balance = None
        self.wallet_id_resolved = None
        self.wallet_recharge_result = None
        self.wallet_capped = False
        self.wallet_tx_summary = None
        self.dl_endpoint_calls = 0   # chiamate a /dl: su questa piattaforma sono tariffate

    # -- helpers ------------------------------------------------------------

    def _api(self, path: str, params: dict = None) -> str:
        url = f"{self.cfg.insula_base}{path}"
        if params:
            clean = {k: v for k, v in params.items() if v not in (None, "")}
            if clean:
                url = f"{url}?{urllib.parse.urlencode(clean)}"
        return url

    def _add(self, probe: Probe) -> Probe:
        self.probes.append(probe)
        icon = "OK " if probe.status == STATUS_OK else "NOK"
        log.info("[%s] %-16s %s  http=%s  %s ms  %s",
                 icon, probe.name, probe.category,
                 probe.http_status, probe.latency_ms,
                 redact(probe.error or "", self.cfg.secrets))
        return probe

    # -- 1. auth ------------------------------------------------------------

    def probe_auth(self) -> Probe:
        p = Probe("auth", CAT_AUTH, self.auth.token_url,
                  "Keycloak password grant (prerequisito di tutte le sonde)")
        token, res = self.auth.fetch_token(force=True)
        if token:
            p.mark_ok(res, realm=self.cfg.kc_realm, client_id=self.cfg.kc_client_id)
        else:
            data = (res.json() if res else None) or {}
            oauth_error = data.get("error")
            hint = OAUTH_HINTS.get(oauth_error)
            msg = http_error_text(res, self.cfg.secrets) if res else "nessuna risposta da Keycloak"
            if hint:
                msg = f"{msg} -> {hint}"
            p.mark_nok(msg, res,
                       realm=self.cfg.kc_realm,
                       client_id=self.cfg.kc_client_id,
                       oauth_error=oauth_error,
                       oauth_error_description=data.get("error_description"),
                       hint=hint)
        return self._add(p)

    # -- 2. catalogue: handshake -------------------------------------------

    def probe_catalogue_parameters(self) -> Probe:
        """
        GET /search/parameters?resolveAll=false
        Vista leggera: verifica che il servizio di catalogo sia raggiungibile e
        risponda con lo schema dei parametri, senza dipendere da una collection.
        """
        url = self._api("/search/parameters", {"resolveAll": "false"})
        p = Probe("catalogue_params", CAT_CATALOGUE, url,
                  "Descrittore parametri di ricerca del catalogo")

        if not self.cfg.check_search_parameters:
            return self._add(p.mark_skipped("CHECK_SEARCH_PARAMETERS = False"))

        res = self.http.get(url, headers=self.auth.auth_header())
        if not res.ok:
            return self._add(p.mark_nok(
                http_error_text(res, self.cfg.secrets), res,
                response_body=body_snippet(res, self.cfg.secrets)))

        data = res.json()
        if not isinstance(data, dict) or not data:
            return self._add(p.mark_nok("risposta non JSON o payload vuoto", res))

        return self._add(p.mark_ok(res, n_parameters=len(data),
                                   parameters=sorted(data.keys())[:20]))

    # -- 3. catalogue: query reale ------------------------------------------

    def probe_catalogue_search(self) -> Probe:
        """
        GET /search  -- e' LA sonda di latenza del KPI.
        `catalogue` e' obbligatorio; `collection` e' obbligatorio se
        catalogue = PLATFORM_PRODUCTS, `refDataCollection` se catalogue = REF_DATA.
        Si chiede 1 solo risultato: si misura la risposta del servizio, non la
        capacita' di paginare.
        """
        params = {
            "catalogue": self.cfg.catalogue,
            "resultsPerPage": self.cfg.catalogue_page_size,
            "page": 0,
        }

        # parametro obbligatorio dipendente dal catalogo scelto
        req_param, req_key, req_value = self.cfg.catalogue_required_param()
        if req_param and req_value:
            params[req_param] = req_value

        # eventuali parametri liberi da CATALOGUE_EXTRA_PARAMS
        params.update(self.cfg.parsed_extra_params())

        if self.cfg.catalogue_lookback_days > 0:
            if self.cfg.catalogue in CATALOGUES_WITH_PRODUCT_DATE:
                start = utc_now() - timedelta(days=self.cfg.catalogue_lookback_days)
                params["productDateStart"] = iso_ms(start)
                params["productDateEnd"] = iso_ms(utc_now())
            else:
                log.warning("CATALOGUE_LOOKBACK_DAYS ignorato: il catalogo %s non "
                            "espone il parametro productDate.", self.cfg.catalogue)

        url = self._api("/search", params)
        p = Probe("catalogue_search", CAT_CATALOGUE, url,
                  "Query sul catalogo prodotti (misura di latenza KPI)")

        # fallimento anticipato: query malformata per configurazione incompleta
        if req_param and not req_value:
            return self._add(p.mark_nok(
                f"configurazione incompleta: CATALOGUE = {self.cfg.catalogue} "
                f"richiede il parametro '{req_param}'. Valorizzare "
                f"[INSULA_KPI] {req_key} nell'ini (usare --discover per i valori ammessi)",
                None, catalogue=self.cfg.catalogue, missing_param=req_param))

        res = self.http.get(url, headers=self.auth.auth_header())
        if not res.ok:
            return self._add(p.mark_nok(
                http_error_text(res, self.cfg.secrets), res,
                response_body=body_snippet(res, self.cfg.secrets)))

        data = res.json()
        if not isinstance(data, dict):
            return self._add(p.mark_nok("risposta non deserializzabile in JSON", res))

        features = data.get("features")
        if features is None:
            return self._add(p.mark_nok("campo 'features' assente: schema inatteso", res))

        page = data.get("page") or {}
        total = page.get("totalElements")
        n_returned = len(features) if isinstance(features, list) else 0

        if self.cfg.catalogue_expect_results and n_returned == 0:
            return self._add(p.mark_nok(
                "il catalogo risponde ma non restituisce prodotti "
                "(CATALOGUE_EXPECT_RESULTS = True)",
                res, total_elements=total, n_returned=0))

        return self._add(p.mark_ok(
            res,
            catalogue=self.cfg.catalogue,
            catalogue_param=req_param,
            catalogue_param_value=req_value or None,
            total_elements=total,
            total_pages=page.get("totalPages"),
            n_returned=n_returned,
        ))

    # -- 3b. processing: il motore dei job risponde -------------------------

    def probe_jobs(self) -> Probe:
        """
        GET /jobs/search/parametricFind
        Verifica che il servizio di processing risponda e che la lista dei job
        sia interrogabile. E' un servizio distinto da catalogo e file: se cade,
        il catalogo puo' restare su ma la piattaforma non elabora piu' nulla.
        Query non tariffata.
        """
        params = {"projection": "shortJob", "size": 1, "page": 0,
                  "sort": "startDateTime,desc"}
        url = self._api("/jobs/search/parametricFind", params)
        p = Probe("jobs_search", CAT_PROCESSING, url,
                  "Ricerca job di processing (il motore risponde)")

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

        page = data.get("page") or {}
        total = page.get("totalElements")
        emb = data.get("_embedded") or {}
        jobs = emb.get("jobs") or []
        n_returned = len(jobs) if isinstance(jobs, list) else 0

        if self.cfg.jobs_expect_any and not total:
            return self._add(p.mark_nok(
                "il servizio risponde ma non risultano job "
                "(JOBS_EXPECT_ANY = True)", res,
                total_elements=total, n_returned=0))

        return self._add(p.mark_ok(res, total_elements=total,
                                   n_returned=n_returned))

    # -- 3c. collections: il servizio collezioni risponde -------------------

    def probe_collections(self) -> Probe:
        """
        GET /collections
        Handshake sul servizio collezioni: e' il registro che tiene insieme
        catalogo e file. Query non tariffata.
        """
        params = {"projection": "shortCollection", "size": 1, "page": 0}
        url = self._api("/collections", params)
        p = Probe("collections_list", CAT_COLLECTIONS, url,
                  "Elenco collezioni (il registro risponde)")

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

        page = data.get("page") or {}
        total = page.get("totalElements")

        if self.cfg.collections_expect_any and not total:
            return self._add(p.mark_nok(
                "il servizio risponde ma non risultano collezioni "
                "(COLLECTIONS_EXPECT_ANY = True)", res, total_elements=0))

        return self._add(p.mark_ok(res, total_elements=total))

    # -- 4. access: individuazione del file canary --------------------------

    def _probe_file_size(self, file_id):
        """
        Determina la dimensione di un platformFile SENZA scaricarlo.
        parametricFind non espone fileSize, quindi si interroga /dl:
          1) HEAD                  -> Content-Length
          2) GET Range: bytes=0-0  -> Content-Range: bytes 0-0/<totale>

        ATTENZIONE: su questa piattaforma anche le richieste parziali possono
        essere tariffate. Se la misura viene negata per credito, il chiamante
        deve degradare con eleganza invece di far fallire l'intera sonda.

        Ritorna (size | None, metodo, errore | None, classe_errore | None).
        """
        method = self.cfg.size_probe_method
        if method == "none":
            return None, "none", "misura disattivata (SIZE_PROBE_METHOD = none)", "skipped"

        url = self._api(f"/platformFiles/{file_id}/dl")
        h = self.auth.auth_header()
        last_err, last_class = None, None
        self.dl_endpoint_calls += 1

        if method in ("auto", "head"):
            self.content_requests += 1
            r = self.http.request("HEAD", url, headers=h, accept="*/*")
            if r.ok:
                n = as_int(r.headers.get("content-length"), -1)
                if n >= 0:
                    return n, "HEAD", None, None
                last_err = "HEAD senza Content-Length"
                last_class = "client"
            else:
                last_err = http_error_text(r, self.cfg.secrets)
                last_class = classify_error(r, self.cfg.secrets)
            if method == "head":
                return None, "HEAD", last_err, last_class

        if method in ("auto", "range"):
            self.content_requests += 1
            rh = dict(h)
            rh["Range"] = "bytes=0-0"
            r = self.http.get(url, headers=rh, max_bytes=1, accept="*/*")
            if r.ok:
                cr = r.headers.get("content-range") or ""
                if "/" in cr:
                    total = as_int(cr.rsplit("/", 1)[-1], -1)
                    if total >= 0:
                        return total, "RANGE", None, None
                n = as_int(r.headers.get("content-length"), -1)
                if n >= 0 and r.status == 200:
                    return n, "RANGE(fallback 200)", None, None
                last_err = "Range senza Content-Range utilizzabile"
                last_class = "client"
            else:
                last_err = http_error_text(r, self.cfg.secrets)
                last_class = classify_error(r, self.cfg.secrets)

        return None, method, last_err or "dimensione non determinabile", last_class

    # -- cache della selezione ----------------------------------------------

    def _cache_path(self) -> str:
        if self.cfg.selection_cache_file:
            return self.cfg.selection_cache_file
        return os.path.join(self.cfg.log_dir, ".insula_kpi_target.json")

    def _cache_read(self):
        try:
            with open(self._cache_path(), "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and data.get("platform_file_id"):
                return data
        except (OSError, ValueError):
            pass
        return None

    def _cache_write(self, payload: dict):
        try:
            with open(self._cache_path(), "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except OSError as e:
            log.warning("Impossibile scrivere la cache di selezione (%s): %s",
                        self._cache_path(), e)

    # -- 4. access: selezione del file da scaricare -------------------------

    def _fetch_candidates(self):
        """Elenco di platformFile candidati (nessuna dimensione disponibile qui)."""
        params = {
            "type": self.cfg.download_lookup_type,
            "projection": "shortPlatformFile",
            "size": self.cfg.candidate_pool_size,
            "page": 0,
            "sort": self.cfg.candidate_sort,
        }
        if self.cfg.download_lookup_filter:
            params["filter"] = self.cfg.download_lookup_filter
        if self.cfg.download_lookup_collection:
            params["collection"] = self.cfg.download_lookup_collection

        url = self._api("/platformFiles/search/parametricFind", params)
        res = self.http.get(url, headers=self.auth.auth_header())
        if not res.ok:
            return None, res, url

        data = res.json() or {}
        embedded = data.get("_embedded") or {}
        # la doc riporta 'collections' per questo endpoint (refuso): si accetta
        # sia 'platformFiles' sia 'collections'
        items = embedded.get("platformFiles") or embedded.get("collections") or []
        return (items if isinstance(items, list) else []), res, url

    def _select_smallest(self, candidates):
        """
        Misura i candidati e sceglie il piu' piccolo entro i limiti.
        Se le misure vengono negate per credito, si ferma dopo poche prove:
        insistere su 25 file costa tempo e non cambia l'esito.
        Ritorna (scelto | None, statistiche).
        """
        measured, skipped_big, skipped_small, unmeasurable = [], 0, 0, 0
        entitlement_blocked = 0
        budget = self.cfg.max_size_probes

        for item in candidates:
            if budget <= 0:
                break
            if entitlement_blocked >= self.cfg.entitlement_abort_after:
                log.info("Misura delle dimensioni negata per credito su %d file "
                         "consecutivi: interrotta, si passa al fallback.",
                         entitlement_blocked)
                break
            fid = item.get("id")
            if fid is None:
                continue
            budget -= 1
            size, method, err, err_class = self._probe_file_size(fid)
            if size is None:
                unmeasurable += 1
                if err_class == "entitlement":
                    entitlement_blocked += 1
                log.debug("Dimensione non determinata per id=%s (%s): %s",
                          fid, method, err)
                continue
            if size > self.cfg.max_download_bytes:
                skipped_big += 1
                continue
            if size < self.cfg.min_download_bytes:
                skipped_small += 1
                continue
            measured.append({
                "platform_file_id": str(fid),
                "filename": item.get("filename"),
                "file_type": item.get("type"),
                "resto_id": item.get("restoId"),
                "size_bytes": size,
                "size_method": method,
            })

        stats = {
            "n_candidates": len(candidates),
            "n_measured": len(measured),
            "n_too_big": skipped_big,
            "n_too_small": skipped_small,
            "n_unmeasurable": unmeasurable,
            "n_entitlement_blocked": entitlement_blocked,
            "probes_used": self.cfg.max_size_probes - budget,
        }
        if not measured:
            return None, stats

        measured.sort(key=lambda c: c["size_bytes"])
        return measured[0], stats

    def probe_file_lookup(self) -> Probe:
        """
        Individua il file da scaricare, in ordine di preferenza:
          1. DOWNLOAD_PLATFORM_FILE_ID pinnato nell'ini
          2. selezione in cache, se ancora valida
          3. discovery: misura i candidati e prende il piu' piccolo
          4. fallback: primo candidato, quando le misure non sono possibili

        PRINCIPIO: questa sonda deve dire solo se un file da scaricare esiste.
        Se la piattaforma nega la MISURA per credito, non e' un problema di
        selezione: si sceglie comunque un file e sara' file_download a riportare
        il vero errore. Un guasto solo deve produrre una sonda rossa, non tre.
        """
        p = Probe("file_lookup", CAT_ACCESS, None, "Selezione del file da scaricare")

        # --- 1) ID pinnato -------------------------------------------------
        if self.cfg.download_file_id:
            self.download_file_id = self.cfg.download_file_id
            p.details["platform_file_id"] = self.download_file_id
            p.details["selection_mode"] = "pinned"
            return self._add(p.mark_skipped("DOWNLOAD_PLATFORM_FILE_ID pinnato"))

        # --- 2) cache ------------------------------------------------------
        cached = self._cache_read() if self.cfg.use_selection_cache else None
        if cached:
            fid = str(cached.get("platform_file_id"))

            # Su questa piattaforma OGNI chiamata a /dl e' tariffata, HEAD
            # compreso. Se file_download tocchera' comunque /dl (in qualsiasi
            # modo: head, ranged o full), misurare qui la dimensione e' una
            # richiesta tariffata in piu' che non aggiunge nulla su un file
            # gia' in cache: si salta e si lascia che sia file_download a dire
            # se il file e' ancora li'. Costo dimezzato, stessa informazione.
            redundant = self.cfg.skip_redundant_size_probe
            if redundant:
                self.download_file_id = fid
                self.download_expected_size = cached.get("size_bytes")
                return self._add(p.mark_ok(
                    None,
                    selection_mode="cache",
                    platform_file_id=fid,
                    filename=cached.get("filename"),
                    file_type=cached.get("file_type"),
                    size_bytes=cached.get("size_bytes"),
                    size_verified=False,
                    size_check_note="verifica delegata a file_download "
                                    "(stessa richiesta, non si paga due volte)",
                    selected_at=cached.get("selected_at")))

            size, method, err, err_class = self._probe_file_size(fid)

            if err_class in ("entitlement", "skipped"):
                # la misura e' negata o disattivata: il file quasi certamente
                # esiste ancora, non ha senso rifare la discovery a ogni run
                self.download_file_id = fid
                self.download_expected_size = cached.get("size_bytes")
                note = ("misura negata per credito"
                        if err_class == "entitlement" else "misura disattivata")
                return self._add(p.mark_ok(
                    None,
                    selection_mode="cache",
                    platform_file_id=fid,
                    filename=cached.get("filename"),
                    file_type=cached.get("file_type"),
                    size_bytes=cached.get("size_bytes"),
                    size_verified=False,
                    size_check_note=note,
                    selected_at=cached.get("selected_at")))

            if size is None:
                log.info("File in cache id=%s non piu' verificabile (%s): "
                         "riparte la discovery.", fid, err)
            elif size > self.cfg.max_download_bytes:
                log.info("File in cache id=%s ora pesa %d byte, oltre il limite: "
                         "riparte la discovery.", fid, size)
            elif size < self.cfg.min_download_bytes:
                log.info("File in cache id=%s ora pesa %d byte, sotto il minimo: "
                         "riparte la discovery.", fid, size)
            else:
                self.download_file_id = fid
                self.download_expected_size = size
                changed = (cached.get("size_bytes") is not None and
                           size != cached.get("size_bytes"))
                if changed:
                    cached["size_bytes"] = size
                    cached["size_changed_at"] = iso_ms(utc_now())
                    self._cache_write(cached)
                return self._add(p.mark_ok(
                    None,
                    selection_mode="cache",
                    platform_file_id=fid,
                    filename=cached.get("filename"),
                    file_type=cached.get("file_type"),
                    size_bytes=size,
                    size_method=method,
                    size_verified=True,
                    size_changed=changed,
                    selected_at=cached.get("selected_at")))

        # --- 3) discovery ---------------------------------------------------
        candidates, res, url = self._fetch_candidates()
        p.url = url
        if candidates is None:
            return self._add(p.mark_nok(
                http_error_text(res, self.cfg.secrets), res,
                selection_mode="discovery",
                response_body=body_snippet(res, self.cfg.secrets)))
        if not candidates:
            return self._add(p.mark_nok(
                "nessun platformFile trovato con i criteri di lookup", res,
                selection_mode="discovery"))

        chosen, stats = self._select_smallest(candidates)

        # --- 4) fallback: candidati presenti ma non misurabili --------------
        if not chosen:
            if stats["n_too_big"] or stats["n_too_small"]:
                # le misure funzionano: e' un problema reale di criteri
                return self._add(p.mark_nok(
                    f"nessun candidato entro i limiti "
                    f"({self.cfg.min_download_bytes}-{self.cfg.max_download_bytes} "
                    f"byte) fra {stats['n_candidates']} esaminati: "
                    f"{stats['n_too_big']} troppo grandi, {stats['n_too_small']} "
                    f"troppo piccoli. Rivedere MAX_DOWNLOAD_BYTES o i filtri "
                    f"di lookup.",
                    res, selection_mode="discovery", **stats))

            # nessuna misura riuscita: si sceglie comunque, in modo deterministico
            first = candidates[0]
            self.download_file_id = str(first.get("id"))
            self.download_expected_size = None
            blocked = stats["n_entitlement_blocked"] > 0
            note = ("misura delle dimensioni negata per credito"
                    if blocked else "dimensioni non determinabili dagli header")
            log.info("Selezione di ripiego: id=%s (%s). Sara' file_download a "
                     "riportare l'esito reale dell'accesso.",
                     self.download_file_id, note)

            if self.cfg.use_selection_cache:
                self._cache_write({
                    "platform_file_id": self.download_file_id,
                    "filename": first.get("filename"),
                    "file_type": first.get("type"),
                    "resto_id": first.get("restoId"),
                    "size_bytes": None,
                    "size_method": "unmeasured",
                    "selected_at": iso_ms(utc_now()),
                })

            return self._add(p.mark_ok(
                res,
                selection_mode="fallback_unmeasured",
                platform_file_id=self.download_file_id,
                filename=first.get("filename"),
                file_type=first.get("type"),
                size_bytes=None,
                size_verified=False,
                size_check_note=note,
                entitlement_blocked=blocked,
                **stats))

        # --- selezione riuscita ---------------------------------------------
        self.download_file_id = chosen["platform_file_id"]
        self.download_expected_size = chosen["size_bytes"]

        if self.cfg.use_selection_cache:
            record = dict(chosen)
            record["selected_at"] = iso_ms(utc_now())
            record["selection_criteria"] = {
                "type": self.cfg.download_lookup_type,
                "collection": self.cfg.download_lookup_collection or None,
                "filter": self.cfg.download_lookup_filter or None,
                "max_bytes": self.cfg.max_download_bytes,
                "min_bytes": self.cfg.min_download_bytes,
            }
            self._cache_write(record)
            log.info("Nuova selezione salvata in %s", self._cache_path())

        log.info("Scelto il file piu' piccolo: id=%s  %d byte (%.2f KB)  %s",
                 chosen["platform_file_id"], chosen["size_bytes"],
                 chosen["size_bytes"] / 1024.0, chosen.get("filename"))

        return self._add(p.mark_ok(res, selection_mode="discovery",
                                   size_verified=True, **chosen, **stats))

    # ========================================================================
    # SONDE OGC (WMS / WFS) -- accesso ai dati NON tariffato
    # ========================================================================
    #
    # Perche' esistono: /platformFiles/{id}/dl e' tariffato a coin, quindi non
    # e' sostenibile in cron. WMS e WFS sono servizi di accesso ai dati pensati
    # per essere interrogati liberamente: una tile GetMap trasferisce contenuto
    # reale (pixel) e non solo header, quindi verifica i "data access services"
    # in modo piu' aderente al nome del KPI di quanto faccia una HEAD.
    #
    # NOTA ONESTA: che WMS/WFS non siano tariffati su questa piattaforma e'
    # l'assunzione ragionevole, non una certezza verificata. Il contatore
    # content_requests NON include queste chiamate: controllare il saldo del
    # wallet dopo qualche run per confermarlo.

    def _ogc_cache_path(self) -> str:
        if self.cfg.ogc_cache_file:
            return self.cfg.ogc_cache_file
        return os.path.join(self.cfg.log_dir, ".insula_kpi_ogc.json")

    def _ogc_cache_read(self):
        try:
            with open(self._ogc_cache_path(), "r", encoding="utf-8") as f:
                d = json.load(f)
            if isinstance(d, dict) and d.get("wms_href"):
                return d
        except (OSError, ValueError):
            pass
        return None

    def _ogc_cache_write(self, payload: dict):
        try:
            with open(self._ogc_cache_path(), "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except OSError as e:
            log.warning("Impossibile scrivere la cache OGC: %s", e)

    def _find_ogc_layer(self):
        """
        Cerca un platformFile che esponga un layer WMS/WFS.
        Solo i prodotti con visualizzazione associata (GEOTIFF, SHAPEFILE,
        MOSAIC) hanno _links.wms / _links.wfs: i file generici non ne hanno.
        Serve la proiezione detailedPlatformFile per vedere i link.
        """
        if self.cfg.ogc_platform_file_id:
            r = self.http.get(
                self._api(f"/platformFiles/{self.cfg.ogc_platform_file_id}",
                          {"projection": "detailedPlatformFile"}),
                headers=self.auth.auth_header())
            if not r.ok:
                return None, http_error_text(r, self.cfg.secrets)
            d = r.json() or {}
            links = d.get("_links") or {}
            wms = (links.get("wms") or {}).get("href")
            if not wms:
                return None, (f"il file {self.cfg.ogc_platform_file_id} non espone "
                              f"un layer WMS")
            return {"platform_file_id": str(self.cfg.ogc_platform_file_id),
                    "filename": d.get("filename"),
                    "wms_href": wms,
                    "wfs_href": (links.get("wfs") or {}).get("href")}, None

        params = {
            "type": self.cfg.ogc_lookup_type,
            "projection": "detailedPlatformFile",
            "size": self.cfg.ogc_candidate_pool,
            "page": 0,
            "sort": "id,desc",
        }
        if self.cfg.ogc_lookup_collection:
            params["collection"] = self.cfg.ogc_lookup_collection

        r = self.http.get(self._api("/platformFiles/search/parametricFind", params),
                          headers=self.auth.auth_header())
        if not r.ok:
            return None, http_error_text(r, self.cfg.secrets)

        data = r.json() or {}
        emb = data.get("_embedded") or {}
        items = emb.get("platformFiles") or emb.get("collections") or []
        for it in items if isinstance(items, list) else []:
            links = it.get("_links") or {}
            wms = (links.get("wms") or {}).get("href")
            if wms:
                return {"platform_file_id": str(it.get("id")),
                        "filename": it.get("filename"),
                        "wms_href": wms,
                        "wfs_href": (links.get("wfs") or {}).get("href")}, None

        return None, (f"nessuno dei {len(items)} file di tipo "
                      f"{self.cfg.ogc_lookup_type} espone un layer WMS. "
                      f"Provare un'altra OGC_LOOKUP_COLLECTION o pinnare "
                      f"OGC_PLATFORM_FILE_ID.")

    @staticmethod
    def _wms_exception_text(res) -> str:
        """
        Estrae il messaggio da un ServiceExceptionReport di GeoServer.
        Il testo dell'eccezione e' la diagnosi vera: troncare il corpo grezzo
        significa perdere proprio la parte che serve.
        """
        if not getattr(res, "body", None):
            return ""
        try:
            root = ET.fromstring(res.body)
        except ET.ParseError:
            raw = res.body.decode("utf-8", errors="replace")
            return " ".join(raw.split())[:400]
        parts = []
        for el in root.iter():
            if el.tag.rsplit("}", 1)[-1] != "ServiceException":
                continue
            code = el.get("code") or el.get("locator") or ""
            txt = " ".join((el.text or "").split())
            parts.append(f"[{code}] {txt}" if code else txt)
        return " | ".join(p for p in parts if p)[:600] or "ServiceException senza testo"

    @staticmethod
    def _ogc_href_params(href: str) -> dict:
        """Parametri OGC gia' presenti nell'href, con chiavi normalizzate."""
        q = urllib.parse.urlsplit(href).query
        return {k.upper(): v for k, v in urllib.parse.parse_qsl(q)}

    @staticmethod
    def _ogc_url(base: str, params: dict) -> str:
        """Compone una URL OGC preservando i parametri gia' presenti nell'href."""
        split = urllib.parse.urlsplit(base)
        existing = dict(urllib.parse.parse_qsl(split.query))
        # i parametri OGC sono case-insensitive: si normalizzano in maiuscolo
        existing = {k.upper(): v for k, v in existing.items()}
        merged = {**existing, **params}
        return urllib.parse.urlunsplit((
            split.scheme, split.netloc, split.path,
            urllib.parse.urlencode(merged), split.fragment))

    def probe_wms_capabilities(self, target):
        """GetCapabilities: il servizio WMS risponde e dichiara i suoi layer."""
        href = target["wms_href"]
        existing = self._ogc_href_params(href)
        version = existing.get("VERSION") or self.cfg.ogc_wms_version

        # per GetCapabilities si ripulisce l'href dai parametri di GetMap:
        # layers, filtri e dimensioni non c'entrano e alcuni server si offendono
        split = urllib.parse.urlsplit(href)
        base = urllib.parse.urlunsplit(
            (split.scheme, split.netloc, split.path, "", ""))
        url = self._ogc_url(base, {
            "SERVICE": "WMS",
            "VERSION": version,
            "REQUEST": "GetCapabilities",
        })

        p = Probe("wms_capabilities", CAT_OGC, url,
                  "WMS GetCapabilities (accesso ai dati non tariffato)")
        p.details["platform_file_id"] = target.get("platform_file_id")

        res = self.http.get(url, headers=self.auth.auth_header(),
                            accept="text/xml, application/xml, */*")
        if not res.ok:
            return self._add(p.mark_nok(
                http_error_text(res, self.cfg.secrets), res,
                error_class=classify_error(res, self.cfg.secrets),
                response_body=body_snippet(res, self.cfg.secrets)))

        try:
            root = ET.fromstring(res.body)
        except ET.ParseError as e:
            return self._add(p.mark_nok(
                f"risposta non XML valido: {e}", res, error_class="client"))

        tag = root.tag.rsplit("}", 1)[-1]
        if tag not in ("WMS_Capabilities", "WMT_MS_Capabilities"):
            return self._add(p.mark_nok(
                f"radice XML inattesa: <{tag}>", res, error_class="client"))

        names = []
        for lay in root.iter():
            if lay.tag.rsplit("}", 1)[-1] != "Layer":
                continue
            for child in lay:
                if child.tag.rsplit("}", 1)[-1] == "Name" and child.text:
                    names.append(child.text.strip())
                    break

        target.setdefault("layers", [])
        target["layers"] = names or target["layers"]
        return self._add(p.mark_ok(res,
                                   wms_version=root.get("version") or version,
                                   n_layers=len(names),
                                   layers=names[:10],
                                   root_element=tag))

    def probe_wms_getmap(self, target):
        """
        GetMap: trasferimento di contenuto reale (una tile raster).
        E' la verifica piu' vicina al concetto di 'accesso al dato' fra quelle
        che non consumano credito.

        Il layer si ricava nell'ordine: parametro LAYERS gia' presente
        nell'href (Insula lo fornisce completo di time e cql_filter), poi
        primo layer dichiarato da GetCapabilities.
        """
        href = target["wms_href"]
        existing = self._ogc_href_params(href)

        layer = existing.get("LAYERS")
        layer_source = "href"
        if layer and self.cfg.ogc_strip_workspace and ":" in layer:
            layer = layer.split(":", 1)[-1]
            layer_source = "href (workspace rimosso)"
        if not layer:
            layers = target.get("layers") or []
            layer = layers[0] if layers else None
            layer_source = "capabilities"

        version = existing.get("VERSION") or self.cfg.ogc_wms_version
        size = max(16, self.cfg.ogc_getmap_size)

        # WMS 1.3.0 usa CRS e BBOX in ordine lat,lon; le 1.1.x usano SRS e lon,lat
        if version.startswith("1.3"):
            axis = {"CRS": "EPSG:4326", "BBOX": "-90,-180,90,180"}
        else:
            axis = {"SRS": "EPSG:4326", "BBOX": "-180,-90,180,90"}

        params = {
            "SERVICE": "WMS",
            "VERSION": version,
            "REQUEST": "GetMap",
            "LAYERS": layer or "",
            "STYLES": "",
            "WIDTH": str(size),
            "HEIGHT": str(size),
            "FORMAT": "image/png",
            "TRANSPARENT": "TRUE",
        }
        params.update(axis)

        url = self._ogc_url(href, params)   # preserva time, cql_filter, ecc.
        if self.cfg.ogc_drop_params:
            sp = urllib.parse.urlsplit(url)
            q = [(k, v) for k, v in urllib.parse.parse_qsl(sp.query)
                 if k.upper() not in self.cfg.ogc_drop_params]
            url = urllib.parse.urlunsplit(
                (sp.scheme, sp.netloc, sp.path, urllib.parse.urlencode(q), sp.fragment))
        p = Probe("wms_getmap", CAT_OGC, url,
                  f"WMS GetMap {size}x{size} (trasferimento di dati reale)")
        p.details.update(platform_file_id=target.get("platform_file_id"),
                         layer=layer, layer_source=layer_source,
                         wms_version=version)

        if not layer:
            return self._add(p.mark_nok(
                "nessun layer: assente sia nell'href (parametro LAYERS) sia "
                "fra quelli dichiarati da GetCapabilities", None,
                error_class="client"))

        res = self.http.get(url, headers=self.auth.auth_header(),
                            max_bytes=4 * 1024 * 1024, accept="image/png, */*")
        if not res.ok:
            return self._add(p.mark_nok(
                http_error_text(res, self.cfg.secrets), res,
                error_class=classify_error(res, self.cfg.secrets),
                response_body=body_snippet(res, self.cfg.secrets)))

        ctype = (res.headers.get("content-type") or "").lower()
        if "image" not in ctype:
            # i server WMS segnalano gli errori con 200 + XML ServiceException
            exc = self._wms_exception_text(res)
            return self._add(p.mark_nok(
                f"ServiceException: {exc}", res, error_class="client",
                content_type=ctype, service_exception=exc,
                hint="eseguire --wms-debug per provare le varianti "
                     "(prefisso workspace, cql_filter, bbox, versione)"))

        if res.bytes < 100:
            return self._add(p.mark_nok(
                f"immagine sospetta: solo {res.bytes} byte", res,
                error_class="client"))

        throughput = (round(res.bytes / (res.latency_ms / 1000.0) / 1024.0, 2)
                      if res.latency_ms else None)
        return self._add(p.mark_ok(res,
                                   layer=layer,
                                   layer_source=layer_source,
                                   image_bytes=res.bytes,
                                   image_kb=round(res.bytes / 1024.0, 2),
                                   content_type=ctype,
                                   size_px=size,
                                   throughput_kbps=throughput))

    def run_ogc_probes(self):
        """Esegue le sonde OGC, se abilitate e se esiste un layer da interrogare."""
        if not self.cfg.ogc_enabled:
            return

        cached = self._ogc_cache_read()
        target, err = (cached, None) if cached else self._find_ogc_layer()

        if not target:
            p = Probe("wms_capabilities", CAT_OGC, None,
                      "WMS GetCapabilities")
            self._add(p.mark_skipped(f"nessun layer WMS disponibile: {err}"))
            log.info("Sonde OGC saltate: %s", err)
            return

        if not cached:
            self._ogc_cache_write(target)
            log.info("Layer OGC selezionato: file id=%s  %s",
                     target.get("platform_file_id"), target.get("wms_href"))

        cap = None
        if self.cfg.ogc_mode in ("capabilities", "both"):
            cap = self.probe_wms_capabilities(target)
            if cap.status == STATUS_OK and self.cfg.ogc_mode == "both":
                self._ogc_cache_write(target)   # salva anche i layer scoperti

        if self.cfg.ogc_mode in ("getmap", "both"):
            has_layer = bool(self._ogc_href_params(target["wms_href"]).get("LAYERS")
                             or target.get("layers"))
            if not has_layer and self.cfg.ogc_mode == "getmap":
                self.probe_wms_capabilities(target)
                has_layer = bool(target.get("layers"))
            # se il layer e' noto dall'href, la GetMap non dipende dall'esito
            # della GetCapabilities
            if has_layer or cap is None or cap.status == STATUS_OK:
                self.probe_wms_getmap(target)

    # ========================================================================
    # WALLET: lettura saldo e ricarica
    # ========================================================================
    #
    # Endpoint ricavati dalla UI Perception (DevTools, 23/07/2026):
    #   POST /secure/api/v2.0/wallets/{walletId}/credit   body {"amount": N}
    #   GET  /secure/api/v2.0/wallets/current             (saldo, da confermare)
    # Autenticazione: Bearer token, lo stesso del password grant.
    #
    # La ricarica automatica e' DISATTIVATA per default (WALLET_AUTORECHARGE).
    # Quando attiva, e' vincolata da: soglia minima, tetto giornaliero sul
    # numero di ricariche e sull'importo totale. Ogni ricarica produce un
    # evento su Elastic (event_type = insula_wallet_recharge) con saldo prima
    # e dopo: serve ad avere un registro consultabile di cosa ha fatto lo
    # script, senza doverlo ricostruire dai log.

    WALLET_BALANCE_KEYS = ("balance", "credit", "credits", "amount",
                           "coins", "value", "available", "remaining")

    def _wallet_state_path(self) -> str:
        if self.cfg.wallet_state_file:
            return self.cfg.wallet_state_file
        return os.path.join(self.cfg.log_dir, ".insula_kpi_wallet.json")

    def _wallet_state_read(self) -> dict:
        try:
            with open(self._wallet_state_path(), "r", encoding="utf-8") as f:
                d = json.load(f)
            if isinstance(d, dict):
                return d
        except (OSError, ValueError):
            pass
        return {}

    def _wallet_state_write(self, state: dict):
        try:
            with open(self._wallet_state_path(), "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
        except OSError as e:
            log.warning("Impossibile scrivere lo stato del wallet: %s", e)

    @classmethod
    def _extract_balance(cls, data):
        """
        Estrae il saldo da una risposta di forma ignota.
        La struttura non e' documentata: si cercano le chiavi plausibili al
        primo livello e in un eventuale oggetto annidato.
        """
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
        """
        Legge il wallet dell'utenza autenticata.
        Ritorna (wallet_id | None, saldo | None, risposta_grezza, errore | None).
        """
        h = self.auth.auth_header()
        candidates = ["/wallets/current"]
        if self.cfg.wallet_id:
            candidates.append(f"/wallets/{self.cfg.wallet_id}")

        last_err = None
        for path in candidates:
            r = self.http.get(self._api(path), headers=h)
            if not r.ok:
                last_err = f"{path}: {http_error_text(r, self.cfg.secrets)}"
                continue
            data = r.json()
            balance, key = self._extract_balance(data)
            wid = None
            if isinstance(data, dict):
                wid = data.get("id") or data.get("walletId")
            return (str(wid) if wid else self.cfg.wallet_id or None,
                    balance, data, None if balance is not None else
                    f"saldo non individuato nella risposta di {path}")

        return None, None, None, last_err or "nessun endpoint wallet raggiungibile"

    def _wallet_credit(self, wallet_id: str, amount: int):
        """POST /wallets/{id}/credit  body {"amount": N}"""
        url = self._api(f"/wallets/{wallet_id}/credit")
        payload = json.dumps({"amount": int(amount)}).encode("utf-8")
        headers = dict(self.auth.auth_header())
        headers["Content-Type"] = "application/json"
        return self.http.request("POST", url, headers=headers, data=payload)

    def tx_debug(self) -> int:
        """Stampa una transaction grezza: serve a ricavare il nome del campo importo."""
        print("\n" + "=" * 78)
        print(" DIAGNOSI TRANSACTIONS WALLET")
        print("=" * 78)
        token, res = self.auth.fetch_token(force=True)
        if not token:
            print(f"\n[FATAL] auth: {http_error_text(res, self.cfg.secrets)}")
            return 2
        wid, _, _, _ = self._wallet_get()
        wid = wid or self.cfg.wallet_id or "53"
        url = self._api(f"/wallets/{wid}/transactions", {"size": 3, "page": 0, "sort": "id,desc"})
        print(f"\nGET {url}\n")
        r = self.http.get(url, headers=self.auth.auth_header())
        if not r.ok:
            print(f"[NOK] {http_error_text(r, self.cfg.secrets)}")
            return 1
        data = r.json() or {}
        print(json.dumps(data, ensure_ascii=False, indent=2)[:3000])
        # elenco dei campi della prima transaction
        emb = data.get("_embedded") or {}
        txs = None
        for v in emb.values():
            if isinstance(v, list):
                txs = v
                break
        if txs is None and isinstance(data, list):
            txs = data
        if txs:
            print("\n" + "-" * 78)
            print("  Campi della prima transaction:")
            for k, v in (txs[0] or {}).items():
                print(f"    {k:24} = {v!r}")
        print("=" * 78 + "\n")
        return 0

    def wallet_info(self) -> int:
        """
        Ricognizione in sola lettura degli endpoint wallet.
        NON esegue ricariche: serve a capire la forma delle risposte prima di
        automatizzare qualsiasi cosa.
        """
        print("\n" + "=" * 78)
        print(" RICOGNIZIONE WALLET  (sola lettura, nessuna ricarica)")
        print("=" * 78)

        token, res = self.auth.fetch_token(force=True)
        if not token:
            print(f"\n[FATAL] autenticazione fallita: "
                  f"{http_error_text(res, self.cfg.secrets)}")
            return 2
        print(f"\nUtenza: {self.cfg.kc_username}  (realm {self.cfg.kc_realm})\n")

        h = self.auth.auth_header()
        paths = ["/wallets/current", "/wallets"]
        if self.cfg.wallet_id:
            paths.insert(0, f"/wallets/{self.cfg.wallet_id}")

        found_balance = None
        for path in paths:
            url = self._api(path)
            r = self.http.get(url, headers=h)
            print(f"GET {path}")
            print(f"    HTTP {r.status}   {r.bytes} byte   {r.latency_ms} ms")
            if not r.ok:
                print(f"    {body_snippet(r, self.cfg.secrets, 200)}\n")
                continue
            data = r.json()
            print("    " + json.dumps(data, ensure_ascii=False, indent=2)
                  .replace("\n", "\n    ")[:1500])
            bal, key = self._extract_balance(data)
            if bal is not None:
                found_balance = bal
                print(f"\n    -> saldo individuato: {bal}  (campo '{key}')")
            print()

        print("-" * 78)
        if found_balance is not None:
            print(f"  Saldo attuale: {found_balance}")
            print(f"  Il collector puo' monitorarlo e allertare sotto soglia.")
        else:
            print("  Saldo non individuato automaticamente.")
            print("  Incollare il JSON qui sopra per adeguare il parser.")
        print(f"\n  Endpoint di ricarica (NON invocato): "
              f"POST /wallets/{{id}}/credit  body {{\"amount\": N}}")
        print("=" * 78 + "\n")
        return 0

    def probe_wallet(self):
        """
        Legge il saldo e, se configurato, ricarica quando scende sotto soglia.
        Il saldo diventa una metrica monitorabile: si puo' allertare PRIMA che
        il KPI si fermi, invece di scoprirlo da un 402.
        """
        if not self.cfg.wallet_monitor:
            return

        wid, balance, raw, err = self._wallet_get()
        if wid is None and self.cfg.wallet_id:
            wid = self.cfg.wallet_id

        p = Probe("wallet_balance", CAT_OGC if False else "wallet",
                  self._api("/wallets/current"), "Saldo del wallet dell'utenza")

        if balance is None:
            self._add(p.mark_skipped(f"saldo non leggibile: {err}"))
            return

        self.wallet_balance = balance
        self.wallet_id_resolved = wid
        low = balance <= self.cfg.wallet_low_threshold
        p.details.update(wallet_id=wid, balance=balance,
                         low_threshold=self.cfg.wallet_low_threshold,
                         balance_low=low)
        self._add(p.mark_ok(None, wallet_id=wid, balance=balance,
                            balance_low=low))

        if low:
            log.warning("Saldo wallet basso: %d coin (soglia %d)",
                        balance, self.cfg.wallet_low_threshold)

        self._maybe_recharge(wid, balance)

        if self.cfg.wallet_read_transactions and wid:
            self._read_wallet_transactions(wid)

    def _read_wallet_transactions(self, wallet_id):
        """
        GET /wallets/{id}/transactions
        Legge il movimento reale del wallet e lo riassume: consente di misurare
        il consumo effettivo invece di stimarlo dalla frequenza del cron.
        Best-effort: se l'endpoint non c'e' o cambia forma, non fa fallire nulla.
        """
        params = {"size": self.cfg.wallet_transactions_size, "page": 0,
                  "sort": "id,desc"}
        r = self.http.get(self._api(f"/wallets/{wallet_id}/transactions", params),
                          headers=self.auth.auth_header())
        if not r.ok:
            log.debug("Transactions non leggibili: %s",
                      http_error_text(r, self.cfg.secrets))
            return

        data = r.json() or {}
        emb = data.get("_embedded") or {}
        # nome della collezione ignoto: si prende la prima lista di dict
        txs = None
        for v in emb.values():
            if isinstance(v, list):
                txs = v
                break
        if txs is None and isinstance(data, list):
            txs = data
        if not txs:
            return

        debit = credit = 0
        sample_keys = set()
        for t in txs:
            if not isinstance(t, dict):
                continue
            sample_keys.update(t.keys())
            # importo: prova una lista ampia di nomi plausibili
            amt = None
            for k in ("amount", "value", "coins", "delta", "credits",
                      "quantity", "coinAmount", "cost", "price"):
                v = t.get(k)
                if isinstance(v, (int, float)):
                    amt = v
                    break
                if isinstance(v, dict):  # es. {"amount": {"value": 5}}
                    for kk in ("value", "amount"):
                        if isinstance(v.get(kk), (int, float)):
                            amt = v.get(kk)
                            break
                    if amt is not None:
                        break
            if amt is None:
                continue
            # segno: dal valore, o dal tipo di transazione se il valore e' positivo
            ttype = str(t.get("type") or t.get("transactionType")
                        or t.get("operation") or "").upper()
            is_debit = amt < 0 or any(
                w in ttype for w in ("DEBIT", "DOWNLOAD", "CONSUM", "SPEND",
                                     "CHARGE", "USAGE"))
            is_credit = amt > 0 and any(
                w in ttype for w in ("CREDIT", "RECHARGE", "TOPUP", "REFUND"))
            mag = abs(amt)
            if is_debit and not is_credit:
                debit += mag
            elif is_credit:
                credit += mag
            elif amt < 0:
                debit += mag
            else:
                credit += mag

        if debit == 0 and credit == 0 and txs:
            # nessun importo riconosciuto: logga i campi visti, cosi' si adegua
            log.debug("Transactions: nessun importo riconosciuto. Campi "
                      "presenti: %s", sorted(sample_keys))

        self.wallet_tx_summary = {
            "n_transactions_read": len(txs),
            "sum_debit": round(debit, 2),
            "sum_credit": round(credit, 2),
            "page_total": (data.get("page") or {}).get("totalElements"),
        }
        log.info("Wallet transactions: %d lette, addebiti %.0f, accrediti %.0f",
                 len(txs), debit, credit)

    def _maybe_recharge(self, wallet_id, balance):
        """Ricarica se abilitata, sotto soglia e entro i tetti giornalieri."""
        if not self.cfg.wallet_autorecharge:
            return
        if balance > self.cfg.wallet_min_balance:
            return
        if not wallet_id:
            log.error("Ricarica impossibile: wallet_id non risolto. "
                      "Valorizzare [INSULA_WALLET] WALLET_ID nell'ini.")
            return

        today = utc_now().strftime("%Y-%m-%d")
        state = self._wallet_state_read()
        if state.get("day") != today:
            state = {"day": today, "recharges": 0, "amount": 0, "events": []}

        if (self.cfg.wallet_max_per_day is not None
                and state["recharges"] >= self.cfg.wallet_max_per_day):
            log.error("Ricarica NON eseguita: raggiunto il tetto di %d ricariche "
                      "giornaliere. Il saldo e' %d: verificare perche' il consumo "
                      "e' piu' alto del previsto.",
                      self.cfg.wallet_max_per_day, balance)
            self.wallet_capped = True
            return

        amount = self.cfg.wallet_recharge_amount
        if (self.cfg.wallet_max_amount_per_day is not None
                and state["amount"] + amount > self.cfg.wallet_max_amount_per_day):
            log.error("Ricarica NON eseguita: supererebbe il tetto di %d coin "
                      "giornalieri (gia' ricaricati %d).",
                      self.cfg.wallet_max_amount_per_day, state["amount"])
            self.wallet_capped = True
            return

        log.info("Saldo %d <= soglia %d: ricarica di %d coin sul wallet %s",
                 balance, self.cfg.wallet_min_balance, amount, wallet_id)
        res = self._wallet_credit(wallet_id, amount)

        if not res.ok:
            log.error("Ricarica fallita: %s", http_error_text(res, self.cfg.secrets))
            self.wallet_recharge_result = {
                "ok": False, "amount": amount, "wallet_id": wallet_id,
                "error": http_error_text(res, self.cfg.secrets),
                "http_status": res.status,
                "balance_before": balance, "balance_after": None,
            }
            return

        _, new_balance, _, _ = self._wallet_get()
        state["recharges"] += 1
        state["amount"] += amount
        state["events"].append({
            "at": iso_ms(utc_now()), "amount": amount,
            "balance_before": balance, "balance_after": new_balance,
            "run_id": self.run_id,
        })
        self._wallet_state_write(state)

        cap_n = ("illimitate" if self.cfg.wallet_max_per_day is None
                 else str(self.cfg.wallet_max_per_day))
        cap_a = ("illimitati" if self.cfg.wallet_max_amount_per_day is None
                 else str(self.cfg.wallet_max_amount_per_day))
        log.info("Ricarica eseguita: %d coin. Saldo %s -> %s. "
                 "Ricariche oggi: %d/%s (%d/%s coin).",
                 amount, balance, new_balance,
                 state["recharges"], cap_n, state["amount"], cap_a)

        self.wallet_balance = new_balance if new_balance is not None else balance
        self.wallet_recharge_result = {
            "ok": True, "amount": amount, "wallet_id": wallet_id,
            "http_status": res.status, "latency_ms": res.latency_ms,
            "balance_before": balance, "balance_after": new_balance,
            "recharges_today": state["recharges"],
            "amount_today": state["amount"],
        }

    def wms_debug(self) -> int:
        """
        Prova varianti di GetMap sullo stesso layer e riporta quale funziona.
        Le cause tipiche di ServiceException su GeoServer sono il prefisso del
        workspace nel nome del layer, il cql_filter, il bbox globale o la
        versione WMS: invece di indovinare, si provano.
        """
        print("\n" + "=" * 78)
        print(" DIAGNOSI WMS GetMap")
        print("=" * 78)

        token, res = self.auth.fetch_token(force=True)
        if not token:
            print(f"\n[FATAL] autenticazione fallita: "
                  f"{http_error_text(res, self.cfg.secrets)}")
            return 2

        cached = self._ogc_cache_read()
        target, err = (cached, None) if cached else self._find_ogc_layer()
        if not target:
            print(f"\n[FATAL] nessun layer WMS individuato: {err}")
            return 2

        href = target["wms_href"]
        existing = self._ogc_href_params(href)
        layer_full = existing.get("LAYERS", "")
        layer_short = layer_full.split(":", 1)[-1] if ":" in layer_full else layer_full
        version = existing.get("VERSION") or "1.1.0"
        size = self.cfg.ogc_getmap_size

        print(f"\nFile      : {target.get('platform_file_id')}  "
              f"{target.get('filename') or ''}")
        print(f"Endpoint  : {urllib.parse.urlsplit(href).path}")
        print(f"Layer     : {layer_full}")
        print(f"Versione  : {version}")
        print(f"Parametri gia' nell'href: "
              f"{', '.join(sorted(k for k in existing if k not in ('SERVICE','VERSION')))}")

        split = urllib.parse.urlsplit(href)
        base_clean = urllib.parse.urlunsplit(
            (split.scheme, split.netloc, split.path, "", ""))

        def axis_for(v):
            if v.startswith("1.3"):
                return {"CRS": "EPSG:4326", "BBOX": "-90,-180,90,180"}
            return {"SRS": "EPSG:4326", "BBOX": "-180,-90,180,90"}

        def build(base, layer, ver, extra=None, drop=()):
            params = {
                "SERVICE": "WMS", "VERSION": ver, "REQUEST": "GetMap",
                "LAYERS": layer, "STYLES": "",
                "WIDTH": str(size), "HEIGHT": str(size),
                "FORMAT": "image/png", "TRANSPARENT": "TRUE",
            }
            params.update(axis_for(ver))
            if extra:
                params.update(extra)
            url = self._ogc_url(base, params)
            if drop:
                sp = urllib.parse.urlsplit(url)
                q = [(k, v) for k, v in urllib.parse.parse_qsl(sp.query)
                     if k.upper() not in drop]
                url = urllib.parse.urlunsplit(
                    (sp.scheme, sp.netloc, sp.path,
                     urllib.parse.urlencode(q), sp.fragment))
            return url

        keep = {k: v for k, v in existing.items()
                if k in ("TIME", "CQL_FILTER")}

        variants = [
            ("href completo, layer con workspace",
             build(href, layer_full, version)),
            ("href completo, layer SENZA workspace",
             build(href, layer_short, version)),
            ("senza cql_filter",
             build(href, layer_full, version, drop=("CQL_FILTER",))),
            ("senza time",
             build(href, layer_full, version, drop=("TIME",))),
            ("senza cql_filter e senza time",
             build(href, layer_full, version, drop=("CQL_FILTER", "TIME"))),
            ("endpoint pulito + time/cql reinseriti",
             build(base_clean, layer_full, version, extra=keep)),
            ("versione 1.1.1",
             build(href, layer_full, "1.1.1")),
            ("versione 1.3.0",
             build(href, layer_full, "1.3.0")),
            ("bbox ridotto (Italia)",
             build(href, layer_full, version,
                   extra={"BBOX": "6,36,19,47" if not version.startswith("1.3")
                          else "36,6,47,19"})),
        ]

        print(f"\n  {'VARIANTE':<42} {'HTTP':<6} {'BYTE':<9} ESITO")
        print("  " + "-" * 74)

        winners = []
        for label, url in variants:
            r = self.http.get(url, headers=self.auth.auth_header(),
                              max_bytes=2 * 1024 * 1024, accept="image/png, */*")
            ctype = (r.headers.get("content-type") or "").lower()
            if r.ok and "image" in ctype:
                verdict = "OK  immagine"
                winners.append((label, url))
            elif r.ok:
                verdict = "NOK ServiceException"
            else:
                verdict = f"NOK {classify_error(r, self.cfg.secrets)}"
            print(f"  {label:<42} {str(r.status or '-'):<6} {str(r.bytes):<9} {verdict}")
            if r.ok and "image" not in ctype:
                print(f"        {self._wms_exception_text(r)[:300]}")

        print()
        if winners:
            print("  VARIANTI FUNZIONANTI:")
            for label, url in winners:
                print(f"    - {label}")
            print(f"\n  URL della prima funzionante:\n    {winners[0][1]}")
            print("\n  -> se e' 'layer SENZA workspace', impostare nell'ini:")
            print("       OGC_STRIP_WORKSPACE = True")
            print("     se e' una variante senza cql_filter o time:")
            print("       OGC_DROP_PARAMS = cql_filter, time")
        else:
            print("  Nessuna variante restituisce un'immagine.")
            print("  Il layer potrebbe non essere pubblicato o richiedere")
            print("  parametri specifici: verificare su GeoServer, oppure")
            print("  provare OGC_MODE = capabilities (solo disponibilita' del")
            print("  servizio, senza trasferimento di dati).")

        print("\n" + "=" * 78 + "\n")
        return 0 if winners else 1

    # -- 5. access: metadati -------------------------------------------------

    def probe_file_metadata(self) -> Probe:
        url = self._api(f"/platformFiles/{self.download_file_id}",
                        {"projection": "detailedPlatformFile"})
        p = Probe("file_metadata", CAT_ACCESS, url, "Metadati del file e link di download")
        p.details["platform_file_id"] = self.download_file_id

        res = self.http.get(url, headers=self.auth.auth_header())
        if not res.ok:
            return self._add(p.mark_nok(
                http_error_text(res, self.cfg.secrets), res,
                response_body=body_snippet(res, self.cfg.secrets)))

        data = res.json() or {}
        dl = ((data.get("_links") or {}).get("download") or {}).get("href")
        p.details["download_href"] = dl
        return self._add(p.mark_ok(res,
                                   filename=data.get("filename"),
                                   file_type=data.get("type"),
                                   resto_id=data.get("restoId")))

    # -- 6. access: download -------------------------------------------------

    def probe_file_download(self) -> Probe:
        """
        Verifica l'accesso effettivo al contenuto del file.

        ATTENZIONE COSTI: su IRIDE CyberItaly il download e' tariffato (wallet a
        coin, HTTP 402 quando il credito e' esaurito). Una sonda in cron ogni 5
        minuti consuma ~288 coin al giorno. DOWNLOAD_MODE consente di scegliere
        quanto "peso" dare alla verifica:

          full   GET completo (fino a MAX_DOWNLOAD_BYTES). Massima fedelta',
                 costo pieno per ogni run.
          ranged GET con Range sui primi DOWNLOAD_RANGE_BYTES byte. Verifica che
                 il contenuto sia servito trasferendo pochi byte.
          head   solo HEAD: conferma che la risorsa e' accessibile senza
                 trasferire contenuto. Costo nullo, fedelta' minima.
        """
        mode = self.cfg.download_mode
        url = self._api(f"/platformFiles/{self.download_file_id}/dl")
        p = Probe("file_download", CAT_ACCESS, url,
                  f"Accesso al contenuto del file (modo: {mode})")
        p.details["platform_file_id"] = self.download_file_id
        p.details["download_mode"] = mode

        headers = dict(self.auth.auth_header())
        expected = self.download_expected_size
        self.dl_endpoint_calls += 1

        if mode == "head":
            self.content_requests += 1
            res = self.http.request("HEAD", url, headers=headers, accept="*/*")
            if res.ok:
                n = as_int(res.headers.get("content-length"), -1)
                return self._add(p.mark_ok(
                    res,
                    expected_bytes=expected,
                    content_length=n if n >= 0 else None,
                    downloaded_bytes=0,
                    billable=False,
                    content_type=res.headers.get("content-type")))
            return self._add(self._download_failed(p, res))

        if mode == "ranged":
            self.content_requests += 1
            want = max(1, self.cfg.download_range_bytes)
            headers["Range"] = f"bytes=0-{want - 1}"
            res = self.http.get(url, headers=headers, max_bytes=want,
                                accept="application/octet-stream, */*")
            if not res.ok:
                return self._add(self._download_failed(p, res))
            n = res.bytes
            if n < min(self.cfg.min_download_bytes, want):
                return self._add(p.mark_nok(
                    f"payload troppo piccolo ({n} byte richiesti {want})", res,
                    error_class="client", downloaded_bytes=n))
            throughput = (round(n / (res.latency_ms / 1000.0) / 1024.0, 2)
                          if res.latency_ms else None)
            return self._add(p.mark_ok(
                res,
                expected_bytes=expected,
                requested_bytes=want,
                downloaded_bytes=n,
                downloaded_kb=round(n / 1024.0, 2),
                partial=(res.status == 206),
                content_range=res.headers.get("content-range"),
                content_type=res.headers.get("content-type"),
                throughput_kbps=throughput))

        # --- mode == "full" -------------------------------------------------
        self.content_requests += 1
        res = self.http.get(url, headers=headers,
                            max_bytes=self.cfg.max_download_bytes,
                            accept="application/octet-stream, */*")
        if not res.ok:
            return self._add(self._download_failed(p, res))

        n = res.bytes
        declared = as_int(res.headers.get("content-length"), -1)

        if n > self.cfg.max_download_bytes:
            return self._add(p.mark_nok(
                f"file oltre il cap ({n} > {self.cfg.max_download_bytes} byte)",
                res, error_class="client",
                downloaded_bytes=n, content_length=declared))

        if n < self.cfg.min_download_bytes:
            return self._add(p.mark_nok(
                f"payload troppo piccolo ({n} byte, minimo "
                f"{self.cfg.min_download_bytes})",
                res, error_class="client",
                downloaded_bytes=n, content_length=declared))

        throughput = (round(n / (res.latency_ms / 1000.0) / 1024.0, 2)
                      if res.latency_ms else None)
        return self._add(p.mark_ok(
            res,
            expected_bytes=expected,
            size_matches_expected=(None if expected is None else n == expected),
            downloaded_bytes=n,
            downloaded_kb=round(n / 1024.0, 2),
            content_length=declared if declared >= 0 else None,
            content_type=res.headers.get("content-type"),
            billable=True,
            throughput_kbps=throughput))

    def _download_failed(self, p: Probe, res) -> Probe:
        """
        Marca il fallimento del download distinguendo il rifiuto per credito
        dal guasto del servizio.
        """
        klass = classify_error(res, self.cfg.secrets)
        msg = http_error_text(res, self.cfg.secrets)

        if klass == "entitlement":
            msg = (f"{msg} -> il servizio risponde ma nega l'accesso per "
                   f"credito/quota esaurita, non per indisponibilita'. "
                   f"Ricaricare il wallet dell'utenza di monitoraggio o "
                   f"chiederne l'esenzione; in alternativa usare "
                   f"DOWNLOAD_MODE = ranged oppure head.")
            if not self.cfg.billing_errors_as_nok:
                p.details.update(error_class=klass,
                                 billing_blocked=True,
                                 downgraded_note="BILLING_ERRORS_AS_NOK = False: "
                                                 "conteggiata come disponibile")
                p.error = msg
                return p.mark_ok(res, error_class=klass, billing_blocked=True)

        return p.mark_nok(msg, res, error_class=klass,
                          billing_blocked=(klass == "entitlement"),
                          response_body=body_snippet(res, self.cfg.secrets))

    def check_access(self, file_id=None, include_full=False) -> int:
        """
        Diagnostica: prova le diverse vie di accesso al contenuto di un file e
        riporta quale funziona e quale viene rifiutata (tipicamente per credito).

        Il GET completo e' TARIFFATO: viene eseguito solo con --include-full.
        """
        print("\n" + "=" * 78)
        print(" DIAGNOSI ACCESSO AL DOWNLOAD")
        print("=" * 78)

        token, res = self.auth.fetch_token(force=True)
        if not token:
            print(f"\n[FATAL] autenticazione fallita: "
                  f"{http_error_text(res, self.cfg.secrets)}")
            return 2
        print(f"\nUtenza    : {self.cfg.kc_username}  (realm {self.cfg.kc_realm})")

        # --- risoluzione del file da testare -------------------------------
        if not file_id:
            cached = self._cache_read()
            if cached:
                file_id = str(cached.get("platform_file_id"))
                print(f"File      : {file_id} (dalla cache di selezione)")
            else:
                candidates, r, _ = self._fetch_candidates()
                if candidates:
                    file_id = str(candidates[0].get("id"))
                    print(f"File      : {file_id} (primo candidato di parametricFind)")
        if not file_id:
            print("\n[FATAL] nessun file da testare: passare un ID con "
                  "--check-access ID")
            return 2

        h = self.auth.auth_header()
        pf_url = self._api(f"/platformFiles/{file_id}/dl")
        search_url = self._api(f"/search/dl/platform/{file_id}")

        # nome e tipo, se recuperabili
        meta = self.http.get(
            self._api(f"/platformFiles/{file_id}", {"projection": "detailedPlatformFile"}),
            headers=h)
        if meta.ok:
            m = meta.json() or {}
            print(f"Nome      : {m.get('filename')}")
            print(f"Tipo      : {m.get('type')}")
            link = ((m.get("_links") or {}).get("download") or {}).get("href")
            print(f"Link dich.: {link}")
        print()

        tests = [
            ("HEAD  /platformFiles/{id}/dl", "HEAD", pf_url, {}, None),
            ("GET   /platformFiles/{id}/dl  (Range 0-4095)", "GET", pf_url,
             {"Range": "bytes=0-4095"}, 4096),
            ("GET   /search/dl/platform/{id}  (Range 0-4095)", "GET", search_url,
             {"Range": "bytes=0-4095"}, 4096),
        ]
        if include_full:
            tests.append(("GET   /platformFiles/{id}/dl  COMPLETO  [TARIFFATO]",
                          "GET", pf_url, {}, self.cfg.max_download_bytes))
            tests.append(("GET   /search/dl/platform/{id}  COMPLETO  [TARIFFATO]",
                          "GET", search_url, {}, self.cfg.max_download_bytes))
        else:
            print("  NB: i GET completi sono esclusi perche' tariffati. "
                  "Aggiungere --include-full per provarli.\n")

        print(f"  {'PROVA':<50} {'HTTP':<6} {'BYTE':<10} ESITO")
        print("  " + "-" * 74)

        results = []
        for label, method, url, extra, cap in tests:
            hh = dict(h)
            hh.update(extra)
            hh["Accept"] = "*/*"
            r = self.http.request(method, url, headers=hh, max_bytes=cap, accept="*/*")
            klass = "-" if r.ok else classify_error(r, self.cfg.secrets)
            verdict = "OK" if r.ok else f"NOK ({klass})"
            print(f"  {label:<50} {str(r.status or '-'):<6} {str(r.bytes):<10} {verdict}")
            if not r.ok:
                snippet = body_snippet(r, self.cfg.secrets, 160)
                if snippet:
                    print(f"        {snippet}")
            results.append((label, r.ok, klass))

        # --- lettura dei risultati ------------------------------------------
        print()
        billing = [l for l, ok, k in results if not ok and k == "entitlement"]
        working = [l for l, ok, _ in results if ok]

        # la domanda che decide la configurazione del KPI
        ranged_billed = any("Range" in l and k == "entitlement"
                            for l, ok, k in results if not ok)
        head_ok = any("HEAD" in l for l, ok, _ in results if ok)
        print("  VERDETTO SULLA MODALITA' DA USARE")
        if ranged_billed:
            print("    Le richieste Range sono TARIFFATE come i download completi.")
            print("    DOWNLOAD_MODE = ranged non aiuta.")
            if head_ok:
                print("    -> usare DOWNLOAD_MODE = head, oppure risolvere "
                      "l'entitlement dell'utenza.")
            else:
                print("    -> nemmeno HEAD passa: serve risolvere l'entitlement.")
        elif any("Range" in l for l, ok, _ in results if ok):
            print("    Le richieste Range NON sono tariffate: "
                  "DOWNLOAD_MODE = ranged e' sostenibile in cron.")
        elif head_ok:
            print("    Solo HEAD risponde: usare DOWNLOAD_MODE = head.")
        print()

        if working:
            print("  Vie di accesso funzionanti:")
            for l in working:
                print(f"    - {l}")
        if billing:
            print("\n  Rifiutate per credito/quota (il servizio e' comunque attivo):")
            for l in billing:
                print(f"    - {l}")
            print("\n  -> Confrontare il saldo del wallet di questa utenza con quello")
            print("     di un'utenza interattiva: se differiscono, il problema e'")
            print("     l'entitlement dell'utenza di monitoraggio, non la piattaforma.")
        if not working:
            print("  Nessuna via di accesso funzionante con questa utenza.")

        print("\n" + "=" * 78 + "\n")
        return 0 if working else 1

    # -- discovery (--discover) ---------------------------------------------

    def discover(self) -> int:
        """
        Interroga la piattaforma per stampare i valori da mettere nell'ini:
          GET /search/parameter/catalogue -> valori ammessi per CATALOGUE
          GET /search/parameters          -> elenco completo dei parametri
          GET /collections                -> identifier delle collection
        Non emette eventi: serve solo a configurare il collector.
        """
        print("\n" + "=" * 74)
        print(" DISCOVERY parametri catalogo Insula")
        print("=" * 74)

        token, res = self.auth.fetch_token(force=True)
        if not token:
            print(f"\n[FATAL] autenticazione fallita: "
                  f"{http_error_text(res, self.cfg.secrets)}")
            return 2
        print(f"\n[OK] token Keycloak ottenuto ({res.latency_ms} ms)")
        h = self.auth.auth_header()

        # --- valori ammessi per 'catalogue' -------------------------------
        print("\n--- Valori ammessi per CATALOGUE "
              "(GET /search/parameter/catalogue) ---")
        r = self.http.get(self._api("/search/parameter/catalogue"), headers=h)
        if r.ok:
            d = r.json() or {}
            values = ((d.get("allowed") or {}).get("values")) or []
            if values:
                for v in values:
                    extra = v.get("parameterName")
                    print(f"  CATALOGUE = {v.get('value')}"
                          f"{'   [' + str(v.get('title')) + ']' if v.get('title') else ''}")
                    if extra:
                        print(f"      -> richiede il parametro aggiuntivo: {extra}")
                print(f"\n  default piattaforma: {d.get('defaultValue')}")
            else:
                print("  nessun valore dichiarato; payload grezzo:")
                print("  " + json.dumps(d)[:800])
        else:
            print(f"  [NOK] {http_error_text(r, self.cfg.secrets)}")

        # --- parametri di ricerca -----------------------------------------
        print("\n--- Parametri di ricerca disponibili "
              "(GET /search/parameters) ---")
        conditional_required = {}   # nome parametro -> lista cataloghi che lo richiedono
        r = self.http.get(self._api("/search/parameters", {"resolveAll": "false"}),
                          headers=h)
        if r.ok:
            d = r.json() or {}
            for k in sorted(d.keys()):
                meta = d[k] if isinstance(d[k], dict) else {}
                req = " [OBBLIGATORIO]" if meta.get("required") else ""
                only_if = meta.get("onlyIf")
                cond = f"  (solo se {json.dumps(only_if)})" if only_if else ""
                print(f"  {k}{req}{cond}")
                if meta.get("required") and isinstance(only_if, dict):
                    cats = only_if.get("catalogue")
                    if cats:
                        conditional_required[k] = cats if isinstance(cats, list) else [cats]
        else:
            print(f"  [NOK] {http_error_text(r, self.cfg.secrets)}")

        # --- valori ammessi dei parametri obbligatori condizionali ---------
        if conditional_required:
            print("\n--- Valori ammessi per i parametri obbligatori "
                  "(GET /search/parameter/{nome}) ---")
        for param, cats in sorted(conditional_required.items()):
            ini_key = next((k for c, (pp, k) in REQUIRED_BY_CATALOGUE.items()
                            if pp == param), None)
            print(f"\n  [{param}]  richiesto da CATALOGUE = {', '.join(cats)}"
                  f"{'   ->  ini: ' + ini_key if ini_key else ''}")
            rp = self.http.get(self._api(f"/search/parameter/{param}"), headers=h)
            if not rp.ok:
                print(f"    [NOK] {http_error_text(rp, self.cfg.secrets)}")
                continue
            dp = rp.json() or {}
            values = ((dp.get("allowed") or {}).get("values")) or []
            if not values:
                print("    nessun valore enumerato (parametro a testo libero)")
                continue
            for v in values[:40]:
                title = v.get("title")
                print(f"    {ini_key or param} = {v.get('value')}"
                      f"{'   [' + str(title) + ']' if title else ''}")
            if len(values) > 40:
                print(f"    ... e altri {len(values) - 40} valori")
            if dp.get("defaultValue"):
                print(f"    default: {dp.get('defaultValue')}")

        # --- collection disponibili ----------------------------------------
        print("\n--- Collection disponibili (GET /collections) ---")
        r = self.http.get(self._api("/collections",
                                    {"projection": "shortCollection", "size": 500}),
                          headers=h)
        if r.ok:
            d = r.json() or {}
            cols = ((d.get("_embedded") or {}).get("collections")) or []
            by_type = {}
            for c in cols:
                by_type.setdefault(str(c.get("fileType")), []).append(c)

            print(f"  totale restituite: {len(cols)} "
                  f"(totalElements dichiarato: {(d.get('page') or {}).get('totalElements')})")
            for ftype in sorted(by_type):
                group = by_type[ftype]
                print(f"\n  fileType = {ftype}  ({len(group)} collection)")
                for c in group[:15]:
                    pt = f"  productsType={c.get('productsType')}" if c.get("productsType") else ""
                    print(f"    id={str(c.get('id')):<6} {c.get('identifier')}{pt}")
                if len(group) > 15:
                    print(f"    ... e altre {len(group) - 15}")

            print("\n  -> per CATALOGUE = PLATFORM_PRODUCTS servono le collection "
                  "con fileType = OUTPUT_PRODUCT")
            print("  -> per CATALOGUE = REF_DATA servono quelle con "
                  "fileType = REFERENCE_DATA")
            print("  -> copiare il campo 'identifier' (non l'id) nell'ini")
        else:
            print(f"  [NOK] {http_error_text(r, self.cfg.secrets)}")

        print("\n" + "=" * 74 + "\n")
        return 0

    def inspect_param(self, name: str) -> int:
        """Stampa lo schema completo di un singolo parametro di ricerca."""
        token, res = self.auth.fetch_token(force=True)
        if not token:
            print(f"[FATAL] autenticazione fallita: "
                  f"{http_error_text(res, self.cfg.secrets)}")
            return 2
        url = self._api(f"/search/parameter/{name}")
        r = self.http.get(url, headers=self.auth.auth_header())
        print(f"\nGET {url}\n")
        if not r.ok:
            print(f"[NOK] {http_error_text(r, self.cfg.secrets)}")
            return 1
        print(json.dumps(r.json(), indent=2, ensure_ascii=False))
        print()
        return 0

    # -- orchestrazione -----------------------------------------------------

    def run(self):
        started = utc_now()
        t0 = time.perf_counter()

        log.info("=" * 74)
        log.info("KPI availability catalogo/accesso  v%s  run_id=%s",
                 SCRIPT_VERSION, self.run_id)
        log.info("Script          : %s", os.path.abspath(__file__))
        log.info("Config          : %s", os.path.abspath(self.cfg.path))
        log.info("Endpoint Insula : %s", self.cfg.insula_base)
        log.info("Keycloak        : %s realm=%s utenza=%s",
                 self.cfg.kc_url, self.cfg.kc_realm, self.cfg.kc_username)
        log.info("Catalogo        : %s", self.cfg.catalogue)
        log.info("Download        : modo=%s%s  cap=%d byte",
                 self.cfg.download_mode,
                 f" ({self.cfg.download_range_bytes} byte)"
                 if self.cfg.download_mode == "ranged" else "",
                 self.cfg.max_download_bytes)
        log.info("=" * 74)

        req_param, req_key, req_value = self.cfg.catalogue_required_param()
        if req_param and not req_value:
            log.warning("CATALOGUE = %s richiede il parametro obbligatorio '%s', "
                        "ma [INSULA_KPI] %s e' vuoto. Insula non risponde 400 ma "
                        "HTTP 500. Eseguire --discover per i valori ammessi.",
                        self.cfg.catalogue, req_param, req_key)
        elif req_param:
            log.info("Catalogo %s con %s=%s", self.cfg.catalogue, req_param, req_value)
        if not self.cfg.download_file_id:
            log.info("File di prova in selezione dinamica: il piu' piccolo entro "
                     "%d byte fra i primi %d candidati%s.",
                     self.cfg.max_download_bytes, self.cfg.candidate_pool_size,
                     ", con cache della scelta" if self.cfg.use_selection_cache else "")

        if (self.cfg.wallet_autorecharge
                and self.cfg.wallet_max_per_day is None
                and self.cfg.wallet_max_amount_per_day is None):
            log.warning("Ricarica automatica attiva SENZA tetti giornalieri: "
                        "un errore o un cambio di tariffazione lato piattaforma "
                        "puo' produrre ricariche senza limite. Monitorare "
                        "l'indice %s sull'evento insula_wallet_recharge.",
                        self.cfg.kpi_index)

        if self.cfg.download_mode == "full":
            per_day = 1440 // max(1, self.cfg.assumed_cron_minutes)
            log.info("DOWNLOAD_MODE = full: scarica il prodotto per intero "
                     "(verifica di accesso completa). Costo ~1 coin/run, "
                     "~%d coin/giorno con cron ogni %d minuti, coperti dalla "
                     "ricarica automatica%s.",
                     per_day, self.cfg.assumed_cron_minutes,
                     "" if self.cfg.wallet_autorecharge else " (DISATTIVATA)")

        auth_probe = self.probe_auth()

        if auth_probe.status != STATUS_OK:
            # senza token nessuna chiamata puo' avere senso: si marcano NOK tutte
            log.error("Autenticazione fallita: le sonde successive sono NOK per dipendenza.")
            for name, cat, desc in (
                ("catalogue_params", CAT_CATALOGUE, "Descrittore parametri catalogo"),
                ("catalogue_search", CAT_CATALOGUE, "Query sul catalogo prodotti"),
                ("file_lookup", CAT_ACCESS, "Ricerca del file da scaricare"),
                ("file_metadata", CAT_ACCESS, "Metadati del file"),
                ("file_download", CAT_ACCESS, "Scaricamento del file"),
                ("jobs_search", CAT_PROCESSING, "Ricerca job di processing"),
                ("collections_list", CAT_COLLECTIONS, "Elenco collezioni"),
            ):
                self._add(Probe(name, cat, None, desc).mark_nok(
                    "non eseguita: autenticazione Keycloak fallita"))
        else:
            self.probe_wallet()
            self.probe_catalogue_parameters()
            self.probe_catalogue_search()
            if self.cfg.probe_jobs_enabled:
                self.probe_jobs()
            if self.cfg.probe_collections_enabled:
                self.probe_collections()

            lookup = self.probe_file_lookup()
            if self.download_file_id:
                self.probe_file_metadata()
                self.probe_file_download()
            else:
                for name, desc in (("file_metadata", "Metadati del file"),
                                   ("file_download", "Scaricamento del file")):
                    self._add(Probe(name, CAT_ACCESS, None, desc).mark_nok(
                        "non eseguita: nessun platformFile individuato "
                        f"({lookup.error or 'lookup fallito'})"))

            self.run_ogc_probes()

        elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
        return self.build_events(started, utc_now(), elapsed_ms)

    # -- costruzione eventi --------------------------------------------------

    def _envelope(self, ts: datetime, event_type: str, started: datetime, ended: datetime):
        """Campi comuni a tutti gli eventi (schema allineato al collector jobs)."""
        return {
            "@timestamp": iso_ms(ts),
            "event_timestamp": iso_ms(ts),
            "platform": PLATFORM_TAG,
            "service_provider_log": SERVICE_PROVIDER_LOG,
            "event_type": event_type,
            "hostname": self.hostname,
            "endpoint": self.cfg.insula_base,
            "kpi": "availability_data_catalogue_and_access_services",
            "collector_version": SCRIPT_VERSION,
            "run_id": self.run_id,
            "window_start": iso_ms(started),
            "window_end": iso_ms(ended),
        }

    def build_events(self, started: datetime, ended: datetime, elapsed_ms: float):
        warn, crit = self.cfg.latency_warn_ms, self.cfg.latency_crit_ms
        events = []

        # --- eventi per singola sonda -------------------------------------
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

        # --- aggregato di disponibilita' ----------------------------------
        executed = [p for p in self.probes if not p.skipped]
        by_cat = {}
        for p in self.probes:
            by_cat.setdefault(p.category, []).append(p)

        def cat_status(*cats):
            pool = [p for c in cats for p in by_cat.get(c, []) if not p.skipped]
            if not pool:
                return STATUS_NOK
            return STATUS_OK if all(p.status == STATUS_OK for p in pool) else STATUS_NOK

        catalogue_status = cat_status(CAT_AUTH, CAT_CATALOGUE)
        access_status = cat_status(CAT_AUTH, CAT_ACCESS)

        proc_pool = [p for p in by_cat.get(CAT_PROCESSING, []) if not p.skipped]
        processing_status = (STATUS_OK if all(p.status == STATUS_OK for p in proc_pool)
                             else STATUS_NOK) if proc_pool else None
        coll_pool = [p for p in by_cat.get(CAT_COLLECTIONS, []) if not p.skipped]
        collections_status = (STATUS_OK if all(p.status == STATUS_OK for p in coll_pool)
                              else STATUS_NOK) if coll_pool else None

        wallet_pool = [p for p in by_cat.get("wallet", []) if not p.skipped]
        ogc_pool = [p for p in by_cat.get(CAT_OGC, []) if not p.skipped]
        ogc_status = (STATUS_OK if all(p.status == STATUS_OK for p in ogc_pool)
                      else STATUS_NOK) if ogc_pool else None

        overall = STATUS_OK if (catalogue_status == STATUS_OK and
                                access_status == STATUS_OK) else STATUS_NOK
        if self.cfg.ogc_counts_in_kpi and ogc_status == STATUS_NOK:
            overall = STATUS_NOK
        if processing_status == STATUS_NOK:
            overall = STATUS_NOK
        if collections_status == STATUS_NOK:
            overall = STATUS_NOK

        n_ok = sum(1 for p in executed if p.status == STATUS_OK)
        n_nok = len(executed) - n_ok
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
            "overall_status": overall,
            "overall_available": overall == STATUS_OK,
            "availability_pct": 100.0 if overall == STATUS_OK else 0.0,
            "catalogue_status": catalogue_status,
            "catalogue_available": catalogue_status == STATUS_OK,
            "access_status": access_status,
            "access_available": access_status == STATUS_OK,
            "processing_status": processing_status,
            "processing_available": (None if processing_status is None
                                     else processing_status == STATUS_OK),
            "jobs_total": det("jobs_search", "total_elements"),
            "jobs_latency_ms": lat("jobs_search"),
            "collections_status": collections_status,
            "collections_available": (None if collections_status is None
                                      else collections_status == STATUS_OK),
            "collections_total": det("collections_list", "total_elements"),
            "collections_latency_ms": lat("collections_list"),
            "ogc_status": ogc_status,
            "ogc_available": (None if ogc_status is None
                              else ogc_status == STATUS_OK),
            "ogc_counts_in_kpi": self.cfg.ogc_counts_in_kpi,
            "wms_capabilities_latency_ms": lat("wms_capabilities"),
            "wms_getmap_latency_ms": lat("wms_getmap"),
            "wms_getmap_bytes": det("wms_getmap", "image_bytes"),
            "wms_layer": det("wms_getmap", "layer"),

            "catalogue_search_latency_ms": lat("catalogue_search"),
            "catalogue_params_latency_ms": lat("catalogue_params"),
            "file_metadata_latency_ms": lat("file_metadata"),
            "file_download_latency_ms": lat("file_download"),
            "auth_latency_ms": lat("auth"),
            "total_elapsed_ms": elapsed_ms,
            "max_latency_ms": max(latencies) if latencies else None,

            "catalogue": self.cfg.catalogue,
            "catalogue_param": det("catalogue_search", "catalogue_param"),
            "catalogue_param_value": det("catalogue_search", "catalogue_param_value"),
            "catalogue_total_elements": det("catalogue_search", "total_elements"),
            "catalogue_n_returned": det("catalogue_search", "n_returned"),

            "platform_file_id": self.download_file_id,
            "file_selection_mode": det("file_lookup", "selection_mode"),
            "download_mode": det("file_download", "download_mode"),
            "content_requests": self.content_requests,
            "wallet_id": self.wallet_id_resolved,
            "wallet_balance": self.wallet_balance,
            "wallet_balance_low": (None if self.wallet_balance is None
                                   else self.wallet_balance
                                   <= self.cfg.wallet_low_threshold),
            "wallet_recharged": bool(self.wallet_recharge_result
                                     and self.wallet_recharge_result.get("ok")),
            "wallet_cap_reached": self.wallet_capped,
            "wallet_tx_read": (self.wallet_tx_summary or {}).get("n_transactions_read"),
            "wallet_tx_debit": (self.wallet_tx_summary or {}).get("sum_debit"),
            "wallet_tx_credit": (self.wallet_tx_summary or {}).get("sum_credit"),
            "billable_dl_calls": self.dl_endpoint_calls,
            "billing_blocked": bool(det("file_download", "billing_blocked")),
            "download_error_class": det("file_download", "error_class"),
            "file_selected_name": det("file_lookup", "filename"),
            "downloaded_bytes": det("file_download", "downloaded_bytes"),
            "downloaded_kb": det("file_download", "downloaded_kb"),
            "throughput_kbps": det("file_download", "throughput_kbps"),

            "n_probes": len(executed),
            "n_probes_ok": n_ok,
            "n_probes_nok": n_nok,
            "failed_probes": failed or None,
            "probe_status": {p.name: p.status for p in self.probes},
            "latency_status": ("CRITICAL" if any(
                (p.latency_ms or 0) >= crit for p in executed)
                else "WARNING" if any((p.latency_ms or 0) >= warn for p in executed)
                else "NORMAL"),
        })
        if self.wallet_recharge_result:
            rec = self._envelope(ended, "insula_wallet_recharge", started, ended)
            rec.update(self.wallet_recharge_result)
            rec["wallet_autorecharge"] = True
            events.append(rec)

        events.append(agg)

        log.info("-" * 74)
        log.info("Richieste sul contenuto dei file (potenzialmente tariffate): %d",
                 self.content_requests)
        log.info("ESITO KPI  overall=%s  catalogue=%s  access=%s%s%s%s  "
                 "(%d/%d sonde OK)",
                 overall, catalogue_status, access_status,
                 f"  processing={processing_status}" if processing_status else "",
                 f"  collections={collections_status}" if collections_status else "",
                 f"  ogc={ogc_status}" if ogc_status else "",
                 n_ok, len(executed))
        if self.dl_endpoint_calls:
            log.info("Chiamate a /dl in questo run: %d "
                     "(tariffate: ~%d coin, ~%d al giorno con cron ogni %d minuti)",
                     self.dl_endpoint_calls, self.dl_endpoint_calls,
                     self.dl_endpoint_calls * (1440 // max(1, self.cfg.assumed_cron_minutes)),
                     self.cfg.assumed_cron_minutes)
        if failed:
            log.warning("Sonde fallite: %s", ", ".join(failed))
        log.info("-" * 74)

        return events


# ============================================================================
# OUTPUT: FILE JSON + ELASTICSEARCH
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

    url = f"{cfg.monitoring_url}/{cfg.kpi_index}/_bulk"
    http = HttpClient(timeout=cfg.http_timeout, retries=cfg.retries,
                      backoff=cfg.retry_backoff_s,
                      verify_certs=cfg.monitoring_verify_certs,
                      use_system_proxy=cfg.use_system_proxy,
                      secrets=cfg.secrets)

    res = http.request(
        "POST", url, data=payload,
        headers={"Content-Type": "application/x-ndjson",
                 "Authorization": f"ApiKey {cfg.monitoring_apikey}"},
    )

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
        description="KPI availability catalogo dati e servizi di accesso (Insula IRIDE)")
    ap.add_argument("-c", "--config", default=DEFAULT_CONFIG,
                    help=f"Percorso del file ini (default: {DEFAULT_CONFIG})")
    ap.add_argument("-n", "--dry-run", action="store_true",
                    help="Esegue le sonde ma non spedisce su Elasticsearch")
    ap.add_argument("-v", "--verbose", action="store_true",
                    help="Log di debug anche su console")
    ap.add_argument("--print-events", action="store_true",
                    help="Stampa gli eventi JSON su stdout")
    ap.add_argument("-V", "--version", action="version",
                    version=f"{SCRIPT_NAME} {SCRIPT_VERSION}")
    ap.add_argument("--wms-debug", action="store_true",
                    help="Prova varianti di GetMap sul layer selezionato e "
                         "riporta quale funziona (prefisso workspace, "
                         "cql_filter, time, bbox, versione WMS).")
    ap.add_argument("--tx-debug", action="store_true",
                    help="Stampa transactions grezze del wallet per ricavare il "
                         "nome del campo importo. Sola lettura.")
    ap.add_argument("--wallet-info", action="store_true",
                    help="Ricognizione in sola lettura degli endpoint wallet: "
                         "stampa saldo e struttura delle risposte. "
                         "Non esegue ricariche.")
    ap.add_argument("--check-access", metavar="ID", nargs="?", const="",
                    help="Diagnostica le vie di accesso al download di un file "
                         "(HEAD, Range, rotta alternativa /search/dl/platform). "
                         "Senza ID usa quello in cache. I GET completi sono "
                         "esclusi salvo --include-full perche' tariffati.")
    ap.add_argument("--include-full", action="store_true",
                    help="Con --check-access, include anche i GET completi. "
                         "ATTENZIONE: consumano credito del wallet.")
    ap.add_argument("--param", metavar="NOME",
                    help="Ispeziona un singolo parametro di ricerca "
                         "(GET /search/parameter/NOME) e ne stampa lo schema "
                         "completo. Es: --param productDate")
    ap.add_argument("-d", "--discover", action="store_true",
                    help="Non esegue il KPI: interroga la piattaforma e stampa i "
                         "valori ammessi per CATALOGUE, i parametri di ricerca e "
                         "le collection disponibili, da riportare nell'ini")
    return ap.parse_args()


def main() -> int:
    args = parse_args()

    try:
        cfg = Config(args.config)
    except (FileNotFoundError, ValueError) as e:
        print(f"[FATAL] {e}", file=sys.stderr)
        return 2

    setup_logging(cfg.log_dir, args.verbose)
    log.debug("Configurazione caricata da %s", cfg.path)

    if args.dry_run:
        cfg.elastic_enabled = False
        log.info("DRY-RUN attivo: nessuna scrittura su Elasticsearch.")

    collector = InsulaKpiCollector(cfg)

    if args.wms_debug:
        try:
            return collector.wms_debug()
        except Exception as e:
            log.exception("Diagnosi WMS fallita: %s", redact(str(e), cfg.secrets))
            return 3

    if args.tx_debug:
        try:
            return collector.tx_debug()
        except Exception as e:
            log.exception("tx-debug fallito: %s", redact(str(e), cfg.secrets))
            return 3

    if args.wallet_info:
        try:
            return collector.wallet_info()
        except Exception as e:
            log.exception("Ricognizione wallet fallita: %s", redact(str(e), cfg.secrets))
            return 3

    if args.check_access is not None:
        try:
            return collector.check_access(args.check_access or None,
                                          include_full=args.include_full)
        except Exception as e:
            log.exception("Diagnosi accesso fallita: %s", redact(str(e), cfg.secrets))
            return 3

    if args.param:
        try:
            return collector.inspect_param(args.param)
        except Exception as e:
            log.exception("Ispezione parametro fallita: %s", redact(str(e), cfg.secrets))
            return 3

    if args.discover:
        try:
            return collector.discover()
        except Exception as e:
            log.exception("Discovery fallita: %s", redact(str(e), cfg.secrets))
            return 3

    try:
        events = collector.run()
    except Exception as e:  # non deve mai morire in cron senza lasciare traccia
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

    log.info("Uscita con codice %d (0=OK, 1=KPI NOK, 4=errore spedizione Elastic)",
             exit_code)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

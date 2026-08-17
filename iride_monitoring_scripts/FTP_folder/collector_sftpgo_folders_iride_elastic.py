"""
collector_sftpgo_folders_iride_elastic.py
==========================================
Collector dell'inventario completo delle cartelle e dei file nel sink
SFTPGo IRIDE CyberItaly (gestito da MEEO).

Usa la REST API di SFTPGo (validata su CIMS-42, refactor finale 25/06/2026
dopo iterazioni multiple sulla discovery del pattern d'autenticazione
corretto su SFTPGo 2.6.6):
  - GET  /api/v2/token              (Basic Auth admin → JWT, per gestione)
  - GET  /api/v2/users              (lista utenti)
  - POST /api/v2/quotas/users/{u}/scan (forza ricalcolo quota — opzionale)
  - GET  /api/v2/users/{u}          (refresh dati utente con used_quota)
  - GET  /api/v2/user/dirs          (lista contenuto cartella)
                                     header: X-SFTPGO-API-KEY: <api_key>
                                     API key user-scope (scope=2) legata
                                     all'utente target via campo "user".

Pattern API key (refactor 25/06/2026):
  Generata via POST /api/v2/apikeys con scope=2 e user="<username>".
  La key e' "bound" all'utente, NIENTE suffisso .username nell'header.
  Pattern alternativo (admin-scope + impersonation via .username suffisso)
  NON funziona su 2.6.6 nonostante la doc lo suggerisca.

Output: 2 tipi di documento JSON DESP-style:
  1. event_type="sftpgo_user_inventory" — 1 doc per utente con:
       used_quota_size, used_quota_files, last_quota_update,
       used_size_per_subfolder, top-level folder structure,
       volume_pct_used / volume_free_bytes (vedi nota capacita' sotto)
  2. event_type="sftpgo_folder_node" — 1 doc per ciascuna cartella
     trovata nel filesystem (anche annidate) con:
       path, file_count, total_bytes, oldest_file_mtime, newest_file_mtime,
       freshness_minutes, oldest_file_age_days, retention_breach

Indici Elastic dedicati:
  metrics-iride-sftpgo-inventory.monitoring-default

Pattern d'uso tipico:
  Ogni 30 min in cron → vedi crescita upload ISPRA + struttura folder.

------------------------------------------------------------------
NOTA CAPACITA' VOLUME (aggiunta 03/08/2026, post-incidente CIMS)
------------------------------------------------------------------
Il campo quota_pct_used viene calcolato SOLO se l'utente ha una
quota_size > 0 impostata in SFTPGo. Sull'utente "ispra" la quota e'
illimitata (quota_size=0), quindi quota_pct_used resta sempre null e
nessun alert puo' scattare su quel campo.

Il vincolo reale non e' la quota utente ma la CAPACITA' DEL FILESYSTEM
del PVC sottostante, che la REST API di SFTPGo non espone: SFTPGo
gestisce le quote logiche degli utenti, non lo spazio del volume. Il
02/08/2026 il volume si e' saturato al 100% causando il fallimento di
tutti gli upload ISPRA senza che nessun alert scattasse.

Il dato viene quindi rilevato da fuori, con cascata a tre livelli
(VOLUME_CAPACITY_MODE nell'INI):
  1. kubectl exec <pod sftpgo> -- df -k -P /var/lib/sftpgo
     Fonte preferita: restituisce capacita', usato e disponibile REALI,
     piu' accurati dei valori derivati da used_quota_size perche'
     includono overhead del filesystem e file non contabilizzati.
  2. cache su disco (.volume_capacity_cache.json), per coprire i run in
     cui kubectl non e' raggiungibile. Solo la capacita' viene riusata:
     usato e disponibile cambiano a ogni run e riproporli sarebbe
     fuorviante.
  3. VOLUME_CAPACITY_BYTES statico dall'INI, come ultima rete.

Il campo volume_capacity_source nel doc dice sempre da quale livello
arriva il valore; volume_metric_basis distingue metriche reali (df) da
metriche derivate (used_quota).

Requisito: kubectl configurato e raggiungibile dall'host di collection
(es. ci-mon-dash-01). Se il collector gira altrove, impostare
VOLUME_CAPACITY_MODE = static e valorizzare VOLUME_CAPACITY_BYTES.

------------------------------------------------------------------
NOTA ETA' ARCHIVIO (aggiunta 03/08/2026)
------------------------------------------------------------------
Il rolling di cancellazione dei file vecchi in MANAGED/NEW dovrebbe
essere gestito da un componente esterno MEEO, ma alla data odierna NON
RISULTA CONFIGURATA ALCUNA RETENTION POLICY: nessun file e' mai stato
cancellato dall'avvio del flusso (02/07/2026), il che ha causato la
saturazione del volume.

RETENTION_DAYS nell'INI NON e' quindi una policy concordata, ma una
soglia interna di allarme che rappresenta la finestra operativa
indicata (14 giorni). Il campo oldest_file_age_days e' un dato
osservato; retention_breach segnala che l'eta' del file piu' vecchio
ha superato tale soglia, ossia che nessuna cancellazione sta avvenendo.

Quando MEEO fornira' la configurazione effettiva, allineare
RETENTION_DAYS alla finestra reale.
"""

import sys
import os
import json
import socket
import time
import logging
import subprocess
import configparser
from datetime import datetime, timezone, timedelta

import requests


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


# ============================================================
# Config
# ============================================================
base_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_dir, 'sftpgo_folders_iride_elastic.ini')
config = configparser.ConfigParser()
config.read(config_path)

ELASTIC_ENABLED = config.getboolean('CONFIG', 'ELASTIC_ENABLED', fallback=False)
MONITORING_URL = config.get('CONFIG', 'MONITORING_URL', fallback='')
MONITORING_APIKEY = config.get('CONFIG', 'MONITORING_APIKEY', fallback='')
MONITORING_VERIFY_CERTS = config.getboolean(
    'CONFIG', 'MONITORING_VERIFY_CERTS', fallback=True)
INVENTORY_INDEX = config.get(
    'CONFIG', 'INVENTORY_INDEX',
    fallback='metrics-iride-sftpgo-inventory.monitoring-default')

SFTPGO_BASE_URL = config.get('SFTPGO', 'BASE_URL')
SFTPGO_USERNAME = config.get('SFTPGO', 'USERNAME')
SFTPGO_PASSWORD = config.get('SFTPGO', 'PASSWORD')

# API Key user-scope legata all'utente target via campo "user" nel POST.
# Generata via POST /api/v2/apikeys con scope=2 e user="<username>".
# Va usata SENZA suffisso .username nell'header (la key e' gia' bound).
# Vedi CIMS-42 + nota refactor 25/06/2026.
SFTPGO_API_KEY = config.get('SFTPGO', 'API_KEY', fallback='')

TRIGGER_QUOTA_SCAN = config.getboolean(
    'SFTPGO_INVENTORY', 'TRIGGER_QUOTA_SCAN', fallback=True)
QUOTA_SCAN_WAIT_SECONDS = config.getint(
    'SFTPGO_INVENTORY', 'QUOTA_SCAN_WAIT_SECONDS', fallback=5)
WALK_FILESYSTEM = config.getboolean(
    'SFTPGO_INVENTORY', 'WALK_FILESYSTEM', fallback=True)
MAX_DEPTH = config.getint('SFTPGO_INVENTORY', 'MAX_DEPTH', fallback=10)
MAX_FOLDERS = config.getint('SFTPGO_INVENTORY', 'MAX_FOLDERS', fallback=500)
HTTP_TIMEOUT = config.getint('SFTPGO_INVENTORY', 'HTTP_TIMEOUT', fallback=30)
FRESHNESS_OK_MINUTES = config.getint(
    'SFTPGO_INVENTORY', 'FRESHNESS_OK_MINUTES', fallback=60)
FRESHNESS_STALE_MINUTES = config.getint(
    'SFTPGO_INVENTORY', 'FRESHNESS_STALE_MINUTES', fallback=1440)

# --- Capacita' volume (post-incidente 02/08/2026) -----------------
# Modalita' di rilevamento della capacita' del filesystem del PVC:
#   auto    = prova kubectl, in fallback usa cache o VOLUME_CAPACITY_BYTES
#   kubectl = solo kubectl (se fallisce, nessuna metrica volume)
#   static  = solo VOLUME_CAPACITY_BYTES dall'INI
VOLUME_CAPACITY_MODE = config.get(
    'SFTPGO_INVENTORY', 'VOLUME_CAPACITY_MODE', fallback='auto').strip().lower()

# Valore di fallback in byte, usato in modalita' static o quando kubectl
# non e' disponibile. 0 = disabilitato.
VOLUME_CAPACITY_BYTES = config.getint(
    'SFTPGO_INVENTORY', 'VOLUME_CAPACITY_BYTES', fallback=0)

# Parametri per la discovery via kubectl.
KUBECTL_BIN = config.get(
    'SFTPGO_INVENTORY', 'KUBECTL_BIN', fallback='kubectl')
K8S_NAMESPACE = config.get(
    'SFTPGO_INVENTORY', 'K8S_NAMESPACE', fallback='adam-ftp')
K8S_POD_SELECTOR = config.get(
    'SFTPGO_INVENTORY', 'K8S_POD_SELECTOR',
    fallback='app.kubernetes.io/name=sftpgo')
VOLUME_MOUNT_PATH = config.get(
    'SFTPGO_INVENTORY', 'VOLUME_MOUNT_PATH', fallback='/var/lib/sftpgo')
KUBECTL_TIMEOUT = config.getint(
    'SFTPGO_INVENTORY', 'KUBECTL_TIMEOUT', fallback=30)
# Validita' della cache su disco, in minuti. La capacita' di un PVC
# cambia solo in caso di espansione, quindi una cache lunga e' sicura e
# copre i run in cui kubectl non e' raggiungibile.
VOLUME_CAPACITY_CACHE_MINUTES = config.getint(
    'SFTPGO_INVENTORY', 'VOLUME_CAPACITY_CACHE_MINUTES', fallback=1440)
VOLUME_CACHE_FILE = os.path.join(base_dir, '.volume_capacity_cache.json')

# --- Soglia eta' archivio (post-incidente 02/08/2026) ------------
# NON e' una retention policy concordata: alla data odierna MEEO non ne
# ha configurata alcuna. E' la soglia interna di allarme, pari alla
# finestra operativa indicata (14 giorni). Se il file piu' vecchio la
# supera (+ RETENTION_GRACE_DAYS di tolleranza) viene alzato
# retention_breach=true, che segnala che nessuna cancellazione avviene.
RETENTION_DAYS = config.getint(
    'SFTPGO_INVENTORY', 'RETENTION_DAYS', fallback=14)
# Tolleranza per evitare falsi positivi quando il rolling gira una
# volta al giorno e il file piu' vecchio e' appena oltre soglia.
RETENTION_GRACE_DAYS = config.getfloat(
    'SFTPGO_INVENTORY', 'RETENTION_GRACE_DAYS', fallback=1.0)
# Cartelle su cui applicare il controllo retention. Match per
# sottostringa sul path, case-insensitive, lista separata da virgole.
# Vuoto = controllo applicato a tutte le cartelle.
RETENTION_PATHS = [
    p.strip().lower()
    for p in config.get('SFTPGO_INVENTORY', 'RETENTION_PATHS',
                        fallback='MANAGED/NEW').split(',')
    if p.strip()
]


logger.info(f"[AVVIO] {script_name} — hostname={hostname}")
logger.info(f"[CONFIG] SFTPGo: {SFTPGO_BASE_URL}")
logger.info(f"[CONFIG] Quota scan: {TRIGGER_QUOTA_SCAN}")
if SFTPGO_API_KEY:
    logger.info(f"[CONFIG] API Key admin-scope: configurata "
                f"({SFTPGO_API_KEY[:20]}...) - filesystem walking ABILITATO")
else:
    logger.warning(
        f"[CONFIG] API Key admin-scope NON configurata nell'INI sotto "
        f"[SFTPGO] API_KEY. Filesystem walking sara' DISABILITATO "
        f"(nessuna cartella verra' esplorata). Solo quota utente sara' raccolta.")
logger.info(f"[CONFIG] Walk filesystem: {WALK_FILESYSTEM} "
            f"(max_depth={MAX_DEPTH}, max_folders={MAX_FOLDERS})")
if VOLUME_CAPACITY_MODE in ('auto', 'kubectl'):
    logger.info(f"[CONFIG] Capacita' volume: modalita' {VOLUME_CAPACITY_MODE} "
                f"- discovery via kubectl su {K8S_NAMESPACE}/{K8S_POD_SELECTOR}"
                f":{VOLUME_MOUNT_PATH}")
    if VOLUME_CAPACITY_MODE == 'auto' and VOLUME_CAPACITY_BYTES <= 0:
        logger.info("[CONFIG] Nessun VOLUME_CAPACITY_BYTES di fallback: se "
                    "kubectl non e' raggiungibile si usera' solo la cache")
elif VOLUME_CAPACITY_BYTES > 0:
    logger.info(f"[CONFIG] Capacita' volume: valore statico "
                f"{round(VOLUME_CAPACITY_BYTES / 1073741824, 2)} GiB")
else:
    logger.warning(
        "[CONFIG] Capacita' volume non determinabile: modalita' static senza "
        "VOLUME_CAPACITY_BYTES. volume_pct_used non sara' calcolato.")
logger.info(f"[CONFIG] Soglia eta' archivio: {RETENTION_DAYS} giorni "
            f"(grace {RETENTION_GRACE_DAYS}g) su "
            f"{RETENTION_PATHS if RETENTION_PATHS else 'TUTTE le cartelle'} "
            f"- soglia interna, nessuna policy MEEO configurata")
if not ELASTIC_ENABLED:
    logger.info(f"[CONFIG] >>> DRY-RUN MODE (no Elastic) <<<")
else:
    logger.info(f"[CONFIG] Elastic ENABLED → {INVENTORY_INDEX}")
logger.info("")


# ============================================================
# Helpers
# ============================================================
def write_log(entry):
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(json.dumps(entry, ensure_ascii=False, default=str) + '\n')


def ship_to_elastic(entry):
    if not ELASTIC_ENABLED:
        return
    try:
        url = f"{MONITORING_URL.rstrip('/')}/{INVENTORY_INDEX}/_doc"
        headers = {'Authorization': f'ApiKey {MONITORING_APIKEY}',
                   'Content-Type': 'application/json'}
        r = requests.post(url, headers=headers, json=entry,
                          verify=MONITORING_VERIFY_CERTS, timeout=30)
        if r.status_code not in (200, 201):
            logger.error(f"[ELASTIC] HTTP {r.status_code}: {r.text[:200]}")
    except Exception as e:
        logger.error(f"[ELASTIC] {type(e).__name__}: {e}")


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# ============================================================
# Capacita' volume — discovery dinamica
# ============================================================
# La REST API di SFTPGo non espone la capacita' del filesystem: gestisce
# le quote logiche degli utenti, non lo spazio del PVC sottostante. Il
# dato va quindi preso da fuori, interrogando il pod via kubectl.
#
# Cascata: kubectl → cache su disco → VOLUME_CAPACITY_BYTES dall'INI.
# Il campo volume_capacity_source nel doc dice sempre da dove arriva il
# valore, cosi' si distingue un dato misurato da un fallback statico.
# ============================================================
def _run_kubectl(args):
    """Esegue kubectl e restituisce stdout, oppure None in caso di errore."""
    cmd = [KUBECTL_BIN] + args
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=KUBECTL_TIMEOUT)
        if proc.returncode != 0:
            logger.debug(f"[KUBECTL] rc={proc.returncode}: "
                         f"{proc.stderr.strip()[:200]}")
            return None
        return proc.stdout.strip()
    except FileNotFoundError:
        logger.debug(f"[KUBECTL] binario '{KUBECTL_BIN}' non trovato nel PATH")
        return None
    except subprocess.TimeoutExpired:
        logger.warning(f"[KUBECTL] timeout dopo {KUBECTL_TIMEOUT}s")
        return None
    except Exception as e:
        logger.debug(f"[KUBECTL] {type(e).__name__}: {e}")
        return None


def _parse_df_line(output):
    """
    Parsa l'output di `df -k -P <path>` e restituisce
    (capacity_bytes, used_bytes, avail_bytes).

    Si usa -k -P (POSIX, blocchi da 1K) e non -B1 perche' -B e' una
    estensione GNU non disponibile su immagini busybox. -P garantisce
    che ogni filesystem stia su una riga sola anche con device dal nome
    lungo, evitando il wrapping che romperebbe il parsing.

      Filesystem     1024-blocks      Used Available Capacity Mounted on
      /dev/sdb         309237645 157207023 151995465      51% /var/lib/sftpgo
    """
    if not output:
        return None
    lines = [l for l in output.splitlines() if l.strip()]
    if len(lines) < 2:
        return None
    fields = lines[-1].split()
    if len(fields) < 4:
        return None
    try:
        capacity = int(fields[1]) * 1024
        used = int(fields[2]) * 1024
        avail = int(fields[3]) * 1024
    except (ValueError, IndexError):
        return None
    if capacity <= 0:
        return None
    return capacity, used, avail


def _read_capacity_cache():
    """Legge la cache su disco se ancora valida, altrimenti None."""
    try:
        with open(VOLUME_CACHE_FILE, 'r', encoding='utf-8') as f:
            cached = json.load(f)
        ts = datetime.fromisoformat(cached['timestamp'])
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age_min = (datetime.now(timezone.utc) - ts).total_seconds() / 60
        if age_min > VOLUME_CAPACITY_CACHE_MINUTES:
            logger.debug(f"[VOLUME] cache scaduta ({round(age_min)} min)")
            return None
        return cached
    except FileNotFoundError:
        return None
    except Exception as e:
        logger.debug(f"[VOLUME] cache illeggibile: {type(e).__name__}: {e}")
        return None


def _write_capacity_cache(stats):
    try:
        payload = dict(stats)
        payload['timestamp'] = datetime.now(timezone.utc).isoformat()
        with open(VOLUME_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(payload, f)
    except Exception as e:
        logger.debug(f"[VOLUME] scrittura cache fallita: {type(e).__name__}: {e}")


def discover_volume_stats():
    """
    Determina capacita', usato e disponibile del volume.

    Restituisce un dict con capacity_bytes, used_bytes, avail_bytes,
    source — oppure None se nessuna fonte e' disponibile.

    used_bytes e avail_bytes sono valorizzati solo dalla fonte kubectl:
    sono i valori reali di df, piu' accurati di quelli derivati dalla
    quota SFTPGo perche' includono l'overhead del filesystem e gli
    eventuali file non contabilizzati nella quota utente.
    """
    # 1) Discovery via kubectl
    if VOLUME_CAPACITY_MODE in ('auto', 'kubectl'):
        pod = _run_kubectl([
            'get', 'pods', '-n', K8S_NAMESPACE,
            '-l', K8S_POD_SELECTOR,
            '--field-selector=status.phase=Running',
            '-o', 'jsonpath={.items[0].metadata.name}'])
        if pod:
            out = _run_kubectl([
                'exec', pod, '-n', K8S_NAMESPACE, '--',
                'df', '-k', '-P', VOLUME_MOUNT_PATH])
            parsed = _parse_df_line(out)
            if parsed:
                capacity, used, avail = parsed
                stats = {
                    'capacity_bytes': capacity,
                    'used_bytes': used,
                    'avail_bytes': avail,
                    'source': 'kubectl_df',
                    'pod': pod,
                }
                _write_capacity_cache(stats)
                logger.info(
                    f"[VOLUME] capacita' rilevata via kubectl su pod {pod}: "
                    f"{round(capacity / 1073741824, 2)} GiB")
                return stats
            logger.warning(f"[VOLUME] df sul pod {pod} non interpretabile, "
                           f"passo al fallback")
        else:
            logger.warning(f"[VOLUME] pod non individuato in namespace "
                           f"{K8S_NAMESPACE} con selector {K8S_POD_SELECTOR}, "
                           f"passo al fallback")
        if VOLUME_CAPACITY_MODE == 'kubectl':
            logger.warning("[VOLUME] modalita' kubectl senza fallback: "
                           "metriche volume non disponibili in questo run")
            return None

    # 2) Cache su disco
    if VOLUME_CAPACITY_MODE == 'auto':
        cached = _read_capacity_cache()
        if cached and cached.get('capacity_bytes'):
            logger.info(
                f"[VOLUME] capacita' da cache "
                f"({cached['timestamp'][:19]}): "
                f"{round(cached['capacity_bytes'] / 1073741824, 2)} GiB")
            return {
                'capacity_bytes': cached['capacity_bytes'],
                # usato e disponibile NON vengono riusati dalla cache:
                # cambiano a ogni run, riproporli sarebbe fuorviante.
                'used_bytes': None,
                'avail_bytes': None,
                'source': 'cache',
                'pod': cached.get('pod'),
            }

    # 3) Valore statico dall'INI
    if VOLUME_CAPACITY_BYTES > 0:
        logger.info(f"[VOLUME] capacita' da valore statico INI: "
                    f"{round(VOLUME_CAPACITY_BYTES / 1073741824, 2)} GiB")
        return {
            'capacity_bytes': VOLUME_CAPACITY_BYTES,
            'used_bytes': None,
            'avail_bytes': None,
            'source': 'ini_static',
            'pod': None,
        }

    logger.warning(
        "[VOLUME] nessuna fonte disponibile per la capacita' del volume. "
        "volume_pct_used non sara' calcolato e nessun alert di saturazione "
        "potra' scattare. Configurare VOLUME_CAPACITY_BYTES nell'INI o "
        "rendere kubectl raggiungibile dall'host di collection.")
    return None


# ============================================================
# SFTPGo API calls
# ============================================================
def get_admin_token(session):
    """Basic Auth su GET /api/v2/token → JWT."""
    try:
        r = session.get(f"{SFTPGO_BASE_URL}/api/v2/token",
                        auth=(SFTPGO_USERNAME, SFTPGO_PASSWORD),
                        timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            logger.error(f"[AUTH] HTTP {r.status_code}: {r.text[:200]}")
            return None
        return r.json().get("access_token")
    except Exception as e:
        logger.error(f"[AUTH] {type(e).__name__}: {e}")
        return None


def get_all_users(session, headers):
    """Lista di tutti gli utenti SFTPGo (con paginazione se serve)."""
    all_users = []
    offset = 0
    limit = 100
    while True:
        try:
            r = session.get(f"{SFTPGO_BASE_URL}/api/v2/users",
                            headers=headers,
                            params={"offset": offset, "limit": limit},
                            timeout=HTTP_TIMEOUT)
            if r.status_code != 200:
                logger.error(f"[USERS] HTTP {r.status_code}: {r.text[:200]}")
                break
            batch = r.json()
            if not batch:
                break
            all_users.extend(batch)
            if len(batch) < limit:
                break
            offset += limit
        except Exception as e:
            logger.error(f"[USERS] {type(e).__name__}: {e}")
            break
    return all_users


def trigger_quota_scan(session, headers, username):
    """Forza un rescan della home dir per ottenere used_quota aggiornato."""
    try:
        r = session.post(
            f"{SFTPGO_BASE_URL}/api/v2/quotas/users/{username}/scan",
            headers=headers, timeout=HTTP_TIMEOUT)
        if r.status_code in (200, 201, 202):
            return True
        # 409 = scan già in corso, ok comunque
        if r.status_code == 409:
            return True
        logger.warning(f"[QUOTA SCAN {username}] HTTP {r.status_code}: "
                       f"{r.text[:150]}")
        return False
    except Exception as e:
        logger.warning(f"[QUOTA SCAN {username}] {type(e).__name__}: {e}")
        return False


def get_user_detail(session, headers, username):
    """GET /api/v2/users/{username} → include used_quota_size aggiornato."""
    try:
        r = session.get(f"{SFTPGO_BASE_URL}/api/v2/users/{username}",
                        headers=headers, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            logger.warning(f"[USER DETAIL {username}] HTTP {r.status_code}")
            return None
        return r.json()
    except Exception as e:
        logger.warning(f"[USER DETAIL {username}] {type(e).__name__}: {e}")
        return None


def list_directory(session, username, path):
    """
    GET /api/v2/user/dirs?path={path}

    Endpoint user-side. Pattern di autenticazione (refactor 25/06/2026):
    user-scope API key legata direttamente all'utente target.

    Header: X-SFTPGO-API-KEY: <api_key>
    (la key e' gia' "bound" all'utente via campo user nel JSON di creazione,
    quindi NIENTE suffisso .username - SFTPGo sa gia' chi sei)

    Storia delle iterazioni:
    - L'endpoint /api/v2/admin/fs/dirs NON esiste su SFTPGo 2.6.6 (sempre 404)
    - Tentativo admin-scope + impersonation via suffisso .username: HTTP 403
      anche con allow_api_key_auth=true sia su admin che su user, anche con
      binding admin=cyberitalyadmin sulla key. Non funziona su 2.6.6.
    - Soluzione definitiva: API key user-scope (scope=2) legata a "ispra"
      via campo "user" nel POST di creazione. Funziona al primo colpo.

    NOTA: questa funzione e' user-specifico. Per monitorare un altro utente
    serve un'API key separata per quel utente. Non c'e' impersonation
    cross-user via API key user-scope.
    """
    if not SFTPGO_API_KEY:
        logger.error(f"[LS {username}:{path}] SFTPGO_API_KEY non configurata "
                     f"nell'INI - impossibile elencare directory.")
        return None

    # User-scope key: niente suffisso .username
    headers = {"X-SFTPGO-API-KEY": SFTPGO_API_KEY}

    try:
        r = session.get(
            f"{SFTPGO_BASE_URL}/api/v2/user/dirs",
            headers=headers,
            params={"path": path},
            timeout=HTTP_TIMEOUT)
        if r.status_code == 200:
            return r.json()
        # 404 = path non esiste, normale al primo run su home vuota o
        #       quando si esplorano subfolder che non ci sono.
        if r.status_code == 404:
            return None
        # 403 = api key non valida per la request o legata ad altro utente.
        if r.status_code == 403:
            logger.warning(
                f"[LS {username}:{path}] HTTP 403 - verifica che l'API key "
                f"sia user-scope legata all'utente '{username}'. "
                f"Body: {r.text[:150]}")
            return None
        logger.warning(f"[LS {username}:{path}] HTTP {r.status_code}: "
                       f"{r.text[:150]}")
        return None
    except Exception as e:
        logger.warning(f"[LS {username}:{path}] {type(e).__name__}: {e}")
        return None


# ============================================================
# Walk filesystem ricorsivo
# ============================================================
def walk_user_filesystem(session, username, start_path="/",
                         max_depth=10, max_folders=500):
    """
    Visita ricorsivamente il filesystem dell'utente.
    Restituisce una lista di dict, uno per cartella visitata.
    Ogni elemento contiene:
      path, depth, file_count_immediate, total_bytes_immediate,
      subfolder_names, oldest_mtime, newest_mtime

    Nota refactor 24/06/2026: il parametro headers e' stato rimosso perche'
    list_directory ora usa l'API key admin-scope con impersonation
    (X-SFTPGO-API-KEY: <key>.<username>), gestita internamente.
    """
    folders_found = []
    queue = [(start_path, 0)]
    visited_count = 0

    while queue and visited_count < max_folders:
        path, depth = queue.pop(0)
        if depth > max_depth:
            continue

        items = list_directory(session, username, path)
        if items is None:
            continue

        visited_count += 1
        files_in_folder = []
        subfolders = []

        for item in items:
            name = item.get("name", "")
            if name in (".", ".."):
                continue
            # SFTPGo restituisce il "mode" come os.FileMode di Go.
            # Il bit di directory in Go e' 0x80000000 (ModeDir), NON il
            # bit Unix tradizionale 0o040000. Verificato 25/06/2026 su 2.6.6:
            # - TEST (cartella) ha mode = 2151678445 (bit 0x80000000 attivo)
            # - file .nc        ha mode = 420         (nessun bit dir)
            mode = item.get("mode", 0)
            is_dir = (mode & 0x80000000) != 0
            # Alcuni SFTPGo restituiscono anche "type" string come fallback
            if not is_dir and "type" in item:
                is_dir = item.get("type", "").lower() in ("directory", "dir", "folder")

            if is_dir:
                subfolders.append(name)
                child_path = path.rstrip("/") + "/" + name
                queue.append((child_path, depth + 1))
            else:
                files_in_folder.append({
                    "name": name,
                    "size": item.get("size", 0),
                    "mtime": item.get("last_modified", item.get("mtime")),
                })

        total_bytes = sum(f["size"] for f in files_in_folder)
        mtimes = [f["mtime"] for f in files_in_folder if f["mtime"]]
        oldest = min(mtimes) if mtimes else None
        newest = max(mtimes) if mtimes else None

        # Top 5 file piu' recenti per mtime (i piu' significativi per
        # capire cosa sta arrivando nel sink). File senza mtime finiscono
        # in coda. Cap a 5 per evitare doc kilometrici quando una folder
        # contiene centinaia di file.
        files_sorted_by_recency = sorted(
            files_in_folder,
            key=lambda f: f.get("mtime") or "",
            reverse=True,
        )
        sample_files_top5 = [f["name"] for f in files_sorted_by_recency[:5]]

        folders_found.append({
            "path": path,
            "depth": depth,
            "file_count_immediate": len(files_in_folder),
            "subfolder_count_immediate": len(subfolders),
            "total_bytes_immediate": total_bytes,
            # i campi *_recursive vengono popolati nel secondo pass post-walk
            "file_count_recursive": None,
            "total_bytes_recursive": None,
            "subfolder_names": subfolders[:50],  # cap per non esplodere
            "oldest_mtime": oldest,
            "newest_mtime": newest,
            "sample_files": sample_files_top5,
        })

    if visited_count >= max_folders:
        logger.warning(f"[WALK {username}] raggiunto limite max_folders="
                       f"{max_folders}, alcune sottocartelle non visitate")

    # ============================================================
    # SECOND PASS: calcolo aggregati ricorsivi per ciascuna cartella.
    # Per ogni folder, sommo total_bytes_immediate e file_count_immediate
    # di se stessa + di tutte le folder che hanno un path che inizia col
    # suo path (sotto-cartelle a qualunque depth).
    # ============================================================
    for folder in folders_found:
        parent_path = folder["path"]
        # Normalizzo per gestire "/" vs "/subfolder" senza falsi match
        prefix = parent_path if parent_path.endswith("/") else parent_path + "/"

        recursive_bytes = folder["total_bytes_immediate"]
        recursive_files = folder["file_count_immediate"]

        for other in folders_found:
            if other["path"] == parent_path:
                continue  # gia' contata come immediate
            # other e' sotto-cartella se il suo path inizia con prefix
            # (esclude se stessa, gestisce root "/" correttamente)
            if other["path"].startswith(prefix):
                recursive_bytes += other["total_bytes_immediate"]
                recursive_files += other["file_count_immediate"]

        folder["total_bytes_recursive"] = recursive_bytes
        folder["file_count_recursive"] = recursive_files

    return folders_found


# ============================================================
# Calcolo freshness
# ============================================================
def calc_freshness_status(mtime_str):
    """Da un mtime ISO/string, calcola minuti dall'ora corrente e status."""
    if not mtime_str:
        return None, "NO_FILES"
    try:
        if isinstance(mtime_str, str):
            mtime = datetime.fromisoformat(mtime_str.replace("Z", "+00:00"))
        elif isinstance(mtime_str, (int, float)):
            # epoch
            if mtime_str > 1e12:
                mtime = datetime.fromtimestamp(mtime_str / 1000, tz=timezone.utc)
            else:
                mtime = datetime.fromtimestamp(mtime_str, tz=timezone.utc)
        else:
            return None, "UNKNOWN"

        if mtime.tzinfo is None:
            mtime = mtime.replace(tzinfo=timezone.utc)

        delta_min = (datetime.now(timezone.utc) - mtime).total_seconds() / 60
        if delta_min < FRESHNESS_OK_MINUTES:
            status = "OK"
        elif delta_min < FRESHNESS_STALE_MINUTES:
            status = "STALE"
        else:
            status = "OLD"
        return round(delta_min, 2), status
    except Exception:
        return None, "UNKNOWN"


def calc_age_days(mtime_str):
    """Eta' in giorni di un mtime. Riusa il parsing di calc_freshness_status."""
    minutes, _ = calc_freshness_status(mtime_str)
    if minutes is None:
        return None
    return round(minutes / 1440, 2)


def is_retention_monitored(path):
    """True se il path rientra fra quelli su cui controllare la retention."""
    if not RETENTION_PATHS:
        return True
    p = (path or "").lower()
    return any(token in p for token in RETENTION_PATHS)


def eval_retention(path, oldest_mtime):
    """
    Valuta lo stato della retention su una cartella.

    Ritorna (age_days, monitored, breach).
      breach=True  → il file piu' vecchio supera RETENTION_DAYS + grace,
                     quindi il rolling di cancellazione non sta girando.
      breach=None  → cartella non monitorata o eta' non determinabile.
    """
    age_days = calc_age_days(oldest_mtime)
    monitored = is_retention_monitored(path)
    if not monitored or age_days is None:
        return age_days, monitored, None
    return age_days, monitored, age_days > (RETENTION_DAYS + RETENTION_GRACE_DAYS)


# ============================================================
# Main
# ============================================================
def main():
    session = requests.Session()

    # 1) Auth admin
    token = get_admin_token(session)
    if not token:
        logger.error("[FINE] Token admin SFTPGo non ottenuto, abort.")
        return 1
    logger.info(f"[AUTH] token admin OK: {token[:30]}...")
    headers = {"Authorization": f"Bearer {token}"}

    # 2) Lista utenti
    users = get_all_users(session, headers)
    logger.info(f"[USERS] {len(users)} utenti trovati")
    if not users:
        logger.warning("[FINE] Nessun utente da processare.")
        return 0

    timestamp = now_iso()

    # 2b) Capacita' volume — rilevata una sola volta per run: e' un dato
    # di infrastruttura condiviso da tutti gli utenti, non per-utente.
    volume_stats = discover_volume_stats()

    # 3) Per ciascun utente
    for u in users:
        username = u.get("username")
        if not username:
            continue

        logger.info(f"\n--- utente: {username} ---")

        # 3a) Trigger quota scan + wait
        if TRIGGER_QUOTA_SCAN:
            if trigger_quota_scan(session, headers, username):
                logger.info(f"  [QUOTA] scan triggerato, "
                            f"attendo {QUOTA_SCAN_WAIT_SECONDS}s...")
                time.sleep(QUOTA_SCAN_WAIT_SECONDS)

        # 3b) Refresh dati utente (incl. used_quota aggiornato)
        u_detail = get_user_detail(session, headers, username) or u
        used_size = u_detail.get("used_quota_size", 0)
        used_files = u_detail.get("used_quota_files", 0)
        quota_size = u_detail.get("quota_size", 0)
        quota_files = u_detail.get("quota_files", 0)
        home_dir = u_detail.get("home_dir", "")
        last_quota_update = u_detail.get("last_quota_update", 0)
        total_data_transfer = u_detail.get("total_data_transfer", 0)

        # 3c) Walk filesystem (se abilitato e se used_size > 0)
        folders = []
        if WALK_FILESYSTEM and (used_size > 0 or used_files > 0):
            logger.info(f"  [WALK] esploro filesystem...")
            folders = walk_user_filesystem(session, username,
                                           start_path="/",
                                           max_depth=MAX_DEPTH,
                                           max_folders=MAX_FOLDERS)
            logger.info(f"  [WALK] {len(folders)} cartelle visitate")
        elif WALK_FILESYSTEM:
            logger.info(f"  [WALK] home vuota (used_size=0), skip walk")

        # 3d) Emit doc inventory per utente
        used_mb = round(used_size / 1048576, 3) if used_size else 0
        used_gb = round(used_size / 1073741824, 4) if used_size else 0

        # Percentuale sulla quota SFTPGo dell'utente. Resta None quando
        # la quota e' illimitata (quota_size=0), che e' il caso di "ispra":
        # per questo non e' utilizzabile come metrica di saturazione.
        quota_pct = None
        if quota_size > 0:
            quota_pct = round((used_size / quota_size) * 100, 2)

        # Percentuale sulla capacita' reale del filesystem del PVC.
        # E' QUESTA la metrica su cui costruire gli alert di saturazione:
        # il 02/08/2026 il volume si e' riempito al 100% mentre
        # quota_pct_used era null. Vedi nota capacita' in testa al file.
        #
        # Quando la fonte e' kubectl si usano i valori reali di df, che
        # includono overhead del filesystem e file non contabilizzati
        # nella quota SFTPGo. Altrimenti si deriva da used_quota_size,
        # che e' una stima leggermente per difetto.
        volume_capacity = None
        volume_source = None
        volume_used = None
        volume_pct = None
        volume_free = None
        volume_basis = None

        if volume_stats:
            volume_capacity = volume_stats['capacity_bytes']
            volume_source = volume_stats['source']
            if volume_stats.get('used_bytes') is not None:
                volume_used = volume_stats['used_bytes']
                volume_free = volume_stats['avail_bytes']
                volume_basis = 'df'
            else:
                volume_used = used_size
                volume_free = volume_capacity - used_size
                volume_basis = 'used_quota'
            if volume_capacity > 0:
                volume_pct = round((volume_used / volume_capacity) * 100, 2)

        # Trova file piu' recente in tutto il filesystem (per metrica "ultimo upload")
        all_newest = [f.get("newest_mtime") for f in folders
                      if f.get("newest_mtime")]
        most_recent_upload = max(all_newest) if all_newest else None
        upload_freshness_min, upload_freshness_status = calc_freshness_status(
            most_recent_upload)

        # Rollup retention a livello utente: prende il caso peggiore fra
        # tutte le cartelle monitorate, cosi' l'alert si puo' costruire sul
        # doc utente senza dover aggregare i doc folder.
        worst_age_days = None
        worst_breach = None
        breach_paths = []
        for f in folders:
            age_d, monitored, breach = eval_retention(f["path"], f["oldest_mtime"])
            if not monitored or age_d is None:
                continue
            if worst_age_days is None or age_d > worst_age_days:
                worst_age_days = age_d
            worst_breach = bool(worst_breach) or bool(breach)
            if breach:
                breach_paths.append(f["path"])

        user_doc = {
            "@timestamp": timestamp,
            "event_timestamp": timestamp,
            "platform": "iride",
            "service_provider_log": "iride_sftpgo",
            "event_type": "sftpgo_user_inventory",
            "response_status": "OK" if used_size >= 0 else "ERROR",
            "hostname": hostname,
            "endpoint": SFTPGO_BASE_URL,
            "username": username,
            "home_dir": home_dir,
            "status": u_detail.get("status", 0),
            "used_quota_size_bytes": used_size,
            "used_quota_size_mb": used_mb,
            "used_quota_size_gb": used_gb,
            "used_quota_files": used_files,
            "quota_size_bytes": quota_size,
            "quota_files": quota_files,
            "quota_pct_used": quota_pct,
            # Capacita' filesystem: metrica di riferimento per gli alert
            # di saturazione volume (vedi nota in testa al file).
            # volume_capacity_source indica da dove arriva il valore
            # (kubectl_df / cache / ini_static), volume_metric_basis se
            # usato e libero sono reali (df) o derivati (used_quota).
            "volume_capacity_bytes": volume_capacity,
            "volume_capacity_gb": (round(volume_capacity / 1073741824, 3)
                                   if volume_capacity else None),
            "volume_capacity_source": volume_source,
            "volume_metric_basis": volume_basis,
            "volume_used_bytes": volume_used,
            "volume_pct_used": volume_pct,
            "volume_free_bytes": volume_free,
            "volume_free_gb": (round(volume_free / 1073741824, 3)
                               if volume_free is not None else None),
            # Rollup eta' archivio: peggior caso fra le cartelle monitorate.
            "oldest_monitored_file_age_days": worst_age_days,
            "retention_breach": worst_breach,
            "retention_breach_paths": breach_paths[:20],
            "last_quota_update_epoch_ms": last_quota_update,
            "total_data_transfer_bytes": total_data_transfer,
            "folders_explored": len(folders),
            "most_recent_upload_mtime": most_recent_upload,
            "upload_freshness_minutes": upload_freshness_min,
            "upload_freshness_status": upload_freshness_status,
            "top_level_subfolders": ([f["path"] for f in folders if f["depth"] == 1][:20]
                                     if folders else []),
        }
        write_log(user_doc)
        ship_to_elastic(user_doc)

        logger.info(f"  [INVENTORY] used={used_mb} MB / {used_files} files  "
                    f"freshness={upload_freshness_status}"
                    + (f" ({upload_freshness_min} min)"
                       if upload_freshness_min is not None else ""))
        if volume_pct is not None:
            level = logger.warning if volume_pct >= 80 else logger.info
            level(f"  [VOLUME] {round(volume_used / 1073741824, 2)} GiB su "
                  f"{round(volume_capacity / 1073741824, 2)} GiB "
                  f"= {volume_pct}% "
                  f"(liberi {round(volume_free / 1073741824, 2)} GiB) "
                  f"[fonte={volume_source}, base={volume_basis}]")
        if worst_breach:
            logger.warning(
                f"  [ARCHIVIO] BREACH: file piu' vecchio a "
                f"{worst_age_days} giorni contro una soglia di "
                f"{RETENTION_DAYS} - nessuna cancellazione sta avvenendo. "
                f"Cartelle: {', '.join(breach_paths[:5])}")
        elif worst_age_days is not None:
            logger.info(f"  [ARCHIVIO] file piu' vecchio a "
                        f"{worst_age_days} giorni (soglia {RETENTION_DAYS})")

        # 3e) Emit 1 doc per ciascuna cartella visitata
        for f in folders:
            folder_freshness_min, folder_freshness_status = (
                calc_freshness_status(f["newest_mtime"]))
            # Eta' del file piu' vecchio: se supera la finestra di retention
            # attesa, il rolling di cancellazione non sta girando su questa
            # cartella. E' l'alert sulla causa radice dell'incidente 02/08.
            folder_age_days, folder_monitored, folder_breach = eval_retention(
                f["path"], f["oldest_mtime"])
            folder_doc = {
                "@timestamp": timestamp,
                "event_timestamp": timestamp,
                "platform": "iride",
                "service_provider_log": "iride_sftpgo",
                "event_type": "sftpgo_folder_node",
                "response_status": "OK",
                "hostname": hostname,
                "endpoint": SFTPGO_BASE_URL,
                "username": username,
                "path": f["path"],
                "depth": f["depth"],
                "file_count_immediate": f["file_count_immediate"],
                "subfolder_count_immediate": f["subfolder_count_immediate"],
                "total_bytes_immediate": f["total_bytes_immediate"],
                "total_mb_immediate": round(f["total_bytes_immediate"] / 1048576, 3),
                # Ricorsivi: includono self + tutte le sotto-cartelle a qualunque depth
                "file_count_recursive": f["file_count_recursive"],
                "total_bytes_recursive": f["total_bytes_recursive"],
                "total_mb_recursive": round(f["total_bytes_recursive"] / 1048576, 3),
                "total_gb_recursive": round(f["total_bytes_recursive"] / 1073741824, 3),
                "oldest_file_mtime": f["oldest_mtime"],
                "newest_file_mtime": f["newest_mtime"],
                "freshness_minutes": folder_freshness_min,
                "freshness_status": folder_freshness_status,
                # --- Eta' archivio ---
                # Nessuna retention policy risulta configurata lato MEEO:
                # oldest_file_age_days e' un dato osservato, retention_breach
                # e' il confronto con la nostra soglia interna di allarme
                # (RETENTION_DAYS nell'INI), non una policy concordata.
                "oldest_file_age_days": folder_age_days,
                "retention_breach": folder_breach,
                "subfolder_names": f["subfolder_names"],
                "sample_files": f["sample_files"],
            }
            write_log(folder_doc)
            ship_to_elastic(folder_doc)

    logger.info("")
    logger.info("[FINE]")
    return 0


if __name__ == "__main__":
    sys.exit(main())

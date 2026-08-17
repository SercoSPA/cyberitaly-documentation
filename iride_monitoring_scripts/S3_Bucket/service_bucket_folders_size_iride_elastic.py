"""
service_bucket_folders_size_iride_elastic.py
============================================
Statistics script: scansiona i bucket S3 OVH del progetto IRIDE CyberItaly
e per ciascuna cartella di primo livello calcola dimensione totale e count
oggetti.

MODALITA' DRY-RUN:
    Quando ELASTIC_ENABLED=False nel .ini, lo script scrive SOLO su file
    locale e NON tenta connessioni a Elasticsearch. Utile mentre l'Elastic
    IRIDE non e' ancora disponibile.

Cadenza suggerita: ogni 6 ore (cron)
"""

import aioboto3
import asyncio
import os
import json
import configparser
import logging
import socket
import aiofiles
from datetime import datetime, timezone
from botocore.config import Config as BotoConfig

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
config_path = os.path.join(base_dir, 'service_bucket_folders_size_iride_elastic.ini')
config = configparser.ConfigParser()
config.read(config_path)

hostname = os.environ.get("HOSTNAME", socket.gethostname())

LOG_FILE_NAME = config['CONFIG']['LOG_FILE_NAME']
log_file = os.path.join(base_dir, LOG_FILE_NAME)

# Flag: abilita o disabilita l'invio a Elasticsearch
# Default: disabilitato finche' l'Elastic IRIDE non e' raggiungibile
ELASTIC_ENABLED = config['CONFIG'].getboolean('ELASTIC_ENABLED', fallback=False)

es = None
MONITORING_INDEX = None
if ELASTIC_ENABLED:
    from elasticsearch import Elasticsearch
    MONITORING_URL = config['CONFIG']['MONITORING_URL']
    MONITORING_APIKEY = config['CONFIG']['MONITORING_APIKEY']
    MONITORING_VERIFY_CERTS = config['CONFIG'].getboolean('MONITORING_VERIFY_CERTS', fallback=True)
    MONITORING_INDEX = config['CONFIG']['MONITORING_INDEX']
    es = Elasticsearch(
        [MONITORING_URL],
        headers={"Authorization": "ApiKey " + MONITORING_APIKEY},
        verify_certs=MONITORING_VERIFY_CERTS
    )
    logger.info(f"[CONFIG] Elasticsearch ABILITATO: {MONITORING_URL}")
else:
    logger.info("[CONFIG] Elasticsearch DISABILITATO (dry-run mode). "
                "Output solo su file locale.")

FRAMEWORK_CREDENTIALS = dict(config['IRIDE_FRAMEWORK_CREDENTIALS'])
INGESTION_CREDENTIALS = dict(config['IRIDE_INGESTION_CREDENTIALS'])

# ============================================================
# Mappa bucket
# ============================================================
BUCKETS = {
    "cyberitaly-fmwk-container":          ("framework", FRAMEWORK_CREDENTIALS, "prod"),
    "cyberitaly-fmwk-output":             ("framework_output", FRAMEWORK_CREDENTIALS, "prod"),
    "cyberitaly-fmwk-output-e2etest":     ("framework_output", FRAMEWORK_CREDENTIALS, "e2etest"),
    "cyberitaly-fmwk-shared":             ("framework_shared", FRAMEWORK_CREDENTIALS, "prod"),
    "cyberitaly-fmwk-shared-e2etest":     ("framework_shared", FRAMEWORK_CREDENTIALS, "e2etest"),
    "cyberitaly-dingest-container":          ("ingestion", INGESTION_CREDENTIALS, "prod"),
    "cyberitaly-dingest-container-e2etest":  ("ingestion", INGESTION_CREDENTIALS, "e2etest"),
}

session = aioboto3.Session()
# NOTA: i primitivi asyncio (Semaphore, Lock) NON vanno creati a livello di
# modulo. In Python 3.9 si agganciano al loop attivo al momento della creazione;
# creati qui (fuori da un loop) finiscono legati a un loop diverso da quello di
# asyncio.run(main()), causando "Future attached to a different loop".
# Vengono quindi inizializzati dentro main(), dove il loop esiste gia'.
general_semaphore = None
write_lock = None

# ============================================================
# Timestamp unico per snapshot
# ============================================================
# Tutti i documenti prodotti in una singola esecuzione condividono lo STESSO
# timestamp. I bucket vengono processati in parallelo (asyncio.gather); se
# ogni documento chiamasse datetime.now() al momento della scrittura otterrebbe
# un istante leggermente diverso (millisecondi). Sui grafici temporali di
# Grafana/Kibana questo sparpaglia i punti di uno stesso snapshot su tempi
# lievemente diversi, rendendo le serie illeggibili (linee verticali / "muro").
# Fissandolo una volta all'avvio, ogni run corrisponde a un singolo punto nel
# tempo e il time series risulta pulito.
RUN_TIMESTAMP = (
    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
)


def create_log_entry(bucket_name, folder_name, folder_size, total_objects,
                     service_name, environment):
    timestamp = RUN_TIMESTAMP
    folder_name = folder_name.strip().replace('\n', '')
    return {
        "@timestamp": timestamp,
        "event_timestamp": timestamp,
        "bucket_name": bucket_name,
        "service_name": service_name,
        "environment": environment,
        "platform": "iride",
        "folder": folder_name,
        "size": int(folder_size),
        "objects": int(total_objects),
        "service_provider_log": "iride_s3_storage",
        "response_status": "OK",
        "event_type": "service_bucketname_folder_size",
        "hostname": hostname,
        "message": "Bucket folder size success"
    }


def create_bucket_summary_entry(bucket_name, total_size, total_objects,
                                 service_name, environment, folder_count):
    timestamp = RUN_TIMESTAMP
    return {
        "@timestamp": timestamp,
        "event_timestamp": timestamp,
        "bucket_name": bucket_name,
        "service_name": service_name,
        "environment": environment,
        "platform": "iride",
        "size": int(total_size),
        "objects": int(total_objects),
        "folder_count": int(folder_count),
        "service_provider_log": "iride_s3_storage",
        "response_status": "OK",
        "event_type": "service_bucketname_size",
        "hostname": hostname,
        "message": "Bucket total size success"
    }


async def log_data(log_entry):
    """Scrive su file locale e (se ELASTIC_ENABLED) indicizza in Elasticsearch."""
    line = json.dumps(log_entry, ensure_ascii=False, separators=(',', ':'))

    # Sempre: scrittura su file locale
    async with write_lock:
        async with aiofiles.open(log_file, 'a') as f:
            await f.write(line + '\n')

    logger.info(f"[SALVATO] {log_entry['bucket_name']}/"
                f"{log_entry.get('folder', '__TOTAL__')} "
                f"size={log_entry['size']} objects={log_entry['objects']}")

    # Opzionale: Elasticsearch
    if ELASTIC_ENABLED and es is not None:
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                lambda: es.index(index=MONITORING_INDEX, body=log_entry)
            )
        except Exception as e:
            logger.warning(f"[ES_FAIL] Indicizzazione fallita: {type(e).__name__}: {e}")


async def get_folder_size(bucket_name, prefix, credentials):
    total_size = 0
    total_objects = 0
    try:
        async with general_semaphore:
            async with session.client(
                's3', **credentials,
                config=BotoConfig(read_timeout=300, connect_timeout=60)
            ) as s3:
                paginator = s3.get_paginator('list_objects_v2')
                async for page in paginator.paginate(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    PaginationConfig={"PageSize": 5000}
                ):
                    for obj in page.get('Contents', []):
                        total_size += obj['Size']
                        total_objects += 1
    except Exception as e:
        logger.error(f"[ERRORE] {bucket_name}/{prefix}: {e}")
    return total_size, total_objects


async def get_folders(bucket_name, credentials, prefix=None):
    folders = set()
    try:
        async with session.client(
            's3', **credentials,
            config=BotoConfig(read_timeout=120, connect_timeout=30)
        ) as s3:
            args = {'Bucket': bucket_name, 'Delimiter': '/'}
            if prefix:
                args['Prefix'] = prefix
            paginator = s3.get_paginator('list_objects_v2')
            async for page in paginator.paginate(**args):
                for cp in page.get('CommonPrefixes', []):
                    folders.add(cp['Prefix'])
    except Exception as e:
        logger.error(f"[ERRORE] Recupero cartelle da {bucket_name} "
                     f"({prefix or 'root'}): {e}")
    logger.info(f"[INFO] {bucket_name} ({prefix or 'root'}): "
                f"trovate {len(folders)} cartelle")
    return list(folders)


async def get_root_size(bucket_name, credentials):
    return await get_folder_size(bucket_name, '', credentials)


async def process_folder(bucket_name, prefix, credentials, service_name, environment):
    size, count = await get_folder_size(bucket_name, prefix, credentials)
    entry = create_log_entry(bucket_name, prefix, size, count,
                             service_name, environment)
    await log_data(entry)
    return size, count


async def process_bucket(bucket_name, service_name, credentials, environment):
    logger.info(f"[BUCKET] Inizio processing: {bucket_name} "
                f"(service={service_name}, env={environment})")

    roots = await get_folders(bucket_name, credentials)

    if not roots:
        logger.info(f"[BUCKET] {bucket_name}: nessuna cartella, conto contenuto flat")
        size, count = await get_root_size(bucket_name, credentials)
        summary = create_bucket_summary_entry(
            bucket_name, size, count, service_name, environment, folder_count=0
        )
        await log_data(summary)
        logger.info(f"[BUCKET] Completed: {bucket_name} (flat, {count} objects)")
        return

    tasks = [
        asyncio.create_task(
            process_folder(bucket_name, r, credentials, service_name, environment)
        )
        for r in roots
    ]
    results = await asyncio.gather(*tasks)

    total_size = sum(r[0] for r in results)
    total_objects = sum(r[1] for r in results)
    summary = create_bucket_summary_entry(
        bucket_name, total_size, total_objects, service_name, environment,
        folder_count=len(roots)
    )
    await log_data(summary)

    logger.info(f"[BUCKET] Completed: {bucket_name} "
                f"({len(roots)} folders, {total_objects} total objects)")


async def main():
    # I primitivi asyncio vengono creati QUI, dentro il loop di asyncio.run(),
    # per evitare il RuntimeError "Future attached to a different loop".
    global general_semaphore, write_lock
    general_semaphore = asyncio.Semaphore(20)
    write_lock = asyncio.Lock()

    logger.info(f"[AVVIO] Script start (hostname={hostname})")
    logger.info(f"[CONFIG] Monitoring bucket count: {len(BUCKETS)}")
    logger.info(f"[CONFIG] Output file: {log_file}")
    if not ELASTIC_ENABLED:
        logger.info("[CONFIG] >>> DRY-RUN MODE attivo <<<")

    await asyncio.gather(*[
        process_bucket(bucket_name, service_name, credentials, environment)
        for bucket_name, (service_name, credentials, environment) in BUCKETS.items()
    ])

    logger.info('[FINE] Tutti i bucket processati')
    logger.info(f"[FINE] Documenti scritti in: {log_file}")


if __name__ == '__main__':
    asyncio.run(main())

#!/bin/bash
# =============================================================================
# cron_run_s3.sh
# =============================================================================
# Lancia in sequenza gli script di monitoring S3 verso Elastic IRIDE:
#   1. kpi_s3_synthetic_iride_elastic_e2e.py  → probe attivo (write/read/delete)
#      Emette 1 doc per bucket probato in logs-iride-s3-health.monitoring-default
#      Impatto S3: piccolo (payload 419 bytes), oggetti sotto __monitoring/ e
#      cancellati subito.
#
#   2. service_bucket_folders_size_iride_elastic.py  → scan storage
#      Emette N doc (uno per folder scoperta) in logs-iride-s3-storage.
#      Impatto: pesante lato I/O (paginator su tutti i bucket), lo mettiamo
#      dopo il probe sintetico che e' rapido.
#
# NOTA no_proxy: lo script eredita l'env di cron. Le variabili no_proxy /
# NO_PROXY devono essere settate in cima al crontab (non in ~/.bashrc, che
# cron non legge) per permettere il traffico verso ci-mon-db-01.
# =============================================================================
CURRENT_SCRIPT=$(readlink -f "$0")
PATH_SCRIPT=$(dirname "$CURRENT_SCRIPT")
cd "${PATH_SCRIPT}" || { echo "[ERROR] cannot cd to ${PATH_SCRIPT}"; exit 1; }
# Attiva il venv (stesso pattern del cron_run.sh esistente)
source /home/monitoring/venv/bin/activate
# -----------------------------------------------------------------------------
# 1. Probe sintetico (health/latency)
# -----------------------------------------------------------------------------
echo "[$(date)][kpi_s3_synthetic] Starting check...."
python3 "${PATH_SCRIPT}/kpi_s3_synthetic_iride_elastic_e2e.py"
echo "[$(date)][kpi_s3_synthetic] Check completed"
sleep 60
# -----------------------------------------------------------------------------
# 2. Scan storage (inventory folders)
# -----------------------------------------------------------------------------
# LOCK ANTI-SOVRAPPOSIZIONE:
# Lo scan storage e' pesante (paginator su tutti i bucket) e puo' durare piu'
# dell'intervallo di cron. Senza lock, run successivi si accavallano e
# moltiplicano i documenti scritti (in passato: ~4M di doc spazzatura + log
# da 2GB). flock garantisce UNA sola istanza per volta: se la precedente e'
# ancora attiva, questo run salta lo scan (exit 0, non errore) e riprovera'
# al prossimo tick del cron.
LOCK_FILE="${PATH_SCRIPT}/.service_bucket_folders_size.lock"

echo "[$(date)][service_bucket_folders_size] Starting check...."
(
    # fd 200 dedicato al lock; -n = non-blocking (non aspetta, salta subito)
    flock -n 200 || {
        echo "[$(date)][service_bucket_folders_size] Run precedente ancora attivo: SKIP"
        exit 0
    }
    python3 "${PATH_SCRIPT}/service_bucket_folders_size_iride_elastic.py"
) 200>"${LOCK_FILE}"
echo "[$(date)][service_bucket_folders_size] Check completed"

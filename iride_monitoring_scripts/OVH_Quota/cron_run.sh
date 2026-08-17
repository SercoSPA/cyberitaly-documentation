#!/bin/bash
# =============================================================================
# cron_run_ovh.sh
# =============================================================================
# Lancia in sequenza i 3 script di monitoring OVH verso Elastic IRIDE:
#
#   1. kpi_iride_ovh_compute_quota_iride_elastic.py  → snapshot quota tenant
#      Emette 1 doc su Elastic con vCPU/RAM/storage tenant-level.
#      Impatto: minimo (1 chiamata API OVH).
#
#   2. kpi_iride_ovh_bucket_totalsize_iride_elastic.py  → totale bucket S3 OVH
#      Emette 1 doc per bucket con size aggregata.
#      Impatto: leggero (metadata API OVH, no I/O sui dati).
#
#   3. kpi_iride_ovh_k8s_nodepools_iride_elastic.py  → dettaglio nodepool K8s
#      Emette 1 doc per nodepool (13 doc su 4 cluster CYIT-01).
#      Impatto: medio (itera su tutti i cluster K8s del tenant).
#
# NOTA env: le variabili http_proxy, https_proxy, no_proxy DEVONO essere
# settate in cima al crontab (non in ~/.bashrc, che cron non legge) per
# permettere sia il traffico verso OVH (via Squid) che verso ci-mon-db-01
# (bypassando Squid).
# =============================================================================

CURRENT_SCRIPT=$(readlink -f "$0")
PATH_SCRIPT=$(dirname "$CURRENT_SCRIPT")
cd "${PATH_SCRIPT}" || { echo "[ERROR] cannot cd to ${PATH_SCRIPT}"; exit 1; }

# Attiva il venv
source /home/monitoring/venv/bin/activate

# -----------------------------------------------------------------------------
# 1. Quota compute tenant OVH
# -----------------------------------------------------------------------------
echo "[$(date)][kpi_iride_ovh_compute_quota] Starting check...."
python3 "${PATH_SCRIPT}/kpi_iride_ovh_compute_quota_iride_elastic.py"
echo "[$(date)][kpi_iride_ovh_compute_quota] Check completed"

sleep 10

# -----------------------------------------------------------------------------
# 2. Totalsize bucket S3 OVH
# -----------------------------------------------------------------------------
echo "[$(date)][kpi_iride_ovh_bucket_totalsize] Starting check...."
python3 "${PATH_SCRIPT}/kpi_iride_ovh_bucket_totalsize_iride_elastic.py"
echo "[$(date)][kpi_iride_ovh_bucket_totalsize] Check completed"

sleep 10

# -----------------------------------------------------------------------------
# 3. Nodepool K8s OVH (13 nodepool su 4 cluster)
# -----------------------------------------------------------------------------
echo "[$(date)][kpi_iride_ovh_k8s_nodepools] Starting check...."
python3 "${PATH_SCRIPT}/kpi_iride_ovh_k8s_nodepools_iride_elastic.py"
echo "[$(date)][kpi_iride_ovh_k8s_nodepools] Check completed"

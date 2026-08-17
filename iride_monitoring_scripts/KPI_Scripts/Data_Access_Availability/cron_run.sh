#!/bin/bash
# =============================================================================
# cron_run.sh — Data_Access_Availability
# =============================================================================
# Lancia il KPI di disponibilita' dell'accesso ai dati (retrieval /
# ingestion / access) IRIDE via SFTPGo verso Elastic:
#
#   kpi_data_access_availability_iride_elastic.py
#      Livello A: auth SFTPGo (GET /api/v2/token, Basic Auth → JWT).
#      Livello B: retrieval reale (GET /api/v2/user/dirs, listing directory
#                 con API key user-scope). Emette 1 doc con response_status
#                 OK/SLOW/FAIL, auth_* e retrieval_*.
#      Impatto: leggero (1 GET token + 1 GET listing).
#
# NOTA env: le variabili http_proxy, https_proxy, no_proxy DEVONO essere
# settate in cima al crontab (non in ~/.bashrc, che cron non legge) per
# permettere il traffico verso ci-mon-db-01 (Elastic) bypassando Squid.
# =============================================================================

CURRENT_SCRIPT=$(readlink -f "$0")
PATH_SCRIPT=$(dirname "$CURRENT_SCRIPT")
cd "${PATH_SCRIPT}" || { echo "[ERROR] cannot cd to ${PATH_SCRIPT}"; exit 1; }

# Attiva il venv
source /home/monitoring/venv/bin/activate

echo "[$(date)][kpi_data_access_availability] Starting check...."
python3 "${PATH_SCRIPT}/kpi_data_access_availability_iride_elastic.py"
echo "[$(date)][kpi_data_access_availability] Check completed"
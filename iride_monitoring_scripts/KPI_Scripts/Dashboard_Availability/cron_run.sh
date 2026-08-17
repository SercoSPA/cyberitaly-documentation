#!/bin/bash
# =============================================================================
# cron_run.sh — Dashboard_Availability
# =============================================================================
# Lancia il KPI di disponibilita' del servizio di visualizzazione /
# dashboard utente IRIDE verso Elastic:
#
#   kpi_dashboard_availability_iride_elastic.py
#      Livello A: reachability HTTP del front-end dashboard (perception).
#      Livello B: login Keycloak + chiamata API Insula (/jobs) che alimenta
#                 la dashboard. Emette 1 doc con response_status OK/SLOW/FAIL,
#                 frontend_* e api_*.
#      Impatto: leggero (2 GET + 1 POST token).
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

echo "[$(date)][kpi_dashboard_availability] Starting check...."
python3 "${PATH_SCRIPT}/kpi_dashboard_availability_iride_elastic.py"
echo "[$(date)][kpi_dashboard_availability] Check completed"

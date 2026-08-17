# cron_run_k8s.sh
# =============================================================================
# Lancia lo script di monitoring health dei pod K8s verso Elastic IRIDE:
#
#   collector_k8s_pod_health_iride_elastic.py  → pod health probe
#      Interroga kubectl sui namespace target (ADAM + Insula + logging).
#      Emette 1 doc per pod con phase, health_status, restart_count,
#      resource requests/limits, cpu/memory real-time (via metrics-server),
#      pod_age vs uptime.
#      Impatto: leggero (kubectl in read-only + kubectl top per namespace).
#
# NOTA env: le variabili http_proxy, https_proxy, no_proxy DEVONO essere
# settate in cima al crontab (non in ~/.bashrc, che cron non legge) per
# permettere il traffico verso ci-mon-db-01 (Elastic) bypassando Squid.
# kubectl usa il kubeconfig in $HOME/.kube/config e non passa da proxy.
# =============================================================================

CURRENT_SCRIPT=$(readlink -f "$0")
PATH_SCRIPT=$(dirname "$CURRENT_SCRIPT")
cd "${PATH_SCRIPT}" || { echo "[ERROR] cannot cd to ${PATH_SCRIPT}"; exit 1; }

# Attiva il venv
source /home/monitoring/venv/bin/activate

# -----------------------------------------------------------------------------
# Pod health probe (ADAM + Insula + logging namespaces)
# -----------------------------------------------------------------------------
echo "[$(date)][collector_k8s_pod_health] Starting check...."
python3 "${PATH_SCRIPT}/collector_sftpgo_folders_iride_elastic.py"
echo "[$(date)][collector_k8s_pod_health] Check completed"

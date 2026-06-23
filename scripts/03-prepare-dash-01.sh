#!/usr/bin/env bash
#
# Host preparation for ci-mon-dash-01 (Kibana, Fleet Server, Heartbeat).
# Run as root on dash-01.
#  - /data layout + ownership
#  - firewall: Kibana (5601) and Fleet Server (8220)
#
set -euo pipefail
if [ "$(id -u)" -ne 0 ]; then echo "Run as root (sudo)." >&2; exit 1; fi

echo ">> Data directories on the 50GB /data volume"
mkdir -p /data/kibana /data/fleet-server /data/heartbeat
# Kibana and Elastic Agent containers run as uid 1000; Heartbeat runs as root.
chown -R 1000:0 /data/kibana /data/fleet-server
chmod -R 770 /data/kibana /data/fleet-server /data/heartbeat

echo ">> Firewall (firewalld): open Kibana (5601) and Fleet Server (8220)"
if systemctl is-active --quiet firewalld; then
  firewall-cmd --permanent --add-port=5601/tcp   # Kibana UI
  firewall-cmd --permanent --add-port=8220/tcp   # Fleet Server (k8s agents enroll here)
  firewall-cmd --reload
  echo "   opened 5601/tcp and 8220/tcp"
else
  echo "   firewalld not active, skipping (ensure 5601 + 8220 are reachable)"
fi

echo ">> dash-01 ready."

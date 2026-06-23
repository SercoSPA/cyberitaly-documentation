#!/usr/bin/env bash
#
# Host preparation for ci-mon-db-01 (Elasticsearch). Run as root on db-01.
#  - kernel tuning required by Elasticsearch
#  - /data layout + ownership (ES container runs as uid 1000)
#  - firewall: expose Elasticsearch HTTP (9200)
#
set -euo pipefail
if [ "$(id -u)" -ne 0 ]; then echo "Run as root (sudo)." >&2; exit 1; fi

echo ">> Kernel: vm.max_map_count (required, ES will not start without it)"
cat > /etc/sysctl.d/99-elasticsearch.conf <<'EOF'
vm.max_map_count=262144
vm.swappiness=1
EOF
sysctl --system >/dev/null
echo "   vm.max_map_count=$(sysctl -n vm.max_map_count)"

echo ">> Data directory on the 100GB /data volume"
mkdir -p /data/elasticsearch
chown -R 1000:0 /data/elasticsearch
chmod -R 770 /data/elasticsearch

echo ">> Firewall (firewalld): open 9200/tcp for Kibana, Fleet and Heartbeat"
if systemctl is-active --quiet firewalld; then
  firewall-cmd --permanent --add-port=9200/tcp
  firewall-cmd --reload
  echo "   opened 9200/tcp"
else
  echo "   firewalld not active, skipping (ensure 9200/tcp is reachable from ci-mon-dash-01)"
fi

echo ">> db-01 ready."

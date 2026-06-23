#!/usr/bin/env bash
#
# Set the password for the built-in 'kibana_system' user.
# Run on ci-mon-db-01 AFTER Elasticsearch is up (docker compose up -d).
# Prints the password to put into dash-01/.env as KIBANA_PASSWORD.
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
set -a && . "${REPO_ROOT}/db-01/.env" && set +a

NEW_PASS="${1:-$(openssl rand -base64 24)}"

echo ">> Setting kibana_system password via Elasticsearch API"
docker exec -i elasticsearch \
  curl -s --cacert config/certs/ca/ca.crt \
    -u "elastic:${ELASTIC_PASSWORD}" \
    -X POST "https://localhost:9200/_security/user/kibana_system/_password" \
    -H 'Content-Type: application/json' \
    -d "{\"password\":\"${NEW_PASS}\"}" >/dev/null

echo ">> Done."
echo "   Put this in dash-01/.env :"
echo ""
echo "   KIBANA_PASSWORD=${NEW_PASS}"

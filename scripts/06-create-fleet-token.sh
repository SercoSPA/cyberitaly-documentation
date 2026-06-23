#!/usr/bin/env bash
#
# Create the Fleet Server service token used by the Fleet Server container.
# Run on ci-mon-db-01 AFTER Elasticsearch is up.
# Prints the token to put into dash-01/.env as FLEET_SERVER_SERVICE_TOKEN.
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
set -a && . "${REPO_ROOT}/db-01/.env" && set +a

TOKEN_NAME="${1:-fleet-server-token-$(hostname)}"

echo ">> Creating Fleet Server service token '${TOKEN_NAME}'"
RESPONSE="$(docker exec -i elasticsearch \
  curl -s --cacert config/certs/ca/ca.crt \
    -u "elastic:${ELASTIC_PASSWORD}" \
    -X POST "https://localhost:9200/_security/service/elastic/fleet-server/credential/token/${TOKEN_NAME}")"

TOKEN="$(echo "${RESPONSE}" | grep -o '"value":"[^"]*"' | cut -d'"' -f4)"

if [ -z "${TOKEN}" ]; then
  echo "ERROR: could not create token. Response was:" >&2
  echo "${RESPONSE}" >&2
  exit 1
fi

echo ">> Done."
echo "   Put this in dash-01/.env :"
echo ""
echo "   FLEET_SERVER_SERVICE_TOKEN=${TOKEN}"

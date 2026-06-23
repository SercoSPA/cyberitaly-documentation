#!/usr/bin/env bash
#
# Generate a private CA and PEM certificates for every component of the
# CyberItaly monitoring stack, driven by certs/instances.yml.
#
# Run this ONCE (on any host that has Docker, e.g. ci-mon-db-01), then copy
# the whole certs/ directory to both VMs. Re-run only when instances.yml
# changes or certs expire.
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CERTS_DIR="${REPO_ROOT}/certs"

# shellcheck disable=SC1091
[ -f "${REPO_ROOT}/.env" ] && set -a && . "${REPO_ROOT}/.env" && set +a
STACK_VERSION="${STACK_VERSION:-8.18.2}"

echo ">> Generating certificates with Elasticsearch ${STACK_VERSION}"

docker run --rm \
  -v "${CERTS_DIR}:/certs" \
  -w /certs \
  "docker.elastic.co/elasticsearch/elasticsearch:${STACK_VERSION}" \
  bash -c '
    set -e
    # 1. Certificate Authority (created only once so re-runs keep the same CA)
    if [ ! -f /certs/ca/ca.crt ]; then
      echo "   creating CA..."
      bin/elasticsearch-certutil ca --silent --pem -out /certs/ca.zip
      unzip -qo /certs/ca.zip -d /certs
    else
      echo "   reusing existing CA"
    fi

    # 2. Per-instance certificates signed by that CA
    echo "   creating instance certificates..."
    bin/elasticsearch-certutil cert --silent --pem \
      --ca-cert /certs/ca/ca.crt \
      --ca-key  /certs/ca/ca.key \
      --in  /certs/instances.yml \
      --out /certs/certs.zip
    unzip -qo /certs/certs.zip -d /certs

    rm -f /certs/ca.zip /certs/certs.zip
    # ES / Kibana / Agent containers run as uid 1000
    chown -R 1000:0 /certs
    find /certs -type d -exec chmod 750 {} \;
    find /certs -type f -exec chmod 640 {} \;
  '

echo ""
echo ">> Done. Generated under ${CERTS_DIR}:"
find "${CERTS_DIR}" -name '*.crt' -o -name '*.key' | sed "s#${CERTS_DIR}#  certs#" | sort

echo ""
echo ">> CA SHA-256 fingerprint (put this in dash-01/.env as CA_TRUSTED_FINGERPRINT):"
"${REPO_ROOT}/scripts/04-ca-fingerprint.sh"

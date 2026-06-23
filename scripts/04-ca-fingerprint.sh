#!/usr/bin/env bash
#
# Print the SHA-256 fingerprint of the CA certificate, formatted the way
# Elastic Agent / Fleet expects (lowercase hex, no colons).
#
# This value goes into dash-01/.env as CA_TRUSTED_FINGERPRINT and is used by
# the Fleet "default" Elasticsearch output so that remote Elastic Agents
# (running in Kubernetes) can trust the Elasticsearch certificate.
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CA_CRT="${REPO_ROOT}/certs/ca/ca.crt"

if [ ! -f "${CA_CRT}" ]; then
  echo "ERROR: ${CA_CRT} not found. Run scripts/01-generate-certs.sh first." >&2
  exit 1
fi

openssl x509 -in "${CA_CRT}" -noout -fingerprint -sha256 \
  | sed 's/^.*=//; s/://g' \
  | tr 'A-Z' 'a-z'

#!/usr/bin/env bash
# render.sh - fill the six REPLACE-* placeholders in a manifest/JSON (stdin -> stdout) from client.env.
#
# It deliberately does NOT touch:
#   - __CONNECT_PSK__ / ${CONNECT_PSK}  (runtime-injected by the gateway from the spark-connect-psk Secret)
#   - __OPENFGA__                       (a literal separator in the env var name LAKEKEEPER__OPENFGA__ENDPOINT)
#   - <T>                               (per-tenant; handled by gen_connect_server.py)
#
# Usage:
#   set -a; . ./client.env; set +a
#   ./render.sh < authz/10-lakekeeper-authz.yaml | kubectl apply -f -
#   for t in $TENANTS; do ./render.sh < spike/eks/warehouse-$t.aws.json > /tmp/wh-$t.json; done
set -euo pipefail

: "${ACCOUNT_ID:?set ACCOUNT_ID (see client.env.example)}"
: "${NAME_PREFIX:?set NAME_PREFIX}"
: "${WAREHOUSE_BUCKET:?set WAREHOUSE_BUCKET}"
: "${EXTERNAL_ID:?set EXTERNAL_ID}"
: "${PG_ENCRYPTION_KEY:?set PG_ENCRYPTION_KEY}"

# REPLACE-name-prefix must be substituted before REPLACE-ACCOUNT etc; the patterns are disjoint so
# order is not load-bearing, but keep the compound one first for clarity.
out="$(sed \
  -e "s#REPLACE-name-prefix#${NAME_PREFIX}#g" \
  -e "s#REPLACE-ACCOUNT#${ACCOUNT_ID}#g" \
  -e "s#REPLACE-warehouse-bucket#${WAREHOUSE_BUCKET}#g" \
  -e "s#REPLACE-external-id#${EXTERNAL_ID}#g" \
  -e "s#REPLACE-PG-ENCRYPTION-KEY#${PG_ENCRYPTION_KEY}#g")"

if printf '%s' "$out" | grep -qE 'REPLACE-'; then
  echo "render.sh: UNFILLED REPLACE-* placeholder(s) remain:" >&2
  printf '%s' "$out" | grep -nE 'REPLACE-' >&2
  exit 1
fi
printf '%s\n' "$out"

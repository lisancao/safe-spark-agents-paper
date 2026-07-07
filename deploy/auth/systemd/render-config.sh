#!/usr/bin/env bash
# Render the Envoy auth-proxy config from its template, substituting the pre-shared bearer token.
#
# The committed envoy.yaml ships with the placeholder __CONNECT_PSK__ so the secret is NEVER baked
# into the repo. This script injects CONNECT_PSK (from the systemd EnvironmentFile) into a runtime
# copy on tmpfs. Run by envoy.service's ExecStartPre.
#
#   CONNECT_PSK        (required) the server's spark.connect.authenticate.token value
#   ENVOY_TEMPLATE     template path  (default /etc/envoy/envoy.yaml.tmpl)
#   ENVOY_RENDERED     output path    (default /run/envoy/envoy.yaml)
set -euo pipefail

: "${CONNECT_PSK:?set CONNECT_PSK (the Connect server pre-shared bearer token) in the EnvironmentFile}"
template="${ENVOY_TEMPLATE:-/etc/envoy/envoy.yaml.tmpl}"
rendered="${ENVOY_RENDERED:-/run/envoy/envoy.yaml}"

if [ ! -f "${template}" ]; then
  echo "template not found: ${template}" >&2
  exit 1
fi

mkdir -p "$(dirname "${rendered}")"

# Substitute only our placeholder; awk avoids issues with '/' or '&' in the token.
awk -v psk="${CONNECT_PSK}" '{ gsub(/__CONNECT_PSK__/, psk); print }' \
  "${template}" > "${rendered}"
chmod 600 "${rendered}"

echo "rendered ${rendered} from ${template}"

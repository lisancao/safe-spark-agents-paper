#!/usr/bin/env bash
# bootstrap-secrets.sh - issue the Connect-layer CA + gateway server cert + one client cert per tenant,
# generate the PSK, and create the k8s Secrets the gateway + per-tenant Connect servers expect. Wraps
# deploy/auth/certs/issue-all.sh (env-overridable). Cert generation runs LOCALLY (openssl); the
# `kubectl` Secret/ConfigMap creation needs the cluster, so it is printed unless you pass --apply.
#
# Usage:
#   set -a; . ./client.env; set +a
#   ./bootstrap-secrets.sh              # issue certs + PSK into ./secrets-out, PRINT the kubectl cmds
#   ./bootstrap-secrets.sh --apply      # also run the kubectl create commands (needs the cluster)
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CERTS_SRC="$(cd -- "${SCRIPT_DIR}/../../auth/certs" && pwd)"
GW_ENVOY="${SCRIPT_DIR}/authz/gateway-envoy.yaml"
OUTDIR="${OUTDIR:-${SCRIPT_DIR}/secrets-out}"
NS="${NS:-spark}"
APPLY=0; [ "${1:-}" = "--apply" ] && APPLY=1

: "${TENANTS:?set TENANTS (e.g. 'tenant_a tenant_b') - the per-tenant client principals}"
: "${SERVER_DNS:=connect.internal localhost}"

mkdir -p "${OUTDIR}"; chmod 700 "${OUTDIR}"
CERTDIR="${OUTDIR}/certs"

echo "[1/3] issuing CA + gateway server cert + per-tenant client certs (${TENANTS}) into ${CERTDIR} ..."
AUTH_CERTS_DIR="${CERTDIR}" PRINCIPALS="${TENANTS}" SERVER_DNS="${SERVER_DNS}" "${CERTS_SRC}/issue-all.sh"

echo "[2/3] PSK ..."
psk_file="${OUTDIR}/connect.psk"
if [ -n "${CONNECT_PSK:-}" ] && [ "${CONNECT_PSK}" != "REPLACE-64-byte-hex" ]; then
  printf '%s' "${CONNECT_PSK}" > "${psk_file}"
else
  openssl rand -hex 32 > "${psk_file}"
  echo "  (no stable CONNECT_PSK in env; generated one at ${psk_file} - record it in client.env)"
fi
chmod 600 "${psk_file}"

echo "[3/3] k8s Secrets + ConfigMap (names verified against authz/*.yaml) ..."
SRV="${CERTDIR}/server"; CA="${CERTDIR}/ca"
cmd_psk="kubectl -n ${NS} create secret generic spark-connect-psk --from-file=token=${psk_file}"
cmd_gw="kubectl -n ${NS} create secret generic connect-gateway-certs --from-file=server.crt=${SRV}/server.crt --from-file=server.key=${SRV}/server.key --from-file=connect-ca.crt=${CA}/connect-ca.crt"
cmd_cfg="kubectl -n ${NS} create configmap connect-gateway-config --from-file=envoy.yaml=${GW_ENVOY}"

if [ "${APPLY}" = "1" ]; then
  echo "  applying to cluster (ns ${NS}) ..."
  eval "${cmd_psk}"; eval "${cmd_gw}"; eval "${cmd_cfg}"
else
  printf '  DRY-RUN (re-run with --apply on the cluster):\n    %s\n    %s\n    %s\n' "${cmd_psk}" "${cmd_gw}" "${cmd_cfg}"
fi

echo
echo "Done. Per-tenant client bundles (for the custodian to present at the gateway): ${CERTDIR}/clients/<tenant>/"

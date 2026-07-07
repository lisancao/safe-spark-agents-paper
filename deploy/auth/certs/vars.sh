#!/usr/bin/env bash
# Shared configuration for the Connect-layer mTLS CA and per-principal certificates.
#
# IMPORTANT: this is the CONNECT-LAYER CA. It is a SEPARATE trust root from B1's Client VPN mTLS
# CA (the network layer). Two independent CAs = two-layer defence in depth. Do NOT reuse the VPN
# CA here, and do NOT reuse this CA for the VPN.
#
# Every value can be overridden from the environment, e.g.:
#   PRINCIPALS="alice bob carol agent_a agent_b" ./issue-all.sh
#
# shellcheck disable=SC2034  # vars are consumed by the scripts that source this file

# Trust domain used to build SAN URIs: spiffe://${TRUST_DOMAIN}/<principal>.
# The Envoy Lua filter takes the URI's last path segment as the principal, which MUST equal the
# Spark Connect user_id the client connects with.
TRUST_DOMAIN="${TRUST_DOMAIN:-safe-spark-agents}"

# Subject org / CA common name.
ORG="${ORG:-Safe Spark Agents}"
CA_CN="${CA_CN:-Safe Spark Agents Connect-layer CA}"

# DNS name(s) the Spark Connect endpoint is reached as (server cert SANs). Space-separated.
# The clients connect to the auth-proxy listener via this name over TLS.
SERVER_DNS="${SERVER_DNS:-connect.internal localhost}"

# Validity (days).
CA_DAYS="${CA_DAYS:-3650}"
CERT_DAYS="${CERT_DAYS:-365}"

# Key strength.
KEY_BITS="${KEY_BITS:-4096}"

# Auth-proxy port, only used to print ready-to-use connection strings in the client bundle.
AUTH_PROXY_PORT="${AUTH_PROXY_PORT:-15009}"

# Principals to issue client certs for: the 3 human users + each agent sandbox.
# CN and SAN of each cert == the principal id == the required Spark Connect user_id.
PRINCIPALS="${PRINCIPALS:-alice bob carol agent_a agent_b}"

# Output layout (override AUTH_CERTS_DIR to relocate, e.g. to a scratch dir for testing).
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
AUTH_CERTS_DIR="${AUTH_CERTS_DIR:-${SCRIPT_DIR}/out}"
CA_DIR="${AUTH_CERTS_DIR}/ca"
SERVER_DIR="${AUTH_CERTS_DIR}/server"
CLIENTS_DIR="${AUTH_CERTS_DIR}/clients"

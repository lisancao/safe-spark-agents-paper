#!/usr/bin/env bash
# Create the Connect-layer mTLS Certificate Authority.
#
# Output: ${CA_DIR}/connect-ca.key (private, keep secret) and connect-ca.crt (the trust root that
# Envoy validates client certs against, and that clients use to trust the proxy's server cert).
#
# Idempotent: refuses to overwrite an existing CA unless FORCE=1 (so you never silently rotate the
# root and invalidate every issued cert).
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=vars.sh disable=SC1091
source "${SCRIPT_DIR}/vars.sh"

mkdir -p "${CA_DIR}"
chmod 700 "${CA_DIR}"

ca_key="${CA_DIR}/connect-ca.key"
ca_crt="${CA_DIR}/connect-ca.crt"

if [ -f "${ca_key}" ] && [ "${FORCE:-0}" != "1" ]; then
  echo "CA already exists at ${ca_key} (set FORCE=1 to regenerate — this invalidates all issued certs)." >&2
  exit 1
fi

echo "Generating Connect-layer CA key (${KEY_BITS}-bit RSA)..."
openssl genrsa -out "${ca_key}" "${KEY_BITS}"
chmod 600 "${ca_key}"

echo "Self-signing CA certificate (CN=${CA_CN}, ${CA_DAYS} days)..."
openssl req -x509 -new -nodes \
  -key "${ca_key}" \
  -sha256 \
  -days "${CA_DAYS}" \
  -subj "/O=${ORG}/CN=${CA_CN}" \
  -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -out "${ca_crt}"

echo "Connect-layer CA ready:"
echo "  key : ${ca_key}   (SECRET — never distribute)"
echo "  crt : ${ca_crt}   (distribute as the trust root)"

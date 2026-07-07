#!/usr/bin/env bash
# Issue the server (leaf) certificate Envoy presents when terminating client TLS on the auth-proxy
# port. Signed by the Connect-layer CA; SANs come from SERVER_DNS (see vars.sh).
#
# Output: ${SERVER_DIR}/server.crt, server.key  -> install as /etc/envoy/certs/server.{crt,key}.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=vars.sh disable=SC1091
source "${SCRIPT_DIR}/vars.sh"

ca_key="${CA_DIR}/connect-ca.key"
ca_crt="${CA_DIR}/connect-ca.crt"
if [ ! -f "${ca_key}" ] || [ ! -f "${ca_crt}" ]; then
  echo "CA not found in ${CA_DIR}. Run ./make-ca.sh first." >&2
  exit 1
fi

mkdir -p "${SERVER_DIR}"
chmod 700 "${SERVER_DIR}"

key="${SERVER_DIR}/server.key"
csr="${SERVER_DIR}/server.csr"
crt="${SERVER_DIR}/server.crt"

# Build subjectAltName from SERVER_DNS (first name also becomes the CN).
primary_cn=""
san=""
for name in ${SERVER_DNS}; do
  if [ -z "${primary_cn}" ]; then
    primary_cn="${name}"
  fi
  if [ -n "${san}" ]; then
    san="${san},"
  fi
  san="${san}DNS:${name}"
done

ext="$(mktemp)"
trap 'rm -f "${ext}"' EXIT
cat > "${ext}" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=${san}
EOF

echo "Generating server key + CSR (CN=${primary_cn}, SAN=${san})..."
openssl genrsa -out "${key}" "${KEY_BITS}"
chmod 600 "${key}"
openssl req -new -key "${key}" -subj "/O=${ORG}/CN=${primary_cn}" -out "${csr}"

echo "Signing server certificate (${CERT_DAYS} days)..."
openssl x509 -req \
  -in "${csr}" \
  -CA "${ca_crt}" -CAkey "${ca_key}" -CAcreateserial \
  -sha256 -days "${CERT_DAYS}" \
  -extfile "${ext}" \
  -out "${crt}"

rm -f "${csr}"
echo "Server certificate ready:"
echo "  crt : ${crt}  -> /etc/envoy/certs/server.crt"
echo "  key : ${key}  -> /etc/envoy/certs/server.key (SECRET)"
echo "  ca  : ${ca_crt} -> /etc/envoy/certs/connect-ca.crt"

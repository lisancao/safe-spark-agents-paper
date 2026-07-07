#!/usr/bin/env bash
# Issue ONE per-principal client certificate bundle, signed by the Connect-layer CA.
#
# Usage: ./issue-client-cert.sh <principal>
#   e.g. ./issue-client-cert.sh agent_a
#
# The cert carries:
#   CN      = <principal>
#   SAN URI = spiffe://${TRUST_DOMAIN}/<principal>
# Envoy derives the principal from the SAN URI's last path segment (CN is the fallback). That
# principal is injected as x-connect-principal and the gRPC interceptor pins the Spark Connect
# user_id to it. THEREFORE the client MUST connect with user_id=<principal> or every RPC is
# rejected.
#
# Output bundle: ${CLIENTS_DIR}/<principal>/{client.crt, client.key, connect-ca.crt, CONNECT.md}
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=vars.sh disable=SC1091
source "${SCRIPT_DIR}/vars.sh"

principal="${1:-}"
if [ -z "${principal}" ]; then
  echo "usage: $0 <principal>" >&2
  exit 2
fi
# Principal must equal a valid Spark user_id and a clean URI path segment.
if ! printf '%s' "${principal}" | grep -qE '^[A-Za-z0-9_.-]+$'; then
  echo "invalid principal '${principal}': use only [A-Za-z0-9_.-]" >&2
  exit 2
fi

ca_key="${CA_DIR}/connect-ca.key"
ca_crt="${CA_DIR}/connect-ca.crt"
if [ ! -f "${ca_key}" ] || [ ! -f "${ca_crt}" ]; then
  echo "CA not found in ${CA_DIR}. Run ./make-ca.sh first." >&2
  exit 1
fi

out="${CLIENTS_DIR}/${principal}"
mkdir -p "${out}"
chmod 700 "${out}"

key="${out}/client.key"
csr="${out}/client.csr"
crt="${out}/client.crt"
uri="spiffe://${TRUST_DOMAIN}/${principal}"

ext="$(mktemp)"
trap 'rm -f "${ext}"' EXIT
cat > "${ext}" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
subjectAltName=URI:${uri}
EOF

echo "Issuing client cert for principal '${principal}' (SAN ${uri})..."
openssl genrsa -out "${key}" "${KEY_BITS}"
chmod 600 "${key}"
openssl req -new -key "${key}" -subj "/O=${ORG}/CN=${principal}" -out "${csr}"
openssl x509 -req \
  -in "${csr}" \
  -CA "${ca_crt}" -CAkey "${ca_key}" -CAcreateserial \
  -sha256 -days "${CERT_DAYS}" \
  -extfile "${ext}" \
  -out "${crt}"
rm -f "${csr}"

cp "${ca_crt}" "${out}/connect-ca.crt"

cat > "${out}/CONNECT.md" <<EOF
# Client bundle for principal: ${principal}

Files:
- client.crt       this principal's certificate (CN=${principal}, SAN=${uri})
- client.key       this principal's PRIVATE key — keep secret
- connect-ca.crt   the Connect-layer CA, used to trust the auth proxy's server cert

The verified principal Envoy injects = the SAN URI's last segment = \`${principal}\`.
You MUST connect with user_id=${principal}; any other user_id is rejected by the interceptor.

## Spark Connect connection string
\`\`\`
sc://connect.internal:${AUTH_PROXY_PORT}/;user_id=${principal};use_ssl=true
\`\`\`
NB: the auth proxy supplies the server's bearer token; clients do NOT pass ;token=.

## PySpark client TLS material
Point the client TLS at this bundle (env / connection options), e.g.:
- root CA  : connect-ca.crt
- client certificate / key : client.crt / client.key (mTLS to the proxy)
EOF

echo "Bundle ready: ${out}"

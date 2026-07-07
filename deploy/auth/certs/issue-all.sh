#!/usr/bin/env bash
# One-shot: build the CA (if absent), issue the Envoy server cert, and issue a client bundle for
# every principal in PRINCIPALS (see vars.sh). Re-runnable.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=vars.sh disable=SC1091
source "${SCRIPT_DIR}/vars.sh"

if [ ! -f "${CA_DIR}/connect-ca.key" ]; then
  "${SCRIPT_DIR}/make-ca.sh"
else
  echo "CA present at ${CA_DIR} — reusing it."
fi

"${SCRIPT_DIR}/issue-server-cert.sh"

for principal in ${PRINCIPALS}; do
  "${SCRIPT_DIR}/issue-client-cert.sh" "${principal}"
done

echo
echo "All certificates issued under ${AUTH_CERTS_DIR}"
echo "  CA      : ${CA_DIR}"
echo "  server  : ${SERVER_DIR}"
echo "  clients : ${CLIENTS_DIR} (${PRINCIPALS})"

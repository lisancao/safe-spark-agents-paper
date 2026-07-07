#!/usr/bin/env bash
# Egress mTLS sidecar entrypoint (Option A).
#
# Holds the per-principal client cert/key, validates that the cert actually belongs to
# AGENT_PRINCIPAL (so a wrong/mismatched cert cannot even start the tunnel), renders the Envoy
# bootstrap from the template, and execs Envoy. The thin-client container shares this network
# namespace and connects in plaintext to 127.0.0.1:LISTEN_PORT; we originate the real mTLS.
set -euo pipefail

# --- Required injection ---------------------------------------------------------------------
: "${AGENT_PRINCIPAL:?inject AGENT_PRINCIPAL (must match the client cert URI SAN)}"
: "${REMOTE_HOST:?inject REMOTE_HOST (the remote NLB / on-box Envoy hostname)}"

REMOTE_PORT="${REMOTE_PORT:-443}"
LISTEN_PORT="${LISTEN_PORT:-15002}"
SNI_HOST="${SNI_HOST:-${REMOTE_HOST}}"
CLIENT_CERT="${CLIENT_CERT:-/etc/egress/client.crt}"
CLIENT_KEY="${CLIENT_KEY:-/etc/egress/client.key}"
CA_CERT="${CA_CERT:-/etc/egress/ca.crt}"
# Optional defence-in-depth: pin the REMOTE SERVER identity too. When REMOTE_SAN is set we also
# require the server cert to carry that SAN (else the CA alone is trusted). REMOTE_SAN_TYPE is the
# SAN kind to match: DNS (default), URI, IP_ADDRESS, or EMAIL.
REMOTE_SAN="${REMOTE_SAN:-}"
REMOTE_SAN_TYPE="${REMOTE_SAN_TYPE:-DNS}"

if ! [[ "${AGENT_PRINCIPAL}" =~ ^[a-z0-9_]+$ ]]; then
  echo "FATAL: AGENT_PRINCIPAL='${AGENT_PRINCIPAL}' must match ^[a-z0-9_]+$." >&2
  exit 1
fi

for f in "${CLIENT_CERT}" "${CLIENT_KEY}" "${CA_CERT}"; do
  if [ ! -r "$f" ]; then
    echo "FATAL: required cert material not readable: $f (mount it into the sidecar, read-only)." >&2
    exit 1
  fi
done

# --- Pin the identity by construction: cert SAN MUST equal this principal --------------------
# The remote derives the principal from this SAN; if the mounted cert is for a different principal
# the tunnel would authenticate as someone else. Refuse to start rather than misrepresent identity.
EXPECT_SAN="spiffe://safe-spark-agents/${AGENT_PRINCIPAL}"
SAN_LINE="$(openssl x509 -in "${CLIENT_CERT}" -noout -ext subjectAltName 2>/dev/null || true)"
if [ -z "${SAN_LINE}" ]; then
  echo "FATAL: could not read subjectAltName from ${CLIENT_CERT} (need a SAN cert from deploy/auth)." >&2
  exit 1
fi
# Match the exact URI SAN as a whole token (avoid prefix collisions like _agent_1 vs _agent_12).
if ! grep -Eq "URI:${EXPECT_SAN}(,|$|[[:space:]])" <<<"${SAN_LINE}"; then
  echo "FATAL: cert SAN mismatch. Expected 'URI:${EXPECT_SAN}', cert has:" >&2
  echo "       ${SAN_LINE}" >&2
  echo "       Refusing to start: this cert does not belong to principal '${AGENT_PRINCIPAL}'." >&2
  exit 1
fi
echo "sidecar: cert SAN verified == URI:${EXPECT_SAN}"

# --- Render the Envoy bootstrap -------------------------------------------------------------
RENDERED=/etc/egress/envoy.yaml
sed -e "s|__LISTEN_PORT__|${LISTEN_PORT}|g" \
    -e "s|__REMOTE_HOST__|${REMOTE_HOST}|g" \
    -e "s|__REMOTE_PORT__|${REMOTE_PORT}|g" \
    -e "s|__SNI_HOST__|${SNI_HOST}|g" \
    -e "s|__CLIENT_CERT__|${CLIENT_CERT}|g" \
    -e "s|__CLIENT_KEY__|${CLIENT_KEY}|g" \
    -e "s|__CA_CERT__|${CA_CERT}|g" \
    /etc/egress/envoy.template.yaml > "${RENDERED}"

# Optional: append server-SAN pinning under validation_context (indentation matches the template:
# trusted_ca is at 12 spaces, so the matcher block sits at the same level).
if [ -n "${REMOTE_SAN}" ]; then
  if ! [[ "${REMOTE_SAN_TYPE}" =~ ^(DNS|URI|IP_ADDRESS|EMAIL)$ ]]; then
    echo "FATAL: REMOTE_SAN_TYPE='${REMOTE_SAN_TYPE}' must be one of DNS|URI|IP_ADDRESS|EMAIL." >&2
    exit 1
  fi
  {
    printf '            match_typed_subject_alt_names:\n'
    printf '            - san_type: %s\n' "${REMOTE_SAN_TYPE}"
    printf '              matcher: { exact: "%s" }\n' "${REMOTE_SAN}"
  } >> "${RENDERED}"
  echo "sidecar: pinning REMOTE server identity ${REMOTE_SAN_TYPE} SAN == '${REMOTE_SAN}'"
else
  echo "sidecar: server identity verified by CA only (set REMOTE_SAN to also pin the server SAN)."
fi

echo "sidecar ready: 127.0.0.1:${LISTEN_PORT} (plaintext h2c) -> mTLS -> ${REMOTE_HOST}:${REMOTE_PORT} (SNI ${SNI_HOST})"

# Validate before serving so a bad render fails fast and visibly.
envoy --mode validate -c "${RENDERED}"

exec envoy -c "${RENDERED}" "$@"

#!/usr/bin/env bash
# spark-omnigent entrypoint (Option A: Envoy mTLS + identity pinning).
#
# This is the THIN-CLIENT (agent) container. It does NOT hold the client cert/key and does NOT
# speak mTLS itself: pyspark-client cannot present a client certificate (see README "Feasibility").
# Instead it connects in plaintext to a LOCAL egress sidecar (see sidecar/) over loopback; the
# sidecar holds the per-principal cert/key and originates mTLS to the remote NLB/Envoy.
#
# Identity: the Spark Connect user_id is DERIVED here from the single AGENT_PRINCIPAL var (never
# injected independently), so the DEFAULT user_id matches the cert. That is a sane default, NOT a
# hard guarantee -- code running in this container is in-process and could build its own session
# asserting a different user_id. The AUTHORITATIVE wall is server-side: the interceptor (deploy/auth,
# PR #3) REJECTS unless user_id == the principal proven by the cert. The sidecar additionally fails
# fast if the mounted cert's SAN != AGENT_PRINCIPAL, so a wrong cert cannot even establish the tunnel.
set -euo pipefail

# --- Required runtime injection (no secrets baked) -----------------------------------------
: "${AGENT_PRINCIPAL:?inject AGENT_PRINCIPAL (the per-agent identity; cert SAN == spiffe://safe-spark-agents/<principal>)}"
: "${GIT_REPO:?inject GIT_REPO (owner/name of the pipeline repo)}"
: "${GIT_TOKEN:?inject GIT_TOKEN (fine-grained, short TTL)}"

# Principal must be schema-/SAN-safe: this same string becomes the UC schema and the gRPC user_id.
if ! [[ "${AGENT_PRINCIPAL}" =~ ^[a-z0-9_]+$ ]]; then
  echo "FATAL: AGENT_PRINCIPAL='${AGENT_PRINCIPAL}' must match ^[a-z0-9_]+$ (it becomes the schema and user_id)." >&2
  exit 1
fi

# Schema is a CONVENTION derived from the principal. If injected, it MUST agree (mismatch = abort).
DERIVED_SCHEMA="sandbox_${AGENT_PRINCIPAL}"
AGENT_SCHEMA="${AGENT_SCHEMA:-${DERIVED_SCHEMA}}"
if [[ "${AGENT_SCHEMA}" != "${DERIVED_SCHEMA}" ]]; then
  echo "FATAL: AGENT_SCHEMA='${AGENT_SCHEMA}' != derived '${DERIVED_SCHEMA}'. Schema is bound to the principal." >&2
  exit 1
fi
export AGENT_SCHEMA

# Loopback address of the egress sidecar (same network namespace via compose: network_mode service).
# Plaintext to the sidecar (use_ssl=false, NO token in the URL so the channel stays insecure on the
# loopback hop); the sidecar does the real mTLS. user_id is pinned to AGENT_PRINCIPAL, full stop.
SIDECAR_GRPC_ADDR="${SIDECAR_GRPC_ADDR:-127.0.0.1:15002}"
export SPARK_REMOTE="sc://${SIDECAR_GRPC_ADDR}/;user_id=${AGENT_PRINCIPAL}"

# Render the catalog config from the template (endpoint + schema are runtime, not baked).
sed -e "s|__SPARK_REMOTE__|${SPARK_REMOTE}|g" \
    -e "s|__AGENT_SCHEMA__|${AGENT_SCHEMA}|g" \
    /opt/spark-omnigent/spark-defaults.template.conf > /workspace/spark-defaults.conf
export SPARK_CONF_DIR=/workspace

# Scoped git credential, in memory only, for this session.
# shellcheck disable=SC2016  # intentional: git expands ${GIT_TOKEN} when it RUNS the helper, not now.
git config --global credential.helper '!f() { echo "username=x-access-token"; echo "password=${GIT_TOKEN}"; }; f'

# --- SDP skill pack: LINKED, not embedded (pyspark-sdp is WIP) -----------------------------
# Mode 1 (dev): a checkout is bind-mounted at /opt/spark-omnigent/skills -> use it live.
# Mode 2 (built image): shallow-clone the skill repo at the pinned ref so it is always current.
SKILL_MOUNT=/opt/spark-omnigent/skills
if [ -n "$(ls -A "$SKILL_MOUNT" 2>/dev/null)" ]; then
  export OMNIGENT_SKILLS="$SKILL_MOUNT"
  echo "skills: using bind-mounted checkout at $SKILL_MOUNT (linked to your WIP repo)"
else
  skill_url="$SDP_SKILL_REPO"
  if [ -n "${SDP_SKILL_TOKEN:-}" ]; then
    skill_url="https://x-access-token:${SDP_SKILL_TOKEN}@${SDP_SKILL_REPO#https://}"
  fi
  if git clone --depth 1 --branch "${SDP_SKILL_REF}" "$skill_url" /opt/pyspark-sdp 2>/dev/null; then
    export OMNIGENT_SKILLS=/opt/pyspark-sdp/.claude/skills
    echo "skills: cloned ${SDP_SKILL_REPO}@${SDP_SKILL_REF} (linked, not embedded)"
  else
    export OMNIGENT_SKILLS=""
    echo "WARN: could not fetch the SDP skill (need read access via SDP_SKILL_TOKEN, or bind-mount it). Continuing without it."
  fi
fi

# Pipeline repo.
git clone "https://github.com/${GIT_REPO}.git" /workspace/repo
cd /workspace/repo

echo "spark-omnigent ready (Option A: mTLS via local egress sidecar):"
echo "  principal : ${AGENT_PRINCIPAL}   (== gRPC user_id == cert SAN local-part)"
echo "  connect   : ${SPARK_REMOTE}      (plaintext loopback -> sidecar -> mTLS -> remote)"
echo "  schema    : ${AGENT_SCHEMA}      (only writable namespace; convention, interceptor-pinned)"
echo "  repo      : ${GIT_REPO}"
echo "  skills    : ${OMNIGENT_SKILLS:-<none>}"

# Hand off to the agent harness when wired (OMNIGENT_SANDBOX.md C4):
#   exec omnigents run --skills "$OMNIGENT_SKILLS" ...
exec "${@:-/bin/bash}"

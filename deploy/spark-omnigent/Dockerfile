# spark-omnigent: a thin-client agent sandbox (Phase 2, see OMNIGENT_SANDBOX.md).
# NO JVM. NO baked secrets. NO production endpoints. NO embedded skill copy.
# Engine-free: all execution runs on a remote, per-agent-authenticated Spark Connect server.
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 1. Thin Spark Connect client (1.8 MB, no jars) + the Connect Python deps.
#    PIP_INDEX_URL defaults to public PyPI; override where PyPI is blocked, e.g.
#    --build-arg PIP_INDEX_URL=https://<your-mirror>/simple
ARG PIP_INDEX_URL=https://pypi.org/simple
# Cleanup is in the SAME layer as the install (removing files in a later layer does not shrink
# the image). We strip debug symbols and drop test suites / bytecode only. We do NOT delete
# pyarrow .so components: pyarrow loads libarrow_substrait at import, so pruning them breaks it.
RUN pip install --no-cache-dir --index-url "${PIP_INDEX_URL}" \
      pyspark-client \
      "pandas>=2.2" pyarrow grpcio grpcio-status googleapis-common-protos zstandard pyyaml \
 && SP=$(python -c 'import site;print(site.getsitepackages()[0])') \
 && find "$SP" -type d -name tests -prune -exec rm -rf {} + 2>/dev/null || true \
 && find "$SP" -type d -name __pycache__ -prune -exec rm -rf {} + 2>/dev/null || true \
 && find "$SP" -name '*.pyc' -delete 2>/dev/null || true

# 2. The Omnigent agent harness (pin a version in real builds).
#    RUN pip install --no-cache-dir omnigents
# (left commented until the wheel source/index is decided; see OMNIGENT_SANDBOX.md C1.)

# 3. Bake ONLY the parts identical for every agent and version-independent.
#    The SDP skill pack is LINKED at runtime, never copied in, because pyspark-sdp is WIP:
#    a baked copy would go stale. entrypoint.sh either uses a bind-mounted checkout (dev) or
#    shallow-clones lisancao/pyspark-sdp at SDP_SKILL_REF (built images).
RUN mkdir -p /opt/spark-omnigent/skills        # bind-mount target for the dev/linked path
COPY conf/spark-defaults.template.conf /opt/spark-omnigent/spark-defaults.template.conf
COPY entrypoint.sh /opt/spark-omnigent/entrypoint.sh
RUN chmod +x /opt/spark-omnigent/entrypoint.sh

# 4. Runtime contract (Option A: mTLS via the egress sidecar; all INJECTED, none baked).
#    This (thin-client) container holds NO cert/key and speaks PLAINTEXT to the loopback sidecar;
#    the sidecar (see sidecar/) holds the per-principal cert and originates mTLS to the remote.
#    AGENT_PRINCIPAL  the per-agent identity. Becomes gRPC user_id AND schema; cert SAN must be
#                     spiffe://safe-spark-agents/<AGENT_PRINCIPAL>. Derives everything below.
#    AGENT_SCHEMA     optional; defaults to sandbox_<AGENT_PRINCIPAL>; if set it MUST equal that.
#    SIDECAR_GRPC_ADDR  loopback addr of the egress sidecar (default 127.0.0.1:15002).
#    GIT_TOKEN        fine-grained, write to the pipeline repo, short TTL (secret, -e at run).
#    GIT_REPO         the pipeline repo to clone (owner/name).
#    SDP_SKILL_REPO   default https://github.com/lisancao/pyspark-sdp.git  (linked, not embedded)
#    SDP_SKILL_REF    default main          (pin to a tag/sha for reproducible fleets)
#    SDP_SKILL_TOKEN  optional; read access to the skill repo if private (else mount it)
# NOTE: SPARK_REMOTE is DERIVED by entrypoint.sh from AGENT_PRINCIPAL + SIDECAR_GRPC_ADDR (not
# injected), so the DEFAULT user_id matches the principal. The AUTHORITATIVE check that user_id ==
# the cert principal is the SERVER-side interceptor (deploy/auth). The remote endpoint + cert live
# only in the sidecar container, never here.
# Non-secret runtime defaults only. Secrets (GIT_TOKEN, SDP_SKILL_TOKEN) are passed at
# `docker run -e ...`, never declared in the image.
ENV AGENT_PRINCIPAL="" AGENT_SCHEMA="" GIT_REPO="" SIDECAR_GRPC_ADDR="127.0.0.1:15002" \
    SDP_SKILL_REPO="https://github.com/lisancao/pyspark-sdp.git" SDP_SKILL_REF="main"

WORKDIR /workspace
ENTRYPOINT ["/opt/spark-omnigent/entrypoint.sh"]

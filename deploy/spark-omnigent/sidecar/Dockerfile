# Egress mTLS sidecar for spark-omnigent (Option A).
# Envoy that presents the per-principal client cert to the remote and exposes a loopback
# plaintext gRPC port for the thin client. Pin a concrete Envoy tag in real builds.
#
# We use the shell-bearing (Ubuntu) Envoy image, NOT :distroless, because the entrypoint runs an
# openssl SAN check + sed render before exec'ing Envoy. The cert/key are mounted at runtime,
# never baked.
FROM envoyproxy/envoy:v1.31-latest

# openssl for the cert-SAN identity check; bash for the entrypoint; curl for the healthcheck.
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openssl ca-certificates bash curl \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /etc/egress
COPY envoy.template.yaml /etc/egress/envoy.template.yaml
COPY entrypoint.sh /usr/local/bin/sidecar-entrypoint.sh
RUN chmod +x /usr/local/bin/sidecar-entrypoint.sh

# Runtime contract (all injected; cert material mounted read-only at run):
#   AGENT_PRINCIPAL  per-agent identity; cert SAN must be spiffe://safe-spark-agents/<principal>
#   REMOTE_HOST      remote NLB / on-box Envoy hostname (required)
#   REMOTE_PORT      default 443
#   LISTEN_PORT      loopback gRPC port for the thin client (default 15002)
#   SNI_HOST         default REMOTE_HOST
#   CLIENT_CERT/CLIENT_KEY/CA_CERT   default /etc/egress/{client.crt,client.key,ca.crt} (mounted)
#   REMOTE_SAN       optional; also pin the remote SERVER cert SAN (defence in depth; CA-only if unset)
#   REMOTE_SAN_TYPE  optional; DNS (default) | URI | IP_ADDRESS | EMAIL
ENV REMOTE_PORT="443" LISTEN_PORT="15002"

ENTRYPOINT ["/usr/local/bin/sidecar-entrypoint.sh"]

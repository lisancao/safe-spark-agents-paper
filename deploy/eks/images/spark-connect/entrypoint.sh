#!/usr/bin/env bash
# entrypoint.sh — render spark-defaults from the template, then dispatch by role.
#
# Roles (same image for all):
#   connect-server   long-running Spark Connect server (the Deployment)
#   driver|executor  delegated to the stock apache/spark k8s entrypoint
#                    (executors are launched by Spark itself using THIS image)
#
# Nothing secret is baked: the Connect PSK and AWS creds arrive at runtime
# (k8s Secret env + IRSA projected token).
set -euo pipefail

SPARK_HOME="${SPARK_HOME:-/opt/spark}"
TEMPLATE="${SPARK_DEFAULTS_TEMPLATE:-/opt/spark-connect/spark-defaults.template.conf}"
# SPARK_CONF_DIR is writable (chmod'd in the build) so we can render into it.
SPARK_CONF_DIR="${SPARK_CONF_DIR:-${SPARK_HOME}/conf}"
RENDERED="${SPARK_CONF_DIR}/spark-defaults.conf"

# ── Runtime config (all injected by the pod spec / Secret / IRSA webhook) ──
ICEBERG_CATALOG="${ICEBERG_CATALOG:-iceberg}"
WAREHOUSE="${WAREHOUSE:-s3a://CHANGE-ME-bucket/warehouse}"
INTERCEPTOR_CLASSES="${INTERCEPTOR_CLASSES:-com.safesparkagents.connect.auth.PrincipalPinningInterceptor}"
SPARK_SERVICE_ACCOUNT="${SPARK_SERVICE_ACCOUNT:-spark}"
# Iceberg JDBC catalog (RDS Postgres). URL is non-secret (ConfigMap); user/password
# come from a k8s Secret. Empty creds => loud warning (catalog ops will fail) rather
# than baking a default.
ICEBERG_JDBC_URL="${ICEBERG_JDBC_URL:-jdbc:postgresql://CHANGE-ME:5432/iceberg_catalog}"
ICEBERG_JDBC_USER="${ICEBERG_JDBC_USER:-}"
ICEBERG_JDBC_PASSWORD="${ICEBERG_JDBC_PASSWORD:-}"
# PSK from a mounted k8s Secret. Empty => leave the directive blank (server will
# reject; we surface a loud warning rather than baking a default token).
CONNECT_AUTH_TOKEN="${CONNECT_AUTH_TOKEN:-}"

render_conf() {
  if [ ! -f "${TEMPLATE}" ]; then
    echo "[entrypoint] FATAL: template not found at ${TEMPLATE}" >&2
    exit 1
  fi
  mkdir -p "${SPARK_CONF_DIR}"
  if [ -z "${CONNECT_AUTH_TOKEN}" ]; then
    echo "[entrypoint] WARNING: CONNECT_AUTH_TOKEN is empty — mount the PSK Secret" >&2
    echo "[entrypoint]          (env CONNECT_AUTH_TOKEN). Connect auth will fail until set." >&2
  fi
  if [ ! -f "${SPARK_HOME}/jars/interceptor.jar" ]; then
    echo "[entrypoint] WARNING: interceptor.jar not baked. spark.connect.grpc.interceptor.classes" >&2
    echo "[entrypoint]          (${INTERCEPTOR_CLASSES}) will not resolve. Rebuild with INTERCEPTOR_JAR set (PR #3)." >&2
  fi
  if [ -z "${ICEBERG_JDBC_USER}" ] || [ -z "${ICEBERG_JDBC_PASSWORD}" ]; then
    echo "[entrypoint] WARNING: ICEBERG_JDBC_USER/PASSWORD empty — mount the spark-iceberg-jdbc" >&2
    echo "[entrypoint]          Secret. Iceberg JDBC catalog ops will fail until set." >&2
  fi

  # Render __PLACEHOLDER__ tokens by LITERAL (non-regex) string replacement — NOT sed.
  # Several rendered values are SECRET-sourced (the Iceberg JDBC password and the Connect
  # PSK token) and credential generators routinely emit punctuation: a backslash or sed
  # replacement metacharacter ('\1', a trailing '\', '&') in a value would corrupt the
  # rendered config or silently mangle the secret under sed. python3 (present in the Spark
  # image) does exact str-for-str substitution in a SINGLE pass (so a value can never be
  # re-substituted), with the replacement inserted verbatim — no regex, no escaping. Values
  # are passed via the child process ENVIRONMENT (never argv, never disk outside the in-pod
  # rendered file); only the non-secret template/output PATHS are on argv.
  export ICEBERG_CATALOG WAREHOUSE ICEBERG_JDBC_URL ICEBERG_JDBC_USER ICEBERG_JDBC_PASSWORD \
         INTERCEPTOR_CLASSES CONNECT_AUTH_TOKEN SPARK_SERVICE_ACCOUNT
  python3 - "${TEMPLATE}" "${RENDERED}" <<'PYRENDER'
import os, re, sys
tmpl_path, out_path = sys.argv[1], sys.argv[2]
# placeholder -> environment variable name (values read from os.environ, inserted LITERALLY)
mapping = {
    "__ICEBERG_CATALOG__":       os.environ.get("ICEBERG_CATALOG", ""),
    "__WAREHOUSE__":             os.environ.get("WAREHOUSE", ""),
    "__ICEBERG_JDBC_URL__":      os.environ.get("ICEBERG_JDBC_URL", ""),
    "__ICEBERG_JDBC_USER__":     os.environ.get("ICEBERG_JDBC_USER", ""),
    "__ICEBERG_JDBC_PASSWORD__": os.environ.get("ICEBERG_JDBC_PASSWORD", ""),
    "__INTERCEPTOR_CLASSES__":   os.environ.get("INTERCEPTOR_CLASSES", ""),
    "__CONNECT_AUTH_TOKEN__":    os.environ.get("CONNECT_AUTH_TOKEN", ""),
    "__SPARK_SERVICE_ACCOUNT__": os.environ.get("SPARK_SERVICE_ACCOUNT", ""),
}
with open(tmpl_path, "r") as f:
    text = f.read()
# Single pass over the fixed, literal placeholder tokens; the replacement function returns
# the env value verbatim (re.sub does NOT process backreferences in a function result), so
# backslashes / '&' / '\1' in a password or PSK are written exactly as-is and an inserted
# value is never itself re-scanned for placeholders.
pattern = re.compile("|".join(re.escape(k) for k in mapping))
text = pattern.sub(lambda m: mapping[m.group(0)], text)
with open(out_path, "w") as f:
    f.write(text)
PYRENDER
  echo "[entrypoint] rendered ${RENDERED}"
}

role="${1:-connect-server}"
case "${role}" in
  connect-server)
    render_conf
    echo "[entrypoint] starting Spark Connect server (foreground)"
    # SPARK_NO_DAEMONIZE keeps spark-daemon in the foreground so the container's
    # PID 1 is the server (k8s liveness + log streaming work correctly). k8s
    # master + executor image come from spark-defaults / extra args after the role.
    export SPARK_NO_DAEMONIZE=1
    shift || true
    exec "${SPARK_HOME}/sbin/start-connect-server.sh" --properties-file "${RENDERED}" "$@"
    ;;
  driver | executor | *)
    # Delegate to the stock apache/spark k8s entrypoint (it dispatches on the
    # SPARK_K8S_CMD / first arg). Executors land here when Spark launches them.
    #
    # IMPORTANT: do NOT render spark-defaults.conf here. Spark-on-k8s mounts its own
    # driver-generated `spark.properties` ConfigMap READ-ONLY over $SPARK_CONF_DIR
    # (/opt/spark/conf) in every executor pod, so writing spark-defaults.conf into that dir
    # fails with "Read-only file system" and the executor exits 1 — which the driver counts as an
    # executor failure, hitting "Max number of executor failures (20) reached" and stopping the
    # SparkContext. Executors do NOT need our template anyway: the driver propagates the full
    # effective SparkConf (catalog/S3A/Iceberg/etc.) to executors via spark.properties. So we skip
    # rendering and hand straight to the stock entrypoint, which reads Spark's mounted conf.
    # (Verified against a live EKS cluster: render here breaks every executor.)
    echo "[entrypoint] delegating role='${role}' to /opt/entrypoint.sh (Spark provides executor conf)"
    exec /opt/entrypoint.sh "$@"
    ;;
esac

#!/usr/bin/env bash
# Start / stop a LOCAL Spark Connect server inside a CI runner.
#
# Why this exists: the SDP CLI (pyspark/pipelines/cli.py) REQUIRES Spark Connect.
# A bare in-process SparkSession fails the dry-run / run with
# ONLY_SUPPORTED_WITH_SPARK_CONNECT, so CI must bring up a real Connect endpoint
# the CLI can dial via SPARK_REMOTE=sc://localhost:<port>. This is the runner-local
# stand-in for the production EKS Connect endpoint (see PRODUCTION_EKS.md).
#
# This is the CI/controller surface -- it is the side that LEGITIMATELY holds a
# Spark session. The agent PR author never runs this script.
#
# Startup mechanism (PROVEN): a pip `pyspark[connect]` install has NO
# sbin/start-connect-server.sh, but it DOES bundle jars/spark-connect_*.jar (which
# contains org.apache.spark.sql.connect.service.SparkConnectServer) and bin/spark-submit
# (which puts jars/* on the classpath). Launching that class via spark-submit with the
# special `spark-internal` primary resource starts the Connect gRPC service.
# Smoke-tested with pyspark[connect] 4.x + JDK 17: server reachable on the port, and
# `SPARK_REMOTE=sc://localhost:<port> cli.py dry-run --spec <valid spec>` -> COMPLETED.
#
# Usage:
#   local_spark_connect.sh start    # launch + wait until the port is reachable
#   local_spark_connect.sh stop     # kill the server started by `start`
#
# Honours: SPARK_HOME (required), CONNECT_PORT (default 15055),
#          RUNNER_TEMP (default /tmp), CONNECT_WAIT_SECS (default 120).
set -euo pipefail

CONNECT_PORT="${CONNECT_PORT:-15055}"
RUNNER_TEMP="${RUNNER_TEMP:-/tmp}"
CONNECT_WAIT_SECS="${CONNECT_WAIT_SECS:-120}"
PID_FILE="${RUNNER_TEMP}/spark-connect-${CONNECT_PORT}.pid"
LOG_FILE="${RUNNER_TEMP}/spark-connect-${CONNECT_PORT}.log"

die() { echo "local_spark_connect: $*" >&2; exit 1; }

port_reachable() {
  # 0 if something is listening on CONNECT_PORT, else 1. Pure-python so we need no
  # nc/ss in the runner image.
  python3 - "$CONNECT_PORT" <<'PY'
import socket, sys
port = int(sys.argv[1])
s = socket.socket()
s.settimeout(1)
try:
    s.connect(("127.0.0.1", port))
    sys.exit(0)
except OSError:
    sys.exit(1)
finally:
    s.close()
PY
}

start() {
  [ -n "${SPARK_HOME:-}" ] || die "SPARK_HOME is not set"
  [ -x "${SPARK_HOME}/bin/spark-submit" ] || die "spark-submit not found under SPARK_HOME=${SPARK_HOME}"

  if port_reachable; then
    echo "local_spark_connect: port ${CONNECT_PORT} already reachable; reusing."
    return 0
  fi

  echo "local_spark_connect: starting Spark Connect server on port ${CONNECT_PORT}..."
  # The SDP CLI dials sc://localhost:${CONNECT_PORT}. spark.sql.artifact.isolation
  # is disabled so the single-runner server serves the CLI directly.
  "${SPARK_HOME}/bin/spark-submit" \
    --class org.apache.spark.sql.connect.service.SparkConnectServer \
    --conf "spark.connect.grpc.binding.port=${CONNECT_PORT}" \
    --conf "spark.sql.warehouse.dir=${RUNNER_TEMP}/spark-warehouse" \
    --conf "spark.sql.artifact.isolation.enabled=false" \
    spark-internal \
    >"${LOG_FILE}" 2>&1 &
  echo $! >"${PID_FILE}"
  echo "local_spark_connect: pid $(cat "${PID_FILE}"), log ${LOG_FILE}"

  local waited=0
  until port_reachable; do
    sleep 2
    waited=$((waited + 2))
    if [ "${waited}" -ge "${CONNECT_WAIT_SECS}" ]; then
      echo "----- spark connect log (tail) -----" >&2
      tail -n 80 "${LOG_FILE}" >&2 || true
      die "Spark Connect did not become reachable on port ${CONNECT_PORT} within ${CONNECT_WAIT_SECS}s"
    fi
  done
  echo "local_spark_connect: reachable on sc://localhost:${CONNECT_PORT} after ${waited}s."
}

stop() {
  if [ -f "${PID_FILE}" ]; then
    local pid
    pid="$(cat "${PID_FILE}")"
    if kill -0 "${pid}" 2>/dev/null; then
      echo "local_spark_connect: stopping pid ${pid}..."
      kill "${pid}" 2>/dev/null || true
      wait "${pid}" 2>/dev/null || true
    fi
    rm -f "${PID_FILE}"
  else
    echo "local_spark_connect: no pid file; nothing to stop."
  fi
}

case "${1:-}" in
  start) start ;;
  stop)  stop ;;
  *) die "usage: $0 {start|stop}" ;;
esac

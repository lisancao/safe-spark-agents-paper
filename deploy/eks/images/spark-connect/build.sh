#!/usr/bin/env bash
# build.sh — build the Spark Connect + Iceberg EKS image (Tier A by default).
#
# Reproducible: every Maven jar is checksum-pinned in jars.sha1; the base image
# can be pinned to a digest via BASE_IMAGE. Versions are build-args so a
# 4.1 -> 4.2/5.0 bump is a flag change (+ a jars.sha1 refresh).
#
# Usage:
#   ./build.sh                                   # Tier A, released 4.1.2 + Iceberg 1.11.0
#   INTERCEPTOR_JAR=/path/to/auth-interceptor.jar ./build.sh
#   ./build.sh --tier-b                          # 5.0-SNAPSHOT base + iceberg-port
#   SPARK_VERSION=4.1.2 ICEBERG_VERSION=1.11.0 IMAGE_TAG=myrepo/spark:tagA ./build.sh
#
# Env knobs (all optional; defaults = the pinned Tier-A BOM):
#   IMAGE_TAG, BASE_IMAGE, SPARK_VERSION, SCALA_BINARY, ICEBERG_SPARK_MODULE,
#   ICEBERG_VERSION, HADOOP_AWS_VERSION, AWS_SDK_BUNDLE_VERSION,
#   KAFKA_CONNECTOR_VERSION, KAFKA_CLIENTS_VERSION, COMMONS_POOL2_VERSION,
#   POSTGRES_JDBC_VERSION, MAVEN_BASE_URL, INTERCEPTOR_JAR, ICEBERG_IN_BASE,
#   ALLOW_MISSING_INTERCEPTOR (1 to build without the PR-#3 jar), PUSH (1 to push)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${HERE}"

# ── Tier defaults (Tier A = released) ───────────────────────────────────────
BASE_IMAGE="${BASE_IMAGE:-apache/spark:4.1.2-scala2.13-java17-python3-ubuntu}"
SPARK_VERSION="${SPARK_VERSION:-4.1.2}"
SCALA_BINARY="${SCALA_BINARY:-2.13}"
ICEBERG_SPARK_MODULE="${ICEBERG_SPARK_MODULE:-4.1}"
ICEBERG_VERSION="${ICEBERG_VERSION:-1.11.0}"
HADOOP_AWS_VERSION="${HADOOP_AWS_VERSION:-3.4.2}"
AWS_SDK_BUNDLE_VERSION="${AWS_SDK_BUNDLE_VERSION:-2.29.52}"
KAFKA_CONNECTOR_VERSION="${KAFKA_CONNECTOR_VERSION:-4.1.2}"
KAFKA_CLIENTS_VERSION="${KAFKA_CLIENTS_VERSION:-3.9.1}"
COMMONS_POOL2_VERSION="${COMMONS_POOL2_VERSION:-2.12.0}"
POSTGRES_JDBC_VERSION="${POSTGRES_JDBC_VERSION:-42.7.4}"
MAVEN_BASE_URL="${MAVEN_BASE_URL:-https://repo1.maven.org/maven2}"
ICEBERG_IN_BASE="${ICEBERG_IN_BASE:-false}"
IMAGE_TAG="${IMAGE_TAG:-spark-connect-iceberg:${SPARK_VERSION}-iceberg${ICEBERG_VERSION}}"

# ── Tier B switch: 5.0-SNAPSHOT base with the Iceberg port baked in ─────────
for arg in "$@"; do
  case "${arg}" in
    --tier-b)
      BASE_IMAGE="${BASE_IMAGE_TIER_B:-lakehouse/spark:5.0.0-snapshot-cdc}"
      ICEBERG_IN_BASE=true
      SPARK_VERSION="${SPARK_VERSION_TIER_B:-5.0.0-SNAPSHOT}"
      IMAGE_TAG="${IMAGE_TAG_TIER_B:-spark-connect-iceberg:5.0.0-snapshot-cdc}"
      echo "[build] Tier B selected: base=${BASE_IMAGE} (Iceberg port baked in base)"
      ;;
    --push) PUSH=1 ;;
    *) echo "[build] unknown arg: ${arg}" >&2; exit 2 ;;
  esac
done
PUSH="${PUSH:-0}"

# ── Stage the auth interceptor jar (PR #3) into the build context ───────────
STAGE="${HERE}/build-context"
rm -rf "${STAGE}"; mkdir -p "${STAGE}"
if [ -n "${INTERCEPTOR_JAR:-}" ]; then
  if [ ! -f "${INTERCEPTOR_JAR}" ]; then
    echo "[build] FATAL: INTERCEPTOR_JAR=${INTERCEPTOR_JAR} not found" >&2
    exit 1
  fi
  cp "${INTERCEPTOR_JAR}" "${STAGE}/interceptor.jar"
  echo "[build] staged interceptor jar: ${INTERCEPTOR_JAR}"
elif [ "${ALLOW_MISSING_INTERCEPTOR:-0}" = "1" ]; then
  printf 'No interceptor.jar staged. Build the jar from deploy/auth/interceptor (PR #3)\nand rebuild with INTERCEPTOR_JAR=<path> for a production image.\n' \
    > "${STAGE}/INTERCEPTOR_MISSING.txt"
  echo "[build] WARNING: building WITHOUT the auth interceptor (ALLOW_MISSING_INTERCEPTOR=1)"
else
  echo "[build] FATAL: set INTERCEPTOR_JAR=<path to PR #3 jar>, or ALLOW_MISSING_INTERCEPTOR=1 to build without it." >&2
  exit 1
fi

echo "[build] image      : ${IMAGE_TAG}"
echo "[build] base       : ${BASE_IMAGE}"
echo "[build] spark      : ${SPARK_VERSION}  scala ${SCALA_BINARY}"
echo "[build] iceberg    : ${ICEBERG_VERSION} (module ${ICEBERG_SPARK_MODULE}, in-base=${ICEBERG_IN_BASE})"
echo "[build] hadoop-aws : ${HADOOP_AWS_VERSION}  aws-sdk-bundle ${AWS_SDK_BUNDLE_VERSION}"

docker build \
  --tag "${IMAGE_TAG}" \
  --build-arg "BASE_IMAGE=${BASE_IMAGE}" \
  --build-arg "SPARK_VERSION=${SPARK_VERSION}" \
  --build-arg "SCALA_BINARY=${SCALA_BINARY}" \
  --build-arg "ICEBERG_SPARK_MODULE=${ICEBERG_SPARK_MODULE}" \
  --build-arg "ICEBERG_VERSION=${ICEBERG_VERSION}" \
  --build-arg "HADOOP_AWS_VERSION=${HADOOP_AWS_VERSION}" \
  --build-arg "AWS_SDK_BUNDLE_VERSION=${AWS_SDK_BUNDLE_VERSION}" \
  --build-arg "KAFKA_CONNECTOR_VERSION=${KAFKA_CONNECTOR_VERSION}" \
  --build-arg "KAFKA_CLIENTS_VERSION=${KAFKA_CLIENTS_VERSION}" \
  --build-arg "COMMONS_POOL2_VERSION=${COMMONS_POOL2_VERSION}" \
  --build-arg "POSTGRES_JDBC_VERSION=${POSTGRES_JDBC_VERSION}" \
  --build-arg "ICEBERG_IN_BASE=${ICEBERG_IN_BASE}" \
  --build-arg "MAVEN_BASE_URL=${MAVEN_BASE_URL}" \
  -f "${HERE}/Dockerfile" \
  "${HERE}"

echo "[build] built ${IMAGE_TAG}"

if [ "${PUSH}" = "1" ]; then
  echo "[build] pushing ${IMAGE_TAG}"
  docker push "${IMAGE_TAG}"
fi

rm -rf "${STAGE}"

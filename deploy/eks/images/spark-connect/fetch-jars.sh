#!/usr/bin/env bash
# fetch-jars.sh — download + checksum-verify the baked jar BOM into $SPARK_HOME/jars.
#
# Runs INSIDE the Docker build (called by the Dockerfile). Versions arrive as
# environment variables (set from Dockerfile ARGs); checksums are pinned in
# jars.sha1 (same directory, copied next to this script during the build).
#
# Reproducibility: every artifact is verified against jars.sha1 via `sha1sum -c`.
# A mismatch (tampered mirror, wrong version, truncated download) fails the build.
set -euo pipefail

MAVEN_BASE_URL="${MAVEN_BASE_URL:-https://repo1.maven.org/maven2}"
SPARK_HOME="${SPARK_HOME:-/opt/spark}"
JARS_DIR="${SPARK_HOME}/jars"
MANIFEST="${MANIFEST:-/tmp/build/jars.sha1}"

# Tier B (5.0-SNAPSHOT + iceberg-port) ships the Iceberg runtime in the base image
# already, so we must NOT also pull a Maven Iceberg jar (it would not load on 5.0).
ICEBERG_IN_BASE="${ICEBERG_IN_BASE:-false}"

# Versions (defaults mirror the Tier-A BOM; overridden by Dockerfile ARGs).
SCALA_BINARY="${SCALA_BINARY:-2.13}"
ICEBERG_SPARK_MODULE="${ICEBERG_SPARK_MODULE:-4.1}"
ICEBERG_RUNTIME_VERSION="${ICEBERG_RUNTIME_VERSION:-1.11.0}"
HADOOP_AWS_VERSION="${HADOOP_AWS_VERSION:-3.4.2}"
AWS_SDK_BUNDLE_VERSION="${AWS_SDK_BUNDLE_VERSION:-2.29.52}"
KAFKA_CONNECTOR_VERSION="${KAFKA_CONNECTOR_VERSION:-4.1.2}"
KAFKA_CLIENTS_VERSION="${KAFKA_CLIENTS_VERSION:-3.9.1}"
COMMONS_POOL2_VERSION="${COMMONS_POOL2_VERSION:-2.12.0}"
POSTGRES_JDBC_VERSION="${POSTGRES_JDBC_VERSION:-42.7.4}"

mkdir -p "${JARS_DIR}"

# Maven repo-relative paths for each artifact, derived from the version vars above.
ice="org/apache/iceberg/iceberg-spark-runtime-${ICEBERG_SPARK_MODULE}_${SCALA_BINARY}/${ICEBERG_RUNTIME_VERSION}/iceberg-spark-runtime-${ICEBERG_SPARK_MODULE}_${SCALA_BINARY}-${ICEBERG_RUNTIME_VERSION}.jar"
paths=(
  "org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"
  "software/amazon/awssdk/bundle/${AWS_SDK_BUNDLE_VERSION}/bundle-${AWS_SDK_BUNDLE_VERSION}.jar"
  "org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY}/${KAFKA_CONNECTOR_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY}-${KAFKA_CONNECTOR_VERSION}.jar"
  "org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY}/${KAFKA_CONNECTOR_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY}-${KAFKA_CONNECTOR_VERSION}.jar"
  "org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"
  "org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"
  "org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"
)

if [ "${ICEBERG_IN_BASE}" = "true" ]; then
  echo "[fetch-jars] ICEBERG_IN_BASE=true (Tier B): skipping Maven Iceberg runtime; base image provides the 5.0 port."
  # Drop the iceberg line from the checksum manifest so `sha1sum -c` does not demand it.
  grep -v 'iceberg-spark-runtime' "${MANIFEST}" > /tmp/build/jars.check.sha1
else
  paths=("${ice}" "${paths[@]}")
  cp "${MANIFEST}" /tmp/build/jars.check.sha1
fi

for p in "${paths[@]}"; do
  fname="$(basename "${p}")"
  echo "[fetch-jars] GET ${MAVEN_BASE_URL}/${p}"
  curl -fSL --retry 5 --retry-all-errors --retry-delay 3 --connect-timeout 20 \
    -o "${JARS_DIR}/${fname}" "${MAVEN_BASE_URL}/${p}"
done

echo "[fetch-jars] verifying checksums against $(basename "${MANIFEST}")"
( cd "${JARS_DIR}" && grep -v '^[[:space:]]*#' /tmp/build/jars.check.sha1 | grep -v '^[[:space:]]*$' | sha1sum -c - )

echo "[fetch-jars] OK — $(grep -vc '^[[:space:]]*#' /tmp/build/jars.check.sha1) artifact(s) verified into ${JARS_DIR}"

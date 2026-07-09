# syntax=docker/dockerfile:1.7
# =============================================================================================
# Spark 4.1.2 + Iceberg 1.11.0 runtime for the Lakekeeper credential-vending de-risk spike.
#
# Used by THREE roles in the local config-shakeout compose (all the same image, so the driver
# and the executor have an IDENTICAL classpath — this matters: the vended credential is
# deserialized on the executor by exactly the Iceberg S3FileIO classes baked here):
#   - spark-master   (standalone master)
#   - spark-worker   (runs the SEPARATE executor JVM — this is where executor-side FileIO happens)
#   - spark-connect  (the client-mode driver == the Spark Connect server)
#
# Version pins mirror the real EKS image BOM (deploy/eks/images/spark-connect): Spark 4.1.2,
# Scala 2.13, Iceberg 1.11.0. We bake ONLY the two jars a REST-catalog + S3FileIO path needs:
#   - iceberg-spark-runtime-4.1_2.13:1.11.0   (Iceberg + Spark 4.1 SQL extensions + iceberg-aws)
#   - iceberg-aws-bundle:1.11.0               (the AWS SDK v2 pieces S3FileIO calls; Iceberg-blessed)
# We DELIBERATELY do NOT bake hadoop-aws / a static-key S3A path: the ONLY way an executor can
# authenticate to object storage here is the vended credential the REST catalog serializes into
# the scan/write tasks. That absence is load-bearing for the proof (see FINDINGS.md).
# =============================================================================================
ARG BASE_IMAGE=apache/spark:4.1.2-scala2.13-java17-python3-ubuntu
FROM ${BASE_IMAGE}

ARG SCALA_BINARY=2.13
ARG ICEBERG_SPARK_MODULE=4.1
ARG ICEBERG_VERSION=1.11.0
ARG MAVEN_BASE_URL=https://repo1.maven.org/maven2

USER root

# Fetch + checksum-verify (against Maven's published .sha1 sidecar) the two Iceberg jars.
# A truncated/corrupt/tampered download fails the build.
RUN set -eux; \
    ice_rt="org/apache/iceberg/iceberg-spark-runtime-${ICEBERG_SPARK_MODULE}_${SCALA_BINARY}/${ICEBERG_VERSION}/iceberg-spark-runtime-${ICEBERG_SPARK_MODULE}_${SCALA_BINARY}-${ICEBERG_VERSION}.jar"; \
    ice_aws="org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    for p in "${ice_rt}" "${ice_aws}"; do \
      fname="$(basename "${p}")"; \
      curl -fSL --retry 5 --retry-all-errors --retry-delay 3 --connect-timeout 20 \
        -o "/opt/spark/jars/${fname}" "${MAVEN_BASE_URL}/${p}"; \
      curl -fSL --retry 5 --retry-all-errors --retry-delay 3 --connect-timeout 20 \
        -o "/tmp/${fname}.sha1" "${MAVEN_BASE_URL}/${p}.sha1"; \
      echo "$(cat /tmp/${fname}.sha1)  /opt/spark/jars/${fname}" | sha1sum -c -; \
      rm -f "/tmp/${fname}.sha1"; \
    done; \
    ls -la /opt/spark/jars/ | grep iceberg

USER 185

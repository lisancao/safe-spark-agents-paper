# syntax=docker/dockerfile:1.7
# ─────────────────────────────────────────────────────────────────────────────
# Spark Connect server + executors for EKS — Iceberg over HMS, S3A/IRSA, auth.
#
# Tier A (default, fully RELEASED bits):
#   BASE_IMAGE = apache/spark:4.1.2-scala2.13-java17-python3-ubuntu  (Hadoop 3.4.2)
#   Iceberg    = org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0
#
# Tier B (AUTO CDC / create_auto_cdc_flow — Spark master/5.0-SNAPSHOT):
#   BASE_IMAGE = lakehouse/spark:5.0.0-snapshot-cdc   (user's source build;
#                ~/lakehouse-stack/docker/spark42 + iceberg-port — the Iceberg
#                runtime is ALREADY baked into that base)
#   build with: --build-arg BASE_IMAGE=lakehouse/spark:5.0.0-snapshot-cdc \
#               --build-arg ICEBERG_IN_BASE=true
#
# A 4.1 -> 4.2/5.0 bump is an ARG/tag change (+ refreshed jars.sha1). See README.
# ─────────────────────────────────────────────────────────────────────────────
ARG BASE_IMAGE=apache/spark:4.1.2-scala2.13-java17-python3-ubuntu
FROM ${BASE_IMAGE}

# ── Version pins (the BOM; checksums live in jars.sha1) ──────────────────────
ARG SPARK_VERSION=4.1.2
ARG SCALA_BINARY=2.13
ARG ICEBERG_SPARK_MODULE=4.1
ARG ICEBERG_VERSION=1.11.0
ARG HADOOP_AWS_VERSION=3.4.2
ARG AWS_SDK_BUNDLE_VERSION=2.29.52
ARG KAFKA_CONNECTOR_VERSION=4.1.2
ARG KAFKA_CLIENTS_VERSION=3.9.1
ARG COMMONS_POOL2_VERSION=2.12.0
ARG POSTGRES_JDBC_VERSION=42.7.4
# Tier B: Iceberg runtime is in the base image already — skip the Maven pull.
ARG ICEBERG_IN_BASE=false
# Mirror override for air-gapped builds (e.g. a Nexus/Artifactory proxy).
ARG MAVEN_BASE_URL=https://repo1.maven.org/maven2

LABEL org.opencontainers.image.title="spark-connect-iceberg-eks" \
      org.opencontainers.image.description="Spark Connect server + executors: Iceberg/HMS/S3A-IRSA/auth for EKS" \
      org.opencontainers.image.source="https://github.com/lisancao/safe-spark-agents" \
      io.omnigent.spark.version="${SPARK_VERSION}" \
      io.omnigent.iceberg.version="${ICEBERG_VERSION}"

# Need root to write into /opt/spark/jars and /opt/spark/conf (base runs as 185).
USER root

# ── Fetch + checksum-verify the jar BOM into $SPARK_HOME/jars ────────────────
# curl is present in the apache/spark base; we install nothing extra.
COPY jars.sha1 fetch-jars.sh /tmp/build/
RUN chmod +x /tmp/build/fetch-jars.sh \
 && MANIFEST=/tmp/build/jars.sha1 \
    MAVEN_BASE_URL="${MAVEN_BASE_URL}" \
    ICEBERG_IN_BASE="${ICEBERG_IN_BASE}" \
    SCALA_BINARY="${SCALA_BINARY}" \
    ICEBERG_SPARK_MODULE="${ICEBERG_SPARK_MODULE}" \
    ICEBERG_RUNTIME_VERSION="${ICEBERG_VERSION}" \
    HADOOP_AWS_VERSION="${HADOOP_AWS_VERSION}" \
    AWS_SDK_BUNDLE_VERSION="${AWS_SDK_BUNDLE_VERSION}" \
    KAFKA_CONNECTOR_VERSION="${KAFKA_CONNECTOR_VERSION}" \
    KAFKA_CLIENTS_VERSION="${KAFKA_CLIENTS_VERSION}" \
    COMMONS_POOL2_VERSION="${COMMONS_POOL2_VERSION}" \
    POSTGRES_JDBC_VERSION="${POSTGRES_JDBC_VERSION}" \
    /tmp/build/fetch-jars.sh \
 && rm -rf /tmp/build

# ── Auth interceptor jar (PR #3: deploy/auth/interceptor) ────────────────────
# NOT vendored here. build.sh stages the compiled jar into ./build-context/ as
# interceptor.jar; if absent (e.g. a structure/lint build before PR #3 merges),
# build.sh writes a marker .txt instead so this COPY never fails — the image
# then logs a loud warning at runtime (see entrypoint.sh) rather than shipping a
# fake interceptor. The COPY pulls the whole staging dir (always non-empty).
COPY build-context/ /opt/spark/interceptor-stage/
RUN if [ -f /opt/spark/interceptor-stage/interceptor.jar ]; then \
        cp /opt/spark/interceptor-stage/interceptor.jar /opt/spark/jars/interceptor.jar ; \
        echo "[build] baked auth interceptor jar" ; \
    else \
        echo "[build] WARNING: no interceptor.jar staged — image built WITHOUT the auth interceptor (PR #3)" ; \
    fi \
 && rm -rf /opt/spark/interceptor-stage

# ── Connect server config template + entrypoint ─────────────────────────────
COPY conf/spark-defaults.template.conf /opt/spark-connect/spark-defaults.template.conf
COPY entrypoint.sh /opt/spark-connect/entrypoint.sh
# entrypoint.sh renders spark-defaults.conf into $SPARK_CONF_DIR (=/opt/spark/conf) at start, for
# BOTH roles: the Connect-server/driver pod AND every Spark-launched executor pod. The base image
# bakes /opt/spark/conf as root:root, and these pods run as uid/gid 185 (the `spark` user), so a
# `chmod g+rwX` is not enough — the group is root, not 185. We must OWN the dir as 185:185 so the
# render succeeds. (A previous `chmod -R g+rwX` left it root-group-writable only, which made the
# driver crash with "spark-defaults.conf: Permission denied" and, more subtly, made every executor
# fail to start — Spark then hit "Max number of executor failures (20) reached" and stopped the
# SparkContext. Verified against a live EKS cluster.)
RUN chmod +x /opt/spark-connect/entrypoint.sh \
 && mkdir -p /opt/spark/conf \
 && chown -R 185:185 /opt/spark/conf \
 && chmod -R u+rwX /opt/spark/conf

# ── Runtime contract (non-secret defaults only; secrets injected at runtime) ─
#   ICEBERG_CATALOG         iceberg
#   WAREHOUSE               s3a://<bucket>/warehouse
#   ICEBERG_JDBC_URL        jdbc:postgresql://<rds>:5432/<catalog-db>  (non-secret)
#   ICEBERG_JDBC_USER       <from k8s Secret spark-iceberg-jdbc — NEVER baked>
#   ICEBERG_JDBC_PASSWORD   <from k8s Secret spark-iceberg-jdbc — NEVER baked>
#   INTERCEPTOR_CLASSES     <fqcn from PR #3>
#   SPARK_SERVICE_ACCOUNT   <IRSA-annotated SA>
#   CONNECT_AUTH_TOKEN      <PSK from k8s Secret — NEVER baked>
#   AWS_ROLE_ARN / AWS_WEB_IDENTITY_TOKEN_FILE  (injected by the EKS IRSA webhook)
# The Iceberg catalog is a JDBC catalog (RDS Postgres) with an S3FileIO warehouse — it does NOT
# use the Hive Metastore thrift service (which had a Hive 2.3.10-client vs 4.0.1-server mismatch).
ENV SPARK_DEFAULTS_TEMPLATE=/opt/spark-connect/spark-defaults.template.conf \
    ICEBERG_CATALOG="iceberg" \
    INTERCEPTOR_CLASSES="com.safesparkagents.connect.auth.PrincipalPinningInterceptor"

# Drop back to the unprivileged base user (uid 185 = spark).
USER 185

# Spark Connect gRPC port.
EXPOSE 15002

ENTRYPOINT ["/opt/spark-connect/entrypoint.sh"]
CMD ["connect-server"]

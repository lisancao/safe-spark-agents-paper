# Hive Metastore (standalone) — Postgres + S3A enabled.
#
# Base: apache/hive:4.0.1. Verified contents of that image (docker inspect, 2026-06):
#   HIVE_HOME=/opt/hive   HADOOP_HOME=/opt/hadoop   Hadoop 3.3.6
#   Already present under $HADOOP_HOME/share/hadoop/tools/lib (but OFF the metastore classpath):
#     - hadoop-aws-3.3.6.jar            (matches the bundled Hadoop 3.3.6 — DO NOT mismatch)
#     - aws-java-sdk-bundle-1.12.367.jar (AWS SDK v1; the pair hadoop-aws 3.3.6 was compiled against)
#   MISSING entirely:
#     - the PostgreSQL JDBC driver (stock image only knows Derby)
#
# So this layer does exactly two things, both deterministic and pinned:
#   1. Add the PostgreSQL JDBC driver (downloaded from Maven Central, sha256-verified).
#   2. Promote the already-present hadoop-aws + aws-sdk bundle onto the Hive Metastore
#      classpath ($HIVE_HOME/lib) so s3a:// works without HADOOP_OPTIONAL_TOOLS gymnastics.
#
# Nothing here bakes secrets, endpoints, or credentials — all of that is injected at runtime
# via the Kubernetes Secret / ServiceAccount (IRSA). See ../README.md.

# --- 0. Fetch + verify the PostgreSQL JDBC driver in a throwaway stage ---------
# The apache/hive image has neither curl nor wget, so we fetch in a tiny Alpine
# stage (busybox wget + sha256sum) and COPY the verified jar into the final image.
# Nothing from this stage other than the one verified jar reaches the result.
ARG HIVE_IMAGE=apache/hive
ARG HIVE_VERSION=4.0.1
ARG PG_JDBC_VERSION=42.7.4
ARG PG_JDBC_SHA256=188976721ead8e8627eb6d8389d500dccc0c9bebd885268a3047180274a6031e
ARG MAVEN_BASE=https://repo1.maven.org/maven2

FROM alpine:3.20 AS fetch
ARG PG_JDBC_VERSION
ARG PG_JDBC_SHA256
ARG MAVEN_BASE
RUN set -eux; \
    url="${MAVEN_BASE}/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar"; \
    wget -q -O /tmp/postgresql.jar "${url}"; \
    echo "${PG_JDBC_SHA256}  /tmp/postgresql.jar" | sha256sum -c -

# --- final image ---------------------------------------------------------------
FROM ${HIVE_IMAGE}:${HIVE_VERSION}
ARG PG_JDBC_VERSION

# --- 1. PostgreSQL JDBC driver -------------------------------------------------
# Pinned + checksum-verified above. 42.7.x is JDBC 4.2, supports PostgreSQL 12–16 (RDS/Aurora).
USER root
COPY --from=fetch /tmp/postgresql.jar ${HIVE_HOME}/lib/postgresql-${PG_JDBC_VERSION}.jar

# --- 2. Put S3A on the metastore classpath ------------------------------------
# hadoop-aws + the matching aws-sdk bundle ship in the image but live under
# $HADOOP_HOME/share/hadoop/tools/lib, which the `hive --service metastore` launcher
# does NOT scan. Symlink (not copy) them into $HIVE_HOME/lib so the version stays
# locked to whatever the base image's Hadoop ships — no drift, no second copy.
RUN set -eux; \
    tools="${HADOOP_HOME}/share/hadoop/tools/lib"; \
    ln -sf "${tools}/hadoop-aws-3.3.6.jar"            "${HIVE_HOME}/lib/hadoop-aws-3.3.6.jar"; \
    ln -sf "${tools}/aws-java-sdk-bundle-1.12.367.jar" "${HIVE_HOME}/lib/aws-java-sdk-bundle-1.12.367.jar"; \
    ls -l "${HIVE_HOME}/lib/" | grep -E 'postgresql|hadoop-aws|aws-java-sdk-bundle'

# Drop back to the non-root hive user the base image runs as.
USER hive

# SERVICE_NAME=metastore and port 9083 are set by the Deployment, not baked here,
# so this same image can also run schematool (the init Job) with an overridden command.
EXPOSE 9083

"""Build a SparkSession wired to an Iceberg catalog backed by HMS + S3.

This is the single place the Iceberg/HMS/S3 wiring lives, shared by the batch
loader (load_to_iceberg.py) and the streaming consumer (stream_to_iceberg.py).
Every value is parameterized (CLI flag > env > default) so nothing about a
specific cluster is baked in:

    spark.sql.catalog.<cat>            = org.apache.iceberg.spark.SparkCatalog
    spark.sql.catalog.<cat>.type       = hive
    spark.sql.catalog.<cat>.uri        = thrift://hive-metastore:9083   (HMS)
    spark.sql.catalog.<cat>.warehouse  = s3://<bucket>/warehouse
    spark.sql.catalog.<cat>.io-impl    = org.apache.iceberg.aws.s3.S3FileIO

S3 auth is IRSA (IAM Roles for Service Accounts): the AWS SDK v2 default
credentials chain that S3FileIO uses picks up the projected web-identity token
automatically, so no keys are configured here. The s3a Hadoop filesystem (used
when the source parquet itself lives on S3) is pointed at the same web-identity
provider for parity.

Parameters resolve in this order: explicit argument > environment variable >
default. Relevant env vars:
    ICEBERG_CATALOG       (default: lakehouse)
    HMS_URI               (default: thrift://hive-metastore:9083)
    S3_BUCKET             (no default — required; the warehouse bucket)
    ICEBERG_WAREHOUSE     (default: s3://<S3_BUCKET>/warehouse)
    ICEBERG_IO_IMPL       (default: org.apache.iceberg.aws.s3.S3FileIO)
    ICEBERG_SPARK_RUNTIME (optional Maven coord for spark.jars.packages)
"""

import os
from typing import Optional

DEFAULT_CATALOG = "lakehouse"
DEFAULT_HMS_URI = "thrift://hive-metastore:9083"
DEFAULT_IO_IMPL = "org.apache.iceberg.aws.s3.S3FileIO"
IRSA_PROVIDER = "software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider"

ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


def resolve(value: Optional[str], env_key: str, default: Optional[str] = None) -> Optional[str]:
    """CLI value > env var > default."""
    if value:
        return value
    return os.environ.get(env_key, default)


def iceberg_conf(
    catalog: str,
    warehouse: str,
    hms_uri: str,
    io_impl: str = DEFAULT_IO_IMPL,
) -> dict:
    """Return the Spark config dict for an HMS-backed Iceberg catalog on S3.

    Usable both to configure a SparkSession.builder and to render a
    spark-defaults.conf for spark-submit.
    """
    return {
        "spark.sql.extensions": ICEBERG_EXTENSIONS,
        f"spark.sql.catalog.{catalog}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog}.type": "hive",
        f"spark.sql.catalog.{catalog}.uri": hms_uri,
        f"spark.sql.catalog.{catalog}.warehouse": warehouse,
        f"spark.sql.catalog.{catalog}.io-impl": io_impl,
        # s3a is only used when the *source* parquet lives on S3; auth via IRSA.
        "spark.hadoop.fs.s3a.aws.credentials.provider": IRSA_PROVIDER,
    }


def build_spark(
    app_name: str,
    catalog: Optional[str] = None,
    bucket: Optional[str] = None,
    warehouse: Optional[str] = None,
    hms_uri: Optional[str] = None,
    io_impl: Optional[str] = None,
    extra_conf: Optional[dict] = None,
):
    """Construct a SparkSession with the Iceberg/HMS/S3 catalog configured.

    Importing pyspark is deferred to call time so this module compiles and is
    importable without Spark on the PATH (the syntax gate does not need Spark).
    """
    from pyspark.sql import SparkSession

    catalog = resolve(catalog, "ICEBERG_CATALOG", DEFAULT_CATALOG)
    hms_uri = resolve(hms_uri, "HMS_URI", DEFAULT_HMS_URI)
    io_impl = resolve(io_impl, "ICEBERG_IO_IMPL", DEFAULT_IO_IMPL)

    bucket = resolve(bucket, "S3_BUCKET")
    warehouse = resolve(warehouse, "ICEBERG_WAREHOUSE")
    if not warehouse:
        if not bucket:
            raise ValueError(
                "No warehouse: pass --bucket/--warehouse or set S3_BUCKET / "
                "ICEBERG_WAREHOUSE (warehouse should be s3://<bucket>/warehouse)."
            )
        warehouse = f"s3://{bucket}/warehouse"

    conf = iceberg_conf(catalog, warehouse, hms_uri, io_impl)
    if extra_conf:
        conf.update(extra_conf)

    # Optional: let the runtime pull the iceberg-spark-runtime + aws bundle jars.
    packages = os.environ.get("ICEBERG_SPARK_RUNTIME")
    if packages:
        conf["spark.jars.packages"] = packages

    builder = SparkSession.builder.appName(app_name)
    for key, val in conf.items():
        builder = builder.config(key, val)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark ready. Iceberg catalog '{catalog}' -> HMS {hms_uri}, warehouse {warehouse}")
    return spark, catalog


def ensure_namespaces(spark, catalog: str, namespaces=("bronze", "silver", "gold")) -> None:
    """Create the medallion namespaces if they do not exist.

    The pipelines expect a medallion layout under the catalog:
        <catalog>.bronze.*   raw, messy, append-only (this loader writes here)
        <catalog>.silver.*   cleansed / deduplicated / quarantine-split
        <catalog>.gold.*     business aggregates
    The loader only writes bronze, but it provisions silver/gold so downstream
    pipeline specs can target them without a separate DDL step.
    """
    for ns in namespaces:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{ns}")
        print(f"   namespace ready: {catalog}.{ns}")

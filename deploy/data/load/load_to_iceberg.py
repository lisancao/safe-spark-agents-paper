"""Batch-load generated test data into Iceberg BRONZE tables (HMS + S3).

Adapted from lakehouse-stack/data/load_to_iceberg.py. Differences from the
original (which hardcoded an `iceberg.bronze.*` catalog and container paths):

  * Catalog name, HMS URI, S3 warehouse bucket and the data dir are all
    parameterized (CLI flag > env > default) via spark_catalog.build_spark.
  * Tables are created with Iceberg DDL — the v2 writer
    `df.writeTo(...).using("iceberg").createOrReplace()` emits `USING iceberg`,
    so every table is a true Iceberg table in the HMS catalog on S3.
  * Namespaces (bronze/silver/gold) are provisioned up front.

Run (post-deploy, needs a live Spark + HMS + S3):

    spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,\
org.apache.iceberg:iceberg-aws-bundle:1.11.0 \
        load_to_iceberg.py \
        --bucket my-lakehouse-bucket \
        --catalog lakehouse \
        --data-dir /data

The --packages coordinate must match your Spark version (Iceberg 1.11.0 ships
runtimes for released Spark 3.5/4.0/4.1; see README "Spark version notes").
This script does NOT start Spark/HMS/S3 — point it at a running cluster.
"""

import argparse
import os
import sys
from pathlib import Path

# Make spark_catalog importable whether run via spark-submit or `python load/...`.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from spark_catalog import build_spark, ensure_namespaces  # noqa: E402

DIMENSIONS = [
    ("categories", "dim_categories"),
    ("brands", "dim_brands"),
    ("items", "dim_items"),
    ("locations", "dim_locations"),
]


def parse_args():
    p = argparse.ArgumentParser(description="Load test data into Iceberg bronze tables")
    p.add_argument("--catalog", default=None, help="Iceberg catalog name (env ICEBERG_CATALOG, default lakehouse)")
    p.add_argument("--bucket", default=None, help="S3 warehouse bucket (env S3_BUCKET)")
    p.add_argument("--warehouse", default=None,
                   help="Full warehouse URI (env ICEBERG_WAREHOUSE, default s3://<bucket>/warehouse)")
    p.add_argument("--hms-uri", default=None, help="HMS thrift URI (env HMS_URI, default thrift://hive-metastore:9083)")
    p.add_argument("--bronze", default="bronze", help="Bronze namespace (default bronze)")
    p.add_argument("--data-dir", default=os.environ.get("DATA_OUTPUT_DIR", "/data"),
                   help="Directory holding dimensions/ and events/ "
                        "(CLI > DATA_OUTPUT_DIR env > /data)")
    p.add_argument("--events-name", default=None,
                   help="Events parquet filename (default: first *.parquet under <data-dir>/events)")
    return p.parse_args()


def _events_path(data_dir: str, events_name: str) -> str:
    events_dir = Path(data_dir) / "events"
    if events_name:
        return str(events_dir / events_name)
    # Default: load whatever single events parquet was generated.
    candidates = sorted(events_dir.glob("*.parquet")) if events_dir.exists() else []
    if not candidates:
        raise FileNotFoundError(
            f"No events parquet found under {events_dir}. "
            f"Run `python -m generator generate` first, or pass --events-name."
        )
    return str(candidates[0])


def main():
    args = parse_args()

    spark, catalog = build_spark(
        app_name="LoadTestData-bronze",
        catalog=args.catalog,
        bucket=args.bucket,
        warehouse=args.warehouse,
        hms_uri=args.hms_uri,
    )

    from pyspark.sql import functions as f

    bronze = args.bronze
    print(f"\nLoading test data into Iceberg tables under {catalog}.{bronze}...")
    ensure_namespaces(spark, catalog)

    # 1. Dimension tables -> <catalog>.<bronze>.dim_*
    print("\n1. Loading dimension tables...")
    dims_path = Path(args.data_dir) / "dimensions"
    for filename, table in DIMENSIONS:
        src = dims_path / f"{filename}.parquet"
        df = spark.read.parquet(str(src))
        fq = f"{catalog}.{bronze}.{table}"
        df.writeTo(fq).using("iceberg").createOrReplace()
        print(f"   - {fq}")

    # 2. Events -> <catalog>.<bronze>.orders
    print("\n2. Loading events table...")
    events_path = _events_path(args.data_dir, args.events_name)
    events_df = spark.read.parquet(events_path)

    # Parse the ISO timestamp string into a real timestamp (with and without micros).
    # Bronze keeps the raw `ts`/`body` strings intact — messy data is preserved for
    # silver to cleanse/quarantine; we only ADD a parsed column.
    events_df = events_df.withColumn(
        "event_timestamp",
        f.coalesce(
            f.to_timestamp("ts", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
            f.to_timestamp("ts", "yyyy-MM-dd'T'HH:mm:ss"),
        ),
    )

    fq_orders = f"{catalog}.{bronze}.orders"
    events_df.writeTo(fq_orders).using("iceberg").createOrReplace()
    print(f"   - {fq_orders} ({events_df.count():,} events)")

    print("\nDone! Bronze tables created:")
    for _, table in DIMENSIONS:
        print(f"  - {catalog}.{bronze}.{table}")
    print(f"  - {catalog}.{bronze}.orders")

    spark.stop()


if __name__ == "__main__":
    main()

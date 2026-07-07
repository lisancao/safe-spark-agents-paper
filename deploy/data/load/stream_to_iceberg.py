"""Structured Streaming consumer: Kafka -> Iceberg BRONZE (HMS + S3).

This is the consumer half of the streaming-ingest demo. The producer
(`python -m generator stream`) replays the messy events into a Kafka topic; this
job reads that topic and appends them to `<catalog>.bronze.orders_stream` as an
Iceberg table, so the streaming path lands in the same medallion bronze layer as
the batch path.

Bronze is deliberately permissive: the Kafka value is parsed as JSON with a
fixed schema, but malformed/late/duplicate records are NOT filtered here — that
is silver's job. The raw JSON string is retained in `raw_value` for forensics.

Run (post-deploy, needs live Spark + Kafka + HMS + S3):

    spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,\
org.apache.iceberg:iceberg-aws-bundle:1.11.0,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        stream_to_iceberg.py \
        --bucket my-lakehouse-bucket --catalog lakehouse \
        --kafka kafka:9092 --topic orders \
        --checkpoint s3://my-lakehouse-bucket/checkpoints/orders_stream

This script does NOT start any service — point it at a running cluster.
"""

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from spark_catalog import build_spark, ensure_namespaces  # noqa: E402


def parse_args():
    p = argparse.ArgumentParser(description="Stream Kafka events into Iceberg bronze")
    p.add_argument("--catalog", default=None, help="Iceberg catalog (env ICEBERG_CATALOG, default lakehouse)")
    p.add_argument("--bucket", default=None, help="S3 warehouse bucket (env S3_BUCKET)")
    p.add_argument("--warehouse", default=None, help="Full warehouse URI (env ICEBERG_WAREHOUSE)")
    p.add_argument("--hms-uri", default=None, help="HMS thrift URI (env HMS_URI)")
    p.add_argument("--bronze", default="bronze", help="Bronze namespace (default bronze)")
    p.add_argument("--kafka", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
                   help="Kafka bootstrap servers (env KAFKA_BOOTSTRAP)")
    p.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", "orders"),
                   help="Kafka topic (env KAFKA_TOPIC)")
    p.add_argument("--table", default="orders_stream", help="Bronze table name (default orders_stream)")
    p.add_argument("--checkpoint", default=None,
                   help="Checkpoint location (env STREAM_CHECKPOINT). Required for exactly-once.")
    p.add_argument("--starting-offsets", default="earliest", help="earliest | latest")
    p.add_argument("--once", action="store_true",
                   help="Run a single micro-batch then stop (availableNow) instead of running forever")
    return p.parse_args()


def main():
    args = parse_args()

    checkpoint = args.checkpoint or os.environ.get("STREAM_CHECKPOINT")
    if not checkpoint:
        raise ValueError(
            "No checkpoint: pass --checkpoint or set STREAM_CHECKPOINT "
            "(e.g. s3://<bucket>/checkpoints/orders_stream)."
        )

    spark, catalog = build_spark(
        app_name="StreamTestData-bronze",
        catalog=args.catalog,
        bucket=args.bucket,
        warehouse=args.warehouse,
        hms_uri=args.hms_uri,
    )

    from pyspark.sql import functions as f
    from pyspark.sql.types import (
        StructType, StructField, StringType, LongType, IntegerType,
    )

    bronze = args.bronze
    fq = f"{catalog}.{bronze}.{args.table}"
    ensure_namespaces(spark, catalog)

    # Create the target Iceberg table up front so the stream can append into it.
    # Mirrors the generated event schema; raw_value keeps the original JSON.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fq} (
            event_id        string,
            event_type      string,
            ts              string,
            ts_seconds      bigint,
            location_id     int,
            order_id        string,
            sequence        int,
            body            string,
            event_timestamp timestamp,
            raw_value       string,
            ingest_ts       timestamp
        ) USING iceberg
    """)
    print(f"   target table ready: {fq}")

    # The producer serializes each event as a JSON object (see generator/producer.py).
    event_schema = StructType([
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("ts", StringType()),
        StructField("ts_seconds", LongType()),
        StructField("location_id", IntegerType()),
        StructField("order_id", StringType()),
        StructField("sequence", IntegerType()),
        StructField("body", StringType()),
    ])

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )

    # from_json on a malformed value yields nulls rather than throwing — messy
    # records survive into bronze (raw_value retained) for silver to quarantine.
    parsed = (
        raw.select(
            f.col("value").cast("string").alias("raw_value"),
            f.from_json(f.col("value").cast("string"), event_schema).alias("e"),
        )
        .select("e.*", "raw_value")
        .withColumn(
            "event_timestamp",
            f.coalesce(
                f.to_timestamp("ts", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
                f.to_timestamp("ts", "yyyy-MM-dd'T'HH:mm:ss"),
            ),
        )
        .withColumn("ingest_ts", f.current_timestamp())
    )

    # toTable on a streaming writer is the idiomatic Iceberg sink; build the
    # query explicitly so the trigger (continuous vs. availableNow) is selectable.
    query_builder = (
        parsed.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
    )
    if args.once:
        query_builder = query_builder.trigger(availableNow=True)

    print(f"Streaming {args.topic} @ {args.kafka} -> {fq} (checkpoint {checkpoint})")
    query = query_builder.toTable(fq)
    query.awaitTermination()


if __name__ == "__main__":
    main()

"""Quantify the silent corruption produced by the semantic/state defects.

The SDP `dry-run` gate (run by run_battery.sh) only proves *whether* a defect is
caught at analysis time. For the silent-wrong defects it is not -- the pipeline
reaches COMPLETED -- so the harm only shows up in the *data*. This script
measures that harm directly by applying the SAME defective transform to the
deterministic seed=42 dataset and counting the rows it corrupts.

It reads the generated orders dataset from an NDJSON file (one Kafka `value` per
line) rather than from Kafka, because the corruption is a property of the parse
/ aggregation logic over the data, not of the source: a TIMESTAMP-typed
epoch-millis string mis-parses identically whether it arrives from Kafka or a
file. This keeps the quantification self-contained and fully reproducible (no
broker, no shared infra) while measuring exactly the rows the defect damages.

Usage:  python3 quantify.py <d2|d6|d7|d8> <orders.ndjson>
Prints one JSON object: {"defect","rows_affected","detail"} to stdout.
"""
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
)


def make_spark(session_tz=None):
    b = (
        SparkSession.builder.master("local[2]")
        .appName("defect_quantify")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
    )
    if session_tz:
        b = b.config("spark.sql.session.timeZone", session_tz)
    s = b.getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    return s


# value column == the raw JSON line (exactly what Kafka would carry).
def load_values(spark, path):
    return spark.read.text(path).select(F.col("value").alias("value"))


STR_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("event_time", StringType()),
        StructField("amount", StringType()),
        StructField("category", StringType()),
    ]
)
TS_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("event_time", TimestampType()),  # D2/D7 injected wrong type
        StructField("amount", DoubleType()),
        StructField("category", StringType()),
    ]
)


def q_d2(spark, path):
    """D2 wrong type: event_time typed TIMESTAMP in from_json. Epoch-millis
    strings are silently read as epoch-SECONDS (far-future year); no error."""
    df = load_values(spark, path).select(
        F.from_json("value", STR_SCHEMA).getField("event_time").alias("raw"),
        F.from_json("value", TS_SCHEMA).getField("event_time").alias("ts"),
    ).cache()
    total = df.count()
    raw_null = df.filter(F.col("raw").isNull()).count()
    # epoch-millis strings: all-digit raw event_time
    epoch = df.filter(F.col("raw").rlike("^[0-9]+$"))
    epoch_n = epoch.count()
    future = epoch.filter(F.year("ts") > 9999).count()
    # rows that had a non-null raw string but silently became null after parse
    lost = df.filter(F.col("raw").isNotNull() & F.col("ts").isNull()).count()
    affected = future + lost
    return affected, {
        "total_rows": total,
        "raw_event_time_null": raw_null,
        "epoch_millis_rows": epoch_n,
        "epoch_misparsed_year_gt_9999": future,
        "nonnull_raw_to_null_after_parse": lost,
        "note": "no exception raised; pipeline COMPLETED",
    }


def q_d6(spark, path):
    """D6 non-deterministic dedup: dropDuplicates(order_id) with no ordering.
    Quantify how many order_id keys carry CONFLICTING payloads, so the surviving
    row is arbitrary across runs."""
    df = load_values(spark, path).select(
        F.from_json("value", STR_SCHEMA).alias("j")
    ).select("j.order_id", "j.amount", "j.category")
    grp = df.groupBy("order_id").agg(
        F.count(F.lit(1)).alias("n"),
        F.countDistinct("amount", "category").alias("distinct_payloads"),
    )
    dup_keys = grp.filter(F.col("n") > 1).count()
    ambiguous = grp.filter(F.col("distinct_payloads") > 1).count()
    after = grp.count()
    before = df.count()
    if ambiguous > 0:
        note = (
            f"{ambiguous} keys carry conflicting payloads -> arbitrary survivor, "
            "non-deterministic output across runs"
        )
    else:
        note = (
            "duplicates are byte-identical (0 conflicting keys) -> dedup is "
            "deterministic on this dataset; the arbitrary-survivor defect is latent"
        )
    return ambiguous, {
        "rows_before_dedup": before,
        "rows_after_dedup": after,
        "rows_dropped": before - after,
        "duplicate_keys": dup_keys,
        "ambiguous_keys_conflicting_payload": ambiguous,
        "note": note,
    }


def q_d7(spark, path):
    """D7 timezone/day-bucket bug: to_date(event_time) evaluated in the session
    local tz instead of UTC. Count rows that land on a DIFFERENT calendar day
    under the session tz vs UTC."""
    # session tz forced to a non-UTC zone (matches an operator on local time).
    spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    parsed = load_values(spark, path).select(
        F.from_json("value", TS_SCHEMA).getField("event_time").alias("ts")
    ).filter(F.col("ts").isNotNull())
    total = parsed.count()
    # to_date uses session tz; convert the same instant to a UTC date for contrast
    diff = parsed.select(
        F.to_date("ts").alias("local_day"),
        F.to_utc_timestamp("ts", "America/Los_Angeles").alias("utc_ts"),
    ).select(
        F.col("local_day"),
        F.to_date("utc_ts").alias("utc_day"),
    ).filter(F.col("local_day") != F.col("utc_day"))
    misbucketed = diff.count()
    return misbucketed, {
        "parsed_rows": total,
        "session_tz": "America/Los_Angeles",
        "rows_bucketed_to_wrong_day_vs_utc": misbucketed,
        "note": "per-day totals silently mis-bucketed; pipeline COMPLETED",
    }


def q_d8(spark, path):
    """D8 absent quarantine: sum(amount) with no filtering of null/malformed
    rows. Numeric-looking JSON *string* amounts and nulls parse to null under a
    DoubleType schema and are silently skipped by SUM -> under-count."""
    df = load_values(spark, path).select(
        F.from_json("value", STR_SCHEMA).getField("amount").alias("raw"),
        F.from_json("value", TS_SCHEMA).getField("amount").alias("num"),
    ).cache()
    total = df.count()
    num_null = df.filter(F.col("num").isNull()).count()
    raw_present_num_null = df.filter(
        F.col("raw").isNotNull() & F.col("num").isNull()
    ).count()
    # dollars present in the raw strings but dropped from the sum
    lost_dollars = df.filter(
        F.col("num").isNull() & F.col("raw").rlike(r"^-?[0-9]+(\.[0-9]+)?$")
    ).agg(F.sum(F.col("raw").cast("double")).alias("d")).collect()[0]["d"]
    return raw_present_num_null, {
        "total_rows": total,
        "amount_null_after_parse": num_null,
        "nonnull_raw_amount_dropped_from_sum": raw_present_num_null,
        "dollars_silently_excluded_from_sum": float(lost_dollars or 0.0),
        "note": "SUM skips nulls; merchant + grand totals under-counted; COMPLETED",
    }


def q_d8_nested(spark, path):
    """D8 over NESTED arrays (corpus v3 §9): some orders carry no scalar `amount`
    but a `line_items` array-of-structs; the true order revenue is
    sum(qty*price). A scalar-only `sum(amount)` silently drops every such row.
    Counts those rows and the line-item dollars a scalar sum excludes. Returns 0
    on a v2 (no-line_items) stream, so it is safe on any orders dataset."""
    from pyspark.sql.types import ArrayType, LongType
    nested = StructType([
        StructField("order_id", StringType()),
        StructField("amount", StringType()),     # scalar (absent on line_items rows)
        StructField("line_items", ArrayType(StructType([
            StructField("sku", StringType()),
            StructField("qty", LongType()),
            StructField("price", DoubleType()),
        ]))),
    ])
    df = load_values(spark, path).select(F.from_json("value", nested).alias("j")).select(
        "j.amount", "j.line_items")
    has_li = df.filter(F.col("line_items").isNotNull() & (F.size("line_items") > 0)) \
               .withColumn("li_rev", F.expr(
                   "aggregate(line_items, cast(0.0 as double), (acc, x) -> "
                   "acc + coalesce(x.qty,0) * coalesce(x.price, cast(0.0 as double)))"))
    # rows a scalar-only sum drops: line_items present AND no usable scalar amount
    dropped = has_li.filter(~F.col("amount").rlike(r"^-?[0-9]+(\.[0-9]+)?$") | F.col("amount").isNull())
    n_dropped = dropped.count()
    dollars = dropped.agg(F.sum("li_rev").alias("d")).collect()[0]["d"]
    return n_dropped, {
        "line_items_rows": has_li.count(),
        "line_items_rows_dropped_by_scalar_sum": n_dropped,
        "nested_dollars_silently_excluded": float(dollars or 0.0),
        "note": "scalar sum(amount) ignores line_items revenue; nested arrays under-count; COMPLETED",
    }


QUANT = {"d2": q_d2, "d6": q_d6, "d7": q_d7, "d8": q_d8, "d8_nested": q_d8_nested}


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in QUANT:
        sys.stderr.write("usage: quantify.py <d2|d6|d7|d8> <orders.ndjson>\n")
        sys.exit(2)
    defect, path = sys.argv[1], sys.argv[2]
    spark = make_spark()
    try:
        affected, detail = QUANT[defect](spark, path)
        print(json.dumps({"defect": defect.upper(), "rows_affected": affected, "detail": detail}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

spark = SparkSession.active()

# D8 absent bad-record handling: sum(amount) per merchant WITHOUT filtering
# malformed / null rows. The orders topic has ~394 rows whose amount is null or
# a non-numeric string (parsed to null) and rows with a null merchant; none are
# quarantined. SUM silently skips the nulls, so the merchant totals and the
# grand total are quietly under-counted. Dry-run passes.
ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("event_time", StringType()),
        StructField("amount", DoubleType()),
        StructField("category", StringType()),
    ]
)


@dp.materialized_view(name="d8_merchant_totals_no_quarantine")
def d8_merchant_totals_no_quarantine():
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "orders")
        .option("startingOffsets", "earliest")
        .load()
    )
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), ORDERS_SCHEMA).alias("j")
    ).select(
        F.col("j.merchant_id").alias("merchant_id"),
        F.col("j.amount").alias("amount"),
    )
    # No filter on null/malformed amount or merchant_id -> silent under-count.
    return parsed.groupBy(F.col("merchant_id")).agg(
        F.sum(F.col("amount")).alias("total_amount"),
        F.count(F.lit(1)).alias("n_rows"),
    )

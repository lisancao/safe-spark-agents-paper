from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
)

spark = SparkSession.active()

# D7 timezone / day-bucket bug: group by to_date(event_time) where event_time
# is parsed as a (zone-less) TIMESTAMP and to_date is evaluated in the session's
# local timezone. Events near midnight UTC land on the wrong calendar day, so
# the per-day totals are silently mis-bucketed. The graph validates -> dry-run
# passes; the day boundary is only wrong in the data.
ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("event_time", TimestampType()),
        StructField("amount", DoubleType()),
        StructField("category", StringType()),
    ]
)


@dp.materialized_view(name="d7_daily_totals_tz_bug")
def d7_daily_totals_tz_bug():
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
        # to_date in session-local tz, not normalized to UTC -> day-boundary bug.
        F.to_date(F.col("j.event_time")).alias("event_day"),
        F.col("j.amount").alias("amount"),
    )
    return parsed.groupBy(F.col("event_day")).agg(
        F.sum(F.col("amount")).alias("total_amount")
    )

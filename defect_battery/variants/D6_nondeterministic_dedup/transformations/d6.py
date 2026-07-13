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

# D6 non-deterministic dedup: a BATCH materialized_view over the orders topic
# dropping duplicates on order_id with NO ordering / sequence key. When the
# same order_id appears with different amount/category, dropDuplicates keeps an
# ARBITRARY survivor -> the aggregate is non-deterministic across runs.
# Silent-wrong: the graph validates and dry-run passes.
ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("event_time", StringType()),
        StructField("amount", DoubleType()),
        StructField("category", StringType()),
    ]
)


@dp.materialized_view(name="d6_nondeterministic_dedup")
def d6_nondeterministic_dedup():
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
        F.col("j.order_id").alias("order_id"),
        F.col("j.amount").alias("amount"),
        F.col("j.category").alias("category"),
    )
    # No ordering -> arbitrary survivor per order_id.
    return parsed.dropDuplicates(["order_id"])

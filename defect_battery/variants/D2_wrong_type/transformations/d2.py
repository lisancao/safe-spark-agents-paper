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

# D2 wrong type in parse schema: event_time typed TIMESTAMP. The orders topic
# carries event_time as mostly ISO-8601 strings PLUS some epoch-millis strings.
# from_json with a TIMESTAMP schema does NOT error and does NOT null the
# epoch-millis rows -- it silently parses them as epoch-SECONDS, landing them
# ~56,000 years in the future. Classic silent-wrong: dry-run passes; the data
# is corrupted only after a run.
ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("event_time", TimestampType()),  # <-- the injected defect
        StructField("amount", DoubleType()),
        StructField("category", StringType()),
    ]
)


@dp.table(name="d2_orders_wrong_type")
def d2_orders_wrong_type():
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "orders")
        .option("startingOffsets", "earliest")
        .load()
    )
    return raw.select(
        F.from_json(F.col("value").cast("string"), ORDERS_SCHEMA).alias("j")
    ).select(
        F.col("j.order_id").alias("order_id"),
        F.col("j.merchant_id").alias("merchant_id"),
        F.col("j.event_time").alias("event_time"),
        F.col("j.amount").alias("amount"),
        F.col("j.category").alias("category"),
    )

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()


# D3 unwatermarked streaming dedup: dropDuplicates on a stream with NO
# watermark. SDP dry-run does NOT catch this (graph validates -> COMPLETED).
# At a real long-running scale the dedup state grows unbounded (see D9).
@dp.table(name="d3_unwatermarked_dedup")
def d3_unwatermarked_dedup():
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "orders")
        .option("startingOffsets", "earliest")
        .load()
    )
    parsed = raw.select(
        F.get_json_object(F.col("value").cast("string"), "$.order_id").alias("order_id"),
        F.col("value").cast("string").alias("payload"),
    )
    # NO withWatermark() before dropDuplicates -> unbounded state keyspace.
    return parsed.dropDuplicates(["order_id"])

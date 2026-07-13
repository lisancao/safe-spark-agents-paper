from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()


# D9 unbounded state -> OOM (cluster-only). This is D3 (unwatermarked streaming
# dedup) under a long-running stream at scale: the dedup keyspace grows without
# bound until the executor OOMs. The battery runner DOES run this variant's SDP
# dry-run (it passes COMPLETED, like D3); only the OOM itself is not reproduced
# on the laptop -- that needs a cluster and a long-running stream.
@dp.table(name="d9_unbounded_state")
def d9_unbounded_state():
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
    # No watermark, long-running -> unbounded state. Resource-failure on cluster.
    return parsed.dropDuplicates(["order_id"])

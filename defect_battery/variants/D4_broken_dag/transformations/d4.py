from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()


# D4 broken DAG / missing upstream: read a table that no flow in the pipeline
# produces and that does not exist in the catalog. SDP resolves the whole graph
# at analysis time -> TABLE_OR_VIEW_NOT_FOUND, before any data is read.
@dp.materialized_view(name="d4_broken_dag")
def d4_broken_dag():
    src = spark.read.table("nonexistent_upstream")
    return src.select(F.col("*"))

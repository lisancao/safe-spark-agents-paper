from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()


# D1 missing/renamed column: select a column that is not in the schema.
# spark.range(10) has exactly one column `id`; `does_not_exist` is absent.
# SDP analyzes the full graph before any data is read -> UNRESOLVED_COLUMN.
@dp.materialized_view(name="d1_missing_col")
def d1_missing_col():
    src = spark.range(10)
    return src.select(F.col("does_not_exist"))

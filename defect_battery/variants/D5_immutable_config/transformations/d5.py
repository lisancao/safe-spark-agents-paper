from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()


# D5 invalid/immutable config: the defect lives in spark-pipeline.yml, which
# sets `spark.jars` in the `configuration:` block. spark.jars is a static,
# cluster-immutable config that cannot be set at session/pipeline scope, so SDP
# rejects the spec at analysis time -> CANNOT_MODIFY_CONFIG. This transform is
# a trivial valid view; it never gets to run because the spec fails first.
@dp.materialized_view(name="d5_immutable_config")
def d5_immutable_config():
    return spark.range(10).select(F.col("id"))

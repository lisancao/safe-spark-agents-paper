"""D1 plain-PySpark arm: missing column, classic local Spark (no SDP).

Contrast with the SDP arm: SDP catches D1 at ANALYSIS (graph resolution, before
any data is read). Plain PySpark builds the DataFrame lazily, so referencing a
missing column does NOT fail at construction -- it only throws when an ACTION
forces analysis/execution. We make construction and the action distinguishable
and print which stage failed.
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.master("local[2]")
    .appName("d1_plain")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

src = spark.range(10)

# Step 1: build the DataFrame referencing a missing column.
# In Spark, select() eagerly analyzes the logical plan, so the failure can
# surface here OR at the action depending on the API. Capture both.
build_ok = False
df = None
try:
    df = src.select(F.col("does_not_exist"))
    build_ok = True
    print("D1_PLAIN build: select() returned without error")
except Exception as e:
    print("D1_PLAIN build FAILED:", type(e).__name__, "-", str(e).splitlines()[0])

if build_ok:
    # Step 2: force an action.
    try:
        n = df.count()
        print("D1_PLAIN action: count() =", n, "(NO error -- unexpected)")
    except Exception as e:
        print("D1_PLAIN action FAILED:", type(e).__name__, "-", str(e).splitlines()[0])
        sys.exit(2)

spark.stop()

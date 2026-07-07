#!/usr/bin/env python3
"""Spark Connect 'change one URL' demo.

The SAME code runs against a local Connect server during development and against
a remote cluster in production. Only the connection string changes -- supplied
via the SPARK_REMOTE env var, never hard-coded.

  dev :  export SPARK_REMOTE="sc://localhost:15002"
  prod:  export SPARK_REMOTE="sc://connect.prod.internal:15002/;token=..."

Run with the lightweight client (no JVM, ~1.8 MB):
  ~/spark-agent-refarch/.venv-client/bin/python connect/client.py <table_or_path>

Usage:
  client.py <table_name>            # read a catalog table over Connect
  client.py --path <parquet_path>   # read a path over Connect
"""
import os, sys
from pyspark.sql import SparkSession

remote = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
spark = SparkSession.builder.remote(remote).getOrCreate()
print(f"connected via {remote}  (client has no local JVM -- all execution is remote)")

if len(sys.argv) >= 3 and sys.argv[1] == "--path":
    df = spark.read.parquet(sys.argv[2])
elif len(sys.argv) >= 2:
    df = spark.table(sys.argv[1])
else:
    # no target given: just prove the session is live and remote
    df = spark.range(5)

print(f"row count: {df.count()}")
df.show(5, truncate=False)
spark.stop()

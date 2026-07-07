"""Create the demo database/schema the SDP specs default into (idempotent).

The rendered specs pin `catalog: spark_catalog`, `database: gitops_demo`. The SDP
CLI (`cli.py dry-run` / `run`) resolves flow names against that default schema and
fails with `[SCHEMA_NOT_FOUND] spark_catalog.gitops_demo ... SQLSTATE 42704` if the
schema does not exist -- a FALSE failure unrelated to any structural defect in the
pipeline. (Verified by smoke test: without this step the gate errors on the missing
schema; with it, a valid spec dry-runs to `Run is COMPLETED`.)

CI runs this once, after starting Spark Connect and before the gate / reconcile, so
the gate's pass/fail reflects the pipeline, not a missing namespace. In production a
real catalog admin creates this schema once; locally CI does it.

This is the CONTROLLER surface: it legitimately holds a Spark session and REQUIRES
`SPARK_REMOTE`. The agent PR author never imports or runs this module.

Usage:
    SPARK_REMOTE=sc://localhost:15055 ensure_schema.py
    SPARK_REMOTE=sc://... ensure_schema.py --catalog spark_catalog --database gitops_demo
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Optional, Sequence

# Defaults mirror sdp_artifact.DEFAULT_CATALOG / DEFAULT_DATABASE.
DEFAULT_CATALOG = "spark_catalog"
DEFAULT_DATABASE = "gitops_demo"


def ensure_schema(catalog: str = DEFAULT_CATALOG,
                  database: str = DEFAULT_DATABASE,
                  spark_remote: Optional[str] = None) -> None:
    """`CREATE SCHEMA IF NOT EXISTS <catalog>.<database>` over Spark Connect."""
    remote = spark_remote or os.environ.get("SPARK_REMOTE")
    if not remote:
        raise RuntimeError(
            "SPARK_REMOTE is not set; ensure_schema needs a reachable Spark Connect "
            "endpoint (it runs a CREATE SCHEMA over the session)."
        )
    from pyspark.sql import SparkSession  # controller side: a session is expected
    spark = SparkSession.builder.remote(remote).getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{database}`")
    print(f"ensured schema `{catalog}`.`{database}`")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Create the SDP demo schema (idempotent).")
    p.add_argument("--catalog", default=DEFAULT_CATALOG)
    p.add_argument("--database", default=DEFAULT_DATABASE)
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    ensure_schema(args.catalog, args.database)
    return 0


if __name__ == "__main__":
    sys.exit(main())

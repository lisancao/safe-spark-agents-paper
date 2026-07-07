"""Short-lived SUBPROCESS Spark Connect helpers for the Part-1 LOCAL backend (Option C).

pyspark's classic-vs-Connect SparkSession mode is process-GLOBAL: once the
long-lived runner process creates an in-process Connect session, the imperative
`LocalSparkExecutor`'s classic `getOrCreate()` fails with `CONNECT_URL_NOT_SET`.
So under `--backend local` the runner process must NEVER create a Connect session.

The two operations that genuinely need one are moved HERE, into a short-lived
subprocess that creates the Connect session, does its work, and exits -- poisoning
only itself, never the parent:

  * ensure-schema   : `CREATE SCHEMA IF NOT EXISTS <catalog>.<database>` so the SDP
                      CLI does not fail with [SCHEMA_NOT_FOUND] (was
                      LocalConnectServer.ensure_schema's in-process session).
  * output-profile  : read the SDP agent's MATERIALIZED output back and run the
                      task's OUTPUT oracle (`output_oracles.build_output_profile`),
                      serialising the OutputProfile fields to a JSON result file
                      (was runner._build_profile's in-process `executor.spark`).

The SDP gate/execute already run `pyspark/pipelines/cli.py` as subprocesses, and the
H2 stage-diff reads the driver UI over HTTP (urllib, not a pyspark session), so those
stay in the parent untouched.

Run as (cwd = the study dir, so `-m harness.connect_helper` resolves):
  python3 -m harness.connect_helper ensure-schema --remote <sc-url> \
        --catalog spark_catalog --database default
  python3 -m harness.connect_helper output-profile --remote <sc-url> \
        --input <file://...> --contract <json> --defects D2,D7,D8 --result <path>
"""
from __future__ import annotations

import argparse
import json
import os
import sys

# allow `python3 -m harness.connect_helper` AND a direct `python3 connect_helper.py`:
# put the study dir (two levels up from this file) on sys.path so `from harness import
# output_oracles` resolves regardless of cwd.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# the OutputProfile fields serialised across the subprocess boundary (kept in ONE
# place so the parent reconstruction in local_connect.py stays in lockstep).
PROFILE_FIELDS = (
    "d2_misparsed_rows", "d6_ambiguous_keys_unhandled", "d7_wrong_day_rows",
    "d8_dollars_dropped", "d8_rows_dropped", "reconciles",
)


def _session(remote: str):
    """A Connect session -- created HERE, in the subprocess, so the parent stays
    classic-capable. `builder.remote(remote)` is the explicit, authoritative knob."""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.remote(remote).getOrCreate()
    try:
        spark.sparkContext  # not all builds expose this on Connect; best-effort quiet
    except Exception:  # noqa: BLE001
        pass
    return spark


def ensure_schema(remote: str, catalog: str, database: str) -> None:
    spark = _session(remote)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{database}`")
    print(f"[connect-helper] ensured schema `{catalog}`.`{database}`", file=sys.stderr)


def output_profile(remote: str, input_path: str, contract: dict,
                   defects: list, result_path: str) -> None:
    from harness import output_oracles
    spark = _session(remote)

    def read_table(name: str):
        return spark.table(name)

    prof = output_oracles.build_output_profile(read_table, spark, input_path, defects, contract)
    payload = {k: getattr(prof, k) for k in PROFILE_FIELDS}
    payload["extra"] = prof.extra
    with open(result_path, "w") as f:
        json.dump(payload, f)
    print(f"[connect-helper] wrote output profile -> {result_path}", file=sys.stderr)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Subprocess Spark Connect helpers (Part-1 local).")
    sub = ap.add_subparsers(dest="cmd", required=True)

    es = sub.add_parser("ensure-schema")
    es.add_argument("--remote", required=True)
    es.add_argument("--catalog", default="spark_catalog")
    es.add_argument("--database", default="default")

    op = sub.add_parser("output-profile")
    op.add_argument("--remote", required=True)
    op.add_argument("--input", required=True)
    op.add_argument("--contract", required=True, help="output_contract as a JSON string")
    op.add_argument("--defects", default="", help="comma-separated defects_in_scope")
    op.add_argument("--result", required=True, help="path to write the profile JSON to")

    args = ap.parse_args(argv)
    if args.cmd == "ensure-schema":
        ensure_schema(args.remote, args.catalog, args.database)
    else:
        contract = json.loads(args.contract)
        defects = [d for d in args.defects.split(",") if d]
        output_profile(args.remote, args.input, contract, defects, args.result)
    return 0


if __name__ == "__main__":
    sys.exit(main())

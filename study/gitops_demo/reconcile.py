"""Reconcile (the merge step): run `spark-pipelines run` for changed/known specs.

This is the CONTROLLER side of the GitOps loop -- it runs AFTER a PR merges to main.
Unlike the agent surface, the controller DOES hold a Spark session: it reads
`SPARK_REMOTE` and invokes the real SDP CLI to materialize each pipeline. This is
exactly the asymmetry the slice demonstrates -- the session lives with the
reconciler, never with the author.

Mechanically: for each changed (or, with --all, every) `spark-pipeline.yml`, run

    python3 "$SPARK_HOME/pipelines/cli.py" run --spec <spec>

over SPARK_REMOTE. `cli.py run` REQUIRES Spark Connect (a bare in-process
SparkSession raises ONLY_SUPPORTED_WITH_SPARK_CONNECT), so SPARK_REMOTE must be set
and a Connect server reachable; the CI workflow brings up a local one.

Usage (typically from CI on push-to-main):
    SPARK_REMOTE=sc://localhost:15055 reconcile.py --base <ref> --head <ref>
    SPARK_REMOTE=sc://localhost:15055 reconcile.py --all
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from typing import List, Optional, Sequence

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # sibling import
import changed_pipelines  # noqa: E402


def spark_home() -> str:
    """Resolve SPARK_HOME, falling back to the installed pyspark package dir.

    Importing pyspark HERE is correct: reconcile.py is the controller, which is
    *supposed* to have Spark. (The agent surface never imports this module.)
    """
    sh = os.environ.get("SPARK_HOME")
    if sh:
        return sh
    import pyspark
    return os.path.dirname(pyspark.__file__)


def cli_path(home: Optional[str] = None) -> str:
    return os.path.join(home or spark_home(), "pipelines", "cli.py")


def reconcile_spec(spec_path: str, *, home: Optional[str] = None,
                   dry: bool = False) -> int:
    """Run `cli.py run --spec <spec>` (or `dry-run` if dry=True) over SPARK_REMOTE.

    Returns the subprocess return code. Requires SPARK_REMOTE in the environment.
    """
    if not os.environ.get("SPARK_REMOTE"):
        raise RuntimeError(
            "SPARK_REMOTE is not set; reconcile needs a reachable Spark Connect "
            "endpoint (cli.py run is ONLY_SUPPORTED_WITH_SPARK_CONNECT)."
        )
    command = "dry-run" if dry else "run"
    argv = ["python3", cli_path(home), command, "--spec", spec_path]
    print(f"+ {' '.join(argv)}", flush=True)
    proc = subprocess.run(argv, env=dict(os.environ))
    return proc.returncode


def reconcile_all(specs: Sequence[str], *, home: Optional[str] = None,
                  dry: bool = False) -> int:
    """Reconcile each spec; return nonzero if ANY spec fails (and report which)."""
    home = home or spark_home()
    failures: List[str] = []
    for spec in specs:
        rc = reconcile_spec(spec, home=home, dry=dry)
        status = "OK" if rc == 0 else f"FAILED (rc={rc})"
        print(f"  {spec}: {status}", flush=True)
        if rc != 0:
            failures.append(spec)
    if failures:
        print(f"reconcile FAILED for {len(failures)} spec(s):", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        return 1
    print(f"reconcile OK for {len(specs)} spec(s).")
    return 0


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Reconcile SDP specs (spark-pipelines run).")
    p.add_argument("--base", default=os.environ.get("BASE_REF"),
                   help="Base git ref (default: $BASE_REF).")
    p.add_argument("--head", default=os.environ.get("HEAD_REF"),
                   help="Head git ref (default: $HEAD_REF).")
    p.add_argument("--all", action="store_true",
                   help="Reconcile every spec, not just changed ones.")
    p.add_argument("--spec", action="append", default=None,
                   help="Explicit spec path(s); repeatable. Overrides diff/scan.")
    p.add_argument("--dry", action="store_true",
                   help="Run dry-run instead of run (structural check only).")
    p.add_argument("--cwd", default=None, help="Directory to resolve specs from.")
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    if args.spec:
        specs = list(args.spec)
    else:
        specs = changed_pipelines.resolve(args.base, args.head, args.all, cwd=args.cwd)
    if not specs:
        print("no specs to reconcile.")
        return 0
    return reconcile_all(specs, dry=args.dry)


if __name__ == "__main__":
    sys.exit(main())

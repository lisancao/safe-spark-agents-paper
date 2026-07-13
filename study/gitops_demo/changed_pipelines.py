"""Print the changed `spark-pipeline.yml` spec paths for CI to iterate.

Given a base/head git ref pair, list the SDP spec files under
`experiments/safe_agent_study/gitops_demo/pipeline-definitions/` that changed (added
or modified). When no git diff is available (e.g. a fresh checkout, or run with
`--all`), fall back to a path scan that lists every spec. CI feeds this list to the
dry-run gate / reconcile step.

This module is part of the CI surface, not the agent surface, but it is still
session-free: it only runs `git` (allowlisted) and scans the filesystem. It imports
NO pyspark and opens NO Spark session.

Usage:
    changed_pipelines.py --base <ref> --head <ref>   # changed specs in the diff
    changed_pipelines.py --all                        # every spec (path scan)
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from typing import List, Optional, Sequence

# The specs live at <repo-root>/<gitops_demo>/pipeline-definitions/<slug>/
# spark-pipeline.yml. Discovery MUST use the full repo-relative path so the git
# diff / scan match where sdp_artifact.py writes and where the workflow `paths:`
# filter triggers -- a bare 'pipeline-definitions' from the repo root finds
# NOTHING, and the gate would silently pass on every PR. The paper repo mounts
# this dir at study/gitops_demo; the original working tree at
# experiments/safe_agent_study/gitops_demo. Derive it from this file's own
# location relative to the git root so both layouts (and the CI checkout) work.
def _gitops_subdir() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    d = here
    for _ in range(6):
        parent = os.path.dirname(d)
        if os.path.exists(os.path.join(parent, ".git")):
            return os.path.relpath(here, parent).replace(os.sep, "/")
        d = parent
    return "study/gitops_demo"


GITOPS_SUBDIR = _gitops_subdir()
PIPELINE_DEFINITIONS_DIRNAME = "pipeline-definitions"
DEFINITIONS_RELPATH = f"{GITOPS_SUBDIR}/{PIPELINE_DEFINITIONS_DIRNAME}"
SPEC_FILENAME = "spark-pipeline.yml"


def _git(argv: Sequence[str], cwd: Optional[str]) -> subprocess.CompletedProcess:
    """Run a git command (the only binary this module ever invokes)."""
    if not argv or os.path.basename(str(argv[0])).lower() != "git":
        raise RuntimeError("changed_pipelines only shells out to git")
    return subprocess.run(list(argv), cwd=cwd, capture_output=True, text=True)


def _repo_root(cwd: Optional[str]) -> Optional[str]:
    res = _git(["git", "rev-parse", "--show-toplevel"], cwd)
    return res.stdout.strip() if res.returncode == 0 else None


def scan_all_specs(root: str) -> List[str]:
    """Every `spark-pipeline.yml` under `root/<DEFINITIONS_RELPATH>/`, sorted."""
    base = os.path.join(root, DEFINITIONS_RELPATH)
    found: List[str] = []
    for dirpath, _, files in os.walk(base):
        if SPEC_FILENAME in files:
            found.append(os.path.join(dirpath, SPEC_FILENAME))
    return sorted(found)


def changed_specs(base_ref: str, head_ref: str, *,
                  cwd: Optional[str] = None, abs_paths: bool = True) -> List[str]:
    """Spec paths changed (Added/Copied/Modified/Renamed) between two refs.

    Restricts the diff to `<DEFINITIONS_RELPATH>/**/spark-pipeline.yml`. Deletions
    are intentionally excluded (a removed pipeline has no spec to gate/reconcile).
    """
    root = _repo_root(cwd) or (cwd or os.getcwd())
    pathspec = f"{DEFINITIONS_RELPATH}/**/{SPEC_FILENAME}"
    res = _git(
        ["git", "diff", "--name-only", "--diff-filter=ACMR",
         f"{base_ref}...{head_ref}", "--", pathspec],
        cwd=root,
    )
    if res.returncode != 0:
        raise RuntimeError(f"git diff failed: {res.stderr.strip()}")
    rels = [ln.strip() for ln in res.stdout.splitlines() if ln.strip()]
    # Keep only the spec files (the pathspec already narrows, but be explicit).
    rels = [r for r in rels if os.path.basename(r) == SPEC_FILENAME]
    if abs_paths:
        return [os.path.join(root, r) for r in rels]
    return rels


def resolve(base: Optional[str], head: Optional[str], scan_all: bool,
            cwd: Optional[str] = None) -> List[str]:
    """Resolve the spec list: scan-all, or a base..head diff (with scan fallback)."""
    root = _repo_root(cwd) or (cwd or os.getcwd())
    if scan_all or not (base and head):
        return scan_all_specs(root)
    try:
        specs = changed_specs(base, head, cwd=cwd)
    except RuntimeError:
        # No usable diff (shallow/missing ref): degrade to a full scan rather than
        # silently gating nothing.
        return scan_all_specs(root)
    return specs


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="List changed SDP spec paths for CI.")
    p.add_argument("--base", default=os.environ.get("BASE_REF"),
                   help="Base git ref (default: $BASE_REF).")
    p.add_argument("--head", default=os.environ.get("HEAD_REF"),
                   help="Head git ref (default: $HEAD_REF).")
    p.add_argument("--all", action="store_true",
                   help="Ignore the diff; list every spec via a path scan.")
    p.add_argument("--cwd", default=None, help="Directory to resolve from.")
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    specs = resolve(args.base, args.head, args.all, cwd=args.cwd)
    for s in specs:
        print(s)
    return 0


if __name__ == "__main__":
    sys.exit(main())

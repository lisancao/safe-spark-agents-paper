"""Path-resolution tests for `changed_pipelines.py` (the gate-fires-on-nothing bug).

The specs live at the FULL repo-relative path
`<gitops_demo>/pipeline-definitions/<slug>/spark-pipeline.yml`, matching
sdp_artifact.py's output and the workflow `paths:` filter; `<gitops_demo>` is
`study/gitops_demo` in the paper repo and `experiments/safe_agent_study/gitops_demo`
in the original working tree, derived at import time by changed_pipelines itself.
A regression where discovery used a bare `pipeline-definitions` from the repo root
would find NOTHING and the gate would silently pass on every PR. These tests build
a real temp git repo with a spec committed at the canonical subpath and assert both
the diff and the scan actually find it.
"""
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
GITOPS_DEMO = os.path.dirname(HERE)
sys.path.insert(0, GITOPS_DEMO)

import changed_pipelines as cp  # noqa: E402

SPEC_RELPATH = os.path.join(
    *cp.GITOPS_SUBDIR.split("/"),
    "pipeline-definitions", "myslug", "spark-pipeline.yml",
)
SPEC_TEXT = (
    "name: gitops_demo__myslug\n"
    "storage: file:///tmp/safe-spark-agents-gitops/myslug/storage\n"
    "catalog: spark_catalog\n"
    "database: gitops_demo\n"
    "libraries:\n  - glob:\n      include: transformations/**\n"
)


def _git(repo, *args):
    subprocess.run(["git", *args], cwd=repo, check=True,
                   capture_output=True, text=True)


def _rev(repo, ref="HEAD"):
    return subprocess.run(["git", "rev-parse", ref], cwd=repo,
                          capture_output=True, text=True).stdout.strip()


def _make_repo(tmp_path):
    repo = str(tmp_path)
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "t@example.com")
    _git(repo, "config", "user.name", "t")
    _git(repo, "commit", "-q", "--allow-empty", "-m", "base")
    return repo


def _commit_spec(repo):
    spec_abs = os.path.join(repo, SPEC_RELPATH)
    os.makedirs(os.path.dirname(spec_abs), exist_ok=True)
    with open(spec_abs, "w") as f:
        f.write(SPEC_TEXT)
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "add pipeline myslug")


def test_changed_specs_finds_spec_at_real_subpath(tmp_path):
    repo = _make_repo(tmp_path)
    base = _rev(repo)
    _commit_spec(repo)
    head = _rev(repo)

    found = cp.changed_specs(base, head, cwd=repo)
    assert any(p.endswith(SPEC_RELPATH) for p in found), found
    # absolute paths, anchored at the repo root
    assert all(os.path.isabs(p) for p in found)


def test_scan_all_specs_finds_spec_at_real_subpath(tmp_path):
    repo = _make_repo(tmp_path)
    _commit_spec(repo)
    found = cp.scan_all_specs(repo)
    assert any(p.endswith(SPEC_RELPATH) for p in found), found


def test_resolve_falls_back_to_scan_on_bad_ref(tmp_path):
    repo = _make_repo(tmp_path)
    _commit_spec(repo)
    # a non-existent base ref must degrade to a full scan, not gate nothing
    specs = cp.resolve("deadbeef", "HEAD", False, cwd=repo)
    assert any(p.endswith(SPEC_RELPATH) for p in specs), specs


def test_definitions_relpath_matches_workflow_paths_filter():
    # guards the derived path against drift from the workflow `paths:` filter:
    # if the gate workflow watches a different subtree than discovery scans,
    # the gate silently passes on every PR.
    d = HERE
    root = None
    for _ in range(8):
        d = os.path.dirname(d)
        if os.path.exists(os.path.join(d, ".git")):
            root = d
            break
    assert root, "no git root above the test dir"
    wf = os.path.join(root, ".github", "workflows", "gitops-sdp-dry-run.yml")
    assert os.path.exists(wf), wf
    with open(wf) as f:
        text = f.read()
    assert f"{cp.DEFINITIONS_RELPATH}/**" in text, (
        f"workflow paths: filter does not watch {cp.DEFINITIONS_RELPATH!r}"
    )

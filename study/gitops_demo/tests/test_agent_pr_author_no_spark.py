"""Hard safety-boundary tests for `agent_pr_author.py`.

The agent surface must be PROVABLY session-free. These tests assert, by
source-inspection + monkeypatch + a clean-interpreter subprocess, that the agent
path:

  * imports NO pyspark (never enters sys.modules even after building the brain);
  * never opens a Spark session / instantiates ConnectExecutor;
  * REFUSES TO RUN (nonzero) when SPARK_REMOTE is set;
  * shells out to NOTHING but git and gh (allowlist enforced).

The brain's `propose()` is MOCKED -- no real Anthropic call, no Spark.
"""
import os
import subprocess
import sys
import types

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
GITOPS_DEMO = os.path.dirname(HERE)
sys.path.insert(0, GITOPS_DEMO)

import agent_pr_author as apa  # noqa: E402

AGENT_SDP_CODE = (
    "from pyspark import pipelines as dp\n\n"
    "@dp.table\n"
    "def orders_silver():\n"
    "    return spark.read.json('x')\n"
)


class FakeBrain:
    """Stand-in for AnthropicBrain: records the call, returns canned SDP code.

    Makes NO network call and touches NO Spark -- exactly what we want to prove the
    surrounding authoring path also does."""

    def __init__(self):
        self.calls = 0

    def propose(self, state, arm):
        self.calls += 1
        return types.SimpleNamespace(code=AGENT_SDP_CODE, command="spark-pipelines dry-run")


# --------------------------------------------------------------------------- #
# Source inspection
# --------------------------------------------------------------------------- #
def _agent_source():
    with open(os.path.join(GITOPS_DEMO, "agent_pr_author.py")) as f:
        return f.read()


def _sdp_artifact_source():
    with open(os.path.join(GITOPS_DEMO, "sdp_artifact.py")) as f:
        return f.read()


def test_agent_source_imports_no_pyspark():
    src = _agent_source() + "\n" + _sdp_artifact_source()
    for line in src.splitlines():
        stripped = line.strip()
        assert not stripped.startswith("import pyspark"), line
        assert not stripped.startswith("from pyspark"), line


def _referenced_identifiers(src: str):
    """Every Name id / Attribute attr referenced in `src` (ignores docstrings &
    comments, which is exactly what we want -- the docstring NAMES SparkSession to
    say it must not be used)."""
    import ast
    tree = ast.parse(src)
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            names.add(node.attr)
    return names


def test_agent_source_never_instantiates_connect_executor_or_session():
    # AST-level: the agent code REFERENCES neither symbol (docstring mentions are OK).
    names = _referenced_identifiers(_agent_source())
    assert "ConnectExecutor" not in names
    assert "SparkSession" not in names
    # and no pyspark module reference at the AST level either.
    assert "pyspark" not in names


def test_allowlist_is_exactly_git_and_gh():
    assert set(apa.ALLOWED_BINARIES) == {"git", "gh"}


# --------------------------------------------------------------------------- #
# Clean-interpreter: importing + building the brain loads no pyspark/anthropic
# --------------------------------------------------------------------------- #
def test_clean_interpreter_loads_no_pyspark_or_anthropic():
    code = (
        "import sys\n"
        f"sys.path.insert(0, {GITOPS_DEMO!r})\n"
        "import agent_pr_author as apa\n"
        "brain, arm = apa.build_brain('a task prompt')\n"
        "assert arm.arm_id == 'B', arm.arm_id\n"
        "assert arm.paradigm == 'sdp'\n"
        "assert arm.dry_run_gate is True\n"
        # spark-safety was scrapped in the locked design (arms/B.json: safety_skill=false);
        # the boundary under test is the dry-run gate + SDP paradigm, not that skill.
        "assert arm.safety_skill is False\n"
        "assert 'pyspark' not in sys.modules, 'pyspark leaked into the agent path'\n"
        "assert 'anthropic' not in sys.modules, 'anthropic client built too eagerly'\n"
        "print('OK')\n"
    )
    res = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert res.returncode == 0, res.stdout + res.stderr
    assert "OK" in res.stdout


# --------------------------------------------------------------------------- #
# SPARK_REMOTE refusal
# --------------------------------------------------------------------------- #
def test_assert_no_spark_remote_raises_when_set():
    with pytest.raises(apa.SafetyBoundaryError):
        apa.assert_no_spark_remote({"SPARK_REMOTE": "sc://localhost:15055"})


def test_main_refuses_when_spark_remote_set(monkeypatch):
    monkeypatch.setenv("SPARK_REMOTE", "sc://localhost:15055")
    rc = apa.main(["--task", "x.json", "--pipeline-slug", "p"])
    assert rc == 2


# --------------------------------------------------------------------------- #
# propose + render: file generation path, no Spark
# --------------------------------------------------------------------------- #
def test_propose_and_render_writes_artifact_no_spark(tmp_path, monkeypatch):
    monkeypatch.delenv("SPARK_REMOTE", raising=False)
    brain = FakeBrain()
    arm = object()  # FakeBrain ignores arm
    task = {"id": "orders_silver_gold", "pipeline_name": "gitops_demo__demo"}

    # REAL assertion (no `or True`): snapshot sys.modules and assert the
    # propose+render CALL imports no pyspark. A delta check is robust to any other
    # test in the process having imported pyspark earlier; the clean-interpreter test
    # above is the authoritative whole-process proof.
    before = set(sys.modules)
    written = apa.propose_and_render(brain, arm, task, "demo", str(tmp_path))
    newly_imported = set(sys.modules) - before
    assert not any(m == "pyspark" or m.startswith("pyspark.") for m in newly_imported), \
        f"propose_and_render imported pyspark: {sorted(newly_imported)}"

    assert brain.calls == 1
    assert written  # files were written
    spec = tmp_path / "pipeline-definitions" / "demo" / "spark-pipeline.yml"
    pipe = tmp_path / "pipeline-definitions" / "demo" / "transformations" / "pipeline.py"
    assert spec.exists() and pipe.exists()
    assert AGENT_SDP_CODE.rstrip() in pipe.read_text()


def test_propose_and_render_rechecks_spark_remote_before_propose(tmp_path, monkeypatch):
    """#5: a SPARK_REMOTE injected AFTER startup must still block the propose step."""
    monkeypatch.setenv("SPARK_REMOTE", "sc://localhost:15055")
    brain = FakeBrain()
    with pytest.raises(apa.SafetyBoundaryError):
        apa.propose_and_render(brain, object(), {"id": "t"}, "demo", str(tmp_path))
    assert brain.calls == 0  # brain.propose was never reached


# --------------------------------------------------------------------------- #
# Allowlist: the authoring path shells out to nothing but git/gh
# --------------------------------------------------------------------------- #
def test_run_cmd_refuses_non_allowlisted_binary(monkeypatch):
    monkeypatch.delenv("SPARK_REMOTE", raising=False)
    for bad in (["spark-submit", "pipeline.py"],
                ["python3", "cli.py", "run"],
                ["spark-pipelines", "run"],
                ["rm", "-rf", "/"]):
        with pytest.raises(apa.SafetyBoundaryError):
            apa.run_cmd(bad)


def test_full_authoring_path_only_uses_git_and_gh(tmp_path, monkeypatch):
    """Drive open_pr end-to-end with subprocess MOCKED; assert every shelled
    command is git or gh and nothing else (no spark-submit/spark-pipelines/cli.py)."""
    monkeypatch.delenv("SPARK_REMOTE", raising=False)
    calls = []

    def fake_run(argv, *a, **kw):
        calls.append(list(argv))
        # _timestamp() / rev-parse read stdout; give a stable value.
        return subprocess.CompletedProcess(argv, 0, stdout="20240101-000000\n", stderr="")

    monkeypatch.setattr(apa.subprocess, "run", fake_run)

    # render a real artifact first (no Spark), then open the PR (mocked git/gh).
    brain = FakeBrain()
    task = {"id": "orders_silver_gold", "title": "demo", "pipeline_name": "gitops_demo__demo"}
    written = apa.propose_and_render(brain, object(), task, "demo", str(tmp_path))
    branch = apa.open_pr(str(tmp_path), "demo", written, "main", task)

    assert calls, "expected git/gh subprocess calls"
    for argv in calls:
        name = apa._binary_name(argv[0])
        assert name in {"git", "gh"}, f"non-allowlisted command shelled: {argv}"
    # a commit happened and carried the omnigent co-author trailer
    commit_calls = [c for c in calls if c[:2] == ["git", "commit"]]
    assert commit_calls, "expected a git commit"
    assert any(apa.COAUTHOR_TRAILER in arg for c in commit_calls for arg in c)
    assert branch.startswith("agent/gitops/demo-")

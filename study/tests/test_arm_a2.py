"""Arm A2 guard (pre-reg §3 addendum, item K): the pure paradigm contrast.

A2 = imperative PySpark + dry-run gate + safety skill -- a byte-for-byte mirror of
Arm B EXCEPT `paradigm`. B is SDP + gate + skill, so B-vs-A2 isolates the paradigm
(declarative-vs-imperative) with the gate and skill held constant; it removes the
gate/skill confound that B-vs-A carries. These tests pin that A2 is exactly that:

  (1) it loads under the identical-except-loop guard with the rest of the arms;
  (2) every SHARED (controlled) field is identical to B;
  (3) the ONLY manifest field that differs from B (besides arm_id/description) is
      paradigm -- so the loop differs from B in exactly one dimension;
  (4) its loop signature is distinct from every other arm (a real arm, not a dupe);
  (5) the runner routes A2 to the IMPERATIVE executor (paradigm, not commands).
"""
import json
import os
import sys

import pytest

# A2 was WITHDRAWN to arms/supplementary/A2.json per paper §6.1 (2026-06-29):
# the locked design is TWO arms + one ablation {A, B, B1}. This whole module pins
# the withdrawn A2 paradigm-contrast arm, so it is skipped wholesale (the file is
# kept for the supplementary appendix).
pytestmark = pytest.mark.skip(
    reason="A2 withdrawn to arms/supplementary per paper §6.1 (2026-06-29)")

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

from harness.arm_manifest import (LOOP_FIELDS, SHARED_FIELDS, load_arms,  # noqa: E402
                                  assert_identical_except_loop)
from harness.backends.local import LocalSparkExecutor  # noqa: E402
from harness.backends.local_connect import LocalConnectExecutor  # noqa: E402

ARMS_DIR = os.path.join(STUDY, "arms")
ARMS = load_arms(ARMS_DIR)


def test_a2_present_and_loads_under_identical_except_loop():
    assert "A2" in ARMS, "arms/A2.json did not load"
    # load_arms already ran the guard; assert it again explicitly for clarity
    assert_identical_except_loop(ARMS)


def test_a2_shared_fields_identical_to_b():
    a2, b = ARMS["A2"], ARMS["B"]
    for k in SHARED_FIELDS:
        assert getattr(a2, k) == getattr(b, k), \
            f"A2.{k}={getattr(a2, k)!r} must equal B.{k}={getattr(b, k)!r}"


def test_a2_mirrors_b_except_paradigm_and_paradigm_bound_commands():
    """The raw manifest differs from B ONLY in arm_id, description, paradigm, and the
    paradigm-BOUND allowed_commands (B drives the SDP CLI; A2 drives the imperative
    `python`/`spark-submit`). The gate + skill + skills are held CONSTANT -- that is
    the paradigm contrast."""
    a2 = json.load(open(os.path.join(ARMS_DIR, "A2.json")))
    b = json.load(open(os.path.join(ARMS_DIR, "B.json")))
    differing = {k for k in set(a2) | set(b) if a2.get(k) != b.get(k)}
    assert differing == {"arm_id", "description", "paradigm", "allowed_commands"}, differing
    assert a2["paradigm"] == "imperative_pyspark"
    assert b["paradigm"] == "sdp"
    # gate + skill + skills held constant (the whole point of the contrast)
    assert a2["dry_run_gate"] is True and a2["safety_skill"] is True
    assert a2["skills"] == b["skills"]
    # A2's commands are the IMPERATIVE set, identical to the other imperative+gate arm (B2)
    b2 = json.load(open(os.path.join(ARMS_DIR, "B2.json")))
    assert a2["allowed_commands"] == b2["allowed_commands"], a2["allowed_commands"]
    # ...and NONE of them is the SDP CLI (would be incoherent for the imperative executor)
    assert not any("spark-pipelines" in c for c in a2["allowed_commands"])


def test_a2_loop_differs_from_b_only_in_paradigm_and_commands():
    a2, b = ARMS["A2"].loop_signature(), ARMS["B"].loop_signature()
    differing = {k for k in LOOP_FIELDS if a2[k] != b[k]}
    assert differing == {"paradigm", "allowed_commands"}, differing


def test_a2_loop_signature_is_distinct_from_every_other_arm():
    sigs = {aid: json.dumps(m.loop_signature(), sort_keys=True) for aid, m in ARMS.items()}
    a2 = sigs.pop("A2")
    assert a2 not in sigs.values(), "A2 duplicates another arm's loop signature"


def test_runner_routes_a2_to_imperative_executor():
    """make_local_factories routes by paradigm: imperative_pyspark -> classic local
    Spark (LocalSparkExecutor), NOT the SDP LocalConnectExecutor."""
    from harness import cost as costmod
    from harness import runner

    class _Server:
        remote = "sc://local:15002/"
        rest_url = "http://localhost:4040"

    cfg = runner.StudyConfig(
        base_model_id="claude-opus-4-8",
        task_prompt_path=os.path.join(STUDY, "prompts", "task_prompt.md"),
        executor_config=costmod.ExecutorConfig(4, 4, 16.0, 0.192, "k8s", "m5.xlarge"),
        spark_remote="sc://x:1/", spark_rest_url=None)
    tasks_by_id = {"p1_medallion": {"id": "p1_medallion",
                                    "output_contract": {"table": "gold", "substrate": "orders"}}}
    os.environ.setdefault("ANTHROPIC_API_KEY", "test-key")
    _make_brain, make_executor = runner.make_local_factories(
        cfg, "PREAMBLE", tasks_by_id, _Server(),
        imperative_warehouse=os.path.join(STUDY, "_wh"), imperative_ui_port=4072)
    ex_a2 = make_executor("p1_medallion", ARMS["A2"], 42)
    ex_b = make_executor("p1_medallion", ARMS["B"], 42)
    assert isinstance(ex_a2, LocalSparkExecutor), type(ex_a2)
    assert isinstance(ex_b, LocalConnectExecutor), type(ex_b)


def test_a2_requested_command_and_executor_dispatch_are_coherent():
    """BLOCKER-3 regression: the command the model is ASKED for (snapped into
    A2.allowed_commands by _parse_proposal) and what the IMPERATIVE executor actually
    DISPATCHES must be consistent -- no 'asks for SDP CLI, runs python' incoherence.

    For every A2 allowed_command: (a) it survives _parse_proposal unchanged (it is in
    the allowed set), and (b) _imperative_execute_argv dispatches it to an imperative
    program invocation (python3 / spark-submit on pipeline.py), NEVER the SDP CLI.
    A disallowed command snaps to allowed[0], which is also imperative. The gate runs
    the agent's program in --analyze-only (not spark-pipelines)."""
    from harness.backends.base import LoopState, Proposal
    from harness.backends.live import ConnectExecutor, _parse_proposal

    a2 = ARMS["A2"]
    ex = ConnectExecutor("sc://x:1/", None)
    st = LoopState(task="t", seed=42, workspace="/ws", dataset_path="s3a://b/in",
                   output_table="gold")

    for cmd in a2.allowed_commands:
        # (a) an allowed command is NOT rewritten away (model gets what the arm offers)
        _code, snapped = _parse_proposal(f"```python\nx=1\n```\nCOMMAND: {cmd}", a2)
        assert snapped == cmd, f"allowed command {cmd!r} was snapped to {snapped!r}"
        # (b) it dispatches to an IMPERATIVE invocation on pipeline.py, never the SDP CLI
        argv = ex._imperative_execute_argv(Proposal(0, "x=1", snapped), a2, "/sh", st)
        assert argv[-1] == "/ws/pipeline.py", argv
        assert argv[0] in ("python3", "/sh/bin/spark-submit"), argv
        assert not any("spark-pipelines" in a or "cli.py" in a for a in argv), argv

    # an OFF-policy command snaps to allowed[0] -- still imperative, still coherent
    _c, snapped = _parse_proposal("```python\nx=1\n```\nCOMMAND: rm -rf /", a2)
    assert snapped == a2.allowed_commands[0]
    argv = ex._imperative_execute_argv(Proposal(0, "x=1", snapped), a2, "/sh", st)
    assert argv[-1] == "/ws/pipeline.py" and "spark-pipelines" not in " ".join(argv)
    # the gate is the agent's program in analyze-only mode, NOT the SDP CLI
    assert ex._imperative_gate_argv(st) == ["python3", "/ws/pipeline.py", "--analyze-only"]


def _run_all():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t(); print(f"PASS {t.__name__}")
        except AssertionError as e:
            failed += 1; print(f"FAIL {t.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            import traceback
            failed += 1; print(f"ERROR {t.__name__}: {type(e).__name__}: {e}"); traceback.print_exc()
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return failed


if __name__ == "__main__":
    sys.exit(1 if _run_all() else 0)

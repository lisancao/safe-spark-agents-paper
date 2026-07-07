"""End-to-end offline validation of the runner -> results.jsonl -> analysis chain.

No LLM, no Spark, no network: the replay backend drives the real loop control,
the real cost model, and the real blind grader, so this exercises every part of
the instrument except the live agent brain and live Connect executor (which need
the finalized backend + an API key).

Checks:
  * the runner emits schema-valid rows for the pilot episodes;
  * the two real pilots both grade silent_defect=False (capability retention);
  * cost invariants hold: a gated arm intercepts failures at the dry-run gate for
    ZERO executor-seconds, and compute-to-correct only counts compute up to green;
  * the identical-except-loop guard actually fires when an arm is perturbed;
  * analyze.py runs on a synthetic sweep and returns a coherent report.
"""
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

from harness import runner  # noqa: E402
from harness.arm_manifest import ArmManifest, assert_identical_except_loop, load_arms  # noqa: E402
from harness.schema import validate_row  # noqa: E402

PILOT = os.path.join(HERE, "fixtures", "pilot_episodes.json")


def _run_pilot(tmp):
    out = os.path.join(tmp, "results.jsonl")
    runner.main([
        "--backend", "replay", "--replay-trace", PILOT,
        "--only-tasks", "orders_silver_gold", "--only-arms", "A,B",
        "--max-seeds", "1", "--out", out, "--work-dir", os.path.join(tmp, "work"),
        "--clock", "1750000000",
    ])
    return [json.loads(l) for l in open(out) if l.strip()]


def test_runner_emits_schema_valid_rows():
    with tempfile.TemporaryDirectory() as tmp:
        rows = _run_pilot(tmp)
        assert len(rows) == 2, f"expected 2 rows, got {len(rows)}"
        for r in rows:
            problems = validate_row(r)
            assert not problems, f"schema problems in {r['run_id']}: {problems}"


def test_pilots_non_silent_and_completed():
    with tempfile.TemporaryDirectory() as tmp:
        rows = {r["arm"]: r for r in _run_pilot(tmp)}
        for arm in ("A", "B"):
            assert rows[arm]["silent_defect"] is False
            assert rows[arm]["exit_class"] == "completed"
            assert rows[arm]["task_success"] is True


def test_gate_intercepts_are_free():
    with tempfile.TemporaryDirectory() as tmp:
        rows = {r["arm"]: r for r in _run_pilot(tmp)}
        b = rows["B"]
        a = rows["A"]
        # Arm B's two dry-run failures were intercepted at the gate ($0, 0 exec-s)
        assert b["dry_run_intercepts"] == 2, b["dry_run_intercepts"]
        assert b["failing_iterations"] == 2
        # Arm A has no gate -> zero intercepts despite a failed iteration
        assert a["dry_run_intercepts"] == 0
        assert a["failing_iterations"] == 1
        # compute-to-correct only counts compute up to the green iteration
        assert b["executor_seconds_to_correct"] == 90.0, b["executor_seconds_to_correct"]
        # gate intercepts added wall time but no executor-seconds / $ beyond the green run
        assert b["executor_seconds"] == 90.0


def test_identical_except_loop_guard_fires():
    arms = load_arms(os.path.join(STUDY, "arms"))
    # perturb a CONTROLLED field on one arm -> the guard must raise
    bad = dict(arms)
    perturbed = ArmManifest.from_dict({
        **{k: getattr(arms["B"], k) for k in (
            "arm_id", "base_model_id", "task_prompt_ref", "max_iterations",
            "temperature", "top_p", "paradigm", "dry_run_gate", "safety_skill",
            "skills", "allowed_commands", "description")},
        "base_model_id": "a-different-model",  # confound!
    })
    bad["B"] = perturbed
    raised = False
    try:
        assert_identical_except_loop(bad)
    except ValueError as e:
        raised = True
        assert "NOT IDENTICAL-EXCEPT-LOOP" in str(e)
    assert raised, "guard failed to catch a base_model_id confound"


def test_analyze_runs_on_synthetic():
    sys.path.insert(0, HERE)
    import make_synthetic_results as msr
    from analysis import analyze  # noqa
    with tempfile.TemporaryDirectory() as tmp:
        res = os.path.join(tmp, "synth.jsonl")
        msr.gen(res)
        # synthetic rows carry the MEASURED field -> analyze as the remote/Connect
        # backend (H2 metric selection now requires an explicit known backend).
        rep = analyze.build_report(res, arms_meta=None, backend="live")
        assert rep["meta"]["n_rows"] == 240
        assert set(rep["H1_per_arm"]) == {"A", "B", "B1", "B2"}
        # locked 2-arm design (paper §6.1): the only registered contrast is A-vs-B,
        # even when ablation arms (B1/B2) are present in the file.
        assert len(rep["H1_contrasts"]) == 1
        # every contrast has a bootstrap CI and a Holm-adjusted p
        for key, d in rep["H1_contrasts"].items():
            assert d["bootstrap_ci95"] is not None, key
            assert d["holm_adjusted_p"] is not None, key
        # H2 reports the gated arms with an intercept fraction
        assert set(rep["H2_compute_to_correct"]) <= {"B", "B2"}
        md = analyze.render_markdown(rep)
        assert "headline numbers" in md


def _run_all():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL {t.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            import traceback
            failed += 1
            print(f"ERROR {t.__name__}: {type(e).__name__}: {e}")
            traceback.print_exc()
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return failed


if __name__ == "__main__":
    sys.exit(1 if _run_all() else 0)

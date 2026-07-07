"""Floor-effect pilot gate as a test (corpus v3 §0/§11).

Runs the deterministic reference-correct vs reference-defective grading for the
high-complexity / elevated tasks (HC-1, HC-2, p13, p14) on a fixed seed and
asserts the floor-effect property holds for each: a correct build PASSES (the
task is not impossible) and the targeted silent defect is CAUGHT (there is
signal). A FLAGGED task here means a possible floor / no signal -- the gate fails
so it is surfaced, not silently kept. Requires pyspark; skips without it.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(STUDY, "pilot"))


def _have_spark():
    try:
        import pyspark  # noqa: F401
        return True
    except Exception:
        return False


def test_floor_effect_gate_one_seed():
    if not _have_spark():
        print("SKIP floor-effect pilot: no pyspark"); return
    import floor_effect_pilot as fep
    report = fep.run_pilot(seeds=(42,))
    assert len(report) == 4, report
    tasks = {r["task"] for r in report}
    assert tasks == {"HC1_fx_trade_ledger", "HC2_session_funnel",
                     "p13_cdc_windowed", "p14_fx_settlement"}, tasks
    for r in report:
        assert r["correct_exit"] == "PASS", f"{r['task']} correct build did not pass: {r}"
        assert r["defective_exit"] == "CAUGHT", f"{r['task']} defect not caught (no signal): {r}"
        assert r["signal"] is True and r["floor_flag"] is False, r
    print("floor-effect gate: " + "; ".join(
        f"{r['task']}=correct/PASS,defect/CAUGHT" for r in report))


if __name__ == "__main__":
    try:
        test_floor_effect_gate_one_seed()
        print("PASS"); sys.exit(0)
    except AssertionError as e:
        print(f"FAIL: {e}"); sys.exit(1)

"""Unified HARNESS-FAULT policy (Part B): a broken INSTRUMENT can never masquerade as an
agent result. These tests prove the (c)->(b)->circuit-breaker design end to end:

  1. CLASSIFICATION: a harness fault is recognized as HARNESS, never an agent failure,
     and never accrues toward max_iterations.
  2. RETRY-ONCE -> QUARANTINE -> CONTINUE: a transient fault that the single retry clears
     is a normal result; a fault that survives the retry is quarantined HARNESS_ERROR
     (underlying reason preserved) and the sweep continues.
  3. CIRCUIT BREAKER: trips and aborts on EACH threshold independently (global > 3,
     per-arm > 1, per-bin > 1), covering BOTH fault paths together (a propose-throttle and
     an SDP/infra fault count toward the SAME breaker).
  4. The quarantine report carries the right fields (task, seed, arm, exit_class, reason).

No network, no Spark: the policy layer is driven through injected run_fn / sleep / cleanup.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

import pytest                                                      # noqa: E402

from harness import harness_faults as hf                           # noqa: E402
from harness.schema import is_harness_fault, HARNESS_FAULT_EXIT_CLASSES  # noqa: E402


class _Row:
    """Minimal stand-in for a ResultRow: the attributes the policy touches."""
    def __init__(self, exit_class, iterations=0, run_id="p2_cdc__B1__seed7"):
        self.exit_class = exit_class
        self.iterations = iterations
        self.harness_fault_reason = None
        self.notes = None
        self.run_id = run_id


# ---------------------------------------------------------------------------
# 1. classification
# ---------------------------------------------------------------------------
def test_propose_and_infra_faults_are_harness_not_agent():
    # BOTH fault paths are harness faults under the single SSOT...
    for ec in ("PROPOSE_TIMEOUT", "PROPOSE_API_ERROR", "PROPOSE_RATE_LIMIT",
               "HARNESS_EXCEPTION", "HARNESS_ERROR"):
        assert is_harness_fault(ec), ec
        assert ec in HARNESS_FAULT_EXIT_CLASSES
    # ...and ordinary AGENT outcomes are NOT (so they keep being scored as agent results).
    for ec in ("completed", "analysis_error", "runtime_error", "max_iterations"):
        assert not is_harness_fault(ec), ec


def test_quarantine_does_not_relabel_as_max_iterations_and_preserves_reason():
    row = _Row("PROPOSE_TIMEOUT", iterations=3)
    hf.quarantine_row(row, "PROPOSE_TIMEOUT")
    assert row.exit_class == "HARNESS_ERROR"            # quarantine bucket
    assert row.exit_class != "max_iterations"           # NEVER an agent cap
    assert row.harness_fault_reason == "PROPOSE_TIMEOUT"  # specific reason preserved
    assert row.iterations == 3                           # accounting left intact
    assert "EXCLUDED from H1-H4" in row.notes


# ---------------------------------------------------------------------------
# 2. retry-once -> quarantine -> continue
# ---------------------------------------------------------------------------
def _runner(seq):
    """A run_fn returning the next exit_class from `seq` on each call."""
    calls = {"n": 0}

    def run_fn():
        ec = seq[min(calls["n"], len(seq) - 1)]
        calls["n"] += 1
        return _Row(ec)

    return run_fn, calls


def test_clean_cell_never_retries():
    run_fn, calls = _runner(["completed"])
    slept = []
    row, reason = hf.process_cell(run_fn, sleep=slept.append, cleanup=lambda: slept.append("cleanup"))
    assert reason is None and row.exit_class == "completed"
    assert calls["n"] == 1 and slept == []          # no retry, no sleep, no cleanup


def test_transient_fault_recovers_on_single_retry():
    run_fn, calls = _runner(["PROPOSE_API_ERROR", "completed"])
    slept, cleaned = [], []
    row, reason = hf.process_cell(run_fn, sleep=slept.append, cleanup=lambda: cleaned.append(1))
    assert reason is None                            # recovered -> not quarantined, not counted
    assert row.exit_class == "completed"
    assert calls["n"] == 2                           # exactly ONE retry
    assert slept == [hf.HARNESS_FAULT_RETRY_DELAY_S]  # waited the named delay once
    assert cleaned == [1]                            # hard-reset before the retry


def test_retry_path_is_not_silent_announces_fault_and_retry():
    """The BLOCKER fix: the retry path must NOT be silent. It announces (a) the cell, (b)
    the specific reason/exit_class, and (c) that it is retrying once after Ns -- BEFORE the
    sleep -- then announces the resolution."""
    run_fn, _ = _runner(["PROPOSE_TIMEOUT", "completed"])   # fault then recover
    logs, order = [], []

    def _sleep(s):
        order.append(("sleep", len(logs)))   # record how many logs preceded the sleep

    row, reason = hf.process_cell(run_fn, sleep=_sleep, cleanup=lambda: None, log=logs.append)
    assert reason is None and row.exit_class == "completed"      # recovered
    # the fault+retry line was emitted BEFORE the (mocked) sleep...
    assert order and order[0][1] >= 1, "no log emitted before the retry sleep"
    first = logs[0]
    assert "HARNESS FAULT" in first
    assert "p2_cdc__B1__seed7" in first                          # (a) the cell
    assert "PROPOSE_TIMEOUT" in first                            # (b) the specific reason
    assert "retrying ONCE after" in first and "5s" in first      # (c) retry-after-Ns
    # ...and the resolution is announced too.
    assert any("RECOVERED on retry" in m for m in logs)


def test_retry_path_logs_default_to_stderr(capsys):
    """With no `log` injected the announcements still surface -- on stderr, in the
    runner's `[runner] ...` style."""
    run_fn, _ = _runner(["HARNESS_EXCEPTION", "completed"])
    hf.process_cell(run_fn, sleep=lambda s: None)
    err = capsys.readouterr().err
    assert "[runner] HARNESS FAULT" in err and "retrying ONCE" in err


def test_quarantine_path_announces_persisted_fault():
    run_fn, _ = _runner(["HARNESS_EXCEPTION", "HARNESS_EXCEPTION"])
    logs = []
    row, reason = hf.process_cell(run_fn, sleep=lambda s: None, log=logs.append)
    assert reason == "HARNESS_EXCEPTION"
    assert any("persisted on retry" in m and "QUARANTIN" in m.upper() for m in logs)


def test_persistent_fault_is_quarantined_and_continues():
    run_fn, calls = _runner(["HARNESS_EXCEPTION", "HARNESS_EXCEPTION", "completed"])
    slept, cleaned = [], []
    row, reason = hf.process_cell(run_fn, sleep=slept.append, cleanup=lambda: cleaned.append(1))
    assert calls["n"] == 2                           # retried ONCE, not more
    assert reason == "HARNESS_EXCEPTION"             # quarantined with the underlying reason
    assert row.exit_class == "HARNESS_ERROR"
    assert row.harness_fault_reason == "HARNESS_EXCEPTION"
    assert len(cleaned) == 2                         # cleanup after BOTH faults (no cascade)


# ---------------------------------------------------------------------------
# 3. circuit breaker -- each threshold independently, both fault paths
# ---------------------------------------------------------------------------
def test_breaker_global_threshold_covers_both_fault_paths():
    t = hf.HarnessFaultTracker()
    # spread across DIFFERENT arms + bins so ONLY the global limit (>3) can trip, and mix
    # propose-path and SDP/infra reasons so the breaker provably covers BOTH together.
    feed = [("a1", 1, "low", "PROPOSE_RATE_LIMIT"), ("a2", 2, "medium", "SDP_SPEC_MISSING"),
            ("a3", 3, "high", "PROPOSE_TIMEOUT"), ("a4", 4, "low", "HARNESS_EXCEPTION")]
    for i, (arm, task, cbin, reason) in enumerate(feed):
        t.record_quarantine(f"t{task}", 1, arm, reason, cbin)
        if i < 3:                                    # 1..3 quarantines: not yet > 3
            t.check_breaker()
    assert t.global_count == 4
    with pytest.raises(hf.CircuitBreakerTripped) as ei:   # the 4th (> 3) trips it
        t.check_breaker()
    assert "global harness-fault count 4 > 3" in str(ei.value)


def test_breaker_per_arm_threshold():
    t = hf.HarnessFaultTracker()
    # two faults in the SAME arm but DIFFERENT bins -> only the per-arm limit (>1) trips.
    t.record_quarantine("t1", 1, "B2", "PROPOSE_TIMEOUT", "low")
    t.check_breaker()                                # 1 in arm B2: ok
    t.record_quarantine("t2", 1, "B2", "SDP_MATERIALIZATION_MISSING", "high")
    with pytest.raises(hf.CircuitBreakerTripped) as ei:
        t.check_breaker()
    assert "per-arm" in str(ei.value) and "B2" in str(ei.value)


def test_breaker_per_bin_threshold():
    t = hf.HarnessFaultTracker()
    # two faults in the SAME complexity bin but DIFFERENT arms -> only the per-bin limit trips.
    t.record_quarantine("t1", 1, "A", "PROPOSE_API_ERROR", "high")
    t.check_breaker()                                # 1 in bin high: ok
    t.record_quarantine("t2", 1, "B", "HARNESS_EXCEPTION", "high")
    with pytest.raises(hf.CircuitBreakerTripped) as ei:
        t.check_breaker()
    assert "per-complexity-bin" in str(ei.value) and "high" in str(ei.value)


def test_sweep_wiring_quarantines_each_cell_then_breaker_aborts_the_run():
    """End-to-end sweep wiring (mirrors runner.main): process_cell -> record_quarantine ->
    check_breaker BEFORE the next cell. A run of persistently-faulting cells across DISTINCT
    arms/bins, mixing BOTH fault paths, quarantines each then ABORTS once the GLOBAL limit
    (>3) is breached -- proving the breaker covers both paths together."""
    t = hf.HarnessFaultTracker()
    # 5 cells; distinct arms + bins so ONLY the global>3 limit can trip; mix propose-path
    # (PROPOSE_*) and SDP/infra (HARNESS_EXCEPTION) reasons.
    cells = [("a1", "low", "PROPOSE_TIMEOUT"), ("a2", "medium", "HARNESS_EXCEPTION"),
             ("a3", "high", "PROPOSE_RATE_LIMIT"), ("a4", "low", "HARNESS_EXCEPTION"),
             ("a5", "medium", "PROPOSE_API_ERROR")]
    processed, aborted_before = 0, None
    for i, (arm, cbin, reason) in enumerate(cells):
        try:
            t.check_breaker()                        # gate BEFORE starting this cell
        except hf.CircuitBreakerTripped:
            aborted_before = i
            break
        run_fn, _ = _runner([reason, reason])        # persistent fault -> quarantined
        row, qreason = hf.process_cell(run_fn, sleep=lambda s: None)
        assert qreason == reason and row.exit_class == "HARNESS_ERROR"
        t.record_quarantine(f"t{i}", 1, arm, qreason, cbin)
        processed += 1
    # 4 cells quarantined (global hits 4 > 3), then the 5th cell is aborted before running.
    assert processed == 4 and aborted_before == 4
    assert t.global_count == 4


def test_breaker_constants_are_the_documented_values():
    # the three NAMED, tunable thresholds (Part B.3).
    assert hf.GLOBAL_HARNESS_FAULT_LIMIT == 3
    assert hf.PER_ARM_HARNESS_FAULT_LIMIT == 1
    assert hf.PER_BIN_HARNESS_FAULT_LIMIT == 1


# ---------------------------------------------------------------------------
# 4. complexity bin + quarantine report fields
# ---------------------------------------------------------------------------
def test_complexity_bin_derivation_and_explicit_override():
    assert hf.task_complexity_bin({"defects_in_scope": ["D1"]}) == "low"           # <=3
    assert hf.task_complexity_bin({"defects_in_scope": ["D1", "D2", "D3", "D4"]}) == "medium"
    assert hf.task_complexity_bin({"defects_in_scope": list("ABCDE")}) == "high"   # >=5
    # an explicit corpus field is honored verbatim (forward-compatible)
    assert hf.task_complexity_bin({"complexity": "high", "defects_in_scope": ["D1"]}) == "high"


def test_quarantine_report_has_required_fields():
    from harness import runner
    t = hf.HarnessFaultTracker()
    t.record_quarantine("p2_cdc", 7, "B1", "SDP_SPEC_MISSING", "medium")
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "r.quarantine.json")
        rep = runner.write_quarantine_report(path, t, "local", clock=1750000000.0)
        assert os.path.isfile(path)
    assert rep["n_quarantined"] == 1
    cell = rep["cells"][0]
    for k in ("task", "seed", "arm", "exit_class", "reason"):
        assert k in cell, k
    assert cell == {"task": "p2_cdc", "seed": 7, "arm": "B1",
                    "exit_class": "HARNESS_ERROR", "reason": "SDP_SPEC_MISSING",
                    "complexity_bin": "medium"}
    # the tunable breaker constants travel with the report for reproducibility
    bc = rep["breaker_constants"]
    assert bc["global_harness_fault_limit"] == 3
    assert bc["per_arm_harness_fault_limit"] == 1
    assert bc["per_bin_harness_fault_limit"] == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

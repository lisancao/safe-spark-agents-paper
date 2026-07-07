"""Part B.5: the analysis layer EXCLUDES quarantined HARNESS_ERROR cells from H1-H4 and
emits a separate quarantine report (the paper's excluded-data appendix) with the right
fields. A broken-instrument row must never move an agent statistic."""
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)
sys.path.insert(0, os.path.join(STUDY, "analysis"))

import analyze  # noqa: E402


def _row(task, seed, arm, exit_class, silent, **extra):
    d = {"task": task, "seed": seed, "arm": arm, "exit_class": exit_class,
         "silent_defect": silent, "reached_correct": True,
         "executor_seconds_to_correct": 1.0, "usd": 0.01,
         "failing_iterations": 0, "dry_run_intercepts": 0}
    d.update(extra)
    return d


def _write(rows, tmp):
    p = os.path.join(tmp, "results.jsonl")
    with open(p, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    return p


def test_quarantined_rows_excluded_from_H1_and_reported():
    rows = [
        # A: 2 clean cells, one silent
        _row("t1", 1, "A", "completed", True),
        _row("t1", 2, "A", "completed", False),
        # B: 2 clean cells, none silent
        _row("t1", 1, "B", "completed", False),
        _row("t1", 2, "B", "completed", False),
        # a QUARANTINED instrument failure in arm B -- must NOT count in B's rate
        _row("t1", 3, "B", "HARNESS_ERROR", False, harness_fault_reason="SDP_SPEC_MISSING"),
        # a raw propose-fault row (legacy / un-quarantined) is ALSO excluded
        _row("t2", 1, "A", "PROPOSE_TIMEOUT", False),
    ]
    with tempfile.TemporaryDirectory() as tmp:
        # backend is required for H2 metric selection after the #40 merge (build_report
        # fails loud on an unresolvable backend); this test exercises the quarantine
        # filter, so any KNOWN backend suffices.
        rep = analyze.build_report(_write(rows, tmp), arms_meta=None, backend="local")

    # H1 denominators EXCLUDE both fault rows: A has 2 analyzed cells, B has 2.
    assert rep["H1_per_arm"]["A"]["n"] == 2, rep["H1_per_arm"]["A"]
    assert rep["H1_per_arm"]["B"]["n"] == 2, rep["H1_per_arm"]["B"]
    assert rep["H1_per_arm"]["A"]["silent_defect_rate"] == 0.5
    assert rep["H1_per_arm"]["B"]["silent_defect_rate"] == 0.0   # the HARNESS_ERROR row didn't dilute it
    assert rep["meta"]["n_rows"] == 4 and rep["meta"]["n_rows_total"] == 6
    assert rep["meta"]["quarantine_excluded_from_H1_H4"] is True

    # the quarantine report carries the required fields for the appendix.
    q = rep["quarantine"]
    assert q["n_quarantined"] == 2
    by_reason = q["by_reason"]
    assert by_reason.get("SDP_SPEC_MISSING") == 1          # HARNESS_ERROR -> underlying reason
    assert by_reason.get("PROPOSE_TIMEOUT") == 1           # raw fault -> exit_class as reason
    fields = {"task", "seed", "arm", "exit_class", "reason"}
    for cell in q["cells"]:
        assert fields <= set(cell), cell
    # the markdown renders an excluded-data appendix
    md = analyze.render_markdown(rep)
    assert "Quarantine" in md and "EXCLUDED from H1" in md


def test_quarantine_out_cli_writes_report():
    rows = [_row("t1", 1, "A", "completed", False),
            _row("t1", 1, "B", "HARNESS_ERROR", False, harness_fault_reason="PROPOSE_RATE_LIMIT")]
    with tempfile.TemporaryDirectory() as tmp:
        results = _write(rows, tmp)
        qout = os.path.join(tmp, "q.json")
        # --assume-backend supplies the H2 backend the CLI now requires (post-#40 merge);
        # this test only asserts the quarantine-out report, so any KNOWN backend suffices.
        analyze.main([results, "--quarantine-out", qout, "--assume-backend", "local"])
        with open(qout) as f:
            q = json.load(f)
    assert q["n_quarantined"] == 1
    assert q["cells"][0]["reason"] == "PROPOSE_RATE_LIMIT"


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-q"]))

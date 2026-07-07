"""H2 cross-arm comparability on the LOCAL backend (DEVIATIONS D-7 follow-up 4).

The registered H2 primary `executor_seconds_to_correct` is the MEASURED stage-diff
executor-seconds. On the LOCAL substrate it is NOT cross-arm comparable:

  * the imperative `LocalSparkExecutor` snapshots executor-seconds BEFORE its
    SparkSession exists, so its measured value collapses to None; and
  * even when non-None, imperative measures via the Spark UI /executors
    totalDuration delta while local SDP measures via the Connect stage-diff.

So on LOCAL backends the analysis pairs H2 on `executor_seconds_wallclock_to_correct`
-- the UNIFORM `wall_s * instances * busy_fraction` proxy computed by an IDENTICAL
formula for every arm (harness/cost.py). These tests prove:

  (1) the uniform proxy is populated and non-None for BOTH an imperative-style and
      a declarative-style local run, and is computed by the SAME formula;
  (2) the analysis no longer drops every local H2 pair -- `_h2_pairs` returns the
      expected non-empty pairing on the proxy where the measured field would lose it;
  (3) for a remote/Connect run the MEASURED field is still used (cluster path intact).

No LLM, no Spark, no network: the rows are built through the REAL cost model
(harness/cost.py) so the wall-clock proxy genuinely comes from the shared formula.
"""
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

from harness import cost as costmod          # noqa: E402
from harness.schema import ResultRow, validate_row  # noqa: E402
from analysis import analyze                 # noqa: E402

# Declared cluster shape (instances=4 drives the wall-clock proxy: wall_s * 4).
CFG = costmod.ExecutorConfig(4, 4, 16.0, 0.192, "local", "local[*]")


def _run_cost(wall_s: float, measured_executor_seconds, failed: bool = False):
    """One green EXECUTE iteration through the REAL cost model -> RunCost.

    `measured_executor_seconds=None` reproduces the LOCAL imperative path (the
    snapshot collapses to None); a number reproduces a path that DID measure
    stage-diff executor-seconds. Either way `executor_seconds_wallclock_to_correct`
    is derived by the SAME `wall_s * instances * busy_fraction` formula.
    """
    it = costmod.execute_iteration_cost(
        wall_s=wall_s, cfg=CFG, failed=failed,
        measured_executor_seconds=measured_executor_seconds,
    )
    return costmod.aggregate([it], green_iter_index=0, completed=True)


def _row(task: str, arm: str, seed: int, rc: costmod.RunCost) -> dict:
    row = ResultRow(
        run_id=f"{task}__{arm}__seed{seed}",
        task=task, arm=arm, seed=seed,
        spark_version="4.1.0.dev4", image_digest="sha256:TEST", git_sha="TEST",
        base_model_id="claude-sonnet-4-6", executor_config=CFG.to_dict(),
        silent_defect=False, defect_classes=[], detection_stage="n/a",
        iterations=1, wall_s=rc.total_wall_s,
        executor_seconds=rc.total_executor_seconds, usd=rc.total_usd,
        exit_class="completed", task_success=True, reached_correct=rc.reached_correct,
        iterations_to_green=1, wall_s_to_green=rc.wall_s_to_green,
        executor_seconds_to_correct=rc.executor_seconds_to_correct,
        cpu_seconds=rc.total_cpu_seconds, cpu_seconds_to_correct=rc.cpu_seconds_to_correct,
        executor_seconds_wallclock=rc.total_executor_seconds_wallclock,
        executor_seconds_wallclock_to_correct=rc.executor_seconds_wallclock_to_correct,
        dry_run_intercepts=0, failing_iterations=0,
        backend="anthropic", timestamp_utc="2026-06-26T00:00:00Z",
        notes="TEST FIXTURE -- not real data",
    )
    d = json.loads(row.to_json())
    assert validate_row(d) == [], d            # rows are schema-valid
    return d


def _local_rows():
    """Two matched (task,seed) cells, LOCAL substrate.

    Arm A  = imperative execute-only  -> measured executor-seconds = None (the bug).
    Arm B  = declarative gated        -> measured executor-seconds present (stage-diff),
                                          but on a DIFFERENT mechanism than A's.
    Both carry the uniform wall-clock proxy.
    """
    rows = []
    for task, seed, wall_a, wall_b, meas_b in (
        ("p1_medallion", 42, 100.0, 80.0, 50.0),
        ("p2_cdc", 1337, 120.0, 70.0, 40.0),
    ):
        rows.append(_row(task, "A", seed, _run_cost(wall_a, measured_executor_seconds=None)))
        rows.append(_row(task, "B", seed, _run_cost(wall_b, measured_executor_seconds=meas_b)))
    return rows


def _remote_rows():
    """Remote/Connect: BOTH arms measured the same way -> measured field is comparable."""
    rows = []
    for task, seed, wall_a, meas_a, wall_b, meas_b in (
        ("p1_medallion", 42, 100.0, 90.0, 80.0, 50.0),
        ("p2_cdc", 1337, 120.0, 110.0, 70.0, 40.0),
    ):
        rows.append(_row(task, "A", seed, _run_cost(wall_a, measured_executor_seconds=meas_a)))
        rows.append(_row(task, "B", seed, _run_cost(wall_b, measured_executor_seconds=meas_b)))
    return rows


def _write(rows):
    fd, path = tempfile.mkstemp(suffix=".jsonl")
    with os.fdopen(fd, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    return path


# ---------------------------------------------------------------------------
# (1) the uniform proxy is populated + non-None for BOTH arm styles, same formula
# ---------------------------------------------------------------------------
def test_local_proxy_populated_for_both_arm_styles_same_formula():
    rows = _local_rows()
    imperative = [r for r in rows if r["arm"] == "A"]
    declarative = [r for r in rows if r["arm"] == "B"]
    assert imperative and declarative

    for r in imperative:
        # the measured primary collapses to None on the imperative local path ...
        assert r["executor_seconds_to_correct"] is None, r
    for r in declarative:
        assert r["executor_seconds_to_correct"] is not None, r

    # ... but the UNIFORM wall-clock proxy is non-None for BOTH and is the SAME
    # formula: wall_s * instances * busy_fraction (busy_fraction defaults to 1.0).
    for r in rows:
        proxy = r["executor_seconds_wallclock_to_correct"]
        assert proxy is not None, r
        assert proxy == r["wall_s"] * CFG.instances, r


def test_resolve_h2_metric_local_picks_wallclock_proxy():
    field, meta = analyze.resolve_h2_metric("local", idx={}, gated=["B"])
    assert field == analyze.H2_METRIC_WALLCLOCK
    assert meta["field"] == analyze.H2_METRIC_WALLCLOCK
    assert "local" in meta["selection"]


# ---------------------------------------------------------------------------
# (2) regression: the analysis no longer drops every local H2 pair
# ---------------------------------------------------------------------------
def test_local_pairs_not_dropped_on_proxy():
    idx = analyze.cell_index(_local_rows())

    # OLD behaviour: pairing on the measured field loses EVERY local pair, because
    # arm A's measured executor_seconds_to_correct is None.
    measured = analyze._h2_pairs(idx, "B", complete_case=False,
                                 metric_field=analyze.H2_METRIC_MEASURED)
    assert measured == [], "regression guard: measured field should drop all local pairs"

    # NEW behaviour: pairing on the uniform proxy recovers both matched cells.
    proxy = analyze._h2_pairs(idx, "B", complete_case=False,
                              metric_field=analyze.H2_METRIC_WALLCLOCK)
    assert len(proxy) == 2, proxy
    for a_cost, g_cost, _ua, _ug in proxy:
        assert a_cost is not None and g_cost is not None


def test_build_report_local_backend_uses_proxy_and_pairs():
    path = _write(_local_rows())
    try:
        rep = analyze.build_report(path, arms_meta=None, backend="local")
    finally:
        os.unlink(path)
    hm = rep["meta"]["h2_metric"]
    assert hm["field"] == analyze.H2_METRIC_WALLCLOCK
    assert rep["meta"]["backend"] == "local"
    b = rep["H2_compute_to_correct"]["B"]
    assert b["metric_field"] == analyze.H2_METRIC_WALLCLOCK
    assert b["itt"]["n_pairs"] == 2, b["itt"]
    # A spends MORE wall-clock than B on the local proxy -> positive exec-s saved.
    assert b["itt"]["mean_exec_s_saved"] > 0, b["itt"]


def test_assume_backend_local_opt_in_uses_proxy():
    """Explicit no-env opt-in: --assume-backend local picks the proxy (the only
    sanctioned way to analyze without an env sidecar)."""
    path = _write(_local_rows())
    try:
        rep = analyze.build_report(path, arms_meta=None, backend="local")
    finally:
        os.unlink(path)
    assert rep["meta"]["h2_metric"]["field"] == analyze.H2_METRIC_WALLCLOCK
    assert rep["H2_compute_to_correct"]["B"]["itt"]["n_pairs"] == 2


# ---------------------------------------------------------------------------
# AUTO can no longer misclassify: unknown/None/unrecognized backend FAILS LOUD
# ---------------------------------------------------------------------------
def test_none_backend_local_rows_does_not_pick_measured():
    """(a) unknown/None backend + local-style rows WITH non-None measured pairs must
    NOT silently select the (non-comparable) measured field -- it raises.

    Both arms here carry a measured value (the worst case: imperative measured it on
    a DIFFERENT mechanism than SDP), so the old data-driven auto would have picked
    measured and emitted a non-comparable local H2. The new code must refuse.
    """
    rows = []
    for task, seed in (("p1_medallion", 42), ("p2_cdc", 1337)):
        rows.append(_row(task, "A", seed, _run_cost(100.0, measured_executor_seconds=88.0)))
        rows.append(_row(task, "B", seed, _run_cost(80.0, measured_executor_seconds=50.0)))
    idx = analyze.cell_index(rows)
    # precondition: measured pairs DO exist -> old auto would have chosen measured.
    assert analyze._h2_pairs(idx, "B", False, analyze.H2_METRIC_MEASURED), "need measured pairs"
    path = _write(rows)
    try:
        raised = False
        try:
            analyze.build_report(path, arms_meta=None, backend=None)
        except analyze.H2MetricSelectionError:
            raised = True
        assert raised, "None backend must fail loud, not infer measured from pair availability"
    finally:
        os.unlink(path)


def test_none_backend_live_rows_does_not_pick_proxy():
    """(b) unknown/None backend + live-style rows with MISSING measured pairs must
    NOT fall back to the wallclock proxy (the wrong primary) -- it raises."""
    rows = _remote_rows()
    # blank out the measured to-correct field so the old auto would have proxy-fallen.
    for r in rows:
        r["executor_seconds_to_correct"] = None
    path = _write(rows)
    try:
        raised = False
        try:
            analyze.build_report(path, arms_meta=None, backend=None)
        except analyze.H2MetricSelectionError:
            raised = True
        assert raised, "None backend must fail loud, not proxy-fall on missing measured pairs"
    finally:
        os.unlink(path)


def test_unrecognized_backend_string_raises():
    """(c) an unrecognized backend string raises rather than guessing."""
    raised = False
    try:
        analyze.resolve_h2_metric("replay", idx={}, gated=["B"])
    except analyze.H2MetricSelectionError:
        raised = True
    assert raised
    path = _write(_local_rows())
    try:
        raised = False
        try:
            analyze.build_report(path, arms_meta=None, backend="databricks")
        except analyze.H2MetricSelectionError:
            raised = True
        assert raised
    finally:
        os.unlink(path)


def test_cli_main_errors_clearly_without_resolvable_backend():
    """The CLI declines (exits non-zero) with an actionable message when no backend
    is resolvable -- it does NOT crash obscurely or emit a guessed H2."""
    path = _write(_local_rows())
    try:
        raised = False
        try:
            analyze.main([path])          # no --env, no --assume-backend
        except SystemExit as e:
            raised = True
            assert e.code != 0
        assert raised, "CLI must exit non-zero when the H2 backend is unresolvable"
    finally:
        os.unlink(path)


# ---------------------------------------------------------------------------
# (3) remote/Connect: the measured field is still used (cluster path intact)
# ---------------------------------------------------------------------------
def test_remote_backend_keeps_measured_field():
    path = _write(_remote_rows())
    try:
        rep = analyze.build_report(path, arms_meta=None, backend="live")
    finally:
        os.unlink(path)
    hm = rep["meta"]["h2_metric"]
    assert hm["field"] == analyze.H2_METRIC_MEASURED
    b = rep["H2_compute_to_correct"]["B"]
    assert b["metric_field"] == analyze.H2_METRIC_MEASURED
    assert b["itt"]["n_pairs"] == 2, b["itt"]


def test_resolve_h2_metric_remote_picks_measured():
    field, meta = analyze.resolve_h2_metric("live", idx={}, gated=["B"])
    assert field == analyze.H2_METRIC_MEASURED
    assert meta["field"] == analyze.H2_METRIC_MEASURED


def test_h2_api_requires_explicit_metric_field():
    """A direct caller that bypasses build_report must NOT be able to compute H2 on
    a defaulted/unstated metric (that would recreate the local drop/distort bug).
    Both the public _h2_pairs and h2_analysis require an explicit metric_field and
    raise on omission or None."""
    rows = _local_rows()
    idx = analyze.cell_index(rows)

    # omitted entirely -> TypeError (no default parameter)
    for raises_on_omit in (
        lambda: analyze._h2_pairs(idx, "B", False),          # type: ignore[call-arg]
        lambda: analyze.h2_analysis(idx, rows, None),        # type: ignore[call-arg]
    ):
        omitted = False
        try:
            raises_on_omit()
        except TypeError:
            omitted = True
        assert omitted, "omitting metric_field must raise (no silent default)"

    # explicit None -> H2MetricSelectionError (clear, actionable)
    for raises_on_none in (
        lambda: analyze._h2_pairs(idx, "B", False, None),
        lambda: analyze.h2_analysis(idx, rows, None, None),
    ):
        nonefail = False
        try:
            raises_on_none()
        except analyze.H2MetricSelectionError:
            nonefail = True
        assert nonefail, "metric_field=None must raise H2MetricSelectionError"


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
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return failed


if __name__ == "__main__":
    sys.exit(1 if _run_all() else 0)

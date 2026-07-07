"""D-5 stage-diff compute attribution — unit tests with MOCKED Spark REST JSON.

The H2 compute figure on the live Spark Connect substrate is a before/after STAGE
diff against the driver Spark UI REST, NOT a `/executors.totalDuration` delta (that
counter is idle-incrementing driver uptime on the ONE long-lived 'Spark Connect
server' app — it does not attribute per-run task compute). These tests cannot reach
the cluster, so they MOCK the REST JSON (`live._get_json`) and assert the method:

  (a) resolves & CACHES the app id from `/applications` -> [0]['id'];
  (b) snapshots stageIds before the run and, after, attributes ONLY the NEW stages
      (stageId not in the before-set) that reached status == 'COMPLETE';
  (c) computes executor_seconds = sum(executorRunTime)/1000 (ms->s) and
      cpu_seconds = sum(executorCpuTime)/1e9 (ns->s) over exactly those stages —
      excluding pre-existing stageIds (even if they completed during the window)
      and new-but-incomplete stages;
  (d) falls back GRACEFULLY to (None, None) when `spark_rest_url` is unset and when
      a REST call raises — never crashing the run;
  (e) threads BOTH measured metrics into the result row through run_cell.

Mirrors the live-validated probe: a subprocess `spark.range(80M).sum()` produced new
stages [60, 62] -> executor_seconds 1.246, cpu_seconds 0.878.
"""
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

from harness import cost as costmod                  # noqa: E402
from harness import runner                           # noqa: E402
from harness.arm_manifest import load_arms            # noqa: E402
from harness.backends import live                     # noqa: E402
from harness.backends.live import ConnectExecutor     # noqa: E402
from harness.backends.local import ScriptedBrain      # noqa: E402

REST = "http://localhost:18080"

APPS = [{"id": "spark-connect-server-abc", "name": "Spark Connect server"}]

# BEFORE the run: stage 1 already COMPLETE, stage 2 still ACTIVE.
BEFORE = [
    {"stageId": 1, "status": "COMPLETE", "executorRunTime": 9999, "executorCpuTime": 9_999_000},
    {"stageId": 2, "status": "ACTIVE", "executorRunTime": 0, "executorCpuTime": 0},
]
# AFTER: stage 2 (PRE-EXISTING id) completed during the window -> must be EXCLUDED;
# stage 60 + 62 are NEW and COMPLETE -> counted; stage 61 is NEW but ACTIVE -> excluded.
AFTER = [
    {"stageId": 1, "status": "COMPLETE", "executorRunTime": 9999, "executorCpuTime": 9_999_000},
    {"stageId": 2, "status": "COMPLETE", "executorRunTime": 500000, "executorCpuTime": 700_000_000_000},
    {"stageId": 60, "status": "COMPLETE", "executorRunTime": 1000, "executorCpuTime": 500_000_000},
    {"stageId": 61, "status": "ACTIVE", "executorRunTime": 777, "executorCpuTime": 777_000_000},
    {"stageId": 62, "status": "COMPLETE", "executorRunTime": 246, "executorCpuTime": 378_000_000},
]
# new+complete = {60, 62}: executor_seconds = (1000+246)/1000 = 1.246;
#                          cpu_seconds = (5e8 + 3.78e8)/1e9 = 0.878.
EXPECT_EXEC_S = 1.246
EXPECT_CPU_S = 0.878


class _FakeRest:
    """Stateful stand-in for live._get_json. Routes by URL; serves the /stages
    sequence (BEFORE, then AFTER) across the two snapshot calls. Optionally raises
    on a chosen /stages call to exercise the graceful-fallback path."""

    def __init__(self, apps=APPS, stages_seq=(BEFORE, AFTER), raise_on_app=False,
                 raise_on_stages_call=None):
        self.apps = apps
        self.stages_seq = list(stages_seq)
        self.raise_on_app = raise_on_app
        self.raise_on_stages_call = raise_on_stages_call
        self.app_calls = 0
        self.stages_calls = 0
        self.urls = []

    def __call__(self, url):
        self.urls.append(url)
        if url.endswith("/applications"):
            self.app_calls += 1
            if self.raise_on_app:
                raise OSError("connection refused")
            return self.apps
        if url.endswith("/stages"):
            i = self.stages_calls
            self.stages_calls += 1
            if self.raise_on_stages_call is not None and i == self.raise_on_stages_call:
                raise OSError("connection refused")
            return self.stages_seq[min(i, len(self.stages_seq) - 1)]
        raise AssertionError(f"unexpected REST url: {url}")


def _patched(fake):
    """Install a fake live._get_json; return a restore() callable."""
    orig = live._get_json
    live._get_json = fake
    return lambda: setattr(live, "_get_json", orig)


# ---------------------------------------------------------------------------
def test_stage_diff_selects_new_complete_and_computes_both():
    fake = _FakeRest()
    restore = _patched(fake)
    try:
        ex = ConnectExecutor("sc://x:1/", REST)
        before = ex._stage_ids_snapshot()
        assert before == {1, 2}, before
        exec_s, cpu_s = ex._stage_compute_since(before)
        assert abs(exec_s - EXPECT_EXEC_S) < 1e-9, exec_s
        assert abs(cpu_s - EXPECT_CPU_S) < 1e-9, cpu_s
    finally:
        restore()


def test_app_id_resolved_once_and_cached():
    fake = _FakeRest()
    restore = _patched(fake)
    try:
        ex = ConnectExecutor("sc://x:1/", REST)
        before = ex._stage_ids_snapshot()      # resolves app id (1st /applications)
        ex._stage_compute_since(before)         # must REUSE the cached app id
        assert ex._cached_app_id == "spark-connect-server-abc"
        assert fake.app_calls == 1, f"/applications hit {fake.app_calls}x (should cache to 1)"
        assert fake.stages_calls == 2, fake.stages_calls
    finally:
        restore()


def test_none_fallback_when_rest_url_unset():
    # No spark_rest_url -> short-circuits before any REST call (and never crashes).
    ex = ConnectExecutor("sc://x:1/", None)
    assert ex._app_id() is None
    assert ex._stage_ids_snapshot() is None
    assert ex._stage_compute_since(None) == (None, None)
    # even if a stray before-set is passed, no rest url -> (None, None)
    assert ex._stage_compute_since({1, 2}) == (None, None)


def test_none_fallback_when_applications_raises():
    fake = _FakeRest(raise_on_app=True)
    restore = _patched(fake)
    try:
        ex = ConnectExecutor("sc://x:1/", REST)
        assert ex._app_id() is None
        assert ex._stage_ids_snapshot() is None
        assert ex._stage_compute_since(None) == (None, None)
    finally:
        restore()


def test_none_fallback_when_after_stages_raises():
    # BEFORE snapshot succeeds (call 0); the AFTER /stages call (1) raises -> (None, None).
    fake = _FakeRest(raise_on_stages_call=1)
    restore = _patched(fake)
    try:
        ex = ConnectExecutor("sc://x:1/", REST)
        before = ex._stage_ids_snapshot()
        assert before == {1, 2}
        assert ex._stage_compute_since(before) == (None, None)
    finally:
        restore()


def test_none_before_set_yields_none_compute():
    # If the before-snapshot was unavailable (None), compute must be (None, None)
    # WITHOUT making any further REST call (distinguishes "no metrics" from "empty").
    fake = _FakeRest()
    restore = _patched(fake)
    try:
        ex = ConnectExecutor("sc://x:1/", REST)
        assert ex._stage_compute_since(None) == (None, None)
        assert fake.stages_calls == 0, "should not query /stages when before-set is None"
    finally:
        restore()


def test_legacy_totalduration_snapshot_removed():
    # Regression lock: the INVALID cumulative /executors.totalDuration method is gone.
    assert not hasattr(ConnectExecutor, "_executor_seconds_snapshot"), \
        "the invalid /executors.totalDuration snapshot must be removed (D-5)"


# ---------------------------------------------------------------------------
# end-to-end: BOTH metrics are threaded into the result row via run_cell
# ---------------------------------------------------------------------------
class _StubConnect(ConnectExecutor):
    """Real stage-diff measurement (over the mocked REST); the subprocess `_run` is
    stubbed to a deterministic success so run_cell completes with no cluster."""

    def __init__(self, rest_url, wall):
        super().__init__("sc://x:1/", rest_url, staging_base=None)
        self._spark = object()   # sentinel; read_table is never reached (no dataset)
        self._wall = wall

    def _run(self, argv, cwd, env_extra):
        return 0, "Run is COMPLETED", self._wall


# input is NOT a .py generator -> generate_dataset returns None -> no subprocess,
# empty dataset_path -> _build_profile skips the oracle (no real Spark needed).
SDP_TASK = {"id": "t_compute", "defects_in_scope": ["D8"], "input": "upstream.published_table",
            "output_contract": {"table": "gold_daily", "substrate": "orders"}}


def _cfg(instances):
    return runner.StudyConfig(
        base_model_id="claude-sonnet-4-6",
        task_prompt_path=os.path.join(STUDY, "prompts", "task_prompt.md"),
        executor_config=costmod.ExecutorConfig(instances, 4, 16.0, 0.192, "k8s", "m5.xlarge"),
        spark_remote="sc://x:1/", spark_rest_url=REST)


def test_run_cell_threads_executor_and_cpu_seconds_into_row():
    fake = _FakeRest()
    restore = _patched(fake)
    try:
        arm = load_arms(os.path.join(STUDY, "arms"))["B1"]  # SDP, no gate (no completion-check read)
        instances, wall = 4, 2.0

        def make_brain(task, a, seed):
            return ScriptedBrain([{"code": "@dp.table\ndef t(): return spark.read.text('x')\n",
                                   "command": "cmd"}])

        def make_executor(task, a, seed):
            return _StubConnect(REST, wall)

        with tempfile.TemporaryDirectory() as tmp:
            row = runner.run_cell(SDP_TASK, arm, 42, _cfg(instances), make_brain,
                                  make_executor, work_dir=tmp, clock=1750000000.0)

        assert row.exit_class == "completed", row.exit_class
        # (1) measured stage-diff executor-seconds is authoritative
        assert abs(row.executor_seconds - EXPECT_EXEC_S) < 1e-9, row.executor_seconds
        # (2) measured CPU-seconds is threaded alongside
        assert row.cpu_seconds is not None and abs(row.cpu_seconds - EXPECT_CPU_S) < 1e-9, row.cpu_seconds
        assert abs(row.cpu_seconds_to_correct - EXPECT_CPU_S) < 1e-9, row.cpu_seconds_to_correct
        # (3) the wall-clock x slots cross-check is carried as a THIRD field
        assert abs(row.executor_seconds_wallclock - wall * instances) < 1e-9, row.executor_seconds_wallclock
        assert abs(row.executor_seconds_wallclock_to_correct - wall * instances) < 1e-9
        # $ is priced on the measured (authoritative) executor-seconds, not the cross-check
        assert abs(row.usd - costmod.usd_from_executor_seconds(EXPECT_EXEC_S, _cfg(instances).executor_config)) < 1e-12
    finally:
        restore()


def test_run_cell_none_metrics_falls_back_to_wallclock():
    # No REST url -> stage-diff returns (None, None) -> the MEASURED surfaces are None
    # (not a misleading 0.0 or the wall estimate); the wall x slots cross-check carries
    # the figure and $ is priced on it.
    arm = load_arms(os.path.join(STUDY, "arms"))["B1"]
    instances, wall = 4, 3.0

    def make_brain(task, a, seed):
        return ScriptedBrain([{"code": "@dp.table\ndef t(): return spark.read.text('x')\n",
                               "command": "cmd"}])

    def make_executor(task, a, seed):
        return _StubConnect(None, wall)   # spark_rest_url=None -> graceful fallback

    cfg = runner.StudyConfig(
        base_model_id="claude-sonnet-4-6",
        task_prompt_path=os.path.join(STUDY, "prompts", "task_prompt.md"),
        executor_config=costmod.ExecutorConfig(instances, 4, 16.0, 0.192, "k8s", "m5.xlarge"),
        spark_remote="sc://x:1/", spark_rest_url=None)

    with tempfile.TemporaryDirectory() as tmp:
        row = runner.run_cell(SDP_TASK, arm, 42, cfg, make_brain, make_executor,
                              work_dir=tmp, clock=1750000000.0)
    assert row.exit_class == "completed", row.exit_class
    # MEASURED surfaces are None (unmeasured), NOT the wall estimate or 0.0
    assert row.executor_seconds is None, row.executor_seconds
    assert row.executor_seconds_to_correct is None, row.executor_seconds_to_correct
    assert row.cpu_seconds is None, row.cpu_seconds
    assert row.cpu_seconds_to_correct is None, row.cpu_seconds_to_correct
    # the always-present cross-check carries the wall x slots figure, and $ uses it
    assert abs(row.executor_seconds_wallclock - wall * instances) < 1e-9
    assert abs(row.usd - costmod.usd_from_executor_seconds(wall * instances, cfg.executor_config)) < 1e-12


def test_gated_arm_unmeasured_compute_is_none_not_zero():
    """BLOCKING regression (cross-review of #20): on a GATED arm, an execute whose
    stage-diff falls back to (None, None) PLUS a driver-only dry-run gate iteration
    must aggregate the MEASURED surfaces to None — NOT 0.0. The gate runs no executors
    (nothing to measure -> None), so its zero must not masquerade as 'measured zero'
    for the arms H2 compares. Earlier fallback tests used B1 (no gate) and missed this."""
    arm = load_arms(os.path.join(STUDY, "arms"))["B"]   # SDP + dry-run gate
    assert arm.dry_run_gate, "test requires a gated arm"
    instances, wall = 4, 2.0

    class _GatedStub(_StubConnect):
        """rest_url=None -> execute stage-diff falls back to (None, None); the gate
        FAILS once (recording a driver-only dry-run iteration) then passes."""
        def __init__(self):
            super().__init__(None, wall)
            self._gate_calls = 0

        def run_gate(self, proposal, arm, state):
            from harness.backends.base import GateOutcome
            self._gate_calls += 1
            if self._gate_calls == 1:
                return GateOutcome(failed=True, wall_s=8.0, error_class="SOME_ANALYSIS_ERR",
                                   log="gate: rejected")
            return GateOutcome(failed=False, wall_s=8.0, log="gate: ok")

    def make_brain(task, a, seed):
        return ScriptedBrain([{"code": "@dp.table\ndef t(): return spark.read.text('x')\n",
                               "command": "cmd"}])

    def make_executor(task, a, seed):
        return _GatedStub()

    cfg = runner.StudyConfig(
        base_model_id="claude-sonnet-4-6",
        task_prompt_path=os.path.join(STUDY, "prompts", "task_prompt.md"),
        executor_config=costmod.ExecutorConfig(instances, 4, 16.0, 0.192, "k8s", "m5.xlarge"),
        spark_remote="sc://x:1/", spark_rest_url=None)

    with tempfile.TemporaryDirectory() as tmp:
        row = runner.run_cell(SDP_TASK, arm, 42, cfg, make_brain, make_executor,
                              work_dir=tmp, clock=1750000000.0)

    assert row.exit_class == "completed", row.exit_class
    # the gate fired once and intercepted a failure ($0), then execute completed
    assert row.dry_run_intercepts == 1, row.dry_run_intercepts
    assert row.failing_iterations == 1, row.failing_iterations
    # THE FIX: measured surfaces are None, NOT 0.0, for both executor- and CPU-seconds
    assert row.executor_seconds is None, f"executor_seconds={row.executor_seconds!r} (should be None, not 0.0)"
    assert row.executor_seconds_to_correct is None, row.executor_seconds_to_correct
    assert row.cpu_seconds is None, f"cpu_seconds={row.cpu_seconds!r} (should be None, not 0.0)"
    assert row.cpu_seconds_to_correct is None, row.cpu_seconds_to_correct
    # the wall-clock cross-check is the EXECUTE's wall x slots (gate adds 0.0) -> positive
    assert abs(row.executor_seconds_wallclock - wall * instances) < 1e-9, row.executor_seconds_wallclock


def _run_all():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t(); print(f"PASS {t.__name__}")
        except AssertionError as e:
            failed += 1; print(f"FAIL {t.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            import traceback; failed += 1
            print(f"ERROR {t.__name__}: {type(e).__name__}: {e}"); traceback.print_exc()
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return failed


if __name__ == "__main__":
    sys.exit(1 if _run_all() else 0)

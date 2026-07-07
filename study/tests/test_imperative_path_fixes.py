"""Regression tests for the two LOCAL IMPERATIVE instrument bugs (Part-1 calibration).

The SDP arms were re-validated separately; these two defects only degenerated the
LOCAL IMPERATIVE execution path:

  BUG 1  runner.py local-imperative gate compared `arm.paradigm == "imperative"`, a
         value `paradigm` NEVER takes (imperative arms carry "imperative_pyspark"),
         so the path/parquet escape-hatch branch was DEAD: the contract output path
         the oracle reconciles against was never set for imperative arms. The fix
         matches the EXACT manifest value "imperative_pyspark" (+ local executor).

  BUG 2  LocalSparkExecutor.stop() stopped the SparkSession but left the process-
         global py4j gateway (and its JVM subprocess + static state) alive, so the
         NEXT imperative cell re-attached to the same JVM and inherited leaked state
         (SPARK-2243). stop() now tears the gateway down -> each cell gets a fresh JVM.

The SDP arms (B/B1) and remote Connect imperative are untouched: the gate returns ""
for them, AND the JVM teardown is provably scoped so it never harms the SDP/Connect
path (see the two test_*connect* tests below).
"""
import os
import sys
from types import SimpleNamespace

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

from harness import runner                                    # noqa: E402
from harness.arm_manifest import VALID_PARADIGMS, load_arms   # noqa: E402

ARMS = load_arms(os.path.join(STUDY, "arms"))
_LOCAL = SimpleNamespace(name="local_spark")      # the in-process classic executor
_REMOTE = SimpleNamespace(name="connect")         # remote/Connect executor (not local)
# A2 is a valid arm id (schema enum) outside the shipped arms/ dir; it is imperative.
_A2 = SimpleNamespace(arm_id="A2", paradigm="imperative_pyspark")


# ---------------------------------------------------------------------------
# BUG 1 -- the local-imperative gate is no longer dead
# ---------------------------------------------------------------------------
def test_dead_gate_literal_never_matched_real_paradigm_values():
    """Document the root cause: the literal "imperative" the old gate tested is NOT a
    value `paradigm` ever takes, so the old condition was unconditionally False for
    every imperative arm. The only valid values are sdp / imperative_pyspark."""
    assert "imperative" not in VALID_PARADIGMS
    assert set(VALID_PARADIGMS) == {"sdp", "imperative_pyspark"}
    # B2 dropped: withdrawn to arms/supplementary per paper §6.1 (2026-06-29).
    for arm_id in ("A",):
        assert ARMS[arm_id].paradigm == "imperative_pyspark"
        assert ARMS[arm_id].paradigm != "imperative", "old gate literal would never match"


def test_local_imperative_gate_fires_for_imperative_arms():
    """The branch is LIVE: local imperative arms (A, A2, B2 -- paradigm
    'imperative_pyspark') get the contract-named parquet escape-hatch path -- the SAME
    path the agent is told (AGENT_OUTPUT_PATH) and the oracle reads back -- so a local
    imperative cell can no longer fail its completion check with a spurious
    OUTPUT_PATH_NOT_FOUND."""
    ws, out_table = "/work/ws", "gold_daily"
    # B2 dropped (withdrawn to arms/supplementary per paper §6.1, 2026-06-29); the
    # synthetic _A2 namespace still exercises the generic imperative_pyspark gate path.
    for arm in (ARMS["A"], _A2):
        path = runner.local_imperative_output_path(arm, _LOCAL, ws, out_table)
        assert path == os.path.join(ws, "gold_daily.parquet"), (arm.arm_id, path)
        assert path != "", f"dead branch: gate did not fire for imperative arm {arm.arm_id}"


def test_gate_stays_off_for_sdp_and_remote_imperative():
    """Fence: the escape hatch must NOT fire for SDP arms (B/B1) -- they keep their
    table contract byte-for-byte -- nor for a remote (non-local) imperative executor."""
    ws, out_table = "/work/ws", "gold_daily"
    for arm_id in ("B", "B1"):
        assert runner.local_imperative_output_path(ARMS[arm_id], _LOCAL, ws, out_table) == ""
    # imperative arm but REMOTE executor -> still off (only LOCAL imperative qualifies)
    assert runner.local_imperative_output_path(ARMS["A"], _REMOTE, ws, out_table) == ""
    assert runner.local_imperative_output_path(_A2, _REMOTE, ws, out_table) == ""


def test_gate_uses_exact_imperative_pyspark_value_not_just_not_sdp():
    """Intent guard (cross-review non-blocking note): the gate matches the EXACT value
    "imperative_pyspark", so a hypothetical FUTURE paradigm that is neither sdp nor
    classic imperative is NOT silently swept into the local path/parquet escape hatch."""
    future = SimpleNamespace(arm_id="X", paradigm="imperative_pandas")
    assert runner.local_imperative_output_path(future, _LOCAL, "/work/ws", "t") == ""


# ---------------------------------------------------------------------------
# BUG 2 -- consecutive imperative cells get a FRESH JVM (no static-state leak)
# ---------------------------------------------------------------------------
def _jvm_pid_and_marker(ex):
    """(jvm process id, value of our leak-probe system property) from ex's session."""
    sc = ex.spark.sparkContext
    pid = sc._jvm.java.lang.management.ManagementFactory.getRuntimeMXBean().getName()
    marker = sc._jvm.java.lang.System.getProperty("safe.agent.leakmarker")
    return pid.split("@", 1)[0], marker


def test_no_jvm_state_leak_between_consecutive_imperative_cells(tmp_path):
    """SPARK-2243: two consecutive LOCAL imperative cells must run in DIFFERENT JVM
    subprocesses, so static JVM state set in cell 1 cannot leak into cell 2.

    Proof: cell 1 records its JVM pid and plants a Java system property, then stop()s
    (the per-cell teardown the runner calls in run_cell's finally). Cell 2 -- a fresh
    LocalSparkExecutor, mirroring make_local_factories which builds one per cell --
    must see a NEW jvm pid AND must NOT see cell 1's planted property."""
    pytest.importorskip("pyspark", reason="pyspark not importable")

    from harness.backends.local import LocalSparkExecutor

    # ---- cell 1 -------------------------------------------------------------
    ex1 = LocalSparkExecutor(out_table="t", warehouse_dir=str(tmp_path / "wh1"), ui_port=4092)
    pid1, _ = _jvm_pid_and_marker(ex1)
    ex1.spark.sparkContext._jvm.java.lang.System.setProperty("safe.agent.leakmarker", "CELL1")
    _, planted = _jvm_pid_and_marker(ex1)
    assert planted == "CELL1", "could not plant the in-JVM leak probe"
    ex1.stop()   # per-cell isolation boundary -> must recycle the JVM

    # ---- cell 2 (a brand-new executor, as the runner builds per cell) -------
    ex2 = LocalSparkExecutor(out_table="t", warehouse_dir=str(tmp_path / "wh2"), ui_port=4093)
    try:
        pid2, leaked = _jvm_pid_and_marker(ex2)
        assert pid2 != pid1, (
            f"SPARK-2243 not fixed: cell 2 reused cell 1's JVM (pid {pid1}); "
            "static state can leak across imperative cells")
        assert leaked is None, (
            f"SPARK-2243 not fixed: cell 1's system property leaked into cell 2 "
            f"(got {leaked!r}); the JVM was not recycled")
        # cell 2 is fully functional on its fresh JVM
        assert ex2.spark.range(5).count() == 5
    finally:
        ex2.stop()


def test_stop_recycles_gateway_so_next_session_is_fresh(tmp_path):
    """Unit-level guard for the BUG 2 fix mechanism: after stop(), the process-global
    classic py4j gateway is cleared, so the next getOrCreate must launch a new JVM
    rather than re-attach to the dead one."""
    pytest.importorskip("pyspark", reason="pyspark not importable")
    from pyspark import SparkContext
    from harness.backends.local import LocalSparkExecutor

    ex = LocalSparkExecutor(out_table="t", warehouse_dir=str(tmp_path / "wh"), ui_port=4094)
    _ = ex.spark                                  # force JVM launch
    assert SparkContext._gateway is not None
    ex.stop()
    assert SparkContext._gateway is None, "stop() left the py4j gateway alive (JVM not recycled)"
    assert ex._spark is None


# ---------------------------------------------------------------------------
# BLOCKER 1 -- the JVM teardown must NOT harm the validated SDP / Spark Connect path
# ---------------------------------------------------------------------------
def test_active_connect_session_present_classifies_sessions(monkeypatch):
    """The Connect-detection the teardown's scope guard relies on: a remote/Connect
    session is recognized, a classic (non-remote) session is not, and 'no session' is
    not. (Force getActiveSession -> None so only the injected singletons drive it.)"""
    pytest.importorskip("pyspark", reason="pyspark not importable")
    from pyspark.sql import SparkSession
    from harness.backends import local

    monkeypatch.setattr(SparkSession, "getActiveSession", staticmethod(lambda: None))
    monkeypatch.setattr(SparkSession, "_activeSession", None, raising=False)

    monkeypatch.setattr(SparkSession, "_instantiatedSession",
                        SimpleNamespace(is_remote=True), raising=False)
    assert local._active_connect_session_present(SparkSession) is True

    monkeypatch.setattr(SparkSession, "_instantiatedSession",
                        SimpleNamespace(is_remote=False), raising=False)
    assert local._active_connect_session_present(SparkSession) is False

    monkeypatch.setattr(SparkSession, "_instantiatedSession", None, raising=False)
    assert local._active_connect_session_present(SparkSession) is False


def test_teardown_bails_out_while_a_connect_session_is_active(monkeypatch):
    """SCOPE proof (no server needed): while a Spark Connect session is live in this
    process, _teardown_local_jvm must clear NOTHING -- gateway, session singletons, and
    PYSPARK_GATEWAY_* env all survive -- so an imperative cell's stop() can never sever
    a Connect client. (Connect's classic-vs-Connect mode is process-global.)"""
    pytest.importorskip("pyspark", reason="pyspark not importable")
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    from harness.backends import local

    fake_connect = SimpleNamespace(is_remote=True)          # detector -> remote
    sentinel_gw = object()                                  # must SURVIVE the teardown

    saved = {
        "gw": getattr(SparkContext, "_gateway", None),
        "active": getattr(SparkSession, "_activeSession", None),
        "port": os.environ.get("PYSPARK_GATEWAY_PORT"),
    }
    try:
        SparkContext._gateway = sentinel_gw
        monkeypatch.setattr(SparkSession, "getActiveSession",
                            staticmethod(lambda: fake_connect))
        SparkSession._activeSession = fake_connect
        os.environ["PYSPARK_GATEWAY_PORT"] = "SENTINEL_PORT"

        assert local._active_connect_session_present(SparkSession) is True
        local._teardown_local_jvm()          # must be a NO-OP under a live Connect session

        assert SparkContext._gateway is sentinel_gw, \
            "teardown cleared the gateway despite a live Connect session"
        assert os.environ.get("PYSPARK_GATEWAY_PORT") == "SENTINEL_PORT", \
            "teardown popped PYSPARK_GATEWAY_* despite a live Connect session"
    finally:
        SparkContext._gateway = saved["gw"]
        SparkSession._activeSession = saved["active"]
        if saved["port"] is None:
            os.environ.pop("PYSPARK_GATEWAY_PORT", None)
        else:
            os.environ["PYSPARK_GATEWAY_PORT"] = saved["port"]


def test_sdp_connect_path_survives_classic_imperative_stop(tmp_path):
    """BLOCKER 1 empirical proof, end to end: an SDP/Connect operation routed EXACTLY as
    the harness routes it (a SUBPROCESS Connect helper against a REAL local Spark Connect
    server) still SUCCEEDS after a classic LocalSparkExecutor.stop() recycles the
    imperative JVM in the SAME process. This is the concrete demonstration that the
    SPARK-2243 teardown does not regress the already-validated SDP arms.

    Skipped (not failed) if a local Connect server cannot boot in this environment."""
    pytest.importorskip("pyspark", reason="pyspark not importable")
    from harness.backends.local import LocalSparkExecutor
    from harness.backends.local_connect import LocalConnectServer
    from harness.backends import local_connect as lc

    server = LocalConnectServer(port=15077, ui_port=4140,
                                warehouse_dir=str(tmp_path / "connect_wh"),
                                wait_secs=90, log_file=str(tmp_path / "connect.log"))
    try:
        try:
            # start() boots the server subprocess AND runs ensure_schema in a Connect
            # helper subprocess -- a first proof the Connect path works at boot.
            server.start(ensure_database="default")
        except Exception as e:  # noqa: BLE001
            pytest.skip(f"local Spark Connect server did not boot in this env: {e}")

        def connect_op(tag):
            return lc._run_connect_helper(
                ["ensure-schema", "--remote", server.remote,
                 "--catalog", "spark_catalog", "--database", "default"])

        before = connect_op("before")
        assert before.returncode == 0, \
            f"connect helper failed BEFORE the classic stop: {before.stdout}\n{before.stderr}"

        # a classic imperative cell runs and stop()s IN THE SAME PROCESS -> JVM recycle
        # (clears the classic py4j gateway and pops PYSPARK_GATEWAY_* from os.environ).
        ex = LocalSparkExecutor(out_table="t", warehouse_dir=str(tmp_path / "imp_wh"),
                                ui_port=4141)
        assert ex.spark.range(3).count() == 3
        ex.stop()

        # the SAME subprocess Connect op STILL succeeds -> the SDP/Connect path is intact
        after = connect_op("after")
        assert after.returncode == 0, (
            "SDP/Connect path BROKE after a classic imperative stop() recycled the JVM "
            f"(rc={after.returncode}):\n{after.stdout}\n{after.stderr}")
    finally:
        server.stop()

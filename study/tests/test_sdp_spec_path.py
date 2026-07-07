"""Part A.1-A.4 — the SDP doubled-path root-cause fix and the imperative output contract.

  * A.1/A.2: the SDP CLI `--spec` is now an ABSOLUTE path and `_run` gets an ABSOLUTE cwd,
    so the relative-spec-vs-workspace-cwd DOUBLED path (the
    PIPELINE_SPEC_FILE_DOES_NOT_EXIST that failed every SDP iteration before the agent code
    ran) cannot recur. A pre-invoke isfile guard raises a HARNESS fault, not an agent
    failure, if the spec is still missing.
  * A.3: a non-empty materialization that does NOT produce the required SDP files raises a
    HARNESS fault (instrument failure), never a misattributed agent failure.
  * A.4: the imperative LOCAL output target the agent is TOLD == what the completion check
    and the oracle READ (path-based parquet; no catalog/Hive plumbing failure).

The doubled-path test is DETERMINISTIC (a resolving stub replicates exactly how the CLI
resolves `--spec` against its cwd): it FAILS under the old relative-spec construction and
PASSES under the fix. An opt-in real-Spark integration test (SSA_RUN_SDP_INTEGRATION=1)
additionally executes a real SDP pipeline end to end.
"""
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

import pytest                                                          # noqa: E402

from harness import runner                                            # noqa: E402
from harness.arm_manifest import load_arms                            # noqa: E402
from harness.backends.base import HarnessFault, LoopState, Proposal   # noqa: E402
from harness.backends.live import ConnectExecutor                     # noqa: E402

ARMS = load_arms(os.path.join(STUDY, "arms"))
SDP_CODE = "from pyspark import pipelines as dp\n@dp.table\ndef t():\n    return spark.read.text('x')\n"


class _ResolvingConnect(ConnectExecutor):
    """A ConnectExecutor whose `_run` REPLICATES the SDP CLI's spec resolution: it resolves
    the `--spec` value against the given cwd exactly as the real subprocess would, then
    'executes' the pipeline only if that resolves to an existing file (else the real
    PIPELINE_SPEC_FILE_DOES_NOT_EXIST). No cluster needed; sensitive to the relative-vs-
    absolute fix."""

    def __init__(self):
        super().__init__("sc://x:1/", None, staging_base=None)
        self.last_spec = None
        self.last_resolved_isfile = None

    def _run(self, argv, cwd, env_extra):
        spec = argv[argv.index("--spec") + 1]
        self.last_spec = spec
        resolved = spec if os.path.isabs(spec) else os.path.join(cwd, spec)
        self.last_resolved_isfile = os.path.isfile(resolved)
        if not self.last_resolved_isfile:
            return 1, f"[PIPELINE_SPEC_FILE_DOES_NOT_EXIST] could not load {resolved}", 0.1
        return 0, "Run is COMPLETED", 0.1


def _materialize_sdp(ws):
    runner.materialize_workspace(ws, SDP_CODE, "sdp", False, "t", "B1",
                                 dataset_path="ds", output_table="gold_daily")


# ---------------------------------------------------------------------------
# A.1/A.2 -- doubled path is gone; the pipeline actually executes
# ---------------------------------------------------------------------------
def test_sdp_spec_absolute_resolves_and_executes_relative_workspace():
    arm = ARMS["B1"]                                    # SDP, no gate
    with tempfile.TemporaryDirectory() as tmp:
        rel = "ws"
        absws = os.path.join(tmp, rel)
        os.makedirs(absws)
        _materialize_sdp(absws)
        old = os.getcwd()
        os.chdir(tmp)                                   # make the RELATIVE workspace meaningful
        try:
            st = LoopState(task="t", seed=1, workspace=rel, dataset_path="")
            ex = _ResolvingConnect()

            # BUG REPRODUCTION: the old relative `--spec` resolved against cwd=workspace
            # doubled the path and did NOT exist -> PIPELINE_SPEC_FILE_DOES_NOT_EXIST.
            buggy_spec = os.path.join(rel, "spark-pipeline.yml")       # relative --spec
            buggy_resolved = os.path.join(rel, buggy_spec)            # cwd == workspace
            assert not os.path.isfile(buggy_resolved), "doubled path should not exist"

            # FIX: the executor builds an ABSOLUTE spec that exists.
            spec = ex._sdp_spec_path(st)
            assert os.path.isabs(spec) and os.path.isfile(spec)

            # and run_execute drives the resolving CLI to COMPLETED (the pipeline ran).
            out = ex.run_execute(Proposal(0, SDP_CODE, "cmd"), arm, st)
            assert out.completed and not out.failed, out.log
            assert ex.last_resolved_isfile is True       # the CLI got a resolvable spec
            assert os.path.isabs(ex.last_spec)
        finally:
            os.chdir(old)


def test_sdp_gate_also_uses_absolute_spec_and_cwd():
    arm = ARMS["B"]                                     # SDP, WITH gate
    with tempfile.TemporaryDirectory() as tmp:
        absws = os.path.join(tmp, "ws")
        os.makedirs(absws)
        _materialize_sdp(absws)
        st = LoopState(task="t", seed=1, workspace=absws, dataset_path="")
        ex = _ResolvingConnect()
        gate = ex.run_gate(Proposal(0, SDP_CODE, "cmd"), arm, st)
        assert not gate.failed, gate.log
        assert os.path.isabs(ex.last_spec) and ex.last_resolved_isfile is True


# ---------------------------------------------------------------------------
# A.2 -- a missing spec is a HARNESS fault, not an agent failure
# ---------------------------------------------------------------------------
def test_require_sdp_spec_raises_harness_fault_when_missing():
    ex = _ResolvingConnect()
    with tempfile.TemporaryDirectory() as tmp:
        st = LoopState(task="t", seed=1, workspace=tmp, dataset_path="")  # nothing written
        with pytest.raises(HarnessFault) as ei:
            ex._require_sdp_spec(st)
    assert ei.value.reason == "SDP_SPEC_MISSING"
    assert ei.value.exit_class == "HARNESS_EXCEPTION"   # recognized as a harness fault


# ---------------------------------------------------------------------------
# A.3 -- post-materialization existence check is a HARNESS fault
# ---------------------------------------------------------------------------
def test_assert_materialized_sdp_missing_is_harness_fault():
    arm = ARMS["B1"]
    with tempfile.TemporaryDirectory() as tmp:
        st = LoopState(task="t", seed=1, workspace=tmp, dataset_path="")
        with pytest.raises(HarnessFault) as ei:
            runner._assert_materialized(st, arm, written=[])           # nothing on disk
    assert ei.value.reason == "SDP_MATERIALIZATION_MISSING"


def test_assert_materialized_passes_for_real_materialization():
    arm = ARMS["B1"]
    with tempfile.TemporaryDirectory() as tmp:
        _materialize_sdp(tmp)
        st = LoopState(task="t", seed=1, workspace=tmp, dataset_path="")
        runner._assert_materialized(st, arm, ["spark-pipeline.yml"])   # must NOT raise


def test_assert_materialized_imperative_missing_is_harness_fault():
    arm = ARMS["A"]                                     # imperative
    with tempfile.TemporaryDirectory() as tmp:
        st = LoopState(task="t", seed=1, workspace=tmp, dataset_path="")
        with pytest.raises(HarnessFault) as ei:
            runner._assert_materialized(st, arm, written=[])
    assert ei.value.reason == "MATERIALIZATION_MISSING"


# ---------------------------------------------------------------------------
# A unified-fault integration: a HarnessFault inside the loop becomes a harness-fault
# cell (NOT max_iterations) and is quarantined by the policy on a persistent fault.
# ---------------------------------------------------------------------------
class _FaultyExec:
    name = "connect"

    def reachable(self):
        return True

    def run_execute(self, proposal, arm, state):
        raise HarnessFault("simulated SDP infra break", reason="SDP_SPEC_MISSING")


def test_harness_fault_in_loop_is_not_scored_max_iterations_and_quarantines():
    from harness import harness_faults as hf
    from harness.schema import is_harness_fault
    arm = ARMS["B1"]                                    # SDP, no gate -> run_execute is called
    sdp_task = {"id": "t", "defects_in_scope": ["D8"], "input": "upstream.published_table",
                "output_contract": {"table": "gold_daily", "substrate": "orders"}}

    def make_brain(task, a, seed):
        from harness.backends.local import ScriptedBrain
        return ScriptedBrain([{"code": SDP_CODE, "command": "cmd"}])

    def make_executor(task, a, seed):
        return _FaultyExec()

    cfg = runner.StudyConfig(
        base_model_id="claude-sonnet-4-6",
        task_prompt_path=os.path.join(STUDY, "prompts", "task_prompt.md"),
        executor_config=runner.costmod.ExecutorConfig(4, 4, 16.0, 0.192, "k8s", "m5.xlarge"),
        spark_remote="sc://x:1/", spark_rest_url=None)

    with tempfile.TemporaryDirectory() as tmp:
        def run_fn():
            return runner._run_cell_safe(sdp_task, arm, 1, cfg, make_brain, make_executor,
                                         work_dir=tmp, clock=1750000000.0, backend="local")
        # the per-cell net classifies the HarnessFault as a HARNESS fault, NEVER as an
        # agent max_iterations outcome -- exit_class is the unified bucket, and the SPECIFIC
        # instrument-fault token survives structurally in harness_fault_reason.
        row0 = run_fn()
        assert is_harness_fault(row0.exit_class), row0.exit_class
        assert row0.exit_class != "max_iterations"
        assert row0.exit_class == "HARNESS_EXCEPTION"
        assert row0.harness_fault_reason == "SDP_SPEC_MISSING"

        # the policy: persistent -> quarantined HARNESS_ERROR (SPECIFIC reason preserved).
        row, reason = hf.process_cell(run_fn, sleep=lambda s: None)
        assert reason == "SDP_SPEC_MISSING"
        assert row.exit_class == "HARNESS_ERROR"
        assert row.harness_fault_reason == "SDP_SPEC_MISSING"


# ---------------------------------------------------------------------------
# A.4 -- imperative LOCAL output target: TOLD == CHECKED == GRADED (path-based)
# ---------------------------------------------------------------------------
def test_imperative_output_target_told_equals_checked_equals_graded():
    from harness.backends.local import LocalSparkExecutor
    from harness import output_oracles
    arm = ARMS["A"]                                     # imperative, no gate
    agent = (
        "import os\n"
        "from pyspark.sql import SparkSession\n"
        "spark = SparkSession.builder.getOrCreate()\n"
        "df = spark.read.json(os.environ['AGENT_INPUT_PATH'])\n"
        "df.write.mode('overwrite').parquet(os.environ['AGENT_OUTPUT_PATH'])\n"
        "print('Run is COMPLETED')\n")
    with tempfile.TemporaryDirectory() as tmp:
        ws = os.path.join(tmp, "ws")
        os.makedirs(ws)
        with open(os.path.join(ws, "pipeline.py"), "w") as f:
            f.write(agent)
        ds = os.path.join(tmp, "in.json")
        with open(ds, "w") as f:
            f.write('{"order_id":"1","amount":1.0}\n{"order_id":"2","amount":2.0}\n')
        ex = LocalSparkExecutor(out_table="gold_daily",
                                warehouse_dir=os.path.join(tmp, "wh"), ui_port=4071)
        st = LoopState(task="t", seed=1, workspace=ws, dataset_path=ds,
                       output_table="gold_daily",
                       output_path=os.path.join(ws, "gold_daily.parquet"))
        told_path = st.imperative_output_path
        try:
            out = ex.run_execute(Proposal(0, agent, "python"), arm, st)
            assert out.completed and not out.failed, out.log
            # CHECKED (completion read-back) target == the TOLD path:
            assert out.output_tables == [told_path]
            # GRADED: the oracle reads the SAME path the agent was told to write.
            prof = output_oracles.build_output_profile(
                None, ex.spark, ds, [], {"table": "gold_daily", "substrate": "orders"},
                read_path=ex.read_output_path, output_path=out.output_tables[0])
            assert prof.extra.get("output_path") == told_path
            assert "output_read_error" not in prof.extra, prof.extra
        finally:
            ex.stop()


# ---------------------------------------------------------------------------
# opt-in: REAL end-to-end SDP execution against a local Spark Connect server.
# ---------------------------------------------------------------------------
@pytest.mark.skipif(os.environ.get("SSA_RUN_SDP_INTEGRATION") != "1",
                    reason="set SSA_RUN_SDP_INTEGRATION=1 to run the real-Spark SDP pipeline")
def test_sdp_pipeline_executes_for_real_local_connect():  # pragma: no cover - heavy/opt-in
    from harness.backends.local_connect import LocalConnectServer, LocalConnectExecutor
    port = int(os.environ.get("SSA_SDP_PORT", "15099"))
    with tempfile.TemporaryDirectory() as tmp:
        wh = os.path.join(tmp, "wh")
        server = LocalConnectServer(port=port, ui_port=port + 1, warehouse_dir=wh,
                                    log_file=os.path.join(tmp, "connect.log"))
        server.start(ensure_catalog="spark_catalog", ensure_database="default")
        try:
            ws = os.path.join(tmp, "ws")
            os.makedirs(ws)
            code = ("from pyspark import pipelines as dp\n"
                    "from pyspark.sql import SparkSession\n"
                    "@dp.materialized_view\n"
                    "def gold_daily():\n"
                    "    spark = SparkSession.active()\n"
                    "    return spark.range(5)\n")
            storage = f"file://{os.path.join(wh, 'storage')}"
            runner.materialize_workspace(ws, code, "sdp", False, "t", "B1",
                                         dataset_path="ds", output_table="gold_daily",
                                         storage_uri=storage)
            ex = LocalConnectExecutor(server.remote, server.rest_url)
            st = LoopState(task="t", seed=1, workspace=ws, dataset_path="",
                           output_table="gold_daily", output_storage=storage,
                           sdp_catalog="spark_catalog", sdp_database="default")
            out = ex.run_execute(Proposal(0, code, "cmd"), ARMS["B1"], st)
            assert out.completed and not out.failed, out.log
            # read the materialized table back in a SUBPROCESS Connect session (Option C:
            # the runner process must not create an in-process Connect session).
            import subprocess
            chk = subprocess.run(
                [sys.executable, "-c",
                 "from pyspark.sql import SparkSession;"
                 f"s=SparkSession.builder.remote({server.remote!r}).getOrCreate();"
                 "print('COUNT=' + str(s.table('gold_daily').count()))"],
                capture_output=True, text=True, timeout=180)
            assert "COUNT=5" in chk.stdout, (chk.stdout, chk.stderr[-2000:])
        finally:
            server.stop()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

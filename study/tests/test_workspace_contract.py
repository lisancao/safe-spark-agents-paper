"""Per-arm WORKSPACE CONTRACT parity guard.

This test runs the runner's materialization step for EACH of the 4 arms and asserts
the EXACT set of files that arm's backend (harness/backends/live.py) will read now
exists and contains the AGENT'S code -- not a baked answer or harness scaffolding.

Mapping enforced (mirrors live.py ConnectExecutor):
  Arm A  imperative, no gate  -> {pipeline.py}                       execute: python3/spark-submit pipeline.py
  Arm B2 imperative + gate    -> {pipeline.py}                       gate: python3 pipeline.py --analyze-only
  Arm B  SDP + gate           -> {spark-pipeline.yml, transformations/pipeline.py}  gate: cli dry-run --spec
  Arm B1 SDP, no gate         -> {spark-pipeline.yml, transformations/pipeline.py}  execute: spark-pipelines run

IMPERATIVE arms (A, B2): the agent OWNS the program. `pipeline.py` is `proposal.code`
VERBATIM -- the harness no longer appends an `_imperative_main` SparkSession/write
or generates a separate `_analyze_only.py` session (validity confound removed; the
agent owns the session, the read/transform/path write, the COMPLETED signal, and the
`--analyze-only` mode). The B2 gate runs the agent's own program in
`--analyze-only` mode; there is no harness-created gate file.

No Spark needed -- this is a pure file-contract unit test. Real-cluster execution
of the SDP/gated paths remains the operator pilot step (no remote mTLS cluster
here); this guard proves all 4 arms are materialized correctly beforehand.
"""
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

from harness import runner                       # noqa: E402
from harness.arm_manifest import load_arms        # noqa: E402
from harness.backends.base import LoopState, Proposal  # noqa: E402

ARMS = load_arms(os.path.join(STUDY, "arms"))

# recognizable agent code per paradigm (the marker proves the agent's code -- not
# a harness/answer -- lands in the file the backend reads).
# The imperative agent now authors a runnable PROGRAM (it owns the session + I/O +
# the --analyze-only mode); the harness writes it VERBATIM.
IMPERATIVE_CODE = (
    "import os, sys  # AGENT_MARKER_IMPERATIVE\n"
    "from pyspark.sql import SparkSession\n"
    "def build(spark, input_path):\n"
    "    return spark.read.text(input_path)\n"
    "if __name__ == '__main__':\n"
    "    spark = SparkSession.builder.getOrCreate()\n"
    "    df = build(spark, os.environ.get('AGENT_INPUT_PATH', 'x'))\n"
    "    if '--analyze-only' in sys.argv:\n"
    "        df.schema; print('analyzed'); sys.exit(0)\n"
    "    df.write.mode('overwrite').parquet(os.environ['AGENT_OUTPUT_PATH'])\n"
    "    print('Run is COMPLETED')\n"
)
SDP_CODE = (
    "from pyspark import pipelines as dp  # AGENT_MARKER_SDP\n"
    "@dp.table\n"
    "def silver():\n"
    "    return spark.read.table('bronze')\n"
)

# B2 dropped from the contract matrix: withdrawn to arms/supplementary per paper
# §6.1 (2026-06-29). The active arms are {A, B, B1}.
EXPECTED = {
    "A":  {"files": {"pipeline.py"}, "code_in": "pipeline.py",
           "absent": {"_analyze_only.py", "spark-pipeline.yml", "transformations/pipeline.py"},
           "gate_reads": None, "execute_reads": "pipeline.py"},
    "B":  {"files": {"spark-pipeline.yml", "transformations/pipeline.py"},
           "code_in": "transformations/pipeline.py",
           "absent": {"pipeline.py", "_analyze_only.py"},
           "gate_reads": "spark-pipeline.yml", "execute_reads": "spark-pipeline.yml"},
    "B1": {"files": {"spark-pipeline.yml", "transformations/pipeline.py"},
           "code_in": "transformations/pipeline.py",
           "absent": {"pipeline.py", "_analyze_only.py"},
           "gate_reads": None, "execute_reads": "spark-pipeline.yml"},
}


def _materialize(arm, tmp):
    ws = os.path.join(tmp, f"ws_{arm.arm_id}")
    os.makedirs(ws, exist_ok=True)
    code = SDP_CODE if arm.paradigm == "sdp" else IMPERATIVE_CODE
    state = LoopState(task="orders_silver_gold", seed=42, workspace=ws,
                      dataset_path="/data/orders.ndjson", output_table="gold_daily",
                      output_path=("/tmp/gold_daily.parquet" if arm.paradigm == "imperative" else ""))
    written = runner._materialize_proposal(state, Proposal(0, code, "cmd"), arm)
    return ws, set(written), code


def test_each_arm_writes_its_full_contract():
    with tempfile.TemporaryDirectory() as tmp:
        for arm_id, exp in EXPECTED.items():
            arm = ARMS[arm_id]
            ws, written, code = _materialize(arm, tmp)
            # exact written set matches the backend's expectation
            assert written == exp["files"], f"arm {arm_id}: wrote {written}, expected {exp['files']}"
            for rel in exp["files"]:
                assert os.path.exists(os.path.join(ws, rel)), f"arm {arm_id}: missing {rel}"
            for rel in exp["absent"]:
                assert not os.path.exists(os.path.join(ws, rel)), f"arm {arm_id}: unexpected {rel}"
            # the AGENT'S code is in the file the backend reads as the pipeline
            marker = "AGENT_MARKER_SDP" if arm.paradigm == "sdp" else "AGENT_MARKER_IMPERATIVE"
            agent_file = open(os.path.join(ws, exp["code_in"])).read()
            assert marker in agent_file, f"arm {arm_id}: agent code not in {exp['code_in']}"


def test_gate_and_execute_inputs_exist_per_arm():
    """The specific file each arm's gate/execute opens (per live.py) must exist."""
    with tempfile.TemporaryDirectory() as tmp:
        for arm_id, exp in EXPECTED.items():
            arm = ARMS[arm_id]
            ws, _, _ = _materialize(arm, tmp)
            if exp["gate_reads"]:
                assert os.path.exists(os.path.join(ws, exp["gate_reads"])), \
                    f"arm {arm_id}: gate input {exp['gate_reads']} missing"
            assert os.path.exists(os.path.join(ws, exp["execute_reads"])), \
                f"arm {arm_id}: execute input {exp['execute_reads']} missing"


def test_sdp_spec_is_boilerplate_not_answer():
    """The generated SDP spec must NOT contain agent logic / answers -- only the
    agent transform file carries the agent's code (no leakage). SDP is unchanged."""
    with tempfile.TemporaryDirectory() as tmp:
        ws, _, _ = _materialize(ARMS["B"], tmp)
        spec = open(os.path.join(ws, "spark-pipeline.yml")).read()
        assert "AGENT_MARKER_SDP" not in spec, "agent code leaked into the SDP spec"
        assert "transformations/**" in spec and "storage:" in spec
        # the spec points at the transform that holds the agent code
        assert "AGENT_MARKER_SDP" in open(os.path.join(ws, "transformations", "pipeline.py")).read()


def test_sdp_spec_declares_catalog_and_database():
    """Regression for arm B PARSE_EMPTY_STATEMENT: the generated SDP spec MUST
    declare `catalog` + `database`. Omitting them made the Connect server receive
    empty catalog/database defaults and fail the SDP gate. `spark_catalog`/`default`
    are the proven-good values from the live Connect endpoint.

    It must ALSO emit NO `configuration:` block: pinning a session config (e.g.
    spark.sql.session.timeZone) would apply only to the SDP arms (B/B1), an
    asymmetric advantage that masks the UTC-normalization silent defect the oracle
    is designed to catch -- a scientific confound. Lock that removal here."""
    with tempfile.TemporaryDirectory() as tmp:
        for arm_id in ("B", "B1"):
            ws, _, _ = _materialize(ARMS[arm_id], tmp)
            spec = open(os.path.join(ws, "spark-pipeline.yml")).read()
            lines = spec.splitlines()
            assert any(ln.startswith("catalog:") for ln in lines), \
                f"arm {arm_id}: SDP spec has no `catalog:` line:\n{spec}"
            assert any(ln.startswith("database:") for ln in lines), \
                f"arm {arm_id}: SDP spec has no `database:` line:\n{spec}"
            assert "catalog: spark_catalog" in spec, f"arm {arm_id}: wrong catalog:\n{spec}"
            assert "database: default" in spec, f"arm {arm_id}: wrong database:\n{spec}"
            # the rest of the proven-good contract is intact
            assert "storage:" in spec and "transformations/**" in spec
            # NO session-config injection -> no asymmetric UTC advantage / defect masking
            assert "configuration:" not in spec, \
                f"arm {arm_id}: SDP spec injects a configuration block (confound):\n{spec}"
            assert "timeZone" not in spec and "session.timeZone" not in spec, \
                f"arm {arm_id}: SDP spec pins a session timezone (defect masking):\n{spec}"
    # the values are overridable from the study config (sdp_catalog/sdp_database)
    custom = runner._sdp_spec("t", "B", "/ws", "s3a://w/x",
                              catalog="my_cat", database="my_db")
    assert "catalog: my_cat" in custom and "database: my_db" in custom
    assert "configuration:" not in custom and "timeZone" not in custom


def test_sdp_catalog_database_threaded_from_config_to_spec():
    """Full wiring proof: StudyConfig.sdp_catalog/sdp_database -> LoopState ->
    materialize_workspace -> _sdp_spec, for BOTH SDP arms. Guards against a
    regression that hardcodes the leaf default and drops the threading."""
    with tempfile.TemporaryDirectory() as tmp:
        for arm_id in ("B", "B1"):
            arm = ARMS[arm_id]
            ws = os.path.join(tmp, f"thread_{arm_id}")
            os.makedirs(ws, exist_ok=True)
            # build the LoopState the runner would (catalog/database come from cfg)
            state = LoopState(task="orders_silver_gold", seed=42, workspace=ws,
                              dataset_path="/data/orders.ndjson", output_table="gold_daily",
                              sdp_catalog="my_cat", sdp_database="my_db")
            runner._materialize_proposal(state, Proposal(0, SDP_CODE, "cmd"), arm)
            spec = open(os.path.join(ws, "spark-pipeline.yml")).read()
            assert "catalog: my_cat" in spec, f"arm {arm_id}: catalog not threaded:\n{spec}"
            assert "database: my_db" in spec, f"arm {arm_id}: database not threaded:\n{spec}"


def test_sdp_execute_invokes_cli_run_with_spec():
    """Regression for PIPELINE_SPEC_FILE_NOT_FOUND + local-server bind (arm B1):
    the SDP execute path must invoke the Python CLI directly with `run --spec
    <spark-pipeline.yml>` (mirroring the working gate over SPARK_REMOTE), NEVER the
    bare `bin/spark-pipelines run` wrapper (which ignores SPARK_REMOTE and omits --spec)."""
    from harness.backends.live import ConnectExecutor
    from harness.backends.base import LoopState, Proposal
    with tempfile.TemporaryDirectory() as tmp:
        ws = os.path.join(tmp, "ws")
        os.makedirs(ws, exist_ok=True)
        # the spec must exist: the executor now defensively returns NO_CODE_PRODUCED
        # for a missing spark-pipeline.yml (empty-proposal guard). This test exercises
        # the argv CONSTRUCTION for a real proposal, so write the spec the agent would.
        with open(os.path.join(ws, "spark-pipeline.yml"), "w") as f:
            f.write("name: t__B1\nstorage: file:///x\ncatalog: spark_catalog\ndatabase: default\n")
        # pin SPARK_HOME so the test needs no pyspark and the cli path is deterministic
        spark_home = os.path.join(tmp, "spark_home")
        prev = os.environ.get("SPARK_HOME")
        os.environ["SPARK_HOME"] = spark_home
        try:
            ex = ConnectExecutor("sc://fake:1/", None)
            captured = {}

            def fake_run(argv, cwd, env_extra):
                captured["argv"] = argv
                return 0, "Run is COMPLETED", 1.0

            ex._run = fake_run  # type: ignore[assignment]
            state = LoopState(task="t", seed=42, workspace=ws, dataset_path="x")
            ex.run_execute(Proposal(0, "code", "cmd"), ARMS["B1"], state)
        finally:
            if prev is None:
                os.environ.pop("SPARK_HOME", None)
            else:
                os.environ["SPARK_HOME"] = prev

    argv = captured["argv"]
    # EXACT argv: direct python CLI form (mirrors the gate) over SPARK_REMOTE --
    # never the bare bin/spark-pipelines wrapper, never a missing --spec.
    expected = ["python3", os.path.join(spark_home, "pipelines", "cli.py"),
                "run", "--spec", os.path.join(ws, "spark-pipeline.yml")]
    assert argv == expected, f"SDP execute argv\n  got     {argv}\n  expected {expected}"


def test_arm_A_unchanged():
    """Arm A still produces exactly pipeline.py holding the agent's program."""
    with tempfile.TemporaryDirectory() as tmp:
        ws, written, _ = _materialize(ARMS["A"], tmp)
        assert written == {"pipeline.py"}
        body = open(os.path.join(ws, "pipeline.py")).read()
        assert "def build(" in body and "AGENT_MARKER_IMPERATIVE" in body


def test_imperative_pipeline_is_agent_code_verbatim():
    """Agent-owned (D-4): imperative pipeline.py is `proposal.code` VERBATIM -- the
    harness injects NO SparkSession, no materialize-main, no analyze-only harness,
    so an imperative failure is attributable to the agent's own code."""
    with tempfile.TemporaryDirectory() as tmp:
        # B2 dropped: withdrawn to arms/supplementary per paper §6.1 (2026-06-29).
        for arm_id in ("A",):
            ws, _, code = _materialize(ARMS[arm_id], tmp)
            body = open(os.path.join(ws, "pipeline.py")).read()
            assert body == code, f"arm {arm_id}: harness modified the agent's imperative code"
            # no harness-injected scaffolding tokens
            assert "_imperative_main" not in body
            assert "_SS.builder" not in body
            # the agent (not the harness) owns the session + path write + COMPLETED signal
            assert "SparkSession.builder.getOrCreate()" in body
            assert "AGENT_OUTPUT_PATH" in body
            assert "saveAsTable" not in body
            assert "AGENT_OUTPUT_TABLE" not in body
            assert "Run is COMPLETED" in body
            # the B2 gate file is NOT created by the harness anymore
            assert not os.path.exists(os.path.join(ws, "_analyze_only.py"))




def test_sdp_user_message_does_not_leak_local_imperative_output_contract():
    """Negative guard for B1: SDP (B/B1) prompts must not mention the local
    imperative AGENT_OUTPUT_PATH/parquet contract, even if a future caller
    accidentally leaves state.output_path populated."""
    from harness.backends.live import AnthropicBrain
    brain = AnthropicBrain("claude-sonnet-4-6", "TASK")
    for arm_id in ("B", "B1"):
        state = LoopState(task="orders_silver_gold", seed=42, workspace="/ws",
                          dataset_path="/data/orders.ndjson", output_table="gold_daily",
                          output_path="/tmp/should_not_leak.parquet")
        msg = brain._user_message(state, ARMS[arm_id])
        system = brain._system_prompt(ARMS[arm_id])
        combined = system + "\n" + msg
        assert "AGENT_OUTPUT_PATH" not in combined
        assert "write the final GOLD DataFrame as parquet" not in combined
        assert "do NOT call saveAsTable" not in combined




def test_local_imperative_table_exposure_instruction_is_defect_neutral():
    """The local path mechanism may tell imperative agents HOW to expose required
    tables, but must not leak defect labels, table purpose, or example table names.
    """
    from harness.backends.live import AnthropicBrain
    brain = AnthropicBrain("claude-sonnet-4-6", "TASK")
    state = LoopState(task="orders_silver_gold", seed=42, workspace="/ws",
                      dataset_path="/data/orders.ndjson", output_table="gold_daily",
                      output_path="/tmp/gold_daily.parquet")
    msg = brain._user_message(state, ARMS["A"])
    assert "AGENT_OUTPUT_PATH=/tmp/gold_daily.parquet" in msg
    assert "createOrReplaceTempView" in msg
    for forbidden in ("D6", "dedup", "silver_orders", "clean_orders"):
        assert forbidden not in msg, f"local imperative prompt leaked {forbidden!r}: {msg}"

def test_remote_imperative_user_message_does_not_leak_path_contract_without_output_path():
    """Remote imperative keeps the original table-backed user contract: no local
    parquet instruction unless runner has positively selected LocalSparkExecutor."""
    from harness.backends.live import AnthropicBrain
    brain = AnthropicBrain("claude-sonnet-4-6", "TASK")
    state = LoopState(task="orders_silver_gold", seed=42, workspace="/ws",
                      dataset_path="s3a://bucket/orders", output_table="gold_daily",
                      output_path="")
    msg = brain._user_message(state, ARMS["A"])
    assert "AGENT_OUTPUT_PATH" not in msg
    assert "write the final GOLD DataFrame as parquet" not in msg



def test_table_backed_missing_required_secondary_output_fails_safe():
    """Table-backed SDP/remote profile path has the same fail-safe as local
    imperative: a missing required secondary table records required_output_read_error
    and runner marks the episode incomplete instead of scoring residual=0 clean.
    """
    from types import SimpleNamespace
    from harness import cost as costmod, output_oracles, runner as runner_mod

    class GoldDF:
        pass

    def read_table(name):
        if name == "gold_daily":
            return GoldDF()
        raise RuntimeError(f"missing table: {name}")

    contract = {"table": "gold_daily", "dedup_table": "required_secondary",
                "key_col": "order_id", "payload_cols": ["amount"], "substrate": "orders"}
    prof = output_oracles.build_output_profile(read_table, None, "unused", ["D6"], contract)
    assert "required_output_read_error" in prof.extra

    ep = runner_mod.EpisodeResult(
        completed=True, green_iter_index=0,
        iter_costs=[costmod.execute_iteration_cost(1.0, costmod.ExecutorConfig(1, 1, 1, 0, "local", "local"), False)],
        per_iteration=[{"execute": {"failed": False, "completed": True, "error_class": None}}],
        analysis_log="", runtime_log="",
        green_exec=SimpleNamespace(failed=False, completed=True, error_class=None, log="ok"),
        exit_class="completed")
    runner_mod._fail_incomplete_required_output(ep, prof)
    assert ep.completed is False
    assert ep.exit_class == "runtime_error"
    assert ep.per_iteration[-1]["execute"]["error_class"] == "REQUIRED_OUTPUT_TABLE_NOT_FOUND"

def test_harness_session_helpers_removed():
    """The harness no longer carries the imperative SparkSession scaffolding."""
    assert not hasattr(runner, "_imperative_main")
    assert not hasattr(runner, "_analyze_only_harness")


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

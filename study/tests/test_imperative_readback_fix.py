"""Regression test for the LOCAL IMPERATIVE read-back bug (Part-1 calibration).

BUG (imperative arms A/A2/B2 went 0/15, all max_iterations): the agent's generated
PySpark program calls `spark.stop()` internally -- idiomatic, nearly every real
pyspark script does -- and that session is SHARED with the executor (the agent's
`SparkSession.builder.getOrCreate()` returns the executor's active session). The
harness then tried to read the output parquet BACK through that now-DEAD session, so
the completion check raised and reported OUTPUT_PATH_NOT_FOUND even though the parquet
directory exists on disk. Every iteration "failed" -> the loop ran to max_iterations.

FIX: the executor's `spark` property detects a dead classic session and rebuilds a
FRESH, independent session it owns, so the output read-back/grading is decoupled from
the agent's session lifecycle. The output parquet is read from DISK regardless of the
agent calling `spark.stop()`.

These tests exercise the REAL in-process Spark path (skipped, not failed, when pyspark
is unavailable). They FAIL on the pre-fix code (the cached dead session raises) and
PASS after the fix.
"""
import os
import sys
from types import SimpleNamespace

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

from harness.backends.base import LoopState, Proposal   # noqa: E402

_ARM = SimpleNamespace(arm_id="A", paradigm="imperative_pyspark")  # run_execute ignores arm


# pipeline.py that materializes the parquet and then tears down its session, exactly
# as a real agent program does. `spark.stop()` on the shared session is what broke the
# pre-fix harness read-back.
_AGENT_PROGRAM_WITH_STOP = """\
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()      # returns the executor's active session
out = os.environ["AGENT_OUTPUT_PATH"]
df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "name"])
df.write.mode("overwrite").parquet(out)
spark.stop()                                     # idiomatic teardown -> kills the SHARED session
"""


def _executor(tmp_path, ui_port):
    from harness.backends.local import LocalSparkExecutor
    return LocalSparkExecutor(out_table="gold_daily",
                              warehouse_dir=str(tmp_path / "wh"), ui_port=ui_port)


def test_read_output_path_survives_agent_calling_spark_stop(tmp_path):
    """Unit-level reproduction: after the agent's session is stopped, the executor
    must still read the output parquet back from disk (via a revived session it owns).

    Pre-fix: `read_output_path` returns the cached DEAD session and `.read.parquet`
    raises -> the read-back is impossible. Post-fix: the property revives a fresh
    session and the on-disk parquet reads correctly."""
    pytest.importorskip("pyspark", reason="pyspark not importable")

    ex = _executor(tmp_path, ui_port=4150)
    out_path = str(tmp_path / "gold_daily.parquet")
    try:
        # The executor owns the session; write the output through it, then simulate the
        # agent tearing that SAME shared session down with a bare stop().
        ex.spark.createDataFrame([(1, "x"), (2, "y")], ["id", "name"]) \
            .write.mode("overwrite").parquet(out_path)
        ex.spark.stop()                       # agent's idiomatic teardown
        assert ex._spark is not None, "precondition: executor still caches the dead handle"

        # The read-back must NOT depend on that dead session -- it reads from DISK.
        df = ex.read_output_path(out_path)
        assert df.count() == 2
        assert set(r["name"] for r in df.collect()) == {"x", "y"}
    finally:
        ex.stop()


def test_run_execute_completes_when_agent_program_calls_spark_stop(tmp_path):
    """End-to-end reproduction through `run_execute`: an agent program that writes the
    parquet and then calls `spark.stop()` must still be graded as COMPLETED -- the exact
    path that made imperative arms go 0/15 (all OUTPUT_PATH_NOT_FOUND -> max_iterations).

    Pre-fix the completion check read through the dead shared session and returned
    error_class=OUTPUT_PATH_NOT_FOUND with completed=False; post-fix it reads the output
    from disk and reports completed=True."""
    pytest.importorskip("pyspark", reason="pyspark not importable")

    ws = tmp_path / "ws"
    ws.mkdir()
    (ws / "pipeline.py").write_text(_AGENT_PROGRAM_WITH_STOP)
    out_path = str(ws / "gold_daily.parquet")
    # a non-empty input path so AGENT_INPUT_PATH is set as in a real cell (unused here)
    dataset = str(tmp_path / "input.ndjson")
    open(dataset, "w").close()

    ex = _executor(tmp_path, ui_port=4151)
    state = LoopState(task="t", seed=0, workspace=str(ws), dataset_path=dataset,
                      output_table="gold_daily", output_path=out_path)
    proposal = Proposal(iteration=0, code=_AGENT_PROGRAM_WITH_STOP, command="python")
    try:
        outcome = ex.run_execute(proposal, _ARM, state)

        assert outcome.error_class != "OUTPUT_PATH_NOT_FOUND", (
            "read-back regressed: the agent's spark.stop() broke the harness output read "
            f"(log:\n{outcome.log})")
        assert outcome.completed is True, (
            f"run did not complete despite a valid on-disk parquet (error_class="
            f"{outcome.error_class!r}, log:\n{outcome.log})")
        assert outcome.failed is False
        assert outcome.output_tables == [out_path]

        # The output the harness read back is the parquet the agent actually wrote.
        df = ex.read_output_path(out_path)
        assert df.count() == 3
        assert os.path.isdir(out_path), "output parquet directory must exist on disk"
    finally:
        ex.stop()

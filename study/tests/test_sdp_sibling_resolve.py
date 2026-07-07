"""SDP dry-run gate: graph-aware sibling-windowing resolution (the harness fix).

Reproduces and locks the LOCAL-only fidelity bug where the SDP `dry-run` gate
eager-resolves a windowed/dedup read of a SIBLING pipeline dataset against the bare
session catalog -> [TABLE_OR_VIEW_NOT_FOUND] / 42P01, even though the code is correct
medallion SDP that real SDP/DLT accepts. The fix (harness/sdp_dryrun.py) pre-seeds
each pipeline dataset's analyzed schema before the authoritative dry-run, in an
ISOLATED scratch database, so the sibling resolves against the dataflow graph.

What is asserted, end to end on a REAL local Spark Connect server + the REAL SDP CLI
analysis path (via the harness's own LocalConnectServer + ConnectExecutor.run_gate):

  (a) a CORRECT windowed-sibling medallion pipeline PASSES the gate post-fix, batch
      AND streaming;
  (b) regression: the SAME pipeline FAILS the STOCK `cli.py dry-run` with
      TABLE_OR_VIEW_NOT_FOUND / 42P01 (the bug) and PASSES the harness gate (fixed);
  (c) FIDELITY -- the gate is NOT made more permissive than real SDP/DLT:
        * a genuinely MISSING upstream still fails 42P01;
        * a genuine UNRESOLVED COLUMN (incl. inside a windowed sibling read) still
          fails UNRESOLVED_COLUMN / 42703;
        * a STALE/external catalog object of the same name as a pipeline dataset does
          NOT rescue a defective pipeline (isolation -- BLOCKING-1 guard);
        * a graph-shape defect (dependency cycle) still fails and is NOT masked by a
          same-name seed temp view (BLOCKING-2 guard).

Needs pyspark[connect] + a JDK; SKIPS cleanly (real pytest.skip) otherwise, so an
unexercised fix shows as SKIP, not a silent PASS.
"""
import atexit
import os
import subprocess
import sys
import tempfile
from typing import Any, NoReturn

HERE = os.path.dirname(os.path.abspath(__file__))
STUDY = os.path.dirname(HERE)
sys.path.insert(0, STUDY)

_SERVER = {"obj": None, "remote": None, "started": False, "skip": None}


def _skip(msg) -> NoReturn:
    """Real skip under pytest (also when run standalone -- `_run_all` below treats the
    pytest Skipped exception as a skip, not an error). Never returns."""
    import pytest
    pytest.skip(msg)


def _pipeline_dir(tmp, slug, transform_src):
    """Write a minimal SDP project (spark-pipeline.yml + transformations/pipeline.py),
    plus a real NDJSON source the inline base reads (the `__SRC__` token is replaced
    with its file:// URI -- an explicit-schema `.json()` still requires the path to
    exist, exactly like the staged agent input the real gate sees)."""
    root = os.path.join(tmp, slug)
    os.makedirs(os.path.join(root, "transformations"), exist_ok=True)
    src = os.path.join(root, "orders.json")
    with open(src, "w") as f:
        f.write('{"order_id":"o1","amount":"10.0","event_time":"2024-01-01T00:00:00Z"}\n'
                '{"order_id":"o1","amount":"10.0","event_time":"2024-01-01T01:00:00Z"}\n'
                '{"order_id":"o2","amount":"20.0","event_time":"2024-01-02T00:00:00Z"}\n')
    storage = f"file://{root}/_storage"
    with open(os.path.join(root, "spark-pipeline.yml"), "w") as f:
        f.write(f"name: {slug}\nstorage: {storage}\ncatalog: spark_catalog\n"
                f"database: default\nlibraries:\n  - glob:\n      include: transformations/**\n")
    with open(os.path.join(root, "transformations", "pipeline.py"), "w") as f:
        f.write(transform_src.replace("__SRC__", f"file://{src}"))
    return root


# --- pipeline fixtures (read no data: explicit schema on the inline source) ---
_SRC_SCHEMA = "order_id STRING, amount STRING, event_time STRING"

# (a)/(b) CORRECT medallion: bronze -> silver (row_number dedup over read.table(bronze))
#         -> gold. The windowed SIBLING read is what detonates pre-fix.
CORRECT_WINDOWED_SIBLING = f'''
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F, Window
spark = SparkSession.active()

@dp.materialized_view(name="bronze")
def bronze():
    return spark.read.schema("{_SRC_SCHEMA}").json("__SRC__")

@dp.materialized_view(name="silver")
def silver():
    src = spark.read.table("bronze")
    w = Window.partitionBy("order_id").orderBy(F.col("event_time").desc())
    return src.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

@dp.materialized_view(name="gold")
def gold():
    return spark.read.table("silver").groupBy("order_id").agg(F.count("*").alias("n"))
'''

# (a) STREAMING form (the original sandbox-b case): silver applies withWatermark +
#     dropDuplicatesWithinWatermark over readStream.table(SIBLING). Rate source, no file.
STREAMING_WINDOWED_SIBLING = '''
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.active()

@dp.table(name="bronze")
def bronze():
    return (spark.readStream.format("rate").option("rowsPerSecond", 1).load()
            .select(F.col("value").alias("order_id"), F.col("timestamp").alias("event_time")))

@dp.table(name="silver")
def silver():
    src = spark.readStream.table("bronze")
    return (src.withWatermark("event_time", "10 minutes")
               .dropDuplicatesWithinWatermark(["order_id"]))
'''

# (c) MISSING upstream: silver windows a read of a table that is NOT a pipeline dataset.
MISSING_UPSTREAM = '''
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F, Window
spark = SparkSession.active()

@dp.materialized_view(name="silver")
def silver():
    src = spark.read.table("nonexistent_upstream")
    w = Window.partitionBy("order_id").orderBy(F.col("event_time").desc())
    return src.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
'''

# (c) UNRESOLVED COLUMN inside a windowed sibling read: bronze is a real dataset, but
#     silver orders the window by a column bronze does not have. Seeding bronze with its
#     REAL schema must NOT mask this -> still UNRESOLVED_COLUMN / 42703.
UNRESOLVED_COLUMN_WINDOWED = f'''
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F, Window
spark = SparkSession.active()

@dp.materialized_view(name="bronze")
def bronze():
    return spark.read.schema("{_SRC_SCHEMA}").json("__SRC__")

@dp.materialized_view(name="silver")
def silver():
    src = spark.read.table("bronze")
    w = Window.partitionBy("order_id").orderBy(F.col("does_not_exist").desc())
    return src.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
'''

# (c) STALE-TABLE MASKING: the pipeline bronze schema has NO `extra` column; silver
#     windows over read.table("bronze") ordering by `extra`. A STALE permanent
#     spark_catalog.default.bronze WITH `extra` is created BEFORE the gate. The gate
#     must resolve silver against the PIPELINE bronze (no `extra`) -> FAIL, never be
#     rescued by the stale table.
STALE_MASKING_SCHEMA = "order_id STRING, event_time STRING"  # NOTE: no `extra`
STALE_MASKING = f'''
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F, Window
spark = SparkSession.active()

@dp.materialized_view(name="bronze")
def bronze():
    return spark.read.schema("{STALE_MASKING_SCHEMA}").json("__SRC__")

@dp.materialized_view(name="silver")
def silver():
    src = spark.read.table("bronze")
    w = Window.partitionBy("order_id").orderBy(F.col("extra").desc())  # `extra` only in STALE
    return src.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
'''

# (c) QUALIFIED-READ stale masking: identical to STALE_MASKING but silver reads the
#     sibling FULLY-QUALIFIED as `spark_catalog.default.bronze`. A qualified read bypasses
#     the scratch current-database, so without the qualified-ref rewrite it would resolve
#     against the STALE `spark_catalog.default.bronze` (which has `extra`) and PASS. It
#     must be rewritten to the pipeline's own bronze (no `extra`) -> still FAIL.
QUALIFIED_STALE_MASKING = f'''
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F, Window
spark = SparkSession.active()

@dp.materialized_view(name="bronze")
def bronze():
    return spark.read.schema("{STALE_MASKING_SCHEMA}").json("__SRC__")

@dp.materialized_view(name="silver")
def silver():
    src = spark.read.table("spark_catalog.default.bronze")  # fully-qualified own sibling
    w = Window.partitionBy("order_id").orderBy(F.col("extra").desc())  # `extra` only in STALE
    return src.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
'''

# (a) POSITIVE qualified sibling: a CORRECT pipeline whose windowed sibling read is
#     fully-qualified to its OWN dataset must still PASS (no new false-fail).
QUALIFIED_SIBLING_OK = f'''
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F, Window
spark = SparkSession.active()

@dp.materialized_view(name="bronze")
def bronze():
    return spark.read.schema("{_SRC_SCHEMA}").json("__SRC__")

@dp.materialized_view(name="silver")
def silver():
    src = spark.read.table("spark_catalog.default.bronze")  # fully-qualified own sibling
    w = Window.partitionBy("order_id").orderBy(F.col("event_time").desc())
    return src.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

@dp.materialized_view(name="gold")
def gold():
    return spark.read.table("silver").groupBy("order_id").agg(F.count("*").alias("n"))
'''

# (c) CASE-VARIANT qualified stale masking: same as QUALIFIED_STALE_MASKING but the
#     qualifier/leaf use a DIFFERENT CASE (`SPARK_CATALOG.DEFAULT.BRONZE`). Spark resolves
#     unquoted identifiers case-insensitively, so this still references the pipeline's own
#     `bronze`; an exact-case classifier would miss it and let the stale table mask it.
CASE_VARIANT_STALE_MASKING = f'''
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F, Window
spark = SparkSession.active()

@dp.materialized_view(name="bronze")
def bronze():
    return spark.read.schema("{STALE_MASKING_SCHEMA}").json("__SRC__")

@dp.materialized_view(name="silver")
def silver():
    src = spark.read.table("SPARK_CATALOG.DEFAULT.BRONZE")  # case-variant own sibling
    w = Window.partitionBy("order_id").orderBy(F.col("extra").desc())  # `extra` only in STALE
    return src.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
'''

# (a) POSITIVE case-variant: a CORRECT pipeline reading its own sibling via a case-variant
#     qualified name still PASSES (the rewrite recognises it as internal).
CASE_VARIANT_SIBLING_OK = f'''
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F, Window
spark = SparkSession.active()

@dp.materialized_view(name="bronze")
def bronze():
    return spark.read.schema("{_SRC_SCHEMA}").json("__SRC__")

@dp.materialized_view(name="silver")
def silver():
    src = spark.read.table("spark_catalog.default.BRONZE")  # case-variant own sibling
    w = Window.partitionBy("order_id").orderBy(F.col("event_time").desc())
    return src.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

@dp.materialized_view(name="gold")
def gold():
    return spark.read.table("silver").groupBy("order_id").agg(F.count("*").alias("n"))
'''

# (c) WHITESPACE-variant qualified stale masking: leading/trailing whitespace around the
#     qualified identifier. Spark ignores it, so it still references the pipeline's bronze.
WHITESPACE_STALE_MASKING = f'''
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F, Window
spark = SparkSession.active()

@dp.materialized_view(name="bronze")
def bronze():
    return spark.read.schema("{STALE_MASKING_SCHEMA}").json("__SRC__")

@dp.materialized_view(name="silver")
def silver():
    src = spark.read.table(" SPARK_CATALOG.DEFAULT.BRONZE ")  # whitespace + case variant
    w = Window.partitionBy("order_id").orderBy(F.col("extra").desc())  # `extra` only in STALE
    return src.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
'''

# (c) SPACES-AROUND-DOTS qualified stale masking (Spark accepts this form).
SPACED_DOTS_STALE_MASKING = f'''
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F, Window
spark = SparkSession.active()

@dp.materialized_view(name="bronze")
def bronze():
    return spark.read.schema("{STALE_MASKING_SCHEMA}").json("__SRC__")

@dp.materialized_view(name="silver")
def silver():
    src = spark.read.table("spark_catalog . default . bronze")  # spaces around dots
    w = Window.partitionBy("order_id").orderBy(F.col("extra").desc())  # `extra` only in STALE
    return src.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
'''

# (a) POSITIVE whitespace-padded qualified sibling read must still PASS.
WHITESPACE_SIBLING_OK = f'''
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F, Window
spark = SparkSession.active()

@dp.materialized_view(name="bronze")
def bronze():
    return spark.read.schema("{_SRC_SCHEMA}").json("__SRC__")

@dp.materialized_view(name="silver")
def silver():
    src = spark.read.table(" spark_catalog.default.bronze ")  # whitespace-padded own sibling
    w = Window.partitionBy("order_id").orderBy(F.col("event_time").desc())
    return src.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

@dp.materialized_view(name="gold")
def gold():
    return spark.read.table("silver").groupBy("order_id").agg(F.count("*").alias("n"))
'''

# (c) GRAPH-SHAPE defect: a dependency cycle (bronze<->silver), both windowed. Invalid
#     in SDP. Both datasets are declined from seeding, so no same-name temp view can
#     mask the cycle -> the real dry-run must reject it.
CYCLE_SHAPE = '''
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F, Window
spark = SparkSession.active()

@dp.materialized_view(name="bronze")
def bronze():
    src = spark.read.table("silver")
    w = Window.partitionBy("order_id").orderBy(F.col("order_id").desc())
    return src.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)

@dp.materialized_view(name="silver")
def silver():
    src = spark.read.table("bronze")
    w = Window.partitionBy("order_id").orderBy(F.col("order_id").desc())
    return src.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
'''


def _ensure_server():
    """Lazily start ONE LocalConnectServer for this module; returns (remote, server)
    or None to signal SKIP (no pyspark/JDK / could not bind)."""
    if _SERVER["started"]:
        return None if _SERVER["skip"] else (_SERVER["remote"], _SERVER["obj"])
    _SERVER["started"] = True
    try:
        import pyspark  # noqa: F401  # pyspark[connect] required for the real gate
    except Exception:
        _SERVER["skip"] = "pyspark not importable"
        return None
    try:
        from harness.backends.local_connect import LocalConnectServer
        wh = tempfile.mkdtemp(prefix="ssa_sibling_wh_")
        srv = LocalConnectServer(port=15099, ui_port=4099, warehouse_dir=wh, wait_secs=120,
                                 log_file=os.path.join(wh, "connect-server.log"))
        srv.start(ensure_database="default", ensure_catalog="spark_catalog")
        atexit.register(srv.stop)
        _SERVER["obj"] = srv
        _SERVER["remote"] = srv.remote
        return _SERVER["remote"], srv
    except Exception as e:  # noqa: BLE001
        _SERVER["skip"] = f"could not start local Spark Connect server: {type(e).__name__}: {e}"
        return None


def _gate(remote, workspace):
    """Run the harness SDP dry-run gate on a workspace; return the GateOutcome."""
    from harness.backends.live import ConnectExecutor
    from harness.backends.base import LoopState, Proposal
    from harness.arm_manifest import load_arms
    arm = load_arms(os.path.join(STUDY, "arms"))["B"]   # SDP, gated
    ex = ConnectExecutor(remote, None)
    state = LoopState(task="t", seed=42, workspace=workspace, dataset_path="x",
                      output_table="gold")
    return ex.run_gate(Proposal(0, "code", "cmd"), arm, state)


def _stock_cli_dry_run(remote, workspace):
    """Run the STOCK `pipelines/cli.py dry-run` (pre-fix path) for the regression
    contrast; returns (rc, combined_log)."""
    from harness.backends.live import _spark_home, ConnectExecutor
    spark_home = os.environ.get("SPARK_HOME") or _spark_home()
    cli = os.path.join(spark_home, "pipelines", "cli.py")
    argv = ["python3", cli, "dry-run", "--spec", os.path.join(workspace, "spark-pipeline.yml")]
    ex = ConnectExecutor(remote, None)
    rc, log, _ = ex._run(argv, workspace, {})
    return rc, log


def _run_sql_subprocess(remote, statements):
    """Run SQL statements on the server in a short-lived SUBPROCESS Connect session
    (Option C: never a Connect session in this test process). Used to plant/drop a
    STALE catalog table for the masking test."""
    script = (
        "import sys\n"
        "from pyspark.sql import SparkSession\n"
        f"s = SparkSession.builder.remote({remote!r}).getOrCreate()\n"
        "for stmt in sys.argv[1:]:\n"
        "    s.sql(stmt)\n"
        "s.stop()\n")
    proc = subprocess.run([sys.executable, "-c", script, *statements],
                          capture_output=True, text=True, env=dict(os.environ))
    return proc.returncode, proc.stdout + proc.stderr


def test_correct_windowed_sibling_passes_gate():
    """(a) A correct medallion that dedups a SIBLING read via a Window passes the gate."""
    got = _ensure_server()
    if got is None:
        _skip(_SERVER["skip"])
    remote, _ = got
    with tempfile.TemporaryDirectory() as tmp:
        ws = _pipeline_dir(tmp, "correct", CORRECT_WINDOWED_SIBLING)
        g = _gate(remote, ws)
        assert g.failed is False, f"correct windowed-sibling medallion was rejected: {g.error_class}\n{g.log}"
    print("PASS gate accepts correct windowed-sibling medallion")


def test_streaming_watermark_dedup_sibling_passes_gate():
    """(a) The original streaming form -- withWatermark + dropDuplicatesWithinWatermark
    over a readStream.table(SIBLING) -- passes the gate post-fix (a streaming sibling is
    seeded as a STREAMING view, so the streaming read resolves)."""
    got = _ensure_server()
    if got is None:
        _skip(_SERVER["skip"])
    remote, _ = got
    with tempfile.TemporaryDirectory() as tmp:
        ws = _pipeline_dir(tmp, "streaming", STREAMING_WINDOWED_SIBLING)
        rc, log = _stock_cli_dry_run(remote, ws)
        assert rc != 0 and "TABLE_OR_VIEW_NOT_FOUND" in log, \
            f"stock CLI did not reproduce the streaming sibling 42P01:\n{log[-1200:]}"
        g = _gate(remote, ws)
        assert g.failed is False, \
            f"streaming watermark/dedup over sibling rejected post-fix: {g.error_class}\n{g.log}"
    print("PASS gate accepts streaming watermark/dedup over sibling")


def test_sibling_windowing_42P01_reproduced_and_fixed():
    """(b) Regression: the SAME pipeline FAILS the stock cli.py dry-run with
    TABLE_OR_VIEW_NOT_FOUND / 42P01 (the bug), and PASSES the harness gate (the fix)."""
    got = _ensure_server()
    if got is None:
        _skip(_SERVER["skip"])
    remote, _ = got
    with tempfile.TemporaryDirectory() as tmp:
        ws = _pipeline_dir(tmp, "regression", CORRECT_WINDOWED_SIBLING)
        rc, log = _stock_cli_dry_run(remote, ws)
        assert rc != 0, "stock cli.py dry-run unexpectedly PASSED the windowed sibling (bug not reproduced)"
        assert "TABLE_OR_VIEW_NOT_FOUND" in log and "42P01" in log, \
            f"expected the 42P01 sibling-windowing failure pre-fix, got:\n{log[-1500:]}"
        g = _gate(remote, ws)
        assert g.failed is False, \
            f"harness gate still fails the windowed sibling post-fix: {g.error_class}\n{g.log}"
    print("PASS reproduced 42P01 on stock CLI; harness gate fixes it")


def test_missing_upstream_still_fails():
    """(c) Fidelity: a genuinely missing upstream still fails the gate with 42P01 --
    pre-seeding never invents a schema for a table the pipeline does not define."""
    got = _ensure_server()
    if got is None:
        _skip(_SERVER["skip"])
    remote, _ = got
    with tempfile.TemporaryDirectory() as tmp:
        ws = _pipeline_dir(tmp, "missing_upstream", MISSING_UPSTREAM)
        g = _gate(remote, ws)
        assert g.failed is True, "missing upstream was NOT caught -- gate became too permissive"
        assert g.error_class and "TABLE_OR_VIEW_NOT_FOUND" in g.error_class, \
            f"expected TABLE_OR_VIEW_NOT_FOUND, got {g.error_class}\n{g.log}"
    print(f"PASS missing upstream still fails ({g.error_class})")


def test_unresolved_column_in_windowed_sibling_still_fails():
    """(c) Fidelity: a genuine unresolved column INSIDE a windowed sibling read still
    fails UNRESOLVED_COLUMN / 42703 -- the sibling is seeded with its REAL schema, so a
    bad column reference is not masked by a loose/empty placeholder."""
    got = _ensure_server()
    if got is None:
        _skip(_SERVER["skip"])
    remote, _ = got
    with tempfile.TemporaryDirectory() as tmp:
        ws = _pipeline_dir(tmp, "unresolved_col", UNRESOLVED_COLUMN_WINDOWED)
        g = _gate(remote, ws)
        assert g.failed is True, "unresolved column was NOT caught -- gate became too permissive"
        assert g.error_class and "UNRESOLVED_COLUMN" in g.error_class, \
            f"expected UNRESOLVED_COLUMN, got {g.error_class}\n{g.log}"
    print(f"PASS unresolved column in windowed sibling still fails ({g.error_class})")


def test_stale_catalog_table_does_not_mask_defect():
    """(c) BLOCKING-1 guard: a STALE permanent `bronze` (with an `extra` column) created
    before the gate must NOT rescue a pipeline whose own `bronze` lacks `extra`. The
    isolated scratch database means silver resolves against the PIPELINE bronze, so the
    bad `extra` reference still fails UNRESOLVED_COLUMN -- the stale object cannot leak in."""
    got = _ensure_server()
    if got is None:
        _skip(_SERVER["skip"])
    remote, _ = got
    rc, _log = _run_sql_subprocess(remote, [
        "CREATE SCHEMA IF NOT EXISTS spark_catalog.default",
        "DROP TABLE IF EXISTS spark_catalog.default.bronze",
        "CREATE TABLE spark_catalog.default.bronze "
        "(order_id STRING, event_time STRING, extra STRING) USING parquet",
    ])
    assert rc == 0, f"could not plant the stale bronze table:\n{_log[-800:]}"
    try:
        with tempfile.TemporaryDirectory() as tmp:
            ws = _pipeline_dir(tmp, "stale_masking", STALE_MASKING)
            g = _gate(remote, ws)
            assert g.failed is True, \
                "stale catalog `bronze` RESCUED a defective pipeline -- isolation broken (false PASS)"
            assert g.error_class and "UNRESOLVED_COLUMN" in g.error_class, \
                f"expected UNRESOLVED_COLUMN (resolved against pipeline bronze), got {g.error_class}\n{g.log}"
    finally:
        _run_sql_subprocess(remote, ["DROP TABLE IF EXISTS spark_catalog.default.bronze"])
    print(f"PASS stale catalog table does not mask the defect ({g.error_class})")


def test_qualified_stale_catalog_table_does_not_mask_defect():
    """(c) BLOCKING-1 (qualified-read) guard: a FULLY-QUALIFIED sibling read
    `spark_catalog.default.bronze` must NOT be rescued by a stale permanent
    `spark_catalog.default.bronze` (with `extra`). The qualified ref is rewritten to the
    pipeline's own bronze (no `extra`), so the bad `extra` reference still fails
    UNRESOLVED_COLUMN -- the stale qualified table cannot leak in."""
    got = _ensure_server()
    if got is None:
        _skip(_SERVER["skip"])
    remote, _ = got
    rc, _log = _run_sql_subprocess(remote, [
        "CREATE SCHEMA IF NOT EXISTS spark_catalog.default",
        "DROP TABLE IF EXISTS spark_catalog.default.bronze",
        "CREATE TABLE spark_catalog.default.bronze "
        "(order_id STRING, event_time STRING, extra STRING) USING parquet",
    ])
    assert rc == 0, f"could not plant the stale bronze table:\n{_log[-800:]}"
    try:
        with tempfile.TemporaryDirectory() as tmp:
            ws = _pipeline_dir(tmp, "qualified_stale", QUALIFIED_STALE_MASKING)
            g = _gate(remote, ws)
            assert g.failed is True, \
                "stale qualified `bronze` RESCUED a defective pipeline -- qualified-read bypass (false PASS)"
            assert g.error_class and "UNRESOLVED_COLUMN" in g.error_class, \
                f"expected UNRESOLVED_COLUMN (resolved against pipeline bronze), got {g.error_class}\n{g.log}"
    finally:
        _run_sql_subprocess(remote, ["DROP TABLE IF EXISTS spark_catalog.default.bronze"])
    print(f"PASS qualified stale catalog table does not mask the defect ({g.error_class})")


def test_qualified_sibling_read_still_passes():
    """(a) A correct pipeline whose windowed sibling read is fully-qualified to its OWN
    dataset still PASSES -- the qualified-ref rewrite must not introduce a false-fail.
    A stale same-name table is present to prove resolution targets the pipeline bronze."""
    got = _ensure_server()
    if got is None:
        _skip(_SERVER["skip"])
    remote, _ = got
    _run_sql_subprocess(remote, [
        "CREATE SCHEMA IF NOT EXISTS spark_catalog.default",
        "DROP TABLE IF EXISTS spark_catalog.default.bronze",
        "CREATE TABLE spark_catalog.default.bronze "
        "(order_id STRING, event_time STRING, extra STRING) USING parquet",
    ])
    try:
        with tempfile.TemporaryDirectory() as tmp:
            ws = _pipeline_dir(tmp, "qualified_ok", QUALIFIED_SIBLING_OK)
            g = _gate(remote, ws)
            assert g.failed is False, \
                f"correct fully-qualified sibling read was rejected (false fail): {g.error_class}\n{g.log}"
    finally:
        _run_sql_subprocess(remote, ["DROP TABLE IF EXISTS spark_catalog.default.bronze"])
    print("PASS correct fully-qualified sibling read passes the gate")


def test_case_variant_qualified_stale_table_does_not_mask_defect():
    """(c) BLOCKING-1 (case-variant) guard: a sibling read qualified with DIFFERENT CASE
    (`SPARK_CATALOG.DEFAULT.BRONZE`) must still be recognised as internal (Spark resolves
    unquoted identifiers case-insensitively) and rewritten to the pipeline's own bronze,
    so a stale same-name table cannot rescue the bad `extra` reference -> UNRESOLVED_COLUMN."""
    got = _ensure_server()
    if got is None:
        _skip(_SERVER["skip"])
    remote, _ = got
    rc, _log = _run_sql_subprocess(remote, [
        "CREATE SCHEMA IF NOT EXISTS spark_catalog.default",
        "DROP TABLE IF EXISTS spark_catalog.default.bronze",
        "CREATE TABLE spark_catalog.default.bronze "
        "(order_id STRING, event_time STRING, extra STRING) USING parquet",
    ])
    assert rc == 0, f"could not plant the stale bronze table:\n{_log[-800:]}"
    try:
        with tempfile.TemporaryDirectory() as tmp:
            ws = _pipeline_dir(tmp, "case_variant_stale", CASE_VARIANT_STALE_MASKING)
            g = _gate(remote, ws)
            assert g.failed is True, \
                "case-variant qualified `BRONZE` RESCUED a defective pipeline (false PASS)"
            assert g.error_class and "UNRESOLVED_COLUMN" in g.error_class, \
                f"expected UNRESOLVED_COLUMN (resolved against pipeline bronze), got {g.error_class}\n{g.log}"
    finally:
        _run_sql_subprocess(remote, ["DROP TABLE IF EXISTS spark_catalog.default.bronze"])
    print(f"PASS case-variant qualified stale table does not mask the defect ({g.error_class})")


def test_case_variant_qualified_sibling_read_still_passes():
    """(a) A correct pipeline reading its own sibling via a case-variant qualified name
    (`spark_catalog.default.BRONZE`) still PASSES -- case-insensitive classification must
    recognise it as internal and not introduce a false-fail."""
    got = _ensure_server()
    if got is None:
        _skip(_SERVER["skip"])
    remote, _ = got
    _run_sql_subprocess(remote, [
        "CREATE SCHEMA IF NOT EXISTS spark_catalog.default",
        "DROP TABLE IF EXISTS spark_catalog.default.bronze",
        "CREATE TABLE spark_catalog.default.bronze "
        "(order_id STRING, event_time STRING, extra STRING) USING parquet",
    ])
    try:
        with tempfile.TemporaryDirectory() as tmp:
            ws = _pipeline_dir(tmp, "case_variant_ok", CASE_VARIANT_SIBLING_OK)
            g = _gate(remote, ws)
            assert g.failed is False, \
                f"correct case-variant qualified sibling read was rejected (false fail): {g.error_class}\n{g.log}"
    finally:
        _run_sql_subprocess(remote, ["DROP TABLE IF EXISTS spark_catalog.default.bronze"])
    print("PASS correct case-variant qualified sibling read passes the gate")


def test_identifier_normalization_closes_case_and_whitespace():
    """Pure unit test (no server): the shared multipart-identifier normalizer recognises
    case- AND whitespace-variant qualifiers to this pipeline's own namespace as INTERNAL
    (returning the canonical name), and leaves a DIFFERENT namespace external."""
    from harness import sdp_dryrun as M
    names = {"bronze", "silver"}
    for ref in (" SPARK_CATALOG.DEFAULT.BRONZE ", "spark_catalog . default . bronze",
                "spark_catalog.  default  .bronze", "`spark_catalog`.`default`.`bronze`",
                "Default.bronze", "BRONZE", " bronze "):
        assert M._internal_leaf(ref, names, "spark_catalog", "default") == "bronze", \
            f"{ref!r} not recognised as internal bronze"
    # a different catalog/db is external even if the leaf matches
    assert M._internal_leaf("other_cat.other_db.bronze", names, "spark_catalog", "default") is None
    print("PASS identifier normalization closes case + whitespace variants")


# Source-parse fixtures for the regex-alignment unit test. `spark` is a never-executed
# sentinel: `_flow_pipeline_refs` only reads these functions' SOURCE TEXT (inspect.getsource),
# it never calls them, so the name need not resolve to a real session.
spark: Any = None  # noqa: E305


def _src_literal_ws():
    return spark.read.table ( "bronze" )  # noqa: E211 -- literal name, whitespace around parens


def _src_dynamic():
    name = "bronze"
    return spark.read.table(name)  # dynamic (variable) arg -> deps unknown


def test_literal_table_call_with_whitespace_is_not_declined():
    """Pure unit test (no server): regex alignment -- a literal `.table ( "bronze" )` with
    whitespace around the parens is recognised as a LITERAL ref (deps = {bronze}), not
    miscounted as dynamic and declined; a genuinely dynamic `.table(name)` returns None."""
    from types import SimpleNamespace
    from harness import sdp_dryrun as M
    refs_lit = M._flow_pipeline_refs(SimpleNamespace(func=_src_literal_ws),
                                     {"bronze"}, "spark_catalog", "default")
    refs_dyn = M._flow_pipeline_refs(SimpleNamespace(func=_src_dynamic),
                                     {"bronze"}, "spark_catalog", "default")
    assert refs_lit == {"bronze"}, \
        f"literal `.table ( \"bronze\" )` with whitespace not recognised as a literal ref: {refs_lit}"
    assert refs_dyn is None, f"dynamic `.table(name)` was not declined (deps must be unknown): {refs_dyn}"
    print("PASS literal-with-whitespace recognised; dynamic declined")


def test_whitespace_qualified_stale_table_does_not_mask_defect():
    """(c) Whitespace-variant guard: ` SPARK_CATALOG.DEFAULT.BRONZE ` (leading/trailing
    whitespace) is normalized to the pipeline's own bronze, so a stale same-name table
    cannot rescue the bad `extra` reference -> still UNRESOLVED_COLUMN / 42703."""
    _assert_qualified_masking_fails(WHITESPACE_STALE_MASKING, "whitespace_stale")


def test_spaces_around_dots_qualified_stale_table_does_not_mask_defect():
    """(c) Spaces-around-dots guard: `spark_catalog . default . bronze` (Spark accepts it)
    is normalized to the pipeline's own bronze -> stale table cannot mask -> 42703."""
    _assert_qualified_masking_fails(SPACED_DOTS_STALE_MASKING, "spaced_dots_stale")


def test_whitespace_qualified_sibling_read_still_passes():
    """(a) A correct pipeline reading its own sibling via a whitespace-padded qualified
    name still PASSES (the normalizer recognises it as internal; no new false-fail)."""
    got = _ensure_server()
    if got is None:
        _skip(_SERVER["skip"])
    remote, _ = got
    _run_sql_subprocess(remote, [
        "CREATE SCHEMA IF NOT EXISTS spark_catalog.default",
        "DROP TABLE IF EXISTS spark_catalog.default.bronze",
        "CREATE TABLE spark_catalog.default.bronze "
        "(order_id STRING, event_time STRING, extra STRING) USING parquet",
    ])
    try:
        with tempfile.TemporaryDirectory() as tmp:
            ws = _pipeline_dir(tmp, "whitespace_ok", WHITESPACE_SIBLING_OK)
            g = _gate(remote, ws)
            assert g.failed is False, \
                f"correct whitespace-padded qualified sibling read was rejected (false fail): {g.error_class}\n{g.log}"
    finally:
        _run_sql_subprocess(remote, ["DROP TABLE IF EXISTS spark_catalog.default.bronze"])
    print("PASS correct whitespace-padded qualified sibling read passes the gate")


def _assert_qualified_masking_fails(fixture, slug):
    """Shared helper: plant a stale `spark_catalog.default.bronze` (with `extra`), run the
    gate on a pipeline whose own bronze lacks `extra`, and assert it still fails 42703."""
    got = _ensure_server()
    if got is None:
        _skip(_SERVER["skip"])
    remote, _ = got
    rc, _log = _run_sql_subprocess(remote, [
        "CREATE SCHEMA IF NOT EXISTS spark_catalog.default",
        "DROP TABLE IF EXISTS spark_catalog.default.bronze",
        "CREATE TABLE spark_catalog.default.bronze "
        "(order_id STRING, event_time STRING, extra STRING) USING parquet",
    ])
    assert rc == 0, f"could not plant the stale bronze table:\n{_log[-800:]}"
    try:
        with tempfile.TemporaryDirectory() as tmp:
            ws = _pipeline_dir(tmp, slug, fixture)
            g = _gate(remote, ws)
            assert g.failed is True, \
                f"stale `bronze` RESCUED a defective pipeline via {slug} (false PASS)"
            assert g.error_class and "UNRESOLVED_COLUMN" in g.error_class, \
                f"expected UNRESOLVED_COLUMN (resolved against pipeline bronze), got {g.error_class}\n{g.log}"
    finally:
        _run_sql_subprocess(remote, ["DROP TABLE IF EXISTS spark_catalog.default.bronze"])
    print(f"PASS {slug} does not mask the defect ({g.error_class})")


def test_graph_shape_cycle_still_fails():
    """(c) BLOCKING-2 guard: a dependency cycle is declined from seeding (no same-name
    temp view stands in for a cycle member), so the real SDP dry-run still rejects it."""
    got = _ensure_server()
    if got is None:
        _skip(_SERVER["skip"])
    remote, _ = got
    with tempfile.TemporaryDirectory() as tmp:
        ws = _pipeline_dir(tmp, "cycle", CYCLE_SHAPE)
        g = _gate(remote, ws)
        assert g.failed is True, \
            "dependency cycle was NOT caught -- a seed temp view masked a graph-shape defect (false PASS)"
    print(f"PASS dependency cycle still fails the gate ({g.error_class})")


def test_local_sdp_gate_execute_target_absolute_spec_for_relative_workspace():
    """Path-wiring regression (PR fix-sdp-spec-path): the LOCAL SDP gate AND execute
    must hand the driver an ABSOLUTE --spec pointing at the actually-staged
    spark-pipeline.yml, even when `state.workspace` is RELATIVE (`--work-dir ./...`,
    as in the Part-1 local smoke).

    BUG: `_run` launches the gate/execute subprocess with cwd=state.workspace, so a
    RELATIVE --spec was re-resolved against that new cwd, DOUBLING the path
    (<ws>/<ws>/spark-pipeline.yml) -> the gate driver raised
    PIPELINE_SPEC_FILE_DOES_NOT_EXIST and the cell burned all 12 iterations ($0,
    nothing executed). This test fails pre-fix (relative --spec) and passes post-fix
    (os.path.abspath in run_gate/run_execute)."""
    got = _ensure_server()
    if got is None:
        _skip(_SERVER["skip"])
    remote, _ = got
    from harness.backends.local_connect import LocalConnectExecutor
    from harness.backends.base import LoopState, Proposal
    from harness.arm_manifest import load_arms
    arm = load_arms(os.path.join(STUDY, "arms"))["B"]   # SDP, gated
    with tempfile.TemporaryDirectory() as tmp:
        abs_ws = _pipeline_dir(tmp, "relws", CORRECT_WINDOWED_SIBLING)
        staged_spec = os.path.join(abs_ws, "spark-pipeline.yml")
        # the exact bug condition: a workspace expressed RELATIVE to the runner cwd.
        rel_ws = os.path.relpath(abs_ws, os.getcwd())
        assert not os.path.isabs(rel_ws), rel_ws
        ex = LocalConnectExecutor(remote, None)
        state = LoopState(task="t", seed=42, workspace=rel_ws, dataset_path="x",
                          output_table="gold")

        # capture the argv handed to every gate/execute subprocess.
        seen: list = []
        orig_run = ex._run

        def _spy(argv, cwd, env_extra, _real=orig_run):
            seen.append(list(argv))
            return _real(argv, cwd, env_extra)

        def _spec_of(argv):
            return argv[argv.index("--spec") + 1]

        # (1) GATE -- run for real; the spec must be FOUND and the pipeline accepted.
        ex._run = _spy
        g = ex.run_gate(Proposal(0, "code", "cmd"), arm, state)
        gate_spec = _spec_of(seen[-1])
        assert os.path.isabs(gate_spec), f"gate --spec not absolute: {gate_spec!r}"
        assert os.path.realpath(gate_spec) == os.path.realpath(staged_spec), \
            f"gate --spec {gate_spec!r} != staged {staged_spec!r}"
        assert g.error_class != "PIPELINE_SPEC_FILE_DOES_NOT_EXIST", \
            f"gate could not find the staged spec via a relative workspace:\n{g.log[-1500:]}"
        assert g.failed is False, \
            f"correct windowed-sibling rejected via relative ws: {g.error_class}\n{g.log}"

        # (2) EXECUTE -- assert its argv wiring WITHOUT materializing (stub the run).
        seen.clear()
        ex._run = lambda argv, cwd, env_extra: (seen.append(list(argv)), (0, "", 0.0))[1]
        ex.run_execute(Proposal(0, "code", "cmd"), arm, state)
        exec_spec = _spec_of(seen[-1])
        assert os.path.isabs(exec_spec), f"execute --spec not absolute: {exec_spec!r}"
        assert os.path.realpath(exec_spec) == os.path.realpath(staged_spec), \
            f"execute --spec {exec_spec!r} != staged {staged_spec!r}"
    print("PASS local SDP gate+execute target the absolute staged spec (relative ws)")


def _run_all():
    import pytest
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = skipped = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
        except pytest.skip.Exception as e:  # type: ignore[attr-defined]
            skipped += 1
            print(f"SKIP {t.__name__}: {e}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL {t.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            import traceback
            failed += 1
            print(f"ERROR {t.__name__}: {type(e).__name__}: {e}")
            traceback.print_exc()
    print(f"\n{len(tests) - failed - skipped}/{len(tests)} passed, {skipped} skipped, {failed} failed")
    return failed


if __name__ == "__main__":
    sys.exit(1 if _run_all() else 0)

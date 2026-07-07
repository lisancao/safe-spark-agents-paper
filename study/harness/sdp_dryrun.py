"""Graph-aware SDP dry-run gate driver (fixes the sibling-windowing resolution bug).

WHY THIS EXISTS
---------------
The SDP arms' structural gate runs `spark-pipelines dry-run` (i.e. pyspark's
`pipelines/cli.py dry-run`) against a local Spark Connect server. That stock path
has a LOCAL-only fidelity bug: when a flow applies a `Window` / `row_number` /
`dropDuplicates(WithinWatermark)` over a read of a SIBLING pipeline dataset
(`spark.read[Stream].table("<other_pipeline_dataset>")`), the windowing operator
forces the server to EAGERLY analyze that flow's relation at flow-DEFINITION time
(`DefineFlow`), BEFORE the dataflow graph has wired the sibling's schema in. The
sibling is therefore resolved against the bare session catalog -- where it does not
exist yet during a dry-run -- and analysis dies with::

    [TABLE_OR_VIEW_NOT_FOUND] The table or view `<sibling>` cannot be found ...
    SQLSTATE: 42P01

Plain sibling refs (select/filter/join/groupBy().agg()) stay lazy and resolve fine
later against the graph; a Window over an INLINE source is fine too. It is
specifically dedup/windowing OVER A SIBLING that detonates -- exactly the silver-layer
dedup the medallion study cares about -- so a declarative agent writes CORRECT
medallion code and then thrashes against a phantom gate failure. This is a LOCAL
divergence from real SDP/DLT, where the graph's dataset schemas are available to flow
analysis, so windowed sibling reads resolve. Databricks DLT / real SDP do NOT reject
these pipelines.

THE FIX (graph-aware pre-resolution, in ISOLATION)
--------------------------------------------------
Before the authoritative dry-run, we register the pipeline's OWN datasets as session
temp views backed by their own (lazy, unexecuted) query plans, in dependency order,
so the eager-analysis path resolves a windowed sibling read against a schema that
MIRRORS what the dataflow graph would supply. We then run the REAL SDP dry-run
(`create_dataflow_graph` + `register_definitions` + `start_run(dry=True)`) in the
same session as the authoritative verdict. The pre-seed only *unblocks* the
eager-analysis trap; it never invents a verdict.

Two properties make this safe and faithful -- NOT more permissive than real SDP/DLT:

1. ISOLATION (no resolution through stale/external catalog objects). The whole gate
   runs in a FRESH, per-invocation scratch database that is the session's current
   database. A pipeline-internal reference therefore resolves ONLY against (a) a temp
   view we seeded for that pipeline dataset, or (b) the dataflow graph -- never a
   pre-existing catalog object. This matters because the SDP arm's prior-iteration
   *executes* materialize real `bronze`/`silver`/... tables into the pipeline's
   catalog/database; without isolation a later gate could resolve a sibling against
   those STALE tables and mask a genuine defect (false PASS). In the scratch database
   a referenced-but-unseeded dataset simply does not resolve, so the real dry-run
   still fails it. An UNqualified sibling read is shadowed by the seeded temp view (and
   falls through to the empty scratch db otherwise); a sibling read FULLY-QUALIFIED to
   the pipeline's own `<catalog>.<database>` (which would otherwise bypass the scratch
   current-database and hit the real, possibly stale, table) is rewritten down to the
   bare name -- via `_rewrite_internal_table_refs` -- so it too resolves to the seed /
   graph and never the stale table. A reference qualified to ANY OTHER namespace is a
   genuine external read and is left untouched.

2. EXPLICIT DEPENDENCY-GATED, MINIMAL SEEDING with GRAPH-SHAPE GUARDS. We statically
   determine each flow's references to pipeline-internal datasets and seed in
   dependency order: a dataset is seeded ONLY when every pipeline-internal sibling it
   reads is ALREADY seeded (so a sibling reference resolves only against an
   already-seeded pipeline temp view, never something incidental). We seed only the
   MINIMAL set actually needed to unblock a downstream read, and we DECLINE to seed
   graph shapes that SDP must judge for itself, so a same-name temp view can never
   stand in for them:
     * a dataset targeted by more than one flow (duplicate / append flows),
     * a flow that reads its own target (self-reference),
     * datasets in a dependency cycle,
     * a flow whose source is not statically analyzable, or which reads a sibling we
       cannot seed (e.g. a SQL-defined dataset; see below).
   Declined datasets are left UNSEEDED and the authoritative dry-run produces the
   verdict.

FIDELITY CONSEQUENCES
---------------------
  * a genuinely MISSING upstream (a `read.table("does_not_exist")` whose name is not a
    pipeline dataset) is never seeded; in the scratch database it does not resolve, so
    the dry-run still fails with TABLE_OR_VIEW_NOT_FOUND / 42P01;
  * a genuine UNRESOLVED COLUMN -- including one *inside* a windowed sibling read --
    still fails with UNRESOLVED_COLUMN / 42703, because the sibling is seeded with its
    REAL schema (its own plan), not a loose placeholder;
  * stale/external catalog objects cannot rescue a defective pipeline (isolation);
  * graph-shape defects (duplicate/append target, self-read, cycle) are not masked by
    a seed temp view (declined + judged by the real dry-run).

DRY-RUN ONLY. Seeding empty/lazy placeholder views is safe precisely because a
dry-run reads no data and materializes nothing -- the views are an analysis-time
scaffold. It must NEVER be applied to the execute/`run` path, where a placeholder
would shadow the real upstream and silently materialize empty/wrong output. This
driver only ever runs `dry=True`.

THREAT MODEL / KNOWN LIMITATIONS (the classification boundary)
--------------------------------------------------------------
The gate decides whether a `.table()` reference is a pipeline-internal sibling (which it
may rewrite/seed) or an external read (which it leaves to the real catalog) by STATIC
parsing of the flow's source. The design bias is deliberate: toward false-FAIL (decline
to seed -> the real dry-run judges) over false-PASS (never mask a genuine defect).

  * Only LITERAL string arguments are classified, with a Spark-compatible multipart
    identifier normalization (`_normalize_identifier_parts`): per-part whitespace and
    backticks stripped, then case-folded. This closes the case- AND whitespace-variant
    classes together -- `bronze`, ` SPARK_CATALOG.DEFAULT.BRONZE `, `Default.bronze`,
    and `spark_catalog . default . bronze` all resolve to the dataset `bronze`. The
    SAME normalizer backs both the dependency classifier and the runtime rewrite, so
    they never disagree. Quoted (backticked) identifiers are treated as unquoted here;
    that is the correct-enough rule for this corpus.
  * A reference whose name is built DYNAMICALLY (f-string, variable, getattr, helper)
    is NOT classified -- the flow is DECLINED (deps unknown) and judged by the real
    dry-run. A windowed read of a dynamic sibling may therefore FALSE-FAIL; it can never
    false-pass. The study corpus uses literal `.table()` for every sibling read.
  * Only the pipeline's OWN `<catalog>.<database>` qualifier marks a ref internal; any
    other namespace (e.g. another pipeline's published `other_db.bronze`) is external.

SQL-DEFINED DATASETS. `register_sql` graph elements are recognized as
pipeline-internal NAMES (so a Python flow reading one is correctly treated as having a
pipeline dependency), but they have no Python query function, so we cannot seed them
and we DECLINE to seed any Python flow that windows over a SQL-defined sibling. Such a
pipeline is judged by the real dry-run (a possible false-FAIL, never a false-PASS).
The study's SDP arm authors only Python `@dp` datasets, so this path is not exercised
there.

INVOCATION
----------
    python3 <this file> --spec /path/to/spark-pipeline.yml

Mirrors `cli.py dry-run --spec`: reads SPARK_REMOTE from the env, prints
"Run is COMPLETED" on success, exits non-zero with the analyzer's bracketed error
class (e.g. `[TABLE_OR_VIEW_NOT_FOUND] ... SQLSTATE: 42P01`) on failure, so the
harness's existing error-class extraction keeps working unchanged.
"""
from __future__ import annotations

import argparse
import contextlib
import importlib
import importlib.util
import inspect
import os
import re
import sys
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# Allow both `python3 harness/sdp_dryrun.py` and `-m harness.sdp_dryrun`: put the
# study dir (parent of `harness/`) on sys.path so sibling harness imports resolve
# regardless of cwd. The gate subprocess runs with cwd = the agent workspace.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Matches `spark.read.table("x")`, `spark.readStream.table('a.b.x')`, `spark.table("x")`
# with a LITERAL string argument. `_TABLE_CALL_RE` matches ANY `.table(` call. Both allow
# whitespace before `(`, so a flow whose `.table()` count exceeds its LITERAL-ref count has
# a `.table(<non-literal>)` (f-string / variable / helper): deps UNKNOWN -> declined, never
# falsely passed. (The two regexes are aligned on `\s*\(` so a benign `.table ("x")` is not
# miscounted as dynamic.)
_TABLE_REF_RE = re.compile(r"\.table\s*\(\s*['\"]([^'\"]+)['\"]")
_TABLE_CALL_RE = re.compile(r"\.table\s*\(")
# Best-effort extraction of dataset names a SQL graph-elements file CREATEs.
_SQL_CREATE_RE = re.compile(
    r"create\s+(?:or\s+replace\s+)?(?:streaming\s+)?(?:materialized\s+view|view|table)\s+"
    r"(?:if\s+not\s+exists\s+)?[`\"]?([A-Za-z0-9_.]+)[`\"]?",
    re.IGNORECASE,
)


class _Capture:
    """Records the pipeline's datasets + flows without running any flow or touching a
    server. `@dp` decorators call register_output / register_flow on this registry."""

    def __init__(self) -> None:
        from pyspark.pipelines.graph_element_registry import GraphElementRegistry

        capture = self

        class _Registry(GraphElementRegistry):  # type: ignore[misc]
            def register_output(self, output) -> None:  # noqa: ANN001
                capture.output_names.add(output.name)

            def register_flow(self, flow) -> None:  # noqa: ANN001
                capture.flows.append(flow)

            def register_sql(self, sql_text, file_path) -> None:  # noqa: ANN001
                for name in _SQL_CREATE_RE.findall(sql_text or ""):
                    capture.sql_dataset_names.add(name.split(".")[-1])

        self.output_names: Set[str] = set()
        self.sql_dataset_names: Set[str] = set()
        self.flows: list = []
        self._registry = _Registry()

    def run(self, spec_path: Path, spec) -> "_Capture":
        from pyspark.pipelines.graph_element_registry import graph_element_registration_context

        with graph_element_registration_context(self._registry):
            for f in _spec_python_files(spec_path, spec):
                module_spec = importlib.util.spec_from_file_location(f"{f.stem}__ssa_cap", str(f))
                if module_spec is None or module_spec.loader is None:
                    continue
                module = importlib.util.module_from_spec(module_spec)
                module_spec.loader.exec_module(module)
        return self

    @property
    def dataset_names(self) -> Set[str]:
        return set(self.output_names) | set(self.sql_dataset_names)


def _spec_python_files(spec_path: Path, spec) -> List[Path]:
    """The .py transform files the spec's libraries glob selects (mirrors cli.py)."""
    base = spec_path.parent
    files: List[Path] = []
    for lib in spec.libraries:
        for p in base.glob(lib.include):
            if p.is_file() and p.suffix == ".py" and "__pycache__" not in p.parts:
                files.append(p)
    return files


def _normalize_identifier_parts(ref: str) -> List[str]:
    """Split a multipart table identifier into Spark-comparable parts. Spark's parser
    ignores whitespace around the identifier and around each `.`-separated part, and
    resolves UNQUOTED identifiers case-insensitively. So for each part we strip
    surrounding whitespace, strip backticks (quoted identifiers are treated as unquoted
    for this corpus), strip again, and casefold -- closing the case AND whitespace
    variant classes at once (` SPARK_CATALOG . DEFAULT . BRONZE ` -> ['spark_catalog',
    'default', 'bronze'])."""
    return [p.strip().strip("`").strip().casefold() for p in str(ref).strip().split(".")]


def _internal_leaf(ref: str, dataset_names: Set[str], catalog: str,
                   database: str) -> Optional[str]:
    """Return the CANONICAL pipeline dataset name a `.table(<ref>)` references, or None
    if <ref> is NOT a pipeline-internal sibling.

    A reference is internal only when its leaf is a pipeline dataset AND it is either
    unqualified, or qualified to THIS pipeline's own catalog/database (the namespace
    SDP materializes the graph into). A reference qualified to any OTHER catalog/db is
    EXTERNAL even if its leaf happens to match a pipeline dataset name -- e.g. a read of
    another pipeline's published `other_db.bronze` -- and must resolve against the real
    catalog, never our seed.

    Both the captured ref and the catalog/database/dataset names are run through
    `_normalize_identifier_parts`, so case AND whitespace variants are matched
    identically here and in the runtime rewrite. Returning the CANONICAL declared name
    keeps the rewrite + dependency planner aligned with the name the seeded temp view /
    graph dataset actually uses."""
    by_fold = {n.casefold(): n for n in dataset_names}
    parts = _normalize_identifier_parts(ref)
    canonical = by_fold.get(parts[-1])
    if canonical is None:
        return None
    if len(parts) == 1:
        return canonical
    if len(parts) == 2:
        return canonical if parts[0] == database.casefold() else None
    if len(parts) == 3:
        return canonical if (parts[0] == catalog.casefold()
                             and parts[1] == database.casefold()) else None
    return None


def _flow_pipeline_refs(flow, dataset_names: Set[str], catalog: str,
                        database: str) -> Optional[Set[str]]:
    """The pipeline-internal dataset names this flow reads (via `.table("name")`), or
    None when the flow's deps cannot be determined statically (=> treat as unknown =>
    decline to seed). Returns None if the source is unreadable OR if the flow has any
    `.table(<non-literal>)` call (a dynamic name -- f-string / variable / helper -- whose
    target we cannot know), so a dynamic sibling read is declined, never falsely passed.
    The study's pipelines use literal `.table()` for every sibling read."""
    try:
        src = inspect.getsource(flow.func)
    except (OSError, TypeError):
        return None
    # Drop the decorator lines: `@dp.table(...)` itself contains a `.table(` that would
    # otherwise be miscounted as a (dynamic) reader call. Analyse from the `def` onward.
    m = re.search(r"^[ \t]*def\s", src, re.MULTILINE)
    if m:
        src = src[m.start():]
    if len(_TABLE_CALL_RE.findall(src)) > len(_TABLE_REF_RE.findall(src)):
        return None  # at least one .table() has a non-literal argument: deps unknown
    refs: Set[str] = set()
    for ref in _TABLE_REF_RE.findall(src):
        leaf = _internal_leaf(ref, dataset_names, catalog, database)
        if leaf is not None:
            refs.add(leaf)
    return refs


@contextlib.contextmanager
def _rewrite_internal_table_refs(dataset_names: Set[str], catalog: str, database: str):
    """Patch Spark Connect `*.table(name)` so a sibling read QUALIFIED to this
    pipeline's own catalog/database (e.g. `spark_catalog.default.bronze`) is rewritten
    to the bare dataset name -- which then resolves to the seeded scratch-namespace view
    (or the dataflow graph), exactly as real SDP/DLT resolves it against the graph.

    Without this, a fully-qualified sibling read bypasses the scratch-database isolation
    and resolves against the REAL (possibly STALE) `<catalog>.<database>.<name>` table,
    masking a genuine defect (false PASS). References qualified to any OTHER namespace
    are left untouched (genuine external reads). Active only for the gate window, then
    restored."""
    targets = []
    for mod, clsname in (
        ("pyspark.sql.connect.readwriter", "DataFrameReader"),
        ("pyspark.sql.connect.streaming.readwriter", "DataStreamReader"),
        ("pyspark.sql.connect.session", "SparkSession"),
    ):
        try:
            module = importlib.import_module(mod)
            targets.append(getattr(module, clsname))
        except Exception:  # noqa: BLE001
            continue

    def _make(orig):
        def table(self, tableName, *args, **kwargs):  # noqa: ANN001
            leaf = _internal_leaf(tableName, dataset_names, catalog, database)
            if leaf is not None and leaf != tableName:
                return orig(self, leaf, *args, **kwargs)
            return orig(self, tableName, *args, **kwargs)
        return table

    originals = [(cls, cls.table) for cls in targets]
    try:
        for cls, orig in originals:
            cls.table = _make(orig)  # type: ignore[method-assign]
        yield
    finally:
        for cls, orig in originals:
            cls.table = orig  # type: ignore[method-assign]


def _seed_plan(cap: "_Capture", catalog: str,
               database: str) -> Tuple[List[str], Dict[str, object], Dict[str, str]]:
    """Decide which datasets to seed and in what dependency order.

    Returns (ordered_names, flow_by_name, declined{name: reason}). Only datasets that
    are (a) targeted by exactly one Python flow, (b) statically analyzable, (c) not
    self-referential, (d) all of whose pipeline-internal siblings are themselves
    seedable, (e) not in a dependency cycle, and (f) actually needed as a sibling by
    some flow, are returned -- in topological order."""
    names = cap.dataset_names
    flows_by_target: Dict[str, list] = {}
    for fl in cap.flows:
        flows_by_target.setdefault(fl.target, []).append(fl)

    declined: Dict[str, str] = {}
    candidates: Dict[str, Tuple[object, Set[str]]] = {}
    for name in sorted(names):
        fls = flows_by_target.get(name, [])
        if len(fls) == 0:
            declined[name] = "no Python flow (e.g. SQL-defined or append target); cannot seed"
            continue
        if len(fls) > 1:
            declined[name] = "multiple flows target it (duplicate/append); SDP must judge"
            continue
        refs = _flow_pipeline_refs(fls[0], names, catalog, database)
        if refs is None:
            declined[name] = "flow source not statically analyzable; deps unknown"
            continue
        if name in refs:
            declined[name] = "flow reads its own target (self-reference); SDP must judge"
            continue
        candidates[name] = (fls[0], refs)

    # Drop candidates whose pipeline-internal siblings are not themselves seedable
    # (e.g. a sibling is SQL-defined or was declined above) -- fixpoint.
    changed = True
    while changed:
        changed = False
        for name in list(candidates):
            _flow, refs = candidates[name]
            unmet = {r for r in refs if r != name and r not in candidates}
            if unmet:
                declined.setdefault(
                    name, f"depends on un-seedable sibling(s) {sorted(unmet)}; SDP must judge")
                del candidates[name]
                changed = True

    # Topological order (Kahn) over the induced subgraph; any node left over is in a
    # cycle and is declined.
    indeg = {n: len({r for r in refs if r != n and r in candidates})
             for n, (_f, refs) in candidates.items()}
    ordered: List[str] = []
    ready = sorted([n for n, d in indeg.items() if d == 0])
    dependents: Dict[str, Set[str]] = {n: set() for n in candidates}
    for n, (_f, refs) in candidates.items():
        for r in refs:
            if r != n and r in candidates:
                dependents[r].add(n)
    while ready:
        n = ready.pop(0)
        ordered.append(n)
        for m in sorted(dependents[n]):
            indeg[m] -= 1
            if indeg[m] == 0:
                ready.append(m)
    for n in candidates:
        if n not in ordered:
            declined[n] = "in a dependency cycle; SDP must judge"

    # Minimal: seed only datasets that are actually read as a sibling by some flow,
    # plus the transitive pipeline deps needed to analyze those siblings.
    referenced: Set[str] = set()
    for fl in cap.flows:
        r = _flow_pipeline_refs(fl, names, catalog, database)
        if r:
            referenced |= r
    needed: Set[str] = set()
    frontier = [n for n in referenced if n in candidates]
    while frontier:
        n = frontier.pop()
        if n in needed:
            continue
        needed.add(n)
        _f, refs = candidates[n]
        frontier.extend(r for r in refs if r != n and r in candidates and r not in needed)
    ordered = [n for n in ordered if n in needed]
    return ordered, {n: candidates[n][0] for n in candidates}, declined


def preseed_sibling_schemas(cap: "_Capture", catalog: str, database: str) -> Set[str]:
    """Register each needed pipeline dataset as a session temp view backed by its OWN
    (lazy, unexecuted) query plan, in dependency order, so windowed sibling reads
    resolve against the graph's view instead of the bare catalog. The temp view carries
    the dataset's REAL analyzed schema AND preserves its kind (streaming vs batch).

    Best-effort and fidelity-preserving: a dataset is seeded only after all its
    pipeline-internal siblings are seeded (so its sibling reads resolve only against
    already-seeded pipeline temp views, never an incidental object), and only if its
    own query then analyzes cleanly. Returns the set of seeded dataset names. Never
    raises -- any failure just leaves a dataset unseeded for the real dry-run to judge.
    Must run inside `_rewrite_internal_table_refs` so a qualified sibling read seeds the
    same bare view a downstream read will resolve to."""
    try:
        order, flow_by_name, declined = _seed_plan(cap, catalog, database)
    except Exception as e:  # noqa: BLE001
        print(f"[sdp-dryrun] seed planning skipped ({type(e).__name__}: {e}); "
              "running dry-run without pre-seed.", file=sys.stderr)
        return set()
    if declined:
        print("[sdp-dryrun] declined to seed (left for the real dry-run): "
              + "; ".join(f"{n} -- {r}" for n, r in sorted(declined.items())), file=sys.stderr)

    seeded: Set[str] = set()
    for name in order:
        flow = flow_by_name[name]
        refs = _flow_pipeline_refs(flow, cap.dataset_names, catalog, database) or set()
        if not {r for r in refs if r != name} <= seeded:
            # a sibling could not be seeded -> do NOT risk an incidental resolution
            continue
        try:
            # createOrReplaceTempView forces analysis of the dataset's plan; with the
            # already-seeded siblings present (and in an isolated scratch db) a windowed
            # sibling read resolves against them. Raises if the query is genuinely
            # broken -> left unseeded -> the real dry-run flags it.
            flow.func().createOrReplaceTempView(name)  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            continue
        seeded.add(name)
    return seeded


def run_dry_run(spec_path: Path) -> None:
    """Pre-seed sibling schemas, then run the authoritative SDP dry-run in the SAME
    session and an ISOLATED scratch database, so pipeline-internal references resolve
    only against seeded pipeline temp views or the dataflow graph -- never a stale or
    external catalog object. Mirrors `pyspark.pipelines.cli.run(..., dry=True)`."""
    from pyspark.sql import SparkSession
    from pyspark.pipelines.cli import load_pipeline_spec, register_definitions
    from pyspark.pipelines.spark_connect_graph_element_registry import (
        SparkConnectGraphElementRegistry)
    from pyspark.pipelines.spark_connect_pipeline import (
        create_dataflow_graph, start_run, handle_pipeline_events)

    spec = load_pipeline_spec(spec_path)
    catalog = spec.catalog or "spark_catalog"
    # The namespace the agent qualifies its OWN sibling reads with (matches what SDP
    # materializes the graph into). Used to recognise + rewrite qualified internal refs.
    database = spec.database or "default"
    scratch = f"_ssa_gate_{uuid.uuid4().hex}"

    spark_builder = SparkSession.builder.config(
        "spark.sql.connect.serverStacktrace.enabled", "false")
    for key, value in spec.configuration.items():
        spark_builder = spark_builder.config(key, value)
    spark = spark_builder.getOrCreate()

    seeded: Set[str] = set()
    created_scratch = False
    try:
        # Isolation: a fresh, empty scratch database is the resolution namespace for the
        # whole gate, so no stale/external catalog object can satisfy a pipeline ref.
        spark.sql(f"CREATE SCHEMA `{catalog}`.`{scratch}`")
        created_scratch = True
        try:
            spark.catalog.setCurrentCatalog(catalog)
        except Exception:  # noqa: BLE001 -- single-catalog servers may not support this
            pass
        spark.catalog.setCurrentDatabase(scratch)
        # Note: the graph's default DB is the scratch schema (below); `register_definitions`
        # only globs the spec's libraries and imports them -- it does not read spec.database
        # -- so the unmodified spec is used everywhere and isolation comes from the scratch
        # current-database + the graph default below.
        cap = _Capture().run(spec_path, spec)

        # Rewrite qualified sibling reads to this pipeline's own namespace down to the
        # bare name, so they resolve to the scratch-namespace seed/graph and a stale
        # `<catalog>.<database>.<name>` table can never satisfy them (UNqualified reads
        # are already shadowed by the seeded temp views + scratch current-db). Active for
        # BOTH the seeding and the authoritative analysis so they resolve identically.
        with _rewrite_internal_table_refs(cap.dataset_names, catalog, database):
            seeded = preseed_sibling_schemas(cap, catalog, database)
            print(f"[sdp-dryrun] pre-seeded sibling dataset schemas: {sorted(seeded)} "
                  f"(isolated db `{catalog}`.`{scratch}`)", file=sys.stderr)

            dataflow_graph_id = create_dataflow_graph(
                spark, default_catalog=catalog, default_database=scratch,
                sql_conf=spec.configuration)
            registry = SparkConnectGraphElementRegistry(spark, dataflow_graph_id)
            register_definitions(spec_path, registry, spec, spark, dataflow_graph_id)
            result_iter = start_run(
                spark, dataflow_graph_id, full_refresh=[], full_refresh_all=False,
                refresh=[], dry=True, storage=spec.storage)
            handle_pipeline_events(result_iter)
    finally:
        for name in seeded:
            try:
                spark.catalog.dropTempView(name)
            except Exception:  # noqa: BLE001
                pass
        if created_scratch:
            try:
                spark.sql(f"DROP SCHEMA `{catalog}`.`{scratch}` CASCADE")
            except Exception:  # noqa: BLE001
                pass
        spark.stop()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Graph-aware SDP dry-run gate (sibling-windowing safe).")
    ap.add_argument("--spec", required=True, help="Path to the pipeline spec (spark-pipeline.yml).")
    args = ap.parse_args(argv)
    spec_path = Path(args.spec)
    if not spec_path.is_file():
        from pyspark.errors import PySparkException
        raise PySparkException(
            errorClass="PIPELINE_SPEC_FILE_DOES_NOT_EXIST",
            messageParameters={"spec_path": args.spec})
    run_dry_run(spec_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())

# Part 3 — Agent-native GitOps for Spark (declarative slice)

A minimal, end-to-end, **honest** demonstration that a declarative pipeline (Spark
Declarative Pipelines, SDP) can be driven entirely through GitOps by an agent that
**never holds a Spark session**, and that this is *exactly why imperative pipelines
can't be GitOps'd the same way* — an imperative job **owns** a session, so there is
nothing declarative to hand to a reconciler.

This directory is **additive**. It does not modify the study harness, the SDP arms,
grading, or manifests. It *reuses* the study's real agent brain (`AnthropicBrain`
from `harness/backends/live.py`) under the **Arm-B** loop config (SDP + dry-run gate
+ safety skill).

## The loop

```
  ┌─────────────┐   files+git+PR    ┌────────────┐   spark-pipelines dry-run   ┌──────────┐
  │   AGENT     │ ────────────────▶ │   PR / CI  │ ──────────────────────────▶ │  human   │
  │ (no session)│   authors SDP     │ (session)  │   REAL structural gate       │ reviewer │
  └─────────────┘   artifact        └────────────┘                              └────┬─────┘
                                                                                     │ merge
                                                                                     ▼
                                                              ┌───────────────────────────┐
                                                              │  CONTROLLER (session)      │
                                                              │  spark-pipelines run        │
                                                              │  = reconcile to desired     │
                                                              └───────────────────────────┘
```

1. **Author.** `agent_pr_author.py` asks the study's Arm-B brain for SDP code, renders
   it into a declarative artifact (`sdp_artifact.py`), and does *only* git + gh:
   `git checkout -b agent/gitops/<slug>-<ts>` → `git add` → `git commit` (with a
   `Co-authored-by: omnigent` trailer) → `git push` → `gh pr create`.
2. **Gate (PR / CI).** `.github/workflows/gitops-sdp-dry-run.yml` runs the **real**
   `spark-pipelines dry-run` on each changed spec (`changed_pipelines.py` lists them).
   A structural defect fails the PR *before* any compute-heavy execution.
3. **Reconcile (merge).** `.github/workflows/gitops-sdp-reconcile-local.yml` runs
   `spark-pipelines run` (via `reconcile.py`) on push-to-main to materialize the
   pipeline — the reconcile step that brings actual state to the declared desired state.

The agent's entire footprint is **files + git + PR**. Only CI and the controller ever
receive `SPARK_REMOTE` or a Spark session.

## File inventory

| File | Role | Holds a session? |
|------|------|------------------|
| `agent_pr_author.py` | agent surface: propose SDP → render → git/gh | **No** (refuses if `SPARK_REMOTE` set) |
| `sdp_artifact.py` | render `spark-pipeline.yml` + `transformations/pipeline.py` | No |
| `changed_pipelines.py` | list changed specs for CI (git diff / scan) | No |
| `local_spark_connect.sh` | start/stop a runner-local Spark Connect server | n/a (infra) |
| `ensure_schema.py` | create `spark_catalog.gitops_demo` before gate/reconcile (idempotent) | **Yes** (controller) |
| `reconcile.py` | merge step: `cli.py run --spec` over `SPARK_REMOTE` | **Yes** (controller) |
| `pipeline-definitions/` | versioned declarative source of truth (agent PRs add subdirs) | No |
| `tasks/orders_silver_gold.json` | sample task brief | No |
| `data/orders_sample.ndjson` | tiny self-contained messy dataset | No |
| `PRODUCTION_EKS.md` | documents (does not enable) the EKS target | n/a |
| `tests/` | artifact-shape + safety-boundary unit tests | No |
| `.github/workflows/gitops-sdp-dry-run.yml` | PR gate (dry-run) | Yes (CI) |
| `.github/workflows/gitops-sdp-reconcile-local.yml` | merge reconcile (run) | Yes (controller) |

## Safety boundary

The thesis is an **asymmetry of capability**. Who can do what:

| Identity | can write files | can open PR | has `SPARK_REMOTE` | can run SDP CLI | has a session |
|----------|:---------------:|:-----------:|:------------------:|:---------------:|:-------------:|
| **Agent (PR author)** | ✅ | ✅ | ❌ | ❌ | ❌ |
| **PR CI (dry-run gate)** | ❌¹ | ❌ | ✅ | ✅ (`dry-run`) | ✅ (driver-only) |
| **Merge reconcile (controller)** | ❌¹ | ❌ | ✅ | ✅ (`run`) | ✅ (full) |
| **Human reviewer** | ✅ | ✅ (approve/merge) | ❌ | ❌ | ❌ |

¹ CI/controller check out the repo read-only for the run; they do not author pipeline
source. The source of truth only changes via an agent/human PR that a human merges.

### The boundary is enforced, not cosmetic

`agent_pr_author.py`:

- imports **no** pyspark (proven by a clean-interpreter test: `pyspark` never enters
  `sys.modules`, even after the Arm-B brain is constructed);
- **refuses to run** (exit nonzero) if `SPARK_REMOTE` is set in the environment;
- never instantiates `ConnectExecutor` / opens a `SparkSession`;
- never invokes `spark-pipelines` / `spark-submit` / `cli.py`;
- funnels every subprocess through an **allowlist of `git` and `gh` only**.

These are asserted in `tests/test_agent_pr_author_no_spark.py` (source-inspection +
monkeypatch + subprocess).

### Why imperative can't be GitOps'd this way

An imperative PySpark job *is* its execution: it acquires a `SparkSession`, reads,
transforms, and writes — the program and the run are the same artifact. To "apply"
it you must run it, which means the actor that owns the file also owns a session.
SDP separates **desired state** (a declarative spec + `@dp.table` definitions, which
are inert text) from **reconciliation** (`spark-pipelines run`, owned by the
controller). Only that separation lets the author be session-free.

## Tie-in to the study (H1 / H2)

This slice operationalizes two **pre-registered** hypotheses from
[`../PREREGISTRATION.md`](../PREREGISTRATION.md):

- **H1 (safety).** Agents in the SDP-guardrailed loop (Arm B) ship a lower
  **silent-defect rate** than unconstrained PySpark agents (Arm A). The CI dry-run
  gate here is the same structural screen that, in the study, catches silent
  structural defects *pre-merge*.
- **H2 (compute).** The dry-run gate reduces **compute-to-correct** (executor-seconds
  and $) by catching structural defects pre-execution — `dry-run` analyzes the plan
  driver-only (no executors, no data materialized) versus a full execute-to-debug
  iteration. No measured magnitudes are claimed here; the study quantifies the effect.

**Instrumentation.** In the study, H1 is measured as the fraction of final outputs
failing an output oracle, and H2 as executor-seconds/USD to first correct output
(read from the Spark REST API — see `harness/backends/live.py`). In *this* slice the
gate's pass/fail and wall-time are visible per spec in the CI logs; the slice is the
delivery mechanism, the study is the measurement.

### Gate scope (per `PREREGISTRATION.md`)

The dry-run gate is a **structural** screen. Pre-registered prediction (prereg §
defect table):

- **Caught (structural):** D1 missing/unresolved column (`UNRESOLVED_COLUMN`,
  42703), D4 broken DAG / missing upstream (`TABLE_OR_VIEW_NOT_FOUND`, 42P01),
  D5 immutable config mutation (`CANNOT_MODIFY_CONFIG`, 46110).
- **NOT caught (semantic / state, by design):** D2 timestamp misparse, D3
  unwatermarked dedup, D6 nondeterministic dedup, D7 timezone/day-bucket error,
  D8 absent quarantine / silent `$` drop, D9 unbounded state.

The gate is a cheap structural filter, **not** a correctness oracle. Semantic
correctness is the human reviewer's job (and the study's output oracles').

## Empirical backing (to be populated from the study's clean runs)

> **Honesty note.** The sweep / pilot has **not run yet**. No measured defect-rate or
> compute numbers are stated anywhere in this slice. The cells below are placeholders
> to be filled from the study's clean runs; until then they are intentionally empty.

| Metric | Arm A (imperative, no gate) | Arm B (SDP + gate) | Source |
|--------|:---------------------------:|:------------------:|--------|
| Silent-defect rate (H1) | _TBD_ | _TBD_ | study clean runs |
| Compute-to-correct, executor-s (H2) | _TBD_ | _TBD_ | study clean runs |
| Structural defects caught pre-merge | _TBD_ | _TBD_ | study clean runs |

## Run it locally (mocked, no API, no Spark)

The file-generation path is exercised without the Anthropic API or Spark by mocking
the brain's `propose()`:

```bash
cd experiments/safe_agent_study
python3 -m pytest gitops_demo/tests/ -q
```

The real PR-authoring path (live brain) and the real dry-run/reconcile gates require
an API key, a Spark Connect server, and a GitHub remote — these run in CI / on the
live cluster, not here.

### Verified locally (gate mechanism, not study magnitudes)

The bootstrap + gate were smoke-tested end to end against a real Spark Connect server
(`pyspark[connect]` 4.x, JDK 17):

- `local_spark_connect.sh start` brings up the server and it becomes reachable on
  `sc://localhost:15055` (the bundled `spark-connect` jar's `SparkConnectServer`
  class is launched via `spark-submit`; a pip `pyspark` has no `sbin/start-connect-server.sh`).
- a valid spec dry-runs to `Run is COMPLETED` (exit 0);
- a structural defect (a transform reading a non-existent upstream table) **fails the
  gate** with `[TABLE_OR_VIEW_NOT_FOUND] ... SQLSTATE 42P01` (exit 1) — i.e. the
  pre-registered structural class **D4**, caught pre-merge.

Two requirements this surfaced (now wired into the workflows):

1. the default schema must exist — `ensure_schema.py` creates
   `spark_catalog.gitops_demo` before the gate/reconcile (otherwise the CLI errors
   with `SCHEMA_NOT_FOUND`, a *false* failure unrelated to the pipeline);
2. an SDP transform references the session via `spark = SparkSession.active()` at
   module top (the CLI imports the module; it does not inject a `spark` global). The
   sample task prompt instructs the agent accordingly. Example shape:

   ```python
   from pyspark import pipelines as dp
   from pyspark.sql import DataFrame, SparkSession
   spark = SparkSession.active()

   @dp.materialized_view
   def orders_gold_daily_revenue() -> DataFrame:
       return spark.read.table("spark_catalog.gitops_demo.orders_silver")  # ... aggregate
   ```

These are *mechanism* checks, not study measurements — no defect/compute magnitudes
are claimed (see the empirical section above).

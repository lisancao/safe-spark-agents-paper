# H3 — Data-Processing Compute (Executor-Seconds): Methodology, Measurement, and Raw-Data Specification

**Status: PLAN / SPECIFICATION. H3 has NOT been run. There are ZERO confirmatory H3 executor-seconds results.**
What exists today is (a) a validated measurement *instrument* (the Spark-UI stage-diff, proven once with an 80M-row micro-probe) and (b) a wall-clock *proxy* metric reported by the local run. Neither is a cross-arm A-vs-B compute result. This document defines the hypotheses, the measurement mechanism, the exact raw-data fields H3 will read, the code prerequisite that currently blocks it, and the uniform-substrate (EKS) requirement — and it is explicit throughout about what is *measured* versus *planned*.

Anchor artifacts (all under `/home/lnc/repos/ssa-powered-run/experiments/safe_agent_study/`): `harness/backends/live.py`, `harness/cost.py`, `harness/schema.py`, `harness/runner.py`, `DEVIATIONS.md` (D-5), `POWERED_HEADLINE.final.md`, `results.powered.AB.n12.final.jsonl`.

---

## 0. Naming disambiguation (read first)

Three unrelated things are called "H3" in this repository. This document is **only** about the third one.

| Sense | Where | Meaning | This doc? |
|---|---|---|---|
| H3 (mechanism/ablation) | `PREREGISTRATION.md:32-35` | Arms B1 (SDP-only) vs B2 (gate-only) — decomposes *where* SDP's safety comes from | No |
| H3 (bounded-state-under-load, E6) | `EXPERIMENT_DESIGN.md:19,135` | Broader lakehouse study, unrelated experiment | No |
| **H3 (data-processing compute, executor-seconds)** | Task #3 (`status=pending`); paper `§4.3`/`§6.2` under N2 cost taxonomy | Cluster/EKS-relevant compute cost, deferred to Phase 2b | **Yes** |

The directory `results/h3_a2_rerun_20260628/` is named "h3" but is **not** H3 compute data: it is an Arm-A2 re-run (arms `['A2','B','B1']`, `--backend local`, `headline_n_valid=False`, GLMM not fitted — "need >=2 arms incl. reference A"), logged as "a post-data instrument fix before any confirmatory H3 claim" (`DEVIATIONS.md:1140`). It contains no A-vs-B executor-seconds.

---

## 1. Hypotheses

Compute cost is split into two named surfaces under the N1/N2 cost taxonomy (paper §3.5): **N1 = token spend** (measured, done — not this doc) and **N2 = data-processing compute** (executor-seconds — this doc). H3 is the N2 pair.

### H3.1 — Wasted-compute-on-failed-attempts
**Statement.** SDP's dry-run gate rejects failed attempts *before* execution, so a rejected attempt processes ~0 data (a driver-only dry-run runs no executors). An imperative failure, by contrast, executes on the cluster and burns compute before failing. Therefore SDP should spend *less* executor-time on attempts that never reach a correct result.

**Direction:** SDP (gated arms B/B2) **lower**. **Prediction is directional and pre-registered.**

**Anti-bypass rule (iteration-level accounting).** Compute is counted at the **iteration/attempt** level, not only on the accepted program. Every attempt in the loop — including a gate-caught attempt that is later fixed — contributes its executor-seconds to the arm's total. A gate-caught (dry-run) attempt contributes measured executor-seconds ≈ 0 by construction (no executors run); an imperative attempt that executes and fails contributes its real, non-zero executor-seconds. This mirrors the §9.2 anti-bypass rule already applied to the defect taxonomy: a gate-caught-then-fixed error still counts.

### H3.2 — Total-compute-to-correct
**Statement.** Summed executor-seconds across all attempts up to (and including) the first correct/green completion, per (task, seed) cell, compared A vs B.

**Direction: OPEN.** SDP may run *more* total iterations (propose → gate-reject → repair loop) even if each rejected attempt is cheap; whether the gated loop's total-compute-to-correct is higher or lower than imperative execute-only is a genuinely open empirical question. The pilot wall-clock proxy showed SDP *higher* (18.5 s vs 10.2 s), **but that figure is a wall-clock proxy on a split substrate — it is not data-compute and must not be cited as the H3.2 result.**

**Joint interpretation with H5.3 (cost-adjusted efficacy).** H3 must be read alongside H5.3: extra SDP iterations/compute are only a true cost if they do *not* buy completion. The reporting unit is **cost-per-correct-completion**, not raw compute. H5.3 is itself blocked on H2 (Arm-B tokens) and H3 (per-attempt compute on a uniform substrate).

---

## 2. Measurement mechanism — the Spark-UI stage diff (D-5)

### 2.1 What is measured, per attempt
For each execute iteration, the harness attributes the compute of *only that attempt* via a **before/after diff of Spark stages** against the driver Spark-UI REST API. In `ConnectExecutor.run_execute` (`live.py:864-897`):

1. `before_ids = _stage_ids_snapshot()` — `GET {spark_rest_url}/api/v1/applications/{app_id}/stages`, collect the set of `stageId`s (`live.py:870, 934-935`). `app_id` is resolved once and cached from `GET {spark_rest_url}/api/v1/applications → [0]["id"]` (`live.py:930`).
2. Run the cell via `_run(...)` (`live.py:871`).
3. `exec_s, cpu_s = _stage_compute_since(before_ids)` (`live.py:872`) — `GET /stages` again; **NEW+COMPLETE** stages are those whose `stageId ∉ before_ids` **AND** `status == "COMPLETE"` (`live.py:978-979`), then:

```
executor_seconds = sum(stage["executorRunTime"] for new_complete) / 1000.0   # ms → s  (live.py:980)
cpu_seconds      = sum(stage["executorCpuTime"] for new_complete) / 1e9       # ns → s  (live.py:981)
```

The two values are returned on `ExecOutcome.executor_seconds` / `ExecOutcome.cpu_seconds` (`live.py:891-892`). The diff wraps `run_execute` **uniformly for both the SDP and the imperative branch** (`live.py:864-869`) — H2/H3 compares them, so they must be measured identically. A **driver-only dry-run gate runs no executors**, so it has nothing to measure and contributes the `None` sentinel (0 executor-seconds by construction), not `0.0`.

### 2.2 Why the cumulative-counter method was invalid (the D-5 correction)
The earlier method read executor-seconds as a before/after delta of the Spark REST `/executors` summed `totalDuration`. That is **invalid on this substrate** and was replaced (`DEVIATIONS.md:328-344`; `live.py:597-600`):

- The live cluster runs **one long-lived, shared `Spark Connect server` application**. Its `/executors` `totalDuration` is **application/driver uptime, not task compute** — it **increments while the cluster is idle**. A before/after delta around a run therefore charges compute for wall-clock the app was merely alive, double-counting and swamping the real per-run signal.
- A cumulative-counter delta is only valid for a **short-lived, single-run** application. The stage diff, by contrast, attributes compute to the specific stages that newly reached `COMPLETE` in the run window.

### 2.3 Why the stage diff works across a subprocess session
The agent's Spark job runs in a **subprocess with its own Connect session**, so a job *tag* cannot cross sessions. The stage diff can, because the harness runs cells **sequentially**: the only stages that newly reach `COMPLETE` within the before→after window belong to *this* run (`live.py:606-609, 958-960`; `DEVIATIONS.md:358-363`). This **sequential-execution assumption** holds for the controlled sweep (one cell at a time) and is documented in the code.

### 2.4 Failure modes and sentinels
- **Graceful fallback.** If `spark_rest_url` is unset/`None` or any REST call raises, `_stage_compute_since` returns `(None, None)` and the run proceeds (`live.py:969-977`); a `None` before-snapshot short-circuits with no further REST call. `cost.py` then derives the wall-clock cross-check (§4). REST fetched via `urllib` in `_get_json` (`live.py:990-993`).
- **`None` is the unmeasured sentinel, never `0.0`** (`cost.py:81-85`). A fallback execute or a driver-only gate reports *unmeasured*, not a misleading measured zero. The aggregate sums only non-`None` values (`_sum_measured`, `cost.py:283-325`), so a fully-gated arm aggregates to `None`, not `0.0`.

### 2.5 Instrument validation — the 80M micro-probe (NOT a result)
The mechanism was proven once against the real cluster (`DEVIATIONS.md:364-368`): a subprocess `spark.range(80_000_000).sum()` produced NEW stages **[60, 62]** → measured **executor_seconds 1.246**, **cpu_seconds 0.878**, correctly attributing the subprocess job's compute while excluding the idle app counter.

> **This validates the INSTRUMENT. It is a single micro-benchmark, not an A-vs-B study result, and must never be presented as an H3 outcome.**

### 2.6 Stage-retention caveat (a hard prerequisite)
The diff sums only stages the UI still returns at the AFTER snapshot. The Spark UI retains the most recent `spark.ui.retainedStages` (default **1000**) stages, so a run creating more completed stages than the bound between BEFORE and AFTER could **undercount** (`live.py:923-929, 962-968`). `_stage_ids_snapshot` emits a `RuntimeWarning` when a live snapshot nears the bound. **For a valid H3 run, `spark.ui.retainedStages` must be raised above the largest cumulative per-window stage count** (Phase-2a was observed hitting 929/1000 on 2026-06-30). This is a real prerequisite, not a nicety.

---

## 3. Raw-data specification — exact fields

### 3.1 Transport out of the executor
`ExecOutcome.executor_seconds` / `ExecOutcome.cpu_seconds` (`base.py:177-178`, commented "measured PER-ITERATION (stage-diff)"), set from `exec_s`/`cpu_s` (`live.py:891-892`).

### 3.2 Per-iteration cost object (`IterationCost`, `cost.py:79-91`)
- `executor_seconds: Optional[float]` — measured stage-diff sum; **`None` sentinel, not 0.0**.
- `cpu_seconds: Optional[float]` — measured CPU-seconds; `None` if no live metric.
- `executor_seconds_wallclock: float` — the always-present wall-clock cross-check (0.0 for a gate).
- `usd`, `failed`, `intercepted_at_dry_run` (True ⇒ gate-caught, no executor cost).

Built by `execute_iteration_cost(...)` (`cost.py:103-140`): the measured value is authoritative for USD when present, else USD is priced on `wall_s × instances × busy_fraction` (`cost.py:127-139`).

### 3.3 Run-level aggregate (`RunCost`, `cost.py:214-226`)
`total_executor_seconds`, `executor_seconds_to_correct`, `total_cpu_seconds`, `cpu_seconds_to_correct`, `total_executor_seconds_wallclock`, `executor_seconds_wallclock_to_correct` — summed via `_sum_measured`, which drops `None` (a gated/fallback run aggregates to `None`, not `0.0`).

### 3.4 Serialized `ResultRow` (`schema.py`) — fields H3 reads
| Field | Type | Required? | Role for H3 |
|---|---|---|---|
| `executor_seconds` | number \| null | **NOT required** (nullable) | **Measured** run-total (H3.2 primary on uniform substrate) |
| `executor_seconds_to_correct` | number \| null | no | **Measured** compute-to-correct (H3.2) |
| `cpu_seconds` | number \| null | no | Measured CPU-seconds (secondary) |
| `cpu_seconds_to_correct` | number \| null | no | Measured CPU-to-correct |
| `executor_seconds_wallclock` | number | **required** (always present) | **PROXY** cross-check (pre-reg §8 anchor) |
| `executor_seconds_wallclock_to_correct` | number \| null | no | **PROXY** compute-to-correct (what the local run reports) |

Schema refs: `schema.py:104,113,119-122`; required list `:252`; JSON-schema types `:269-280`; validator `:193-203`. `run_cell` maps `RunCost → ResultRow` (`runner.py:841,849-853`). `executor_seconds` is deliberately nullable so an unmeasured/gated row is honest; `executor_seconds_wallclock` is the required always-present figure that pre-reg §8 anchors on now that the measured surface is nullable. Both a measured row and an unmeasured-gated row pass `validate_row` + `RESULTS_JSON_SCHEMA` + the published `results_schema.json` (drift-guarded by `tests/test_published_schema.py`).

### 3.5 Per-attempt fields H3.1 needs — and the code prerequisite (§6.6(3))
H3.1 (wasted-compute-on-failed-attempts) requires **per-attempt** compute, keyed on whether the attempt was gate-intercepted. The target fields are:

```
per_iteration[].executor_seconds        # measured, per attempt
per_iteration[].cpu_seconds             # measured, per attempt
per_iteration[].intercepted_at_dry_run  # True => gate-caught, ~0 executor cost
per_iteration[].usd, per_iteration[].wall_s   # for cost-per-correct
```

**These are NOT serialized today — this is the blocking code change.** In `run_episode` (`runner.py:147-299`), per-attempt compute reaches the in-memory `iter_costs` list via `execute_iteration_cost(..., measured_executor_seconds=exec_out.executor_seconds, measured_cpu_seconds=exec_out.cpu_seconds)` (`runner.py:253-257`), but the serialized per-iteration record stores only:

```python
rec["execute"] = {"failed": ..., "completed": ..., "error_class": ...}   # runner.py:258-259
rec["gate"]    = {"failed": ..., "error_class": ...}                      # runner.py:236
```

— **no executor_seconds / cpu_seconds / usd / wall_s per attempt.** `iter_costs` is consumed *only* by `costmod.aggregate(ep.iter_costs, ...)` (`runner.py:820`), which collapses it to the run-level `RunCost` totals; the per-attempt `IterationCost` values are then discarded (there is no per-iteration cost field in `schema.py`; `per_iteration` is `List[Dict]`, `schema.py:126`). `analyze.py` reads `per_iteration` only for error-class event counts (`analyze.py:710-714`), confirming no per-attempt compute is available downstream.

**Consequence:** the emitted `results.jsonl` `per_iteration` carries `{error_class, failed, completed, tokens, stop_reason}` but **not** per-attempt executor-seconds/cpu-seconds. Per-attempt compute cannot be reconstructed from existing artifacts (verified: `per_iteration[0]` keys = `['command','execute','iter','stop_reason','tokens']`; `execute` keys = `['completed','error_class','failed']`).

**Fix (≈10-line runner change + an analyze.py H3 reader):** stamp `exec_out.executor_seconds` / `cpu_seconds` (and `wall_s`, `usd`, `intercepted_at_dry_run`) into `rec["execute"]` (and the gate branch) *before* `per_iteration.append(rec)` at `runner.py:258-260`, then add the H3 reader in `analyze.py`. This is prerequisite (i) for the real EKS H3 run.

---

## 4. The proxy metric (what the local run actually reports)

Because per-attempt compute is not serialized and the local substrate is split (§5), the local run reported N2 via a **wall-clock proxy**, `executor_seconds_wallclock_to_correct`, explicitly disclaimed as "a proxy, not data-compute" (paper §4.3, L146).

**Definition.** `executor_seconds_wallclock = wall_s × instances × busy_fraction` (`cost.py:127`), always present, uniform across arms (so cross-arm comparable *as a proxy*), driven by a **declared** `executor_config` — a costing assumption, not a k8s measurement.

**Provenance (verified).** Env sidecar `results.powered.AB.n12.backfill.env.json`: `backend="local"`, `spark_remote="sc://localhost:15041/;user_id=alice"`, `git_sha ca48c8c…`, `spark_version 4.1.0.dev4`; `executor_config = {instances:4, cores_per_executor:4, memory_gb:16, price_usd_per_executor_hour:0.192, provider:"k8s", instance_type:"m5.xlarge-equivalent"}` — the `provider:"k8s"` is a costing label on a **local** run, not evidence of a k8s measurement.

**Published proxy headline** (`POWERED_HEADLINE.final.md`, metric `executor_seconds_wallclock_to_correct`; 528 rows = A 264 + B 264):

| gated arm | mode | n pairs | median exec-s saved | mean | 95% CI | $ saved | intercept frac |
|---|---|---|---|---|---|---|---|
| B | intention_to_treat | 264 | 9.1 | −12.0 | [−23.8, −2.9] (sig) | $0.34 | 69.5% (353/508) |
| B | complete_case | 251 | 9.1 | −3.5 | [−9.5, +1.6] (spans 0) | $0.32 | 69.5% (353/508) |

> The proxy shows the gate rejecting failed attempts for ≈0 compute (ITT median 9.1 exec-s saved); the complete-case CI spans 0. **This is a proxy, not the H3 result.** The pilot 18.5 s vs 10.2 s figure is likewise proxy and substrate-confounded.

**Measured-but-not-comparable field (present, deliberately NOT the headline).** The final jsonl also carries 284 non-null `executor_seconds` / `executor_seconds_to_correct` and 264 non-null `cpu_seconds` values (verified). These come from the **local split substrate** — imperative arms use classic `local[*]` `/executors.totalDuration` delta; SDP arms use a local Connect stage-diff — and per `DEVIATIONS.md` D-7 follow-up 4 (lines 575-593) the two mechanisms are **non-comparable across arms**. That is why the local H2/N2 primary is the wall-clock proxy. **These are NOT H3 executor-seconds and must not be presented as A-vs-B compute.**

---

## 5. The uniform-substrate requirement (why EKS is needed)

The validated `local` backend **splits by paradigm** (`runner.py:1204-1239`; `local_connect.py`): imperative → classic local Spark (`local[*]`, `/executors.totalDuration` delta, `cpu_seconds=None`); SDP → local Spark Connect (stage-diff). Two different compute-measurement mechanisms ⇒ **local A-vs-B compute is not apples-to-apples**. A field check confirms the asymmetry: `executor_seconds` populated counts by arm are ~`{A:2, A2:1, B:60, B1:61, B2:1}` — only the SDP/Connect arms carry it (paper SECTION1 §5, L165).

**Scope of the threat.** The substrate split is a threat **only for the deferred N2 data-compute claim** (paper §5, L150). H1 (safety), H2 (tokens), H4 (conciseness) are substrate-independent and unaffected.

**Resolution.** Both arms must run on **one** substrate = the **`live`/Connect** backend, whose `ConnectExecutor` handles both paradigms with the identical stage-diff (`live.py:569-581, 835-849`). On EKS the `live` substrate was stood up on cluster `${EKS_CLUSTER}` (2026-06-24); Arm A materialized silver/gold/quarantine tables remotely and the in-cluster stage-diff was demonstrated (the 80M probe). The remaining work is **completing the live run with per-attempt compute serialized — not building the capability.**

---

## 6. Current state — measured vs planned

| Item | Status | Evidence |
|---|---|---|
| H3.1 / H3.2 hypothesis statements | **Written** (locked prose) | paper §6.2 L178-180 |
| Stage-diff measurement mechanism | **Built + validated once** (80M probe: exec_s 1.246, cpu_s 0.878) | `live.py:864-982`; `DEVIATIONS.md:364-368` |
| Wall-clock proxy metric | **Reported** (local run, disclaimed as proxy) | `POWERED_HEADLINE.final.md`; §4 above |
| Cross-arm A-vs-B executor-seconds (the H3 result) | **NOT run — ZERO results** | Task #3 `status=pending` |
| Per-attempt compute serialization | **NOT done** (blocking code change) | §3.5; `runner.py:258-260` |
| Uniform Connect substrate for both arms (full run) | **NOT done** (Arm A + stage-diff demonstrated; Arm B SDP-green blocker) | §5; `DEVIATIONS.md:184-185` |

**Bottom line:** the only compute numbers that exist are (a) the wall-clock proxy from the local run and (b) the single 80M instrument-validation probe. No confirmatory H3 executor-seconds have been collected on any substrate. **No H3 numbers are fabricated in this document.**

---

## 7. Phase-2b run plan (EKS uniform Connect)

The real H3 run is gated behind five prerequisites (paper SECTION1 §9.3; `repro/REPRODUCE.md`):

1. **Per-attempt compute serialized** (§3.5) — stamp `executor_seconds`/`cpu_seconds`/`usd`/`wall_s`/`intercepted_at_dry_run` into `rec["execute"]`/`rec["gate"]` at `runner.py:258-260`; add the `analyze.py` H3 reader (≈10-line runner change + reader).
2. **Remote Arm-B SDP completes + grades green on EKS** — the D-2 → D-3 → PIPELINE_SPEC blocker chain must be clear (Arm A + stage-diff already demonstrated 2026-06-24). "Green" = SDP prints `Run is COMPLETED`/rc==0 **and** the contract output table reads back (`live.py:874-886`).
3. **`spark.ui.retainedStages` raised above the largest per-window cumulative stage count** — else the stage diff silently undercounts (default 1000; Phase-2a hit 929/1000).
4. **Cost-bounded task/seed subset behind a spend GATE** — no dollar spend-cap exists in code today; the real controls are `cost.py`'s USD-per-executor-hour price (`0.192`), `max_iterations=12`, and the per-iteration exec-time budget (`AGENT_EXEC_TIMEOUT_S`, hard 150 s). A subset + budget gate must be added before spend.
5. **UC-OSS / Iceberg full-refresh truncate semantics tolerated** by the reconcile/grade path (Iceberg JDBC catalog cannot truncate on MV refresh; confirmed UC-OSS-only 2026-07-02; does not affect local Phase-2a).

**Recommended scope.** EKS smoke first (2–3 tasks × 1 seed × arms A,B on the `live` Connect substrate), then a ~40-cell cost-bounded run behind the spend gate. Both arms on the single Connect substrate; per-attempt compute serialized; `retainedStages` sized above the per-window stage count; driver Spark-UI REST reachable (else the stage-diff degrades to `(None,None)` + the wall-clock cross-check).

**Reporting on completion.** Report H3.1 as per-attempt executor-seconds split by `intercepted_at_dry_run` (expected: gate-caught ≈ 0, imperative-fail > 0); H3.2 as `executor_seconds_to_correct` A-vs-B (direction open), both with CPU-seconds secondary and the wall-clock proxy retained as an independent cross-check. Interpret jointly with H5.3 as cost-per-correct-completion. Until then, N2 remains **proxy-only and explicitly deferred.**

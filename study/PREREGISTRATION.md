# Pre-Registration — Safe Agentic Spark Development Study

**Status:** PRE-DATA. Committed before any experimental runs are collected.
**Date registered:** 2026-06-23
**Authors:** safe-spark-agents project (orchestrated build)
**Design calibrated against:** Asawa, Zhu, O'Neill, Zaharia, Dimakis, Gonzalez,
*How to Train Your Advisor: Steering Black-Box LLMs with Advisor Models*,
arXiv:2510.02453v2.

> This document fixes the hypotheses, arms, metrics, sample-size rule, and
> analysis plan **before** data collection. Any deviation made after seeing data
> is logged in `DEVIATIONS.md` with rationale. The intent is that every headline
> number is traceable to a procedure declared here.

---

## 1. Research question

Does constraining an AI coding agent's Spark development loop with **Spark
Declarative Pipelines (SDP) + a safety skill + a fast structural dry-run gate**
make it ship **fewer silent data-correctness defects** than an unconstrained
imperative-PySpark agent — and does **dry-run iteration save compute** relative
to execute-to-debug?

## 2. Hypotheses (directional, pre-specified)

- **H1 (safety).** Agents working in the SDP-guardrailed loop (Arm B) ship a
  lower **silent-defect rate** than unconstrained PySpark agents (Arm A).
- **H2 (compute).** The dry-run gate reduces **compute-to-correct** (executor-
  seconds and $) versus execute-to-debug iteration, by catching structural
  defects pre-execution.
- **H3 (mechanism / ablation).** The safety improvement is attributable to
  identifiable components: SDP structure (Arm B1) and/or the dry-run gate
  (Arm B2), not merely "a different tool." We report the decomposition rather
  than assuming it.

### Falsification conditions (declared up front)
- H1 is **not supported** if the Arm A vs B silent-defect-rate difference has a
  95% CI that includes 0, or the paired effect is not significant at the
  pre-set threshold (§7).
- H2 is **not supported** if gated compute-to-correct is statistically
  indistinguishable from or greater than execute-only.
- We commit to reporting these outcomes **as-is**, including nulls.

## 3. Arms (the loop is the ONLY manipulated variable)

All arms use the **same base coding model**, the **same task prompts**, the
**same input data per matched seed**, and the **same live Spark Connect
backend**. Only the development loop differs.

| Arm | Loop | Purpose |
|-----|------|---------|
| **A** | Imperative PySpark, execute-to-debug, no gate | Baseline ("traditional") |
| **B** | SDP + safety skill + structural dry-run gate | Full treatment |
| **B1** | SDP only (no safety skill, no dry-run gate) | Ablation: is it SDP structure? |
| **B2** | Imperative PySpark + dry-run gate only (no SDP) | Ablation: is it the gate alone? |

Ablations B1/B2 are mandatory: they convert "B is better" into "*here is which
component does the work*."

## 4. Task corpus & defect oracles

**Tasks.** Drawn from and extended beyond the existing SDP pipeline corpus
(`pipelines/p1_medallion` … `p5_mart`) plus the Kafka orders silver/gold task.
Target: **≥12 tasks** spanning medallion ETL, CDC, windowed aggregation,
fan-out, and marts, each presenting opportunities for one or more defect classes
below. Final task list is frozen in `TASKS.lock.json` at registration time.

**Defect taxonomy (ground-truth oracles, reused from the E3 battery).** Each
defect class has an automated oracle — NO human grading:

| ID | Class | Detectable at dry-run? | Oracle |
|----|-------|------------------------|--------|
| D1 | Missing/unresolved column | Yes (structural) | `UNRESOLVED_COLUMN`, SQLSTATE 42703 |
| D2 | Wrong type / timestamp misparse | No (semantic) | quantifier: misparsed rows |
| D3 | Unwatermarked dedup | No (state) | runtime/scale only |
| D4 | Broken DAG / missing upstream | Yes (structural) | `TABLE_OR_VIEW_NOT_FOUND`, 42P01 |
| D5 | Immutable config mutation | Yes (structural) | `CANNOT_MODIFY_CONFIG`, 46110 |
| D6 | Nondeterministic dedup | No (semantic) | quantifier: conflicting-payload keys |
| D7 | Timezone/day-bucket error | No (semantic) | quantifier: local-vs-UTC day drift |
| D8 | Absent quarantine / silent drop | No (semantic) | quantifier: non-null values lost from aggregate ($ dropped) |
| D9 | Unbounded state | No (state) | cluster-scale only |

The **structural vs. semantic/state split is itself a pre-registered prediction**:
the dry-run gate should catch D1/D4/D5 for free and miss D2/D3/D6/D7/D8/D9. This
is the honest two-sided story — guardrails are not magic.

## 5. Primary & secondary outcomes

**Primary — H1:** **silent-defect rate** = fraction of runs whose final output
reaches `COMPLETED`/materialized while containing ≥1 oracle-detected defect.
(A defect caught and fixed before completion does NOT count as silent.)

**Primary — H2:** **compute-to-correct** = executor-seconds and USD consumed to
reach the first correct output, per task, Arm A (execute-only) vs the gated
loop. Reported with the % of failing iterations intercepted at dry-run.

**Secondary / honesty controls:**
- Per-defect-class detection rate (structural vs semantic breakdown).
- Fraction of defects caught **pre-execution** vs at runtime vs never.
- Iterations-to-green and wall-clock-to-green.
- **Task success rate** (retention check): proves the guardrailed loop does not
  simply reduce completion or stall agents — mirrors the exemplar's
  capability-retention check.

## 6. Sample-size rule (power-driven, no upper cap)

Staged, declared in advance:
1. **Calibration (1 task, all arms):** validate harness + cost instrumentation;
   produce a per-run cost figure.
2. **Pilot:** **N = 10 seeds/arm/task** across the frozen corpus. Estimate the
   between-seed variance of the silent-defect rate.
3. **Power analysis:** compute the N needed for a 95% CI half-width ≤ 0.05 on the
   A−B difference (two-proportion, paired-by-task). Run **at least** that N.
   No upper bound — additional seeds only tighten intervals.
4. Seeds are fixed integers, listed in `SEEDS.lock.json`, published with results.

## 7. Statistical analysis plan (pre-specified)

- **Effect estimate:** A−B difference in silent-defect rate, paired by task.
- **Intervals:** bootstrap 95% CIs, **percentile method, B = 10,000 resamples**,
  resampling at the (task, seed) level. CI computation method, seed, and run
  count are all reported — explicitly closing the exemplar's stated rigor gap.
- **Inference:** mixed-effects logistic regression of `silent_defect ~ arm` with
  random intercepts for **task** and **seed**; report odds ratios + marginal
  effects. Primary threshold **α = 0.05**.
- **Multiple comparisons:** Holm correction across the arm contrasts
  (A vs B, A vs B1, A vs B2, B vs B1, B vs B2).
- **H2:** paired comparison of compute-to-correct (A vs gated); report median,
  mean, 95% CI, and total $ saved at the measured scale.

## 8. Reproducibility commitments (intended to exceed the exemplar)

- Frozen `TASKS.lock.json`, `SEEDS.lock.json`, defect oracles, and arm manifests.
- Every run row records: Spark version, image digest, git SHA, base-model id,
  seed, executor config, wall time, executor-seconds, exit/error class.
- Harness, oracles, raw `results.jsonl`, and analysis notebook released in-repo.
- Headline claims in the writeup must cite the metric, N, CI, and test.

## 9. Threats to validity (acknowledged pre-data)

- **Base-model stochasticity** → addressed via multiple seeds + random-effects model.
- **Task-selection bias** → corpus frozen pre-data; defect opportunities balanced
  across classes; per-class reporting prevents averaging away misses.
- **Oracle incompleteness** → oracles detect the declared D1–D9 classes only;
  undetected defect types are out of scope and stated as such.
- **Single-environment** → one Spark version/cluster; optional cross-model run
  (same loop, second base model) to test generalization if pursued.

---

## Addendum (pre-data amendments)

These items amend the registration **before any experimental data is collected**
(per the §preamble contract). Each is a pre-data addition, logged here and in
`DEVIATIONS.md`, that changes the instrument's arms/hypotheses without touching the
already-collected results (there are none). Items **A–J** are the pre-data
instrument revisions already recorded in `DEVIATIONS.md` (the corpus-v3 / H4 set,
**D-CORPUS-V3**, which also added the moderation hypothesis **H4** narrated as
Part 1.5). Items **K onward** are registered below; **K** is the Arm A2 item already
cited by `arms/A2.json`.

### Item K — Arm A2 (paradigm-matched imperative control)

The §3 arm table is extended with one arm:

| Arm | Loop | Purpose |
|-----|------|---------|
| **A2** | Imperative PySpark **+ safety skill + structural dry-run gate** | Pure paradigm contrast for B |

A2 is a byte-for-byte mirror of **Arm B** EXCEPT the paradigm: B is **SDP** +
skill + gate; A2 is **imperative PySpark** + the SAME skill + the SAME gate (its
`allowed_commands` are the paradigm-bound imperative equivalents, identical to the
imperative arm B2, since the SDP CLI is incoherent for an imperative executor). The
identical-except-loop invariant (`harness/arm_manifest.assert_identical_except_loop`)
still holds: every controlled field is identical across all five arms; only the loop
fields differ, and A2 differs from B in `paradigm` (and the paradigm-bound
`allowed_commands`) ALONE.

**Why A2 is needed.** The headline A-vs-B contrast confounds *three* things at once
(paradigm **and** gate **and** skill). **B-vs-A2 holds the gate and skill constant**,
so any B-vs-A2 difference is attributable to the **declarative-vs-imperative
paradigm** itself — the clean paradigm contrast. A2 participates in the descriptive
per-arm tables; it is **not** added to the pre-registered 5-contrast Holm family for
H1 (that family — A-B, A-B1, A-B2, B-B1, B-B2 — is fixed at original registration and
unchanged), so no multiple-comparison budget is silently altered.

### Item L — H5 (conciseness / decision-surface area)

- **H5 (conciseness).** A declarative agent (Arm B, SDP) ships a **smaller decision
  surface** — it writes **less code** to pass the same gate on the same task — than
  the paradigm-matched imperative control (Arm A2). Measured on the **final accepted
  program** (the agent-authored source that reached `COMPLETED`), captured per cell
  with the results.

**Metrics (pre-specified).** Two size metrics, each reported BOTH **raw** and
**transform-body-only**:
- `final_program_loc` — non-blank, non-comment source lines. Comment/blank detection
  is **token-based** (Python's `tokenize`): only genuine `COMMENT` tokens are
  excluded, so a `#`-leading line **inside a string/docstring** is correctly counted
  as code (it is part of a string statement, not a comment).
- `ast_node_count` — total Python `ast` nodes.

The **body-only** variants exclude only the mandatory, **decision-free** scaffolding,
computed identically for both paradigms, so declarative is **not penalised** for the
bare `@dp` wrapper + `def` it is required to write while the imperative program's
hand-rolled `SparkSession` + input/output plumbing legitimately counts as its own
decision surface. Excluded scaffolding = `import` statements, the `def`/`class`
**header** (signature through the colon), and **BARE** structural decorators only.

A decorator is treated as **scaffolding (strippable) iff it carries NO arguments**
(`@dp.table`, `@dp.materialized_view`, `@dp.view`, `@dp.table()`); a decorator with
**any** positional or keyword argument is **logic-bearing and COUNTED** in body-only.
This is essential for validity: in SDP a `@dp` decorator routinely encodes agent
**decisions** — data-quality expectations (`@dp.expect(...)` / `expect_all` /
`expect_or_drop`), `table_properties`, `partition_cols`/`cluster_by`, schema hints, the
chosen `name`/`comment`. Stripping every decorator would drop those declarative
decisions from body-only while the equivalent imperative quality logic (a `.filter(...)`,
a `partitionBy(...)`) stays counted — quietly biasing the metric toward the hypothesis
under test. The predicate is **paradigm-agnostic** (never special-cased by arm or
decorator name): the same scaffolding-vs-logic test applies to any decorator in either
arm. Definitions are single-sourced in `harness/program_metrics.py`.

**Nullability on parse failure (explicit).** "Nullable" does **not** mean "all fields
null." An empty/None program → all four fields null. A program that **fails to
`ast.parse`** (a `SyntaxError`) keeps the (token-based) raw `final_program_loc` — still
a meaningful size — and nulls only the three parse-dependent fields
(`final_program_loc_body`, `ast_node_count`, `ast_node_count_body`).

**The `spark-pipeline.yml` decision (explicit).** The SDP project's
`spark-pipeline.yml` is **EXCLUDED** from both metrics. It is harness boilerplate
(`runner._sdp_spec` emits it from the study config — catalog/database/storage/glob,
**no agent logic**, enforced by the no-leak guard in
`tests/test_workspace_contract.py`); the agent never authors it. Counting it would
attribute harness-written YAML to the declarative agent and inflate the very surface
under test. Conciseness is therefore an apples-to-apples comparison of the two
agent-authored Python programs.

**Analysis.** Paired over (task, seed) on the **B-vs-A2** contrast, reusing the §7
percentile bootstrap (B = 10,000, resampling at the (task, seed) level); the
difference is reported as **A2 − B** (positive ⇒ declarative is smaller), with the
paired mean, median, and 95% bootstrap CI per metric. Cells where either arm never
completed carry a null metric and are dropped from the pairing (no final program to
measure).

**Falsification.** H5 is **not supported** if the paired A2 − B difference on
`final_program_loc` (and its body-only variant) has a 95% CI that includes 0. As with
H1/H2, the outcome is reported as-is, including a null.

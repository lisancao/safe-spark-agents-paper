# Agent-Native Spark Development Under Guardrails: A Pre-Registered Study of Imperative PySpark versus Spark Declarative Pipelines, with the Spark Connect Loop, GitOps Deployment, and Evaluation Harness That Support It

> **Status:** Draft master document for the safe-agent study. The experimental sweep has not yet produced clean, reportable numbers. Every empirical result, estimate, cost, p-value, effect size, or figure in this draft is intentionally a placeholder marked **`[PENDING clean run — N=TBD]`**. The infrastructure, harness, skill, and GitOps descriptions are grounded in repository artifacts that exist and (where noted) have been smoke-tested; the *experimental outcomes* are the only thing held back pending a clean live run.

## How to read this document

This is the single master document for the study. It is deliberately exhaustive so the team can review the whole program — design, substrate, infrastructure, and apparatus — in one place. It is organized into four major parts, each self-contained and grounded in the repository:

- **Part 1 — SDP versus imperative: the pre-registered experiment.** The controlled comparison itself: hypotheses H1/H2/H3, arms and shared controls, the frozen task corpus and dataset generators, the empirical defect taxonomy (D1, D2, D4–D8 — the seven classes the offline grader measures), blind grading, exit classes, metrics, the statistical plan, and the results *shells* (every empirical cell is an explicit `[PENDING clean run — N=TBD]`).
- **Part 2 — SDP on Spark Connect: the agent-native development loop.** Why Spark Declarative Pipelines (SDP) is a *safer agent surface*. The thesis of this part is a single control-boundary claim: **SDP enforces a strict control boundary — the agent authors; the system orchestrates and executes.** The agent writes declarative transforms only; the SDP runtime builds the dataflow graph, resolves inter-table dependencies, and materializes outputs. We cover the empirically-verified open-source SDP API and the Spark Connect compatibility matrix, and we argue that imperative-PySpark-fails-on-Connect is a *paradigm-intrinsic finding*, not a harness artifact.
- **Part 3 — Zero-Trust Agent Boundaries.** The trust boundary that enforces the agent-safety invariant — *the agent never holds a Spark session and never executes* — at two independent layers: the runtime substrate (mTLS termination and server-side principal pinning that fail closed on a forged identity) and a GitOps workflow in which an agent authors a declarative SDP artifact, opens a pull request, CI runs the *real* `spark-pipelines` dry-run as the agent-safety gate, a human merges, and a controller reconciles. The exhaustive infrastructure specification is relocated to companion documents so the boundary argument stays in focus.
- **Part 4 — The agentic harness we built.** The engineering: the controlled `AnthropicBrain`, the per-paradigm executors, the propose → gate → execute → blind-grade loop, the identical-except-loop guard, the H2 stage-diff compute measurement, substrate routing, the robustness hardening that controlled-execution evaluation required, and the threat-modeling and cross-vendor validity verification that caught real validity bugs. Deviations D-1…D-7 are cited throughout.

The **Discussion**, **Limitations** (including a harness-engineering and execution-validity group), a **Future work** section, and a brief **Reproduction** pointer follow the four parts, then references and appendices. The exhaustive step-by-step reproduction and the full infrastructure specification are kept in the repository companion documents (`experiments/safe_agent_study/paper/REPRODUCTION.md`, `ARCHITECTURE.md`, `deploy/eks/RUNBOOK.md`) rather than in this narrative.

## Abstract

Large language model (LLM) coding agents are increasingly used as data-engineering collaborators, but Spark development exposes a sharp safety problem: the same agent that hallucinates schemas, configuration, session state, or cluster topology can also execute those hallucinations as imperative cluster commands. In imperative PySpark, the paradigm hands the agent ownership of the Spark session, the read and write paths, runtime configuration, and the execution lifecycle. A mistaken assumption can therefore become a live job, a mutated session, or a materialized table with silent data-correctness defects — defects that compilation- and test-pass-oriented agent benchmarks do not measure.

This paper studies a restricted alternative for agent-native Spark development: agents author **transforms only**, while a controlled orchestrator owns the Spark session, staging, structural validation, execution, output read-back, and blind grading. The treatment uses Spark Declarative Pipelines (SDP), a safety skill, and a structural dry-run gate; the baseline is unconstrained imperative PySpark execute-to-debug. Controlled arms decompose the effect into declarative structure, the safety skill, and the gate; in particular, an imperative arm that mirrors the full treatment in every respect but paradigm isolates the *pure paradigm effect*, defeating the objection that the treatment merely received a better prompt. The study is pre-registered, with all deviations logged, the arm manifests frozen, and the task corpus and seeds frozen ahead of any clean run.

Beyond the controlled comparison, the document describes the surrounding system: the SDP development loop in which SDP itself orchestrates and executes the agent's declarative transforms (Part 2); a zero-trust deployment and GitOps demonstration that enforces the agent-safety boundary at the level of git and CI rather than runtime trust — the agent authors files and opens a pull request but never holds a Spark session (Part 3); and the controlled evaluation harness, including the robustness engineering and the independent validity review (threat-modeling and cross-vendor verification) that hardened it (Part 4).

We report no empirical conclusions yet. The primary outcomes are silent-defect rate (compared across paradigms) and compute-to-correct (compared only within a paradigm, since the two paradigms run on different engines). The planned analysis uses paired task and seed units, bootstrap confidence intervals, a mixed-effects logistic model, and Holm correction across pre-specified contrasts. Results tables and figure shells are included for reproducibility, but all cells remain **`[PENDING clean run — N=TBD]`** until the live sweep completes.

## Introduction and motivation

### Agent-native data engineering changes the failure mode

Traditional Spark development assumes a human engineer owns the edit–run–inspect loop. The developer chooses the session, knows the catalog, reads stack traces, and controls when a plan becomes a job. LLM coding agents change the loop. The agent is asked to synthesize code, infer schemas, decide how to read inputs, recover from errors, and propose follow-up commands. In a data platform, these decisions are not confined to source files: they can materialize tables, launch executors, mutate runtime configuration, and silently transform production-scale data.

The failure mode is therefore not merely "the agent writes wrong code." The sharper failure mode is:

1. the agent hallucinates a fact about context, schema, configuration, time zone, or cluster topology;
2. the programming interface gives the agent imperative control over the session and execution lifecycle; and
3. the hallucination is executed as a cluster command or materialized as a table.

In Spark, this is especially dangerous because many data-quality bugs are **silent**. A missing column may fail at analysis, but a timestamp parse, local-time day bucket, nondeterministic deduplication, or inner join against an incomplete dimension can produce a plausible table that is wrong. The study therefore focuses on **silent data-correctness defects**, not just runtime failures.

### Why imperative PySpark is a dangerous agent surface

Imperative PySpark is a powerful human interface. For agents, that power is also an attack surface against correctness. An imperative PySpark agent typically owns:

- the `SparkSession` builder and session-level options;
- direct DataFrame construction, reads, writes, and table creation;
- driver-side Python control flow and environment assumptions;
- Spark configuration mutation;
- execution commands such as `spark-submit` or Python entrypoints; and
- ad hoc debugging loops in which the first validation may be a live cluster run.

Those affordances amplify common agent errors. If an agent guesses that a local path exists on the cluster, the job can fail only after a remote executor tries to read it. If it assumes JVM access is available under Spark Connect, it can call APIs that the Connect client deliberately does not expose. If it uses local time rather than UTC, it may materialize a daily aggregate whose row counts look plausible but whose buckets are wrong. If it drops corrupt records without quarantine, the result may be cleanly materialized and silently undercount revenue.

The project's own deviations log records examples of this class of infrastructure/context mismatch encountered while building the instrument: local filesystem paths worked for an in-process executor but failed against a remote Spark Connect cluster, so the live path was changed to stage input into cluster-reachable storage. That incident is not a headline result, but it motivates the design principle: agent-authored code should not own environment-sensitive cluster mechanics.

### The dual penalty imperative agents pay — and why we measure each engine on its home turf

These two problems compound. On an agent-native deployment, an imperative-PySpark agent pays a **dual penalty**:

1. **Architectural incompatibility with Connect-isolated deployment.** The deployment this study targets (Part 3) deliberately denies the agent a driver: the agent is a constrained Spark Connect client with no local JVM, no `sparkContext`, and no shared filesystem. Imperative code that assumes driver ownership — JVM internals, static-config mutation, local filesystem paths, shelling out to a local Spark install — cannot run there at all. This is not a harness choice; it is what Connect isolation *is* (Part 2).
2. **High silent-defect rates.** Even where imperative code does run, the same imperative control over session, parsing, time zone, and write path is what lets a hallucination become a silently-wrong table (above).

These two penalties are different in kind, and conflating them would corrupt the science. A study that ran the imperative *baseline* on the Connect-isolated cluster would observe only the first penalty: the baseline would simply fail to run (penalty 1), and we would never measure its silent-defect rate (penalty 2) — the more interesting quantity, and the only one that compares paradigms on equal footing on the failure mode we actually care about. We therefore make a deliberate scoping decision: **we measure each paradigm's silent-defect rate (penalty 2) on its own native engine** — imperative on classic local Spark, SDP on Spark Connect — so that penalty 1 does not crash the baseline before penalty 2 can be observed. Penalty 1 (imperative incompatibility with Connect isolation) is then reported separately, as a *paradigm-level architectural finding* (Part 2), not as a baseline that "failed to run." The two penalties are reported honestly and apart, rather than collapsed into a single artifactual "imperative loses." This same separation governs the compute analysis: because the paradigms run on different engines, compute-to-correct is compared only *within* a paradigm, never across it (H2; Part 1, §8–§9.4).

### Restricted declarative scope as a safer agent surface

Spark Declarative Pipelines (SDP) offer a narrower interface: the agent authors table and view transforms, while an orchestrator interprets dependencies, storage, and execution. This study evaluates SDP not as syntax preference, but as a safer **agent surface**.

The treatment loop is shaped by a CI/CD principle: agents produce artifacts; a controlled system validates and deploys them. In this study, the artifact is the transform module. The orchestrator owns the Spark Connect session, writes the SDP project manifest, stages inputs, runs structural gates, executes jobs, reads materialized outputs, and invokes blind oracles. This design aims to reduce both correctness defects and wasted compute by catching structural mistakes before executor work.

The document makes four claims, the first empirical and pending, the rest demonstrated in the repository:

- **Claim 1 (the experiment).** A restricted SDP guardrailed loop should yield fewer silent data-correctness defects and safer iteration than unconstrained imperative PySpark. The empirical answer is **`[PENDING clean run — N=TBD]`** (Part 1).
- **Claim 2 (the loop).** A development loop exists in which the agent authors declarative transforms and SDP itself orchestrates and executes them on Spark Connect — and the imperative paradigm's incompatibility with Spark Connect is intrinsic to the paradigm, not the harness (Part 2).
- **Claim 3 (the deployment).** The loop maps onto a realistic EKS/Spark-Connect/Iceberg deployment, and onto a GitOps workflow that enforces the agent-safety boundary through git and CI rather than runtime trust (Part 3).
- **Claim 4 (the apparatus).** A controlled, reproducible harness can measure the experiment honestly, and the engineering required to make it honest — bounded execution, process isolation, valid compute measurement, blind grading, and adversarial independent review — is itself a contribution (Part 4).

---

## Related Work & Context

This study sits at the intersection of two literatures that rarely meet: the evaluation of LLM coding agents, and the engineering of data-quality assurance. Each has a blind spot that the other fills, and the gap between them is exactly where agent-authored data pipelines fail.

**Coding-agent benchmarks reward compilation and test-pass, not data correctness.** The dominant benchmarks for LLM coding agents — SWE-Bench [CITE] and its descendants, together with execution-based suites such as HumanEval [CITE] and the agentic software-task harnesses that have followed [CITE] — define success as a patch that compiles and makes a held-out test suite pass. That criterion is appropriate for application code, where a passing test is strong evidence of correct behavior. It is the wrong criterion for data engineering. A Spark job can compile, run to completion, satisfy every schema assertion, and still silently materialize a table whose daily revenue is bucketed into the wrong calendar day, whose deduplication kept a nondeterministic survivor, or whose corrupt rows were dropped without a trace. None of these surface as a thrown exception or a failing test; they surface — if at all — much later, as a reconciliation discrepancy. A benchmark whose oracle is "did it crash / did the test go green" is structurally blind to this entire failure class. The unit of correctness in data engineering is the *materialized data at scale*, not the *control flow*, so an agent evaluation that never reads the materialized output cannot see the defects that matter most. This is the central methodological gap the present study targets: silent, data-scale *side effects* that compilation- and test-pass-oriented agent benchmarks do not measure.

**Data-quality and testing frameworks check the data but assume a trusted author.** On the other side, mature data-testing frameworks — dbt tests [CITE], Great Expectations [CITE], and the broader family of declarative-constraint and "data contract" tools [CITE] — do read materialized data and assert semantic properties (uniqueness, not-null, accepted ranges, referential integrity, row-count reconciliation). But they are designed as guardrails around a *trusted human author* who writes both the transformation and its expectations and who iterates on a development environment they fully control. They answer "is this table correct?" with an engineer in the loop; they do not answer "what happens when the author is an LLM agent that can hallucinate the schema, the time zone, or the cluster topology — and can execute that hallucination?" Crucially, the expectation suite is itself agent-authorable: an agent that misunderstands the contract can write expectations that pass on wrong data. Such frameworks are therefore complementary to — not a substitute for — an *independent, blind* semantic oracle that derives ground truth from the input rather than trusting the agent's own assertions.

**Why data-engineering agents specifically need silent-defect semantic evaluation.** Combining the two observations: agent coding evaluations have the right subject (an autonomous, possibly-mistaken author) but the wrong oracle (crash / test-pass); data-quality frameworks have the right oracle (semantic checks on materialized data) but the wrong threat model (a trusted author). An honest evaluation of a data-engineering agent needs both at once — an autonomous agent author *and* a blind, input-derived semantic oracle that reads the table the agent actually shipped and counts residual defects it never disclosed. That combination is what this study builds. It is also why the safety question here is not "can the agent write code that runs," but "when the agent's code runs cleanly, how often is the resulting data silently wrong, and does the development paradigm change that rate?" To our knowledge no prior benchmark measures silent data-correctness defects in agent-authored Spark pipelines on a per-defect-class basis; positioning relative to any closely-related concurrent work remains [CITE].

---

# Part 1 — SDP versus imperative: the pre-registered experiment

Part 1 is the controlled comparison. It is pre-registered: the research question, hypotheses, arms, metrics, sample-size rule, analysis plan, reproducibility commitments, and threats to validity were fixed before clean result collection. The pre-registration is `experiments/safe_agent_study/PREREGISTRATION.md`; deviations and implementation decisions are logged in `experiments/safe_agent_study/DEVIATIONS.md`. **No empirical results appear in this part.** Every results cell is an explicit placeholder.

## 1. Pre-registration

### 1.1 Research question

> Does constraining an AI coding agent's Spark development loop with **SDP + a safety skill + a fast structural dry-run gate** make it ship **fewer silent data-correctness defects** than an unconstrained imperative-PySpark agent — and does **dry-run iteration save compute** relative to execute-to-debug?

### 1.2 Hypotheses

The pre-registered hypotheses are:

> **H1 (safety).** Agents working in the SDP-guardrailed loop (Arm B) ship a lower silent-defect rate than unconstrained PySpark agents (Arm A).
>
> **H2 (compute, intra-paradigm).** *Within the imperative paradigm*, the dry-run gate reduces compute-to-correct (executor-seconds and USD) versus execute-to-debug iteration, by catching structural defects pre-execution. The primary contrast is **A versus B2** (imperative without versus with the gate). H2 is deliberately scoped intra-paradigm: compute is **not** compared across paradigms, because imperative runs on classic `local[*]` and SDP runs on a Spark Connect server, so executor-seconds are not commensurable across the two engines (§9.4; Part 4, §21.1).
>
> **H3 (mechanism / ablation).** The safety improvement is attributable to identifiable components — SDP structure (Arm B1), the dry-run gate (Arm B2), the safety skill (Arm A2 versus B2), and the declarative paradigm itself (Arm B versus A2, with the gate and safety skill held constant on both) — not merely "a different tool." In particular, the B-versus-A2 contrast isolates the *pure paradigm effect* and defeats the objection that any B advantage is just "a better prompt." The decomposition is reported rather than assumed.

### 1.3 Falsification criteria

The falsification criteria are pre-specified, and nulls are reported as-is:

- **H1 is not supported** if the Arm A versus Arm B silent-defect-rate difference has a 95% confidence interval including zero, or the paired effect is not significant at the pre-set threshold (α = 0.05).
- **H2 is not supported** if gated compute-to-correct is statistically indistinguishable from or greater than execute-only.
- Nulls are reported as-is, with the metric, N, CI, and test cited for any headline claim.

## 2. Arms and shared controls

The experiment has five arms, implemented as JSON manifests under `experiments/safe_agent_study/arms/`. The validity crux is that **the loop is the only manipulated variable**: every other control is held identical across arms.

| Arm | Manifest | Paradigm | Dry-run gate | Safety skill | Skills | Allowed commands | Purpose |
|---|---|---|---|---|---|---|---|
| A | `arms/A.json` | `imperative_pyspark` | off | off | none | `python`, `spark-submit` | Baseline (execute-to-debug) |
| B | `arms/B.json` | `sdp` | on | on | `pyspark-sdp`, `spark-safety` | `spark-pipelines dry-run`, `spark-pipelines run` | Full treatment |
| A2 | `arms/A2.json` | `imperative_pyspark` | on | on | `spark-safety` | `python --analyze-only`, `python`, `spark-submit` | Imperative mirror of B (gate + safety skill held constant; paradigm is the only difference) |
| B1 | `arms/B1.json` | `sdp` | off | off | `pyspark-sdp` | `spark-pipelines run` | Ablation: SDP structure |
| B2 | `arms/B2.json` | `imperative_pyspark` | on | off | none | `python --analyze-only`, `python`, `spark-submit` | Ablation: gate alone |

**Arm A2 is introduced in this revision.** It is the imperative mirror of the full SDP treatment (Arm B): the dry-run gate is on and the `spark-safety` skill is loaded in *both*, so the only thing that differs between B and A2 is the paradigm. (A2 does not carry the `pyspark-sdp` API skill, which is meaningless for imperative code; that skill is intrinsic to the SDP paradigm itself — see the skill-asymmetry limitation in §29.) A2 enables two contrasts the original four arms could not isolate:

- **B vs A2 — the pure paradigm effect.** With the gate and the safety skill held constant on both arms, any B–A2 difference is attributable to declarative-versus-imperative *paradigm*, not to "Arm B got a better prompt." This directly defeats the prompt-versus-paradigm objection that the original design could only acknowledge as a limitation.
- **A2 vs B2 — the safety skill's effect on the imperative arm.** Both are imperative with the gate on; A2 adds the `spark-safety` skill and B2 does not, so A2–B2 isolates what safe-engineering guidance buys *within* the imperative paradigm.

Its manifest follows the same schema as the other arms and is added alongside the clean-run configuration. Every arm shares these controlled values: `base_model_id = claude-opus-4-8`, `task_prompt_ref = prompts/task_prompt.md@v1`, `max_iterations = 12`, `temperature = 0.0`, `top_p = 1.0`. (On the transmit side these sampling values are recorded as provenance but not all sent to the model; see Part 4 and deviations D-2/D-6.)

### 2.1 The identical-except-loop guard

A guard in the harness enforces the validity crux structurally. It partitions each arm's manifest fields into two sets: the **loop fields** that are allowed to vary — paradigm, dry-run gate, safety skill, the loaded skills, and the allowed commands — and the **shared fields** that must be identical across every arm — the base model, the task-prompt reference, the iteration budget, and the sampling controls (temperature and top-p).

The guard raises if any shared field differs across arms (which would confound the comparison), and it also verifies that the arms' loop signatures genuinely *differ*, so there are no accidental duplicate arms. The sampling discipline is made explicit as a nested invariant: the values *recorded and validated identical* (temperature and top-p) are a superset of the values actually *transmitted* to the model, which are in turn a subset of the shared controls — so a transmit-side change can never silently relax a control. A second check ties the manifest controls to the runtime sources, so the arms must agree not only with each other but also with the actually-configured base model and the shared prompt file. With Arm A2 added, the five arms — A, B, A2, B1, B2 — present five distinct loop signatures, which the guard also confirms are mutually non-duplicate.

The intent is that, given these guards, any observed difference between arms is attributable to the loop — paradigm, gate, and skills — and not to a base-model, prompt, sampling, seed, or iteration-budget difference.

## 3. Task corpus and dataset generation

### 3.1 The frozen corpus

The frozen task corpus locks **22 tasks** across six independent data substrates — orders, customer CDC, multi-currency payments, trades, clickstream, and emails — exceeding the pre-registration's target of ≥12 tasks (deviation D-0). The tasks span the realistic data-engineering shapes a Spark agent encounters in production, grouped by category:

- **medallion ETL** (multi-layer bronze→silver→gold pipelines, including a streaming medallion);
- **CDC / slowly-changing dimensions** (SCD Type 1/2 over change streams, with no-overlap invariants);
- **event-time windowing** (windowed aggregation, late/out-of-order data with allowed lateness, streaming dedup with watermark);
- **fan-out and marts** (one stream materialized into multiple tables; batch marts over CDC output);
- **multi-currency / FX** (currency normalization to USD; daily FX settlement per currency);
- **enrichment joins** (stream–static dimension enrichment);
- **schema evolution** (evolution-tolerant ingest); and
- **quarantine / dead-letter** (explicit DLQ routing of contract-violating rows).

The complete per-task list — task names, the substrate each uses, and the defect classes in scope for each — is given in **Appendix B**.

The corpus is balanced so that **every empirical defect class (D1, D2, D4–D8) is exhibited by at least five tasks** (the lock file records the coverage matrix: D1 in 12 tasks, D2 in 6, D4 in 7, D5 in 5, D6 in 6, D7 in 5, D8 in 8). Per-class reporting is therefore meaningful rather than dominated by a single task. (The tasks also exercise streaming-state behavior relevant to the state-class defects D3 and D9, but those are not scored by the offline grader; see *Future work*.)

### 3.2 The `orders_silver_gold` example

For `orders_silver_gold`, the task prompt asks the agent to build a streaming medallion pipeline over a Kafka-style `orders` topic: produce a cleaned/deduplicated/enriched `silver_orders` table (coerce `event_time`, deduplicate on `order_id` with a deterministic survivor, LEFT-join a merchants dimension) and a `gold_daily` revenue rollup per UTC day per canonical category, quarantining unparseable rows. The output contract names the graded output table `gold_daily` with columns `event_date` (DATE, UTC), `revenue`, `order_id`, `amount`, `category`, plus the deduplicated table `silver_orders` keyed by `order_id`. The defects in scope for this task are D1, D2, D6, D7, and D8, and each task declares the oracle quantifiers that apply to it (for orders: `quantify.d2`, `quantify.d6`, `quantify.d7`, `quantify.d8`).

Each task's `output_contract` carries the fields the oracle needs: the output `table`, an optional `revenue_col` (for D8 revenue reconciliation), a `date_col` (for D2/D7 date checks), a `key_col` and `dedup_table` (for D6 determinism), `payload_cols` (for D6 survivor comparison), and the `substrate` (which selects the applicable oracle quantifiers).

### 3.3 Generators and seeds

The substrates are produced by deterministic generators referenced by each task: `infra/gen_messy_orders.py` (orders), `infra/gen_customers_cdc.py` (customer CDC), and `infra/gen_payments.py` (payments). The orders generator emits deterministic NDJSON under a seed and deliberately injects duplicates, late/out-of-order timestamps, null or unknown merchant IDs, missing fields, string amounts, mixed timestamp formats, malformed JSON, and unknown merchants. The runner chooses the generator declared by each task through `generate_dataset` in `harness/runner.py` rather than assuming a single global input (deviation D-4/B4).

The seed lock originally fixed **10 deterministic seeds** for the pilot (`SEEDS.lock.json` v1.0.0-pilot); the confirmatory sweep uses **12** (v1.1.0-power). The expansion from 10 to 12 is power-driven, not arbitrary: the N=3 pilot was underpowered for the registered A–B contrast (§8.5), so per pre-registration §6 stage 3 — which explicitly permits *appending* seeds in a new lock version to tighten the confidence interval, with no upper cap — two seeds (16180, 14142) were appended *after* the original ten, giving 12 seeds × 22 tasks = **264 matched cells per arm** (deviation D-SEEDS-POWER). Seeds are fixed integers, published with results, and never removed or renumbered: the original ten keep their order and only new seeds are added. Because generation is seed-deterministic, each arm sees **byte-identical input** for a matched `(task, seed)` cell, and the generator's injected-defect counts are verified by repository fixtures as a regression check (these fixture counts are properties of the data generator, not experimental outcomes). The exact seed integers are listed in **Appendix B**.

## 4. Defect taxonomy (empirical: D1, D2, D4–D8)

The **empirical taxonomy the offline grader measures is seven classes — D1, D2, D4, D5, D6, D7, D8** — implemented as `DEFECT_TAXONOMY` in `experiments/safe_agent_study/harness/oracles.py`. Each class carries a name, a class (structural / semantic), a dry-run-detectability flag, an output-oracle quantifier key (for semantic classes), and error signatures (for structural classes). Two further defect classes — **D3 (unwatermarked dedup) and D9 (unbounded state)** — are state-class defects whose evidence is only visible at runtime/cluster scale; the offline/semantic grader cannot observe them, so they are *excluded from the empirical taxonomy* and described instead under *Future work*.

| ID | Class name | Type | Dry-run detectable | Detection signal |
|---|---|---|---|---|
| D1 | missing/unresolved column | structural | yes | `UNRESOLVED_COLUMN` / SQLSTATE 42703 |
| D2 | wrong type / timestamp misparse | semantic | no | residual misparsed/impossible-date rows in completed output (`quantify.d2`) |
| D4 | broken DAG / missing upstream | structural | yes | `TABLE_OR_VIEW_NOT_FOUND` / SQLSTATE 42P01 |
| D5 | immutable config mutation | structural | yes | `CANNOT_MODIFY_CONFIG` / SQLSTATE 46110 |
| D6 | nondeterministic dedup | semantic | no | surviving row per key disagrees with deterministic truth (`quantify.d6`) |
| D7 | timezone / day-bucket error | semantic | no | output day buckets disagree with UTC-correct truth (`quantify.d7`) |
| D8 | absent quarantine / silent drop | semantic | no | output revenue fails reconciliation against ground truth (`quantify.d8`) |

A key pre-registered prediction is the **structural/semantic split**: the structural dry-run should catch the structural classes D1/D4/D5 (they surface as analysis-time error signatures) but *not* the semantic classes D2/D6/D7/D8. This matters because the treatment is not allowed to claim that guardrails solve semantic correctness by magic: the output oracle must still read completed tables to detect the semantic classes. The state-class defects D3 and D9 are outside this empirical split entirely — a structural gate cannot catch them and the offline grader cannot score them; they are deferred to *Future work*.

## 5. Oracle and blind grading

The grading path is automated and **arm-blind by construction**: the data structures handed to the grader carry no arm, base-model, skill, dry-run, or paradigm labels, so the grader cannot "know" which loop produced a run.

- `experiments/safe_agent_study/harness/oracles.py` defines `DEFECT_TAXONOMY`, `TaskOracleSpec` (which defect classes are in scope for a task), `RunOutcome`, `OutputProfile`, `GradeResult`, and `grade_run`.
- `RunOutcome` contains only `completed`, `analysis_log`, `runtime_log`, and an optional `output` profile — it intentionally omits arm/model/skill/gate labels and any pre-computed gate hints. The grader *derives* the detection stage from where an error signature surfaced (analysis log versus runtime log).
- `OutputProfile` records residual corruption in a *completed* output: `d2_misparsed_rows`, `d6_ambiguous_keys_unhandled`, `d7_wrong_day_rows`, `d8_dollars_dropped`, `d8_rows_dropped`, and a `reconciles` flag (output sum equals ground-truth sum).
- `GradeResult` reports `silent_defect`, the list of `defect_classes` silently present, a run-level `detection_stage`, and a per-defect `per_defect_detection` map.

The grading logic in `grade_run` is:

- **Structural (D1/D4/D5):** an error signature found in the analysis log ⇒ `dry_run` (caught at the gate); found in the runtime log ⇒ `runtime`; no signature and the run completed ⇒ `n/a` (the defect was not triggered).
- **Semantic (D2/D6/D7/D8):** if the run never reached completed/materialized output ⇒ `n/a` (no corruption was shipped); if it completed with a residual count > 0 ⇒ `never` (a silent defect — it shipped uncaught) and the class is added to the silent set; if it completed with a residual count of 0 ⇒ `n/a` (the agent mitigated it).
- **State (D3/D9):** out of empirical scope — the offline grader does not score state-class defects (*Future work*).

`experiments/safe_agent_study/harness/output_oracles.py` builds `OutputProfile` for the live path. Its `build_output_profile(read_table, spark, input_path, defects_in_scope, contract)` reads the table the agent actually materialized (through the executor's `read_table`) and compares it against ground truth derived from the matched-seed input: UTC-correct day totals for orders/payments (D7), the deterministic latest-by-sequence survivor for CDC (D6), the true numeric/FX-converted revenue total (D8), and impossible dates from epoch-millisecond misparses (D2). The reconciliation tolerance is small and fixed (`RECON_TOL`). The live grading question is therefore precise: *did the agent ship a completed table, and does that table still contain an oracle-detected defect?* A defect caught and fixed before completion is not counted as silent.

## 6. Exit classes and result rows

The runner emits one result row per `(task, arm, seed)` cell. The schema is declared in `experiments/safe_agent_study/results_schema.json` and represented as the `ResultRow` dataclass in `harness/schema.py` (`SCHEMA_VERSION = "1.0.0"`).

The pre-registered exit classes are `EXIT_CLASSES = ("completed", "analysis_error", "runtime_error", "max_iterations", "harness_error")`:

- `completed` — the loop reached completed materialization;
- `analysis_error` — a structural analysis (dry-run / eager analysis) failed and was not recovered;
- `runtime_error` — execution failed on the cluster and was not recovered;
- `max_iterations` — the iteration budget was exhausted after observed failures or non-green proposals;
- `harness_error` — the harness itself failed to produce meaningful agent execution attempts.

Detection stages are `DETECTION_STAGES = ("dry_run", "runtime", "never", "n/a")`. Each result row carries, among other fields:

- **identity/matching:** `run_id`, `task`, `arm`, `seed`;
- **reproducibility provenance** (pre-reg §8): `spark_version`, `image_digest`, `git_sha`, `base_model_id`, `executor_config`;
- **H1 primary outcome:** `silent_defect`, `defect_classes`, `detection_stage`;
- **H2 / cost:** `iterations`, `wall_s`, `executor_seconds` (nullable — `None` means *unmeasured*, never `0.0`), `usd`, `exit_class`, plus `executor_seconds_to_correct`, `iterations_to_green`, `wall_s_to_green`, and the always-present cross-check `executor_seconds_wallclock`;
- **secondary / honesty controls:** `task_success` (a completion/retention check — a safer loop should not look safer merely by failing to complete), `reached_correct` (false ⇒ compute-to-correct is an intention-to-treat imputation), `dry_run_intercepts`, `failing_iterations`, and `per_defect_detection`;
- **audit:** `backend`, `transcript_path`, `schema_version`, `timestamp_utc`, `notes`.

`validate_row` enforces the required-field set, and `executor_seconds` is deliberately nullable while `executor_seconds_wallclock` is always present, so every row carries at least the wall-clock cross-check measure required by the pre-registration.

## 7. Metrics

The primary and secondary metrics are fixed in `PREREGISTRATION.md` and implemented in `harness/cost.py`, `harness/oracles.py`, and `analysis/analyze.py`.

**Primary H1 metric: silent-defect rate.** A run is silent-defective when it reaches completed/materialized output *and* the blind oracle finds at least one residual defect. Non-completing runs are not silent defects; they are separately included in task-success and compute-to-correct analyses.

**Primary H2 metric: compute-to-correct.** Compute-to-correct is measured in executor-seconds and USD to reach the first correct output. It is compared **only within a paradigm** (primary contrast A vs B2, both imperative): because imperative arms run on classic `local[*]` and SDP arms on a Spark Connect server, executor-seconds are not commensurable across the two engines, so no cross-paradigm compute contrast is reported (§8, §9.4). The live Connect executor reads per-iteration executor-second deltas via the Spark driver REST stage-diff (Part 4); dry-run gates are driver-only and counted as zero executor-seconds by construction. `harness/cost.py` models three compute surfaces per run (deviation D-5): the measured `total_executor_seconds` (stage-diff sum, `None` if never measured), the measured `total_cpu_seconds`, and the always-present `total_executor_seconds_wallclock` cross-check. The success-bias rule (B9) is pre-declared: for a green run, compute-to-correct sums executor-seconds up to and including the first correct iteration; for a non-green-but-completed run, it sums all compute and flags `reached_correct = False`; for an incomplete run it is unmeasured.

**Dry-run interception rate.** For gated arms, the interception fraction is the number of failing iterations stopped by the dry-run gate divided by all failing iterations.

**Secondary controls.** Per-defect-class detection; pre-execution versus runtime versus never detection; iterations-to-green; wall-clock-to-green; and task success rate.

## 8. Statistical plan

The statistical plan is pre-specified and implemented in `experiments/safe_agent_study/analysis/analyze.py`:

- **Primary effect estimate.** The A–B difference in silent-defect rate, paired by `(task, seed)` cell.
- **Intervals.** Bootstrap percentile confidence intervals, `BOOTSTRAP_B = 10000` resamples, deterministic `BOOTSTRAP_SEED = 20260623` (reported with results), resampling at the `(task, seed)` level.
- **Primary inference.** A mixed-effects logistic regression `silent ~ C(arm)` with random intercepts for task and seed, fit via `statsmodels` `BinomialBayesMixedGLM` (`fit_glmm` / `glmm_contrasts`), reporting per-arm odds ratios and average marginal effects on the probability scale. The primary threshold is `ALPHA = 0.05`. An exact McNemar test (`mcnemar_p`) is the always-available paired fallback and cross-check.
- **Multiple comparisons.** Holm correction (`holm`) across the pre-registered contrast set. The original four-arm contrasts are `(A,B), (A,B1), (A,B2), (B,B1), (B,B2)`; this revision adds the two Arm-A2 contrasts `(B,A2)` (pure paradigm effect, gate and safety skill held constant) and `(A2,B2)` (safety-skill effect within the imperative paradigm), giving seven contrasts in total. Holm correction is applied across the full set.
- **H2 (intra-paradigm only).** Paired comparisons of compute-to-correct *within the imperative paradigm* — primarily A versus B2 (gate effect), secondarily A versus A2 (gate + safety skill) — reported both intention-to-treat (all matched cells, with imputation for non-green runs) and complete-case (both-arms-green cells only), following the B9 rule. The cross-paradigm A-versus-B compute comparison is **excluded by design**: imperative arms run on classic `local[*]` and SDP arms on a Spark Connect server, so executor-seconds reflect two different engines and are not comparable. Reporting an A-vs-B compute "saving" would conflate a gate effect with an engine difference; the engine confound is therefore handled by restricting H2 to intra-paradigm contrasts rather than by attempting to normalize across engines.
- **Power rule.** `required_n_for_halfwidth` computes the paired-cell count needed for a 95% CI half-width ≤ `CI_HALF_WIDTH_TARGET = 0.05` on the A–B difference, from the observed paired-difference standard deviation; the analysis refuses to validate headline claims below that requirement.

All numeric outputs from this plan are pending clean runs and are represented in this paper only as placeholders.

## 8.5 Reading the statistics

The plan above lists *which* methods are used; this section explains *what* each one does and *why* it is the right tool for this study. It is written for a careful reader who is not a statistician. The throughline is that every choice serves one goal: to compare loops on the same problem, account for the structure of the data, and refuse to over-claim. Throughout, the worked numbers are the N=3 preliminary mechanism round (66 cells/arm); **a pre-registered N=12 power-scaled confirmation is in progress** (§8.5.7, §3.3).

### 8.5.1 The paired `(task, seed)` design — why we compare matched pairs

Because data generation is seed-deterministic (§3.3), every arm faces *byte-identical input* for a given `(task, seed)` cell. We exploit this: rather than comparing the average defect rate of one arm against the average of another (a between-subjects comparison), we compare arms **cell by cell** — the same task, the same seed, the same input bytes, only the loop differs. This is a within-subjects (paired) design, and pairing matters for one concrete reason: tasks differ enormously in difficulty (a streaming as-of join is far harder than a single windowed aggregate), and seeds differ in how many defects the generator injects. In an unpaired comparison, that task-and-seed difficulty variance lands in the noise term and swamps the loop effect we care about. Pairing *removes* it: each cell is its own control, so the question becomes the much sharper "on this exact problem, did changing the loop change the outcome?" The variance we are left with is the variance of the *difference*, which is what every interval and test below is built on.

### 8.5.2 Mixed-effects logistic regression (GLMM) — modeling a binary outcome with repeated structure

The H1 outcome is binary: a run either ships a silent defect or it does not. The natural model for a yes/no outcome is **logistic regression**, which models the log-odds of the event as a linear function of predictors — here a single predictor, the arm. But ordinary logistic regression assumes every observation is independent, and ours are not: the same 22 tasks and the same seeds recur across all arms, and each task and each seed has its own baseline propensity to produce defects (some tasks are simply more defect-prone). Treating 66 cells as 66 independent observations would *understate* the uncertainty and overstate our confidence.

The fix is a **mixed-effects** model (a GLMM, generalized linear mixed model): arm enters as a **fixed effect** (the thing we want to estimate), and task and seed enter as **random intercepts** — each task and each seed gets its own baseline shift, drawn from a distribution the model estimates. This tells the model that two runs sharing a task are correlated, so it spends confidence appropriately. We fit `silent ~ C(arm)` with random intercepts for task and seed via `statsmodels`' `BinomialBayesMixedGLM`, with **Arm A as the reference level** — every odds ratio below is read against unconstrained imperative PySpark. (When the reference arm is absent from a round — as in the N=3 mechanism round, which lacked Arm A — the GLMM cannot be fit and we fall back to McNemar; §8.5.6.)

### 8.5.3 Odds ratio (OR) and average marginal effect (AME) — two ways to read the same effect

The GLMM's coefficient is on the log-odds scale, which is hard to interpret directly, so we report two derived quantities.

The **odds ratio** is `exp(coefficient)`. For the H1 A–B contrast the OR is **2.145**, meaning B's *odds* of shipping a silent defect are about 2.1× A's. An OR above 1 means *more* defects, below 1 means *fewer*; the OR of 2.145 points, notably, *against* the safety hypothesis (B trends toward more silent defects than unconstrained A — interpreted in §28a.2). Odds, however, are not intuitive — "2.1× the odds" is not the same as "2.1× as likely," and the gap widens as rates move away from 50%. So we also report the **average marginal effect**, which re-expresses the model's effect on the plain probability scale: the AME of **+0.095** reads directly as "B ships silent defects about 9.5 percentage points more often than A, averaged across the cells." OR is the model-native effect size; AME is the same effect translated into the units a reader actually reasons about.

### 8.5.4 Bootstrap percentile confidence intervals — assumption-light intervals that respect the pairing

For the *rate differences* (as opposed to the model's odds ratios) we put confidence intervals on them by **bootstrapping**. The procedure: resample the `(task, seed)` cells *with replacement* `BOOTSTRAP_B = 10000` times under a fixed seed (`BOOTSTRAP_SEED = 20260623`, published for reproducibility), recompute the rate difference on each resample, and take the 2.5th and 97.5th percentiles of those 10,000 values as the 95% interval. Two design points matter. First, resampling is done **at the cell level**, not the row level — we resample whole `(task, seed)` cells so the paired structure is preserved in every bootstrap replicate; resampling rows independently would break the very pairing §8.5.1 relies on. Second, the bootstrap is **assumption-light**: it does not assume the difference is normally distributed or that the rate is far from 0 or 1, both of which can be false at small N with rates near 0.3. For the H1 rate difference this yields a 95% CI of **[−0.182, +0.015]** — an interval that comfortably includes zero, which is the formal sense in which H1 is *not* supported at this N.

We deliberately report **both** the bootstrap CIs and the GLMM, because they triangulate. The bootstrap gives assumption-light intervals on the interpretable rate difference; the GLMM gives proper odds-ratio inference *with* the random-effects structure that the bootstrap's cell-level resampling only partially captures. When an assumption-light interval and a model-based test agree, the conclusion is robust to the choices either one makes alone.

### 8.5.5 Holm correction for multiple contrasts — controlling the family-wise error rate

We pre-registered seven contrasts: the original five `(A,B), (A,B1), (A,B2), (B,B1), (B,B2)` plus the two Arm-A2 contrasts `(B,A2)` and `(A2,B2)`. Each test at α = 0.05 has a 5% chance of a false positive *on its own*; run seven and the chance that *at least one* fires spuriously climbs well above 5%. The **Holm step-down correction** controls this family-wise error rate: it sorts the raw p-values, tests the smallest against the strictest threshold and each subsequent one against a progressively looser threshold, and stops at the first non-rejection. Holm is uniformly more powerful than the blunter Bonferroni correction (it does not simply multiply every p by the number of tests) while still strictly controlling the family-wise rate. Its effect is visible in the worked numbers: B–B2 has a raw p of **0.010** but a Holm-adjusted p of **0.0509** — it survives as a single test but *just misses* significance once the price of looking at seven contrasts is paid honestly. We report both the raw and the adjusted p so a reader can see exactly what the correction costs.

### 8.5.6 McNemar exact test — the paired fallback when the GLMM cannot fit

For a *single* paired binary contrast, the **McNemar exact test** is the classical tool. It ignores the cells where both arms agree (both clean, or both defective — uninformative about a difference) and asks only whether the **discordant** pairs are balanced: of the cells where exactly one arm shipped a defect, are roughly half "A-only" and half "B-only," or is the split lopsided? A lopsided split is evidence of a real difference. We use McNemar as the always-available paired cross-check, and specifically as the **fallback when the GLMM cannot be fit** — most concretely in the N=3 mechanism round, where Arm A (the GLMM reference level) was absent, so the B–A2 and B–B1 contrasts were tested with exact McNemar plus bootstrap CIs rather than the model. It is also the test behind the A2-vs-B2 result (Δ +0.091, McNemar p = 0.109): a trend toward the skill arm shipping *more* defects, not significant.

### 8.5.7 The power rule — why we gate headline claims, and why N moved to 12

A null result is only meaningful if the study was capable of detecting an effect; a "no difference" finding from an underpowered study is uninformative, not reassuring. So before we are willing to report the headline A–B contrast as confirmatory, we check that the sample is large enough. `required_n_for_halfwidth` takes the observed standard deviation of the paired A−B difference and computes the number of matched cells needed for the 95% CI half-width to reach the target `CI_HALF_WIDTH_TARGET = 0.05`. With the observed paired sd of **0.404**, that requirement is **N = 252 cells**. The N=3 pilot had only **66** — far below the bar — so the analysis sets `headline_n_valid = False` and **refuses to report the A–B contrast as confirmatory**.

This is a feature, not a hedge: the apparatus is built to *not* over-claim from a small sample, and it triggered exactly as designed. It is also the direct cause of the seed expansion (§3.3, deviation D-SEEDS-POWER): scaling to **12 seeds** gives 12 × 22 = **264 cells per arm**, which clears the 252 requirement, so the confirmatory sweep can put a tight enough interval on A–B to make the headline claim — null or not — on solid ground.

## 9. Results (shells only)

**Partial results — the H3 mechanism round (arms B, A2, B1 at N=3: 22 tasks × 3 seeds = 66 cells/arm, 198 rows).** Cells below report *verified aggregates only*. Rows that require the unconstrained Arm A or Arm B2 — the registered **H1 (A vs B)** and **H2 (compute)** analyses — are marked **[PENDING — Decision-B phase]**; do not read them as results. The H3 contrasts (B–A2 paradigm, B–B1 ablation) and conciseness (§9.8) are complete.

### 9.1 Run completion and sample accounting

| Quantity | Value |
|---|---|
| Clean run identifier | H3 round 2026-06-28 (A2 re-run on `instrument-v3.1`; B/B1 from the N=3 primary sweep) |
| Git SHA used for sweep | A2 `1d28563` (tag `instrument-v3.1`); B/B1 `295d725` (seed 42) + `54834a1` (seeds 1337/2718) |
| Number of tasks included | 22 |
| Number of seeds included | 3 (42, 1337, 2718) |
| Number of matched `(task, seed)` cells | 66 per arm (198 rows) |
| Arms included | A2, B, B1 — Arm A and B2 [PENDING — Decision-B phase] |
| Result rows passing schema validation | 198 / 198 |
| Power-rule status | H3 contrasts at N=3 (66 cells/arm); registered A–B headline N not yet met (Arm A pending) — [PENDING — Decision-B phase] |

### 9.2 H1: silent-defect rate by arm

| Arm | Loop | Silent-defect count | Runs | Silent-defect rate | 95% CI |
|---|---|---:|---:|---:|---|
| A | Imperative PySpark, execute-to-debug | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] |
| B | SDP + safety skill + dry-run gate | 23 | 66 | 0.348 | [0.242, 0.470] |
| A2 | Imperative PySpark + dry-run gate + safety skill | 23 | 66 | 0.348 | [0.242, 0.470] |
| B1 | SDP only | 23 | 66 | 0.348 | [0.242, 0.470] |
| B2 | Imperative PySpark + dry-run gate | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] |

### 9.3 H1 contrasts and GLMM inference

| Contrast | Estimand | Estimate | 95% CI | GLMM odds ratio | Raw p-value | Holm-adjusted p-value | Interpretation |
|---|---|---:|---|---:|---:|---:|---|
| A–B | Difference in silent-defect rate | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] |
| A–B1 | Difference in silent-defect rate | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] |
| A–B2 | Difference in silent-defect rate | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] |
| B–B1 | Difference in silent-defect rate | +0.000 | [−0.091, +0.091] | n/a (GLMM unfitted; McNemar fallback) | 1.0000 | 1.0000 | No measurable effect — the safety skill adds no silent-defect reduction over SDP structure (clean null). |
| B–B2 | Difference in silent-defect rate | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] |
| B–A2 | Pure paradigm effect (gate + safety skill held constant) | +0.000 | [−0.091, +0.091] | n/a (GLMM unfitted) | 1.0000 (McNemar exact) | — (outside the registered Holm family) | Pure paradigm null — SDP vs imperative shows no silent-defect difference with gate+skill held constant; the declarative advantage is in code surface area (§9.8), not silent-defect safety. |
| A2–B2 | Safety-skill effect within the imperative paradigm | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] |

_GLMM not fitted this round: `statsmodels` is not installed and the registered GLMM needs Arm A as the reference level; B–B1 and B–A2 use the exact-McNemar fallback with bootstrap percentile CIs (10k, seed 20260623, resampled at the (task,seed) level)._

### 9.4 H2: compute-to-correct (intra-paradigm only)

H2 is reported **only within the imperative paradigm**. The primary contrast is A versus B2 (imperative without versus with the gate); A versus A2 (imperative without versus with gate + safety skill) is a secondary intra-paradigm contrast. The cross-paradigm **A-versus-B compute comparison has been removed**: imperative arms run on classic `local[*]` Spark and SDP arms run on a Spark Connect server, so a per-iteration executor-second on one arm is not the same physical unit as on the other. Differencing them would report an engine difference as if it were a gate saving — methodologically invalid. The engine confound is therefore handled by scoping, not by normalization: compute is differenced only between arms that share an engine. (Across paradigms, the substrate-independent contrast is H1, the silent-defect rate; wall-clock and iteration counts serve as the common within-substrate proxy for effort, §7.)

| Comparison | Analysis set | Matched pairs | Median executor-seconds saved | Mean executor-seconds saved | 95% CI | USD saved at measured scale |
|---|---|---:|---:|---:|---|---:|
| A vs B2 (primary; gate effect, imperative) | Intention-to-treat | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] |
| A vs B2 (primary; gate effect, imperative) | Complete-case sensitivity | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] |
| A vs A2 (secondary; gate + safety skill, imperative) | Intention-to-treat | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] |
| A vs A2 (secondary; gate + safety skill, imperative) | Complete-case sensitivity | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] |

### 9.5 Dry-run interception rate

| Arm | Failing iterations | Dry-run interceptions | Interception fraction | Structural classes intercepted |
|---|---:|---:|---:|---|
| B | 153 | 74 | 0.484 | D1/D4/D5 (structural) |
| A2 | 9 | 2 | 0.222 | D1/D4/D5 (structural) |
| B2 | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] | [PENDING — Decision-B phase] |

### 9.6 Ablation decomposition

| Mechanism question | Contrast(s) | Result placeholder | Planned interpretation rule |
|---|---|---|---|
| Does full SDP+safety+gate improve over imperative? | A–B | [PENDING — Decision-B phase] | Supports Claim 1 only if CI/test meet pre-registered criteria |
| Is SDP structure sufficient? | A–B1, B–B1 | **B–B1 null (Δ +0.000, p=1.0): SDP structure is sufficient; the safety skill adds nothing measurable over it. (A–B1 pending Arm A.)** | If B1 approaches B, structure explains much of the effect; if not, gate/skill matter |
| Is a gate alone sufficient? | A–B2, B–B2 | [PENDING — Decision-B phase] | If B2 catches structural defects but silent semantic defects remain, gate-alone is insufficient |
| Is the *paradigm* itself responsible, controlling for gate + safety skill? | B–A2 | **Paradigm null (Δ +0.000, McNemar p=1.0): with gate+skill held constant, SDP shows no silent-defect advantage over imperative. The declarative win is in code surface area (§9.8).** | The pure paradigm effect: if B improves on A2 with gate and safety skill held constant on both, the declarative paradigm has an effect beyond prompt/gate; if not, the observed B advantage is attributable to the shared gate+skill, not to SDP structure |
| Does the safety skill help the imperative arm? | A2–B2 | [PENDING — Decision-B phase] | If A2 ships fewer silent defects than B2 (both imperative + gate), safe-practice guidance helps imperative agents; quantifies the skill's paradigm-independent value |
| Does the gate reduce compute-to-correct (intra-paradigm)? | A–B2 H2 table | [PENDING — Decision-B phase] | Supports H2 only if gated compute is lower under the pre-specified test; measured only within the imperative paradigm (cross-paradigm compute is not comparable, §9.4) |

### 9.7 Figure shells

**Figure 1. Agent-native Spark loop.** `[PENDING clean run — diagram finalization]` A schematic of propose → structural gate → execute → output oracle → feedback, with the agent limited to transform authoring and the orchestrator owning Spark Connect.

**Figure 2. Silent-defect rate by arm.** `[PENDING clean run — N=TBD]` Bar plot with bootstrap confidence intervals for Arms A, B, A2, B1, and B2.

**Figure 3. Compute-to-correct (intra-paradigm).** `[PENDING clean run — N=TBD]` Paired distribution of executor-seconds-to-correct within the imperative paradigm (A versus B2, and A versus A2); no cross-paradigm compute comparison is shown, as the engines differ (§9.4).

**Figure 4. Defect-stage decomposition.** `[PENDING clean run — N=TBD]` Stacked bars for dry-run, runtime, never, and n/a detection stages by defect class and arm.

### 9.8 Conciseness — declarative vs imperative surface area (C1)

Paired over (task, seed) on the final accepted program, n=**40** cells where both B and A2 completed. Gate + safety skill are held constant, so the difference is the declarative-vs-imperative paradigm. Positive Δ (A2−B) means the declarative agent wrote less.

| Metric | B (declarative) | A2 (imperative) | Δ (A2−B) | 95% CI (bootstrap) | B smaller by |
|---|---:|---:|---:|---|---:|
| Final program LOC | 68.1 | 116.7 | +48.5 | [+41.7, +55.6] | 41.6% |
| LOC (transform body only) | 62.0 | 109.6 | +47.6 | [+41.1, +54.5] | 43.4% |
| AST node count | 638.9 | 1024.5 | +385.6 | [+322.4, +455.1] | 37.6% |
| AST nodes (body only) | 624.4 | 998.1 | +373.8 | [+310.7, +442.3] | 37.4% |

The declarative agent writes **~42% fewer lines and ~38% fewer AST nodes** for the same task under the same skill+gate — a large effect with tight CIs. Read jointly with the H3 nulls (§9.2–9.6): SDP collapses the *code surface area* the agent must author, but structure alone does not reduce silent semantic defects.

---

# Part 2 — SDP on Spark Connect: the agent-native development loop

Part 1 framed SDP as a *safer agent surface*. Part 2 explains what that surface actually is and why it is safe. The whole part hangs on one thesis.

## 10. SDP enforces a strict control boundary: the agent authors; the system orchestrates and executes

> **The agent authors declarative transforms only. The SDP runtime is both the orchestrator — it builds the dataflow graph and resolves inter-table dependencies — and the executor — it owns the session and materializes the outputs. The agent never owns a `SparkSession` and never manages execution.**

This is the heart of the safety argument, and it is what separates SDP from imperative PySpark as an agent interface.

In **imperative PySpark**, the program *is* its execution. The agent's code acquires a `SparkSession`, reads inputs, transforms DataFrames, and writes tables; to "run" the artifact is to execute the agent's own session-owning program. The agent therefore owns the orchestration (what runs, in what order, against which catalog) *and* the execution (when a plan becomes a job, on which session, with which configuration). Every hallucination the agent has about context — a path, a config key, JVM availability, the session time zone — is one `getOrCreate()` away from becoming a live job or a materialized, silently-wrong table.

In **SDP**, those two responsibilities are taken away from the agent and given to the runtime:

- **The agent supplies declarative transforms.** It writes Python functions decorated with `@dp.materialized_view` (batch) or `@dp.table` (streaming). Each function returns an *unresolved* DataFrame describing a dataset. The function body is a description, not a run.
- **The SDP runtime orchestrates.** Given the decorated module and a specification file, the runtime discovers the datasets, reads the `spark.read.table("…")` references inside the transform bodies to infer inter-table dependencies, and builds the dataflow graph (a topological order of materializations). The agent never writes orchestration code; it never calls `.start()`, never sequences jobs, never wires one table's output into another's input by hand.
- **The SDP runtime executes.** The `spark-pipelines run` command (under the hood, `pipelines/cli.py run --spec …`) acquires the Spark Connect session, materializes each dataset in dependency order under the declared storage, and signals completion. The agent never holds the session and never owns the execution lifecycle.

The orchestrator-and-executor split is visible directly in the repository. The runner's `materialize_workspace` (in `harness/runner.py`) writes the SDP project as two kinds of file: a **pure-boilerplate specification** generated by `_sdp_spec(...)` (no agent logic), and the agent's transform code placed *verbatim* under `transformations/pipeline.py`. The specification, generated by the harness, looks like:

```yaml
name: <task>__<arm_id>
storage: <cluster-reachable URI, or file://… locally>
catalog: spark_catalog
database: default
libraries:
  - glob:
      include: transformations/**
```

The agent contributes only the contents of `transformations/pipeline.py`. The `ConnectExecutor.run_execute` method (in `harness/backends/live.py`) then invokes `python3 $SPARK_HOME/pipelines/cli.py run --spec spark-pipeline.yml` — it is the SDP CLI, not the agent's code, that opens the session and materializes the graph. The GitOps demonstration (Part 3) states the same boundary from the deployment side: a banner in each generated `pipeline-definitions/<slug>/transformations/pipeline.py` records that "the agent that wrote this file never held a Spark session."

This is why SDP is GitOps-reconcilable and imperative code is not (developed fully in Part 3): a declarative spec plus inert transform functions are *desired state* that a controller can reconcile by running the runtime; an imperative session-owning program is *its own execution*, so whoever applies it must hold a session.

## 11. The empirically-verified open-source SDP API

The SDP arms use the **open-source Apache Spark `pipelines` framework** (PySpark 4.1), *not* Databricks Delta Live Tables. This distinction is load-bearing: a base model that drifts toward the Databricks `import dlt` API produces code that does not run here. The `pyspark-sdp` skill (`experiments/safe_agent_study/skills/pyspark-sdp/SKILL.md`) documents the API that was empirically verified against the runtime, and is loaded into the SDP arms' system prompt (Arm B and B1 carry the `pyspark-sdp` skill). The verified contract is:

- **Import.** Use the OSS `pyspark.pipelines` module (conventionally aliased `dp`); the Databricks `import dlt` API does not exist on this substrate and fails immediately. *Constraint:* the agent must target the OSS framework, not the Databricks one.
- **Decorator must match the relation.** Batch relations are declared with `@dp.materialized_view`; genuine streams with `@dp.table`. Mismatching the decorator to the relation is rejected at dry-run. *Constraint:* choose the decorator by whether the source is actually a stream.
- **Session acquisition.** Reach the session through the active-session accessor; the CLI injects no `spark` global, and the convenience read-helpers a model tends to invent on the pipelines module do not exist. *Constraint:* there is exactly one supported way to obtain the session.
- **Inter-table dependencies are declared by reads.** Reference an upstream dataset by reading it as a table; the runtime infers dependency order from those reads, and an unqualified name that fails to resolve must be catalog-qualified. *Constraint:* dependencies are expressed as reads, never wired by hand.
- **No analysis in query functions.** A transform body must return an *unresolved* plan: any eager action that forces analysis or execution is rejected, and only lazy plan-builders are permitted. *Constraint:* the function describes a dataset, it does not run one.
- **Specification requirements.** The pipeline spec must name the pipeline, give `storage` as a URI (bare paths are rejected), include both a catalog and a database, and point its libraries at the transforms with a recursive glob. *Constraint:* the spec must fully and unambiguously locate the transforms and their output catalog.

The dry-run gate workflow follows directly: write the transforms, run the **driver-only dry-run** (no executors; seconds of wall-clock; zero executor cost), and on failure the bracketed error class returned by the runtime names the fix directly (wrong decorator, an eager action to remove, an unqualified name to qualify, a missing catalog/database in the spec); re-run until clean, then run to materialize. The structural classes the gate catches map exactly onto D1/D4/D5 from Part 1.

### 11.1 A minimal two-dataset pipeline

The shape the agent authors — and nothing more — is:

```python
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

@dp.materialized_view(name="silver_orders")
def silver_orders():
    src = SparkSession.active().read.table("bronze_orders")
    return src.where(F.col("order_id").isNotNull())

@dp.materialized_view(name="gold_daily")
def gold_daily():
    src = SparkSession.active().read.table("silver_orders")
    return (src.groupBy(F.to_date("event_ts").alias("event_date"))
               .agg(F.sum("amount").alias("revenue")))
```

There is no `SparkSession.builder`, no `.write.saveAsTable(...)`, no `.start()`, no `.awaitTermination()`, no execution sequencing. The dependency `gold_daily → silver_orders` is *declared* by the `read.table("silver_orders")` call, and the runtime orders the materializations accordingly. This is the entire agent surface for an SDP arm.

## 12. The safety skill

Arm B additionally carries the `spark-safety` skill (`experiments/safe_agent_study/skills/spark-safety/SKILL.md`), which encodes general safe-engineering practices for exactly the silent-correctness classes the oracle grades. Each practice maps to a defect class:

- **Watermarks (state robustness; relates to D3).** Bound stateful streaming aggregations with `df.withWatermark("<event_ts>", "<delay>")`; apply only to genuine streams (`@dp.table` over `readStream`), never bolt onto pure batch. (This practice targets the D3 unwatermarked-dedup failure mode, a state-class defect the offline grader does not score; it is retained in the skill for engineering realism and discussed under *Future work*.)
- **Deterministic deduplication (D6).** Deduplicate with an explicit window — `Window.partitionBy("<entity_key>").orderBy(F.col("<event_ts>").desc_nulls_last(), F.col("<seq>").desc())` plus `row_number() == 1` — partitioning by the true entity key and ordering by a real timestamp or monotonic sequence with a deterministic tiebreaker, so the same input always yields the same survivor. Never `dropDuplicates()` across all columns when which row survives matters.
- **UTC normalization (D7).** Branch on timestamp format (epoch-milliseconds string versus ISO-8601) before parsing, and set `spark.sql.session.timeZone` to UTC explicitly. Do not declare an event-time column as `TIMESTAMP` in a `from_json` schema if it can arrive as an epoch string — it misparses and buckets land in the wrong day.
- **Quarantine / expectations (D8).** Route unparseable/contract-violating rows to a quarantine dataset and apply explicit validity predicates; type external columns as strings first, validate, then cast. The invariant is rows-in = clean rows + quarantined rows, with aggregates computed only over rows that passed every expectation — nothing lost without a trace.

Because the safety skill overlaps the oracle's defect classes by construction, Arm B's H1 effect must be read precisely as *"an agent given safe-practice guidance ships fewer silent defects,"* not *"declarative structure alone is safer."* This is exactly why H3 separates the safety skill (Arm B carries it, B1 does not) from SDP structure (both B and B1) and from the gate (B and B2). The limitation is restated in the Discussion.

## 13. The Spark Connect compatibility matrix

Spark Connect separates client and server, which is what makes it a natural substrate for agent-native development: the agent runs as a constrained client while execution happens on a controlled server. But Connect is not neutral plumbing — it *rejects* patterns that imperative PySpark agents routinely reach for. The study treats those rejections as part of the paradigm under test: if a surface encourages patterns that are unavailable or unsafe under Connect, that is a property of the surface.

| Pattern | Works on Connect? | Why | Study framing |
|---|---:|---|---|
| Agent authors SDP transforms; runtime writes/reads the spec and runs dry-run/run | Yes — the intended path | The agent supplies declarative table logic; session, graph, and execution are owned by the runtime | Safe agent-native surface |
| Agent returns a DataFrame from a constrained function; orchestrator materializes the output table | Yes, with caveats | The orchestrator still owns the entrypoint, table name, and output read-back | Baseline/ablation surface |
| Agent uses `spark.sparkContext`, `_jvm`, or direct JVM attributes | **No** under the Connect client | Connect deliberately does not expose local JVM/driver internals; failures surface as `JVM_ATTRIBUTE_NOT_SUPPORTED` | Paradigm-intrinsic risk of imperative code that assumes driver ownership |
| Agent mutates static Spark configs after session creation | **No** / unsafe | Static configs cannot change once the session/server is established; surfaces as `CANNOT_MODIFY_CONFIG`/`CANNOT_MODIFY_STATIC_CONFIG` (SQLSTATE 46110) | Structural defect class D5 |
| Agent assumes local filesystem paths are visible to driver/executors | **No** for remote EKS Connect | Client, driver pod, and executor pods do not share a filesystem; input must be staged to cluster-readable storage | Removed as a harness confound via Connect staging (D-3) |
| Agent shells out to local CLIs or assumes a local Spark install/topology | Fragile | The command may run in the client sandbox, not on the cluster; no shared classpath, catalog, auth, or storage | Imperative surface hazard |
| Orchestrator stages NDJSON through Connect into S3 and threads the staged path to both agent and oracle | Yes — the intended live path | Rows cross the Connect protocol; executors read/write S3 via IRSA | Controlled substrate mechanism |

These error classes are not hypothetical: `JVM_ATTRIBUTE_NOT_SUPPORTED` and `CANNOT_MODIFY_CONFIG` are among the structural error signatures the harness extracts from analysis/runtime logs (`harness/backends/live.py`; `harness/oracles.py`), and the local-filesystem assumption is precisely the failure that deviation D-3 records and fixes via `ConnectExecutor.stage_input`.

## 14. Imperative-fails-on-Connect is a finding, not a harness artifact

This is the first half of the **dual penalty** introduced in the Introduction: imperative agents are architecturally incompatible with Connect-isolated deployment (penalty 1), independently of how often their code produces silently-wrong data (penalty 2). The two are deliberately measured apart — penalty 1 is the architectural finding of this section, observed on the remote Connect substrate (Part 4, §21.1); penalty 2 (the silent-defect rate, H1) is measured in Part 1 on each paradigm's *native* engine precisely so that penalty 1 does not pre-empt the baseline. Stating this plainly resolves an apparent tension: there is no contradiction between "imperative fails on Connect" (penalty 1, here) and "we ran the imperative silent-defect baseline on local Spark" (the measurement of penalty 2, Part 1) — they are two distinct findings about two distinct failure modes, not a result and a hedge against it.

A natural objection is that imperative arms fail on Connect because the harness set them up to fail. The design specifically forecloses that reading, and deviation D-4 records the work done to make the failure attributable to the *agent's code* rather than to asymmetric scaffolding:

1. **Shared controls.** All arms share base model, task prompt, seed, sampling controls, iteration budget, and the same live Spark Connect backend; `harness/arm_manifest.py` enforces this before a run starts (Part 1, §2.1).
2. **Complete per-arm file contract.** The runner writes exactly the files each backend will read — SDP arms get the spec plus transform module; imperative arms get the executable module; the imperative gated arm additionally supports an analyze-only pass — so no arm fails merely because the runner forgot to write a file (deviation D-4/B2).
3. **The imperative agent genuinely owns its program.** Under D-4 the harness stopped appending its own `SparkSession.builder.getOrCreate()` and `saveAsTable` wrapper to imperative code. The agent's `pipeline.py` must itself acquire a session, read `AGENT_INPUT_PATH`, materialize the contract output table, and print the completion sentinel; the harness injects only a neutral environment and verifies completion by reading the contract table back. This means an imperative agent's classic/JVM Spark choices are the agent's, so a Connect incompatibility is the agent's, not the harness's.
4. **Uniform staging.** Every arm stages input to cluster-readable storage by the identical code path, removing the local-filesystem break as a differential confound (D-3).
5. **No free time-zone advantage to SDP.** The SDP spec deliberately omits a `configuration:` block; pinning `spark.sql.session.timeZone` there would hand only the SDP arms correct UTC behavior and mask the D7 defect the oracle is designed to catch. UTC handling is the agent's job in every arm (Part 4, §22; the in-code comment on `_sdp_spec`).

Given these controls, when an imperative agent writes code that assumes JVM access, mutates static config, or shells out to unavailable local tools, that is evidence about the danger of an imperative Spark surface under Connect isolation — exactly the safety thesis of Part 1 — and not evidence that the harness was unfair. This architectural incompatibility (penalty 1) is a paradigm-level finding in its own right; it is *not* the silent-defect comparison. The clean cross-paradigm contrast on the failure mode we care about is H1 (silent-defect rate), and to keep penalty 1 from crashing the imperative baseline before that rate can be observed, H1 is measured on each paradigm's native engine (Part 1; Part 4, §21.1), never on Connect for the imperative arms. Whether imperative arms *can run at all* on remote Connect is the penalty-1 observation; the magnitude of the penalty-2 (silent-defect) difference remains **`[PENDING clean run — N=TBD]`**.

---

# Part 3 — Zero-Trust Agent Boundaries

Part 2 established the loop in the abstract: the agent authors declarative transforms, and the runtime orchestrates and executes them. Part 3 shows that this boundary is **enforced, not cosmetic** — and enforced twice over, at two independent layers: in the *runtime* substrate (mTLS and principal pinning, so a forged identity is refused at the server) and in the *deployment* workflow (GitOps, so the agent's only footprint is files and a pull request). The organizing principle of the whole part is one zero-trust invariant: **the agent never holds a Spark session and never executes.** Everything else is mechanism in service of that invariant.

The boundary is stated once, up front, as the contract every identity in the workflow must satisfy:

| Identity | write files | open PR | has `SPARK_REMOTE` | run the CLI | holds a session |
|---|:---:|:---:|:---:|:---:|:---:|
| Agent (PR author) | ✅ | ✅ | ❌ | ❌ | ❌ |
| PR CI (dry-run gate) | ❌ | ❌ | ✅ | ✅ (`dry-run`) | ✅ (driver-only) |
| Merge reconcile (controller) | ❌ | ❌ | ✅ | ✅ (`run`) | ✅ (full) |
| Human reviewer | ✅ | ✅ (approve) | ❌ | ❌ | ❌ |

The agent identity has propose-and-author rights only: it can write files and open a pull request, but it never possesses the Connect endpoint (`SPARK_REMOTE`), never runs the pipeline CLI, and never holds a session. Sessions belong exclusively to the CI gate (driver-only dry-run) and the controller (full run). The rest of Part 3 shows how each layer makes the four ❌ cells in the agent row *impossible to flip* rather than merely discouraged — §15 at the runtime/network layer, §16 at the git/CI layer.

## 15. The zero-trust runtime: trust boundary, mTLS, and principal pinning

The live substrate is a layered, mTLS-fronted Spark Connect cluster on Amazon EKS, with an Iceberg-on-S3 warehouse behind a Hive Metastore. For the trust argument only the boundary matters; the full provisioning specification is relocated to the companion documents (§15.3).

### 15.1 The request path is a one-way funnel to a loopback server

The path is built so the agent can never address the Connect server directly. A client (agent sandbox or operator) connects through a local sidecar to an internal network load balancer, which forwards by TCP passthrough to an **Envoy sidecar that terminates mTLS**. Envoy verifies the client certificate, derives the principal from it, injects a pre-shared key, and forwards over loopback to a **Spark Connect server bound to `127.0.0.1`**. The driver schedules executor pods; the catalog and warehouse (Hive Metastore, Iceberg-on-S3, with IRSA-scoped credentials and no static keys) sit behind the driver. The load-bearing property is the last hop: **the Connect gRPC port is loopback-bound and no Kubernetes Service targets it**, so there is no network path to the server that does not first pass through Envoy's mTLS termination.

### 15.2 mTLS and principal pinning fail closed

Identity is enforced in two independent layers, so one bypass is not enough to impersonate another principal. First, **certificate identity**: per-principal client bundles bind an identity so that *principal = certificate CN = certificate SAN = the required Spark `user_id`*, and Envoy accepts only certificates in the trust domain, strips any client-supplied principal header, and re-sets it from the verified certificate. Second, **server-side pinning**: a principal-pinning gRPC interceptor on the Connect server **fails closed** on every data-plane RPC — it rejects any request that did not traverse Envoy (no injected principal header), any request with a blank or absent `user_id`, and any request whose `user_id` does not match the verified principal (with a narrow allowlist for health/reflection). The guarantee the experiment relies on is the practical one: **a malicious in-process agent that forges a different `user_id` is refused at the server**, not merely asked not to. The local client path keeps the private key out of the agent process entirely — a key-less, JVM-less sandbox speaks loopback to an egress sidecar that holds the certificate — so the Connect endpoint and identity are *derived* from an injected principal and never trusted from the agent.

### 15.3 Provisioning detail (relocated to companion documents)

The exhaustive infrastructure specification — the Terraform stack (EKS cluster, VPC and subnets, system/executor node groups, IRSA roles, the S3 warehouse, an RDS-backed Hive Metastore, an optional SSM bastion) and the Kubernetes manifests (the mTLS-fronted Connect Deployment with its Envoy sidecar, the Hive Metastore deployment, executor pod templates, dynamic allocation, and the Iceberg catalog wiring) — is preserved in the repository rather than in this narrative, where it would bury the trust argument. See **`ARCHITECTURE.md`** for the architecture overview, **`deploy/eks/RUNBOOK.md`** for operation, and **`experiments/safe_agent_study/paper/REPRODUCTION.md`** for the full step-by-step infrastructure and run reproduction. None of that detail changes the boundary established above; it is the mechanism that stands the boundary up, not part of the boundary itself.

## 16. The GitOps demonstration

The deployment above answers "where does execution happen." The GitOps demonstration (`experiments/safe_agent_study/gitops_demo/`) answers "how does the agent's work reach execution *without the agent ever holding a session*." It is the SDP-as-orchestrator thesis (Part 2) realized as a deployment workflow: because a declarative spec plus inert transform functions are *desired state*, a controller can reconcile them, and the author can stay session-free.

### 16.1 The loop

```
 AGENT            PR / CI                       HUMAN          CONTROLLER
 (no session)     (session: dry-run gate)       reviewer       (session: run)
 ───────────      ────────────────────────      ─────────      ──────────────────
 author SDP   ──► spark-pipelines dry-run   ──► review/   ──► spark-pipelines run
 artifact         (REAL structural gate)        merge          (reconcile to state)
 → git + PR
```

The agent authors a declarative SDP artifact and opens a pull request (files + git only). On the PR, CI runs the *real* `spark-pipelines dry-run` as the agent-safety gate. A human reviews and merges. On merge, a controller reconciles by running `spark-pipelines run`. The agent never receives a Spark session at any point.

### 16.2 The components

| File | Role | Holds a session? |
|---|---|---|
| `gitops_demo/agent_pr_author.py` | Agent surface: propose transforms → render artifact → `git`/`gh` | **No** (refuses to run if `SPARK_REMOTE` is set) |
| `gitops_demo/sdp_artifact.py` | Render `spark-pipeline.yml` + `transformations/pipeline.py` (hand-rendered YAML, no `pyspark` import) | No |
| `gitops_demo/changed_pipelines.py` | CI: list changed specs via `git diff` over the definitions path (falls back to a full scan) | No |
| `gitops_demo/reconcile.py` | Controller: run `cli.py run --spec` per changed spec over `SPARK_REMOTE` | **Yes** (controller) |
| `gitops_demo/ensure_schema.py` | Idempotent `CREATE SCHEMA IF NOT EXISTS spark_catalog.gitops_demo` | **Yes** (controller) |
| `gitops_demo/local_spark_connect.sh` | Start/stop a runner-local Connect server (`spark-submit … SparkConnectServer … spark-internal`, port 15055) | n/a (infra) |
| `gitops_demo/pipeline-definitions/<slug>/` | Versioned declarative source of truth | No |
| `.github/workflows/gitops-sdp-dry-run.yml` | PR gate: start local server → ensure schema → dry-run each changed spec | Yes (CI) |
| `.github/workflows/gitops-sdp-reconcile-local.yml` | Merge reconcile: run each changed spec | Yes (controller) |

### 16.3 Enforcement: the agent path cannot acquire a session

The agent row of the boundary table that leads Part 3 is not a convention — it is *enforced* in code and asserted by tests. `agent_pr_author.py` imports no `pyspark` (verified by a clean-interpreter test that asserts `pyspark` never enters `sys.modules` even after the brain is constructed), refuses to run if `SPARK_REMOTE` is set in the environment (and strips it from every child process), never instantiates a `ConnectExecutor` or opens a `SparkSession`, never invokes `spark-pipelines`/`spark-submit`/`cli.py`, and funnels every subprocess through an allowlist of exactly `git` and `gh` (`ALLOWED_BINARIES == {"git", "gh"}`). These properties are asserted by `gitops_demo/tests/test_agent_pr_author_no_spark.py` via source inspection, AST analysis, a binary-allowlist check, and a clean-subprocess import test. The artifact renderer (`sdp_artifact.py`) likewise imports no `pyspark` and only writes text, hand-rendering the YAML (stable key order, default catalog `spark_catalog`, default database `gitops_demo`, libraries glob `transformations/**`) so the agent path stays free of any Spark or heavy import.

### 16.4 Why imperative code cannot be GitOps'd this way

The demo's README states the structural reason directly: an imperative PySpark job *is* its execution — it acquires a session, reads, transforms, and writes, so the program and the run are the same artifact, and to "apply" it you must run it, which means the actor that owns the file also owns a session. SDP separates *desired state* (the declarative spec plus `@dp.table`/`@dp.materialized_view` definitions — inert text) from *reconciliation* (`spark-pipelines run`, owned by the controller). Only that separation lets the author be session-free. This is the same orchestrator-and-executor split from Part 2, now expressed as a deployment property: declarative artifacts are reconcilable; imperative session-owning code is not.

### 16.5 Gate scope (tie-in to the pre-registration)

The CI gate runs the same structural dry-run as the experiment's gated arms, so it catches the same classes: the structural defects D1 (`UNRESOLVED_COLUMN`, 42703), D4 (`TABLE_OR_VIEW_NOT_FOUND`, 42P01), and D5 (`CANNOT_MODIFY_CONFIG`, 46110) are caught pre-merge; the semantic classes (D2, D6, D7, D8) are, by design, *not* caught by a structural gate and remain the responsibility of review, the safety skill, and (in the study) the output oracle. This is the GitOps embodiment of the structural/semantic split from Part 1.

### 16.6 Demonstrable now versus documented as production

The repository is explicit about what is runnable today versus what is documented for production:

- **Demonstrable now (local in-runner Connect).** The dry-run gate and the reconcile step run end-to-end against a Connect server started *inside the CI runner* by `local_spark_connect.sh` (`spark-submit --class org.apache.spark.sql.connect.service.SparkConnectServer … spark-internal`, listening on `sc://localhost:15055`, storage under `file:///tmp/safe-spark-agents-gitops/<slug>/storage`). The smoke tests confirm that a valid spec dry-runs to "Run is COMPLETED" and that a transform reading a non-existent upstream fails with `[TABLE_OR_VIEW_NOT_FOUND] SQLSTATE 42P01` — the pre-registered D4 defect, caught pre-merge.
- **Documented as production (EKS via OIDC).** `gitops_demo/PRODUCTION_EKS.md` describes the production wiring without enabling it: the controller authenticates through GitHub OIDC to an AWS IAM role (no static keys), a private self-hosted runner inside the VPC reaches `sc://spark-connect.<ns>.svc.cluster.local:15002` directly (the Connect port is never exposed publicly), storage moves to `s3a://<bucket>/gitops_demo` with executors writing via IRSA, and an optional GitHub Environment gate requires a human approval before the reconcile job runs. The identity asymmetry is the crux: the controller identity has the IAM role and network path to open a session; the agent identity has neither and refuses to run if handed `SPARK_REMOTE`.

In other words, the *safety boundary* (agent never holds a session) is demonstrated locally today and is preserved — indeed strengthened, by IAM and network isolation — in the documented production path.

---

# Part 4 — The agentic harness we built

Parts 1–3 describe the experiment, the substrate, and the deployment. Part 4 describes the apparatus that runs the experiment and the engineering that makes its measurements trustworthy. A controlled agent-evaluation loop is not a thin wrapper around an API call: making it *honest* — controlled, bounded, isolated, validly measured, and blind — required real engineering, and the bugs that nearly invalidated it were subtle. We document the apparatus and that hardening here, because both recur in any evaluation of code-generating agents.

## 17. The controlled brain: `AnthropicBrain`

The agent's reasoning is driven by `AnthropicBrain` (in `harness/backends/live.py`), which makes **direct, fully-parameterized `client.messages.create(...)` calls** to the Anthropic API for the configured `claude-opus-4-8` model — not calls through an agent platform or framework. The request object is assembled inline by `build_request`, the Anthropic client is lazily constructed (reading `ANTHROPIC_API_KEY`), and every response's token usage is accumulated (`input_tokens`, `output_tokens`) alongside the `stop_reason`, so per-iteration request shape, stop reason, token counts, and derived cost are all recorded.

Request shape is adapted to the model family. `claude-opus-4-8` uses **adaptive (extended) thinking**: `_is_adaptive_thinking_model` recognizes the opus-4 family, and for it `build_request` sets `thinking={"type": "adaptive"}` with a high effort and transmits *no* `temperature`/`top_p`/`top_k`/`budget_tokens` (the model rejects them with HTTP 400). Legacy Claude 4.x models instead receive only a single sampling knob. Crucially, the arm manifests still carry `temperature=0.0` and `top_p=1.0` as controlled-variable *provenance* even though those values are no longer transmitted for opus — a transmit-side de-duplication, not a value change (deviations D-2 and D-6).

The choice of a direct controlled call over an agent platform is deliberate and is itself a validity property:

- **Transparency and auditability.** Every request parameter is assembled in one place; there is no middleware injecting hidden defaults, tools, ret-ries, or prompt material that could differ across arms or across runs.
- **Sampling and skill control.** The brain enforces exactly the sampling discipline and exactly the skill set declared by the arm manifest; an SDP arm's system prompt includes the `pyspark-sdp` (and, for Arm B, `spark-safety`) skill text, and an imperative arm's does not — with no platform able to add or remove capabilities behind the experiment's back.
- **Reproducibility.** With a fixed model, fixed prompt, and a controlled (or absent) sampling surface, the only intended sources of variation are the matched seed (input data) and the model's own stochasticity — which is exactly why the analysis uses multiple seeds and a random-effects model rather than assuming determinism.

The proposal parser is intentionally forgiving (§21): it extracts a fenced Python block and a `COMMAND:` line, defaults a missing command to the arm's first allowed command, and snaps any off-policy command back into the allowlist, so a malformed model turn becomes a recoverable failed iteration rather than a crash or an off-policy execution.

## 18. Per-paradigm executors

Because the two paradigms run on different engines, the harness defines a small family of executors behind a common interface (`run_gate`, `run_execute`, `read_table`, plus staging):

- **`LocalSparkExecutor`** (`harness/backends/local.py`) — classic, in-process `local[*]` Spark for imperative arms in the Part-1 LOCAL substrate. It executes the agent's `pipeline.py` in-process (passing `--analyze-only` for the gate), measures executor-seconds from the local Spark UI REST, and verifies completion with a neutral read-back of the contract output table. Its UI port is pinned distinctly so it never collides with a co-running Connect server's UI.
- **`ConnectExecutor`** (`harness/backends/live.py`) — remote Spark Connect for the Part-2 REMOTE substrate. It stages input to cluster-readable S3 (`stage_input`), runs the gate (SDP: `pipelines/cli.py dry-run --spec …`; imperative: `pipeline.py --analyze-only`), executes (SDP: `pipelines/cli.py run --spec …`; imperative: the agent's chosen command with a neutral environment), measures compute via the driver REST stage-diff (§20), and verifies completion by reading the contract table back.
- **`LocalConnectExecutor`** (`harness/backends/local_connect.py`) — a thin subclass of `ConnectExecutor` for SDP arms in the Part-1 LOCAL substrate. It targets a local single-node Connect server and replaces S3 staging with a `file://` URI (the single node shares a filesystem), but is otherwise identical, including the stage-diff measurement.
- **Subprocess CLI for SDP.** In every case, SDP gate/run go through `python3 $SPARK_HOME/pipelines/cli.py {dry-run,run} --spec …` as a **subprocess**, not an in-process call. This is required (the SDP CLI demands Connect, failing in-process with `ONLY_SUPPORTED_WITH_SPARK_CONNECT`) and is also the mechanism that keeps the runner process from ever holding a Connect session (§21).

The local single-node Connect server is managed by `LocalConnectServer` (`harness/backends/local_connect.py`). Since a pip `pyspark[connect]` install ships no `sbin/start-connect-server.sh`, the server is launched via `spark-submit --class org.apache.spark.sql.connect.service.SparkConnectServer … spark-internal` with the gRPC port and UI port pinned explicitly (so the H2 REST target is deterministic), and `CREATE SCHEMA IF NOT EXISTS spark_catalog.default` is issued at startup to avoid spurious `[SCHEMA_NOT_FOUND]` failures. The runner starts one such server at `--backend local` startup and stops it in a `finally` block.

## 19. The loop: propose → [gate] → execute → blind-grade

The episode loop (`run_episode` in `harness/runner.py`) is fixed and identical across arms; only the arm's loop fields change what happens inside it. For up to `max_iterations` (12) iterations:

1. **Propose.** `brain.propose(state, arm)` returns a `Proposal(code, command, iteration)`.
2. **Materialize the workspace.** `materialize_workspace` writes the exact per-arm file contract — imperative arms get `pipeline.py` (agent code verbatim); SDP arms get the boilerplate `spark-pipeline.yml` plus `transformations/pipeline.py`; the imperative gated arm additionally supports the analyze-only pass. Only `proposal.code` is agent content.
3. **Gate (if `arm.dry_run_gate`).** `executor.run_gate(...)` runs the structural check. A driver-only gate costs zero executor-seconds by construction; if it fails, the error class is fed back to the agent and the iteration ends *before any executor work* (counting as a dry-run interception).
4. **Execute.** `executor.run_execute(...)` runs on the engine; on failure the runtime error class is fed back; on success-with-completion the loop records the green iteration index and stops.
5. **Classify and grade.** The exit class is assigned (`completed` / `analysis_error` / `runtime_error` / `max_iterations` / `harness_error`), then `_build_profile` reads the materialized output table back through the executor and `grade_run` (Part 1, §5) grades it arm-blind.

The same `_build_profile` → `grade_run` path serves both the live and replay backends (deviation B1/B3), so a result graded offline and a result graded live are produced by identical grading logic. The arm-manifest identical-except-loop guard (`assert_identical_except_loop`) and the runtime-controls check (`assert_runtime_controls_match`) run before the sweep, so the loop above is provably the only thing that varies (Part 1, §2.1).

## 20. H2 compute measurement: the Spark driver REST stage-diff

Measuring compute-to-correct correctly was one of the harder validity problems, and getting it wrong would have silently corrupted the H2 result. Deviation D-5 records the fix.

**Why the obvious approach was invalid.** The first implementation took a before/after delta of `/api/v1/applications/{app}/executors` `totalDuration` on the Spark Connect server. But the Connect server is **long-lived and shared** across all runs: its `totalDuration` is application/driver uptime, which **increments while the application is idle**. A before/after delta therefore charges H2 for wall-clock time the application happened to be alive, double-counting and swamping the real signal.

**The valid replacement: a stage-diff.** `ConnectExecutor` now snapshots the set of stage IDs before execution (`_stage_ids_snapshot`) and, after execution, sums over only the **newly-`COMPLETE` stages** (`_stage_compute_since`): `executor_seconds = Σ executorRunTime / 1000` (ms→s) and `cpu_seconds = Σ executorCpuTime / 1e9` (ns→s). The diff is valid across the subprocess boundary even though stage *tags* are session-local, because the harness runs cells sequentially: only stages that newly reach `COMPLETE` in the execution window belong to this run. The same method is applied uniformly to both SDP and imperative branches (since H2 compares them, they must be measured identically), and a retention warning fires if the snapshot nears the default 1000-stage UI retention bound.

The cost model (`harness/cost.py`) records three compute surfaces per run (Part 1, §7): the measured stage-diff `executor_seconds`, the measured `cpu_seconds`, and the always-present `executor_seconds_wallclock` cross-check. The "unmeasured" sentinel is `None`, never `0.0` — a distinction that matters because a driver-only gate runs no executors (legitimately `None`), and conflating that with `0.0` is exactly the bug §22 describes. If the REST endpoint is unavailable, measurement degrades gracefully to `(None, None)` and the wall-clock cross-check carries the row.

## 21. Substrate routing and robustness engineering

### 21.1 Substrate routing

The runner selects a backend with `--backend {replay,live,local}` (`harness/runner.py`):

- **Part-1 LOCAL (`--backend local`).** `make_local_factories` routes per paradigm onto each engine's *home* substrate: imperative arms (A, B2) to `LocalSparkExecutor` on classic `local[*]` Spark, and SDP arms (B, B1) to `LocalConnectExecutor` on the local single-node Connect server — with all Connect work delegated to short-lived subprocess helpers, so the runner process itself never holds a Connect session. The brain is the same `AnthropicBrain` for every arm.
- **Part-2 REMOTE (`--backend live`).** `make_live_factories` routes every arm to `ConnectExecutor` against the remote EKS Spark Connect cluster, with per-cell S3 staging.

This split exists because of a hard constraint discussed next: the two paradigms cannot share a process, and on remote Connect the imperative arms cannot run at all (the dual penalty's penalty 1; Part 2, §14). Part 1 therefore measures each paradigm's silent-defect rate (penalty 2) on its home local engine (deviation D-7) — a deliberate scoping decision so that penalty 1 does not pre-empt the imperative baseline, not an accommodation that makes imperative look better. A direct consequence is that Part-1 H2 (compute) is measured on two different engines and is **not comparable across paradigms**; compute-to-correct is therefore analyzed only *within* a paradigm (H2, §8). The clean cross-paradigm contrast in Part 1 is H1 (silent-defect rate), with wall-clock and iteration counts as the common within-substrate proxy for compute.

### 21.2 Bounded and killable execution

Agents can emit non-terminating programs — most concretely, an imperative agent that reads the *streaming* framing literally and issues an unbounded `awaitTermination()`. Every execution attempt is therefore bounded by a hard timeout (150 s) and a timed-out attempt is scored as a failed iteration with corrective feedback. Subprocess executions (the Connect/SDP path) are bounded at the process level and torn down on timeout; the local single-node server is terminated with a 30 s grace and then killed. The in-process watchdog used by the local imperative executor — chosen to preserve the same-session output read-back used for grading — has a known limit recorded in deviation D-7: it cannot force-terminate a pathological pure-Python infinite loop, so it abandons a daemon thread while the run proceeds. The cap is honest about its cost: a job that would complete only beyond 150 s is recorded as a failure.

### 21.3 Process isolation

PySpark's classic-versus-Connect session mode is **process-global**: classic `local[*]` Spark and a Spark Connect client cannot coexist in one Python process (mixing them triggers errors such as `CANNOT_MODIFY_STATIC_CONFIG`/`JVM_ATTRIBUTE_NOT_SUPPORTED`). This is precisely why imperative and SDP arms cannot run in one runner process, why SDP work is delegated to subprocess CLI helpers, and why Part 1 routes the two paradigms onto separate executors/engines. Process isolation is not an optimization here; it is a correctness requirement of the underlying library.

### 21.4 Graceful degradation

The loop absorbs malformed, empty, or truncated model outputs and runtime errors as **recoverable failed iterations** with targeted feedback, not crashes: a missing code block becomes an empty (failing) proposal, a missing or off-policy command is snapped into the arm's allowlist, and an execution error class is appended to the agent's feedback for the next turn. Iteration counts and exit classes therefore *include* such failures by design, and a run is counted complete only after a neutral read-back of the contract output table.

### 21.5 Model-API-drift handling

Request shape is keyed off model family (`_is_adaptive_thinking_model`), isolating provider API changes — adaptive thinking, the rejection of sampling knobs, the migration from `claude-sonnet-4-6` to `claude-opus-4-8` — to one place rather than scattering them across the loop (deviation D-6). The base-model migration was itself forced by capability: the earlier model could not reliably produce valid OSS SDP and drifted to the Databricks `dlt` API, which is why D-6 also fixed default skill loading so the `pyspark-sdp` skill is reliably injected.

### 21.6 Resource-teardown invariants

The local Connect server is always stopped in a `finally` block even if a cell fails, so the JVM server cannot leak across the sweep; executors expose a `stop()` for session cleanup; and staging writes are namespaced per `(task, arm, seed)` so cells cannot read each other's inputs. These invariants keep a long multi-cell sweep from accumulating leaked sessions, ports, or cross-contaminated state.

## 22. Harness Threat Modeling & Validity Verification

The harness was hardened by an explicit **threat-modeling and validity-verification** process: the apparatus was treated as an adversarial target and audited for the ways it could produce *plausible but invalid* results, including by an **independent cross-vendor review** (a different model/vendor reading the diff and contract). This caught several bugs of the most dangerous kind — the kind that do not crash. The fixes are logged in `DEVIATIONS.md` (the PR-#14 review fix cycle resolved nine blockers, B1–B9) and in the D-series deviations.

- **The session-timezone confound (would have masked a defect).** An early SDP spec could have pinned `spark.sql.session.timeZone` (e.g., to UTC) in a `configuration:` block. That would have applied only to the SDP arms (B/B1), handing them correct UTC bucketing *for free* and **masking the very D7 timezone defect the oracle exists to catch** — a confound biasing H1 in favor of SDP. The fix: `_sdp_spec` emits no `configuration:` block, and an in-code comment records the reasoning; UTC handling is the agent's job in every arm, and the oracle derives UTC-correct truth independently (`output_oracles.py`).
- **The skill solution-leak (would have faked H1/H3).** With the skills directory unset, the brain loaded no skill, so SDP agents hallucinated the Databricks `dlt` API and produced zero completions — which would have made SDP look catastrophically worse for a *framework* reason rather than a paradigm reason, corrupting both H1 and the H3 ablation. The fix (deviation D-6): default the skills directory to the in-repo `experiments/safe_agent_study/skills/` when unset, so `pyspark-sdp`/`spark-safety` are reliably injected; asserted by `tests/test_skill_loading.py`.
- **The gated-arm metric sentinel (would have faked zero compute).** When a gated arm's iteration was intercepted at the dry-run gate and a fallback execute produced no live metric, summing the gate's `0.0` with an unmeasured execute as `0.0` would have reported a gated run as having consumed **zero compute** — a misleading H2 advantage. The fix (deviation D-5): the unmeasured sentinel is `None`, not `0.0`; aggregation sums only measured values, so an unmeasured gated run aggregates to `None`, not a false `0.0`.
- **Schema drift (would have desynchronized the published contract).** The published `results_schema.json` could diverge from the code's `ResultRow` dataclass under manual edits. The fix: `tests/test_published_schema.py` asserts the published schema equals the code's schema and validates real emitted rows (both measured and unmeasured-gated) against it.

These catches are the reason the apparatus can be trusted to report nulls honestly: each was a path to a *false positive for SDP*, and each was closed before any clean run.

## 23. Deviations index

All deviations are within the pre-registration (they change implementation, not hypotheses, arms, metrics, or the analysis plan) and are logged in `experiments/safe_agent_study/DEVIATIONS.md`:

- **D-0 (resolved).** Task corpus expanded from 6 to 15 tasks across three substrates, meeting the pre-reg's ≥12 target with every D-class covered by ≥5 tasks. Subsequently expanded to **22 tasks** under corpus v3 (see D-CORPUS-V3).
- **D-1.** The live loop was validated *offline* only at build time (the live sweep awaits operator GO, a reachable Connect backend, and `ANTHROPIC_API_KEY`); a pending step, not a pre-reg deviation.
- **D-2 (resolved).** Sampling: send only `temperature` (the model rejected both `temperature` and `top_p` together); both remain recorded as provenance.
- **D-3 (resolved).** Stage input/output to cluster-reachable storage via `ConnectExecutor.stage_input` (local `file:/` paths are invisible to remote pods).
- **D-4 (resolved).** Imperative arms genuinely own the `SparkSession` and output materialization, so a Connect incompatibility is attributable to the agent's code, not harness scaffolding.
- **D-5 (resolved).** H2 compute measured by before/after stage-diff (executor-seconds and CPU-seconds), replacing the invalid cumulative `totalDuration` delta; three compute surfaces reported; `None` is the unmeasured sentinel.
- **D-6 (resolved).** Base model → `claude-opus-4-8`; sampling params recorded but not transmitted; adaptive thinking; default skill loading fixed.
- **D-7 (resolved).** Part-1 substrate is LOCAL — imperative on classic `local[*]`, SDP on a local single-node Connect server — with the H2 cross-engine caveat made explicit.

---

# Discussion

## 24. Expected contribution

If the clean run supports H1 and H2, the contribution will be twofold. First, it will provide evidence that a restricted declarative SDP surface can reduce silent data-correctness defects relative to imperative PySpark for agent-authored Spark work (Claim 1). Second, it will show that a Spark Connect orchestrator can support an agent-native development loop in which the agent writes transforms while controlled infrastructure stages inputs, validates structure, executes, reads outputs, and grades correctness (Claim 2) — a loop that, as Parts 2–3 show, generalizes from the experiment harness to a GitOps deployment.

If the clean run does not support H1 or H2, that result is still informative. The pre-registration commits to reporting nulls. A null H1 would suggest that SDP structure, safety prompting, and dry-run gating are insufficient for the studied defect classes or agent model. A null H2 would suggest that dry-run iteration does not reduce compute under the observed failure distribution, or that gate overhead/agent behavior offsets saved executor work. The ablations are specifically included to avoid over-attributing any observed difference to "SDP" as a monolith.

## 25. Why the session-timezone confound was removed

Timezone/day-bucket errors are semantic defects in the study (D7), not incidental configuration drift. A naive evaluation could accidentally create or erase D7 by changing the Spark session timezone across arms or environments. The study therefore treats UTC bucket truth as part of the oracle contract, and `output_oracles.py` derives truth under UTC for the input substrate, while the SDP spec deliberately omits any `configuration:` time-zone pin (Part 4, §22). This prevents the results from being explained by one arm inheriting a different session timezone rather than by agent-authored transform behavior.

## 26. Why the harness-scaffolding confound was removed

The deviation log records a fix in which the runner now writes the complete workspace contract each backend path reads (deviation D-4/B2). This matters because an earlier differential file-generation issue could have made SDP or gated arms fail for scaffolding reasons unrelated to the manipulated loop. The current design makes the arm-specific files explicit and unit-tested: each arm receives exactly the files its backend will read, and only the agent's proposal code contains transform logic (Part 4, §19; `tests/test_workspace_contract.py`). This is necessary before interpreting imperative-vs-SDP differences as paradigm differences.

## 27. Spark Connect as both substrate and constraint

Spark Connect is not neutral plumbing. It is the safety boundary that lets the agent be a client rather than a driver owner. But it also rejects patterns that imperative PySpark agents often reach for: local JVM access, `sparkContext` assumptions, static config mutation, and local filesystem paths (Part 2, §13). The study's position is that these failures are part of the agent-surface evaluation. An agent-native Spark API should make safe patterns natural and unsafe patterns hard or impossible.

## 28. Generalizability

The study is grounded in one repository, one harness, one Spark Connect deployment design, one base model setting, and a frozen task corpus. The results may not generalize to every model, every Spark version, every data platform, or every organization's governance stack. However, the defect classes are intentionally common to production data engineering: schema drift, timestamp parsing, deduplication, time zones, missing quarantine, broken DAGs, immutable config, and state growth. The architecture also matches a realistic remote Spark deployment: EKS, Spark Connect, executor pods, Hive Metastore, Iceberg, S3, and IRSA (Part 3).

## 28a. Why the findings look this way

This section explains the *mechanism* behind the preliminary results — why they came out the way they did. The register here is different from §9: the numbers in §9 are **measured results** (facts from the data, stated plainly); everything in this section is **interpretation** — our best account of *why* those numbers arose, offered as hypothesis, not established fact. Where a claim follows by construction from how the apparatus is built, we say so; where it is a causal story we cannot yet prove, we label it explicitly. The preliminary numbers are from the N=3 mechanism round; the N=12 confirmation is in progress.

### 28a.1 Why a structural dry-run gate cannot reduce silent defects — by construction

This is the load-bearing observation, and it is not a hypothesis: it follows from the defect taxonomy itself (§4). The seven empirical classes split cleanly in two. The **structural** classes — D1 (unresolved column), D4 (missing upstream table), D5 (illegal config mutation) — are failures of *resolvability*: the code does not analyze, compile, or wire up. The **semantic/state** classes — D2 (timestamp misparse), D6 (nondeterministic survivor), D7 (timezone day-bucket), D8 (silent row drop) — are failures of *correctness*: the code resolves cleanly, runs to completion, and computes the *wrong answer*.

A dry-run validates **resolvability, not correctness**. It asks "does this plan analyze?" — it never reads a row of output. It therefore *can* catch the structural classes (and §9.5 confirms it does: Arm B's gate intercepts 48% of failing iterations) and shift them left at zero executor cost, but it is **provably blind** to the semantic classes. And the semantic classes are exactly the ones that survive into a "completed" output — which is precisely what the H1 silent-defect metric counts (§7: a silent defect requires a *completed, materialized* table). So the gate operates on one half of the taxonomy while the H1 metric is defined on the other half. A mechanism that cannot, in principle, touch the measured quantity cannot move it. This is why the gate's value shows up in compute (§9.4, §28a.4) and structural safety, never in the H1 silent-defect rate — and why no amount of gate tuning would change that.

### 28a.2 Why the constrained arms don't ship fewer silent defects — and trend slightly higher (interpretation)

The measured fact: the constrained arms do **not** beat unconstrained imperative on silent defects, and the point estimates trend the *other* way (the A–B odds ratio is 2.145, AME +0.095, both favoring more defects under B — though not significant, §8.5.3–8.5.4). This is surprising only if one expected guardrails to help with semantic correctness; §28a.1 already shows they target a different failure class. But *why the slight trend toward more*? We offer two interacting mechanisms, **explicitly as hypotheses** we cannot yet confirm.

- **(a) Completion selection (a selection effect, not a quality effect).** A silent defect can only be counted on a run that *completes* (§7). The structure and skill help an agent reach a *runnable* pipeline at least as reliably as the bare imperative agent — so the constrained arms have at least as many *opportunities* to "complete-but-wrong." A bare imperative agent that thrashes and crashes never reaches the completed-with-a-silent-defect state at all: its failures are loud (a crash, a `max_iterations`), not silent. The constrained arm converts some would-be-loud failures into quiet completions, and a quiet completion is exactly where a silent defect can hide. On this reading the trend is partly an artifact of *who gets to the finish line*, not of the code being worse.
- **(b) Wrong-target optimization.** The gate optimizes for structural well-formedness and the skill for idiomatic shape — neither optimizes for *semantic ground truth*, which nothing in the loop checks until the blind oracle reads the table. A pipeline that is wrong-but-well-formed therefore *looks* done: it analyzes, it runs, it materializes. A semantic bug that a thrashing bare agent might have stumbled over and failed on (loudly) can pass cleanly through a well-formed constrained pipeline. The guardrails make the pipeline look more finished without making it more correct.

The framing that matters: this is **not** the constrained agent being "worse." It is the guardrails targeting the wrong failure mode *for this specific metric*. Structural guardrails do their job (structural safety, compute); they were never semantic guardrails, and H1 is a semantic metric.

### 28a.3 Why conciseness is the robust, large win

The measured fact: Arm B writes ~42% fewer lines and ~38% fewer AST nodes than its imperative mirror A2 for the same task, with tight CIs even at N=3 (§9.8). The mechanism is mechanical and needs no causal hedging. The declarative surface *removes a category of code the agent would otherwise have to author* — session acquisition, orchestration and job sequencing, write-path and storage wiring, imperative control flow — because the SDP runtime owns all of it (Part 2, §10). Fewer authored lines means a smaller decision surface: fewer places for the agent to encode a mistake. Crucially, this win is **independent of semantic correctness** — it is a property of the paradigm's division of labor, not of whether the answer is right — which is exactly why it is large *and* tight even at small N, while the silent-defect effects are noisy and null. Conciseness is the cleanest signal in the study precisely because it does not depend on the hardest-to-measure thing.

### 28a.4 Why the gate still earns its place — compute

The gate cannot help H1 (§28a.1), but it is not idle. Every structural stumble it catches at the **$0 driver-only dry-run** is an executor-second not spent on a doomed run. The preliminary H2 proxy (§9.4 caveats apply — local engine, wall-clock proxy) shows the imperative+gate arm (B2) saving a median **13.6 executor-seconds** vs ungated A, and the SDP arm (B) saving **7.1**; the gate intercepts 27% (B2) to 48% (B) of failing iterations at zero executor cost. The value is **real but conditional**: it scales with (i) the per-second cost of execution — modest on a local `local[*]`, substantial on a paid cluster — and (ii) the agent's structural error rate, since the gate only earns its keep when there are structural stumbles to catch. The headline compute number therefore awaits the cluster-measured H2 (Part 4, §21.1), where executor-seconds carry their true dollar weight; the local proxy here is directional, not the claim.

### 28a.5 The synthesis — why this localizes safety and motivates a core-API change

Put together, the study *localizes where safety comes from*, and the map is clean:

- **Structure (SDP) buys conciseness** — a smaller authored surface (§28a.3).
- **The gate buys compute and structural safety** — D1/D4/D5 shifted left at $0 (§28a.1, §28a.4).
- **Neither buys semantic safety** — the D2/D6/D7/D8 silent-defect rate is untouched (§28a.1–28a.2).

The throughline is that semantic safety requires a *semantic* guardrail, and the current loop has none: nothing enforces correctness until the blind oracle reads the table *after* the fact. The constructive conclusion — stated as the study's interpretation of the honest null — is that the missing piece is a **runtime-enforced semantic layer**: declarative data-quality *expectations* the runtime checks during materialization, and graph-level conservation invariants (row-count/amount reconciliation across the dataflow graph) the runtime enforces between tables — the proposed "B+" expectations layer. To be explicit: **the honest H1 result is the argument *for* this API change, not an argument against SDP.** SDP delivers exactly what its control boundary promises (conciseness, structural safety, compute discipline); it simply does not, and structurally cannot, deliver semantic safety on its own — and that gap is precisely what a semantic guardrail at the *core API* level would close. The null is not a failure of the thesis; it is the thesis pointing at where the next layer must go.

## 29. Limitations

- **Oracle scope.** Automated oracles cover the seven empirical classes (D1, D2, D4–D8). Undeclared correctness defects may exist and are out of scope. The state-class defects D3 and D9 are deliberately not scored offline; they are treated as future work (see the *Future work* section) rather than as silent-defect outcomes.
- **Model dependence.** The study controls the base model across arms but does not claim model-independent truth until cross-model replication is run.
- **Prompt dependence.** The shared prompt is a validity control, but a different prompt could change absolute rates.
- **Single substrate implementation.** The live substrate is a specific Spark Connect/EKS stack; other Spark deployments may differ.
- **No clean numbers yet.** All empirical conclusions remain pending.

**Harness engineering and execution-validity limitations.** Building a controlled agent-evaluation loop surfaced engineering constraints that bound what the apparatus can claim. We record them for transparency, and because they recur in any evaluation of code-generating agents.

- **Bounded execution.** Agents can emit non-terminating programs --- for example, an imperative agent that reads the *streaming* framing literally and issues an unbounded `awaitTermination()`. The harness bounds every execution attempt with a hard timeout (150 s) and scores a timed-out attempt as a failed iteration with corrective feedback. This caps observable behavior (a job that would complete only beyond the timeout is recorded as a failure), and the in-process watchdog --- chosen to preserve the same-session output read-back used for grading --- cannot force-terminate a pathological pure-Python infinite loop; it abandons a daemon thread while the run proceeds (DEVIATIONS D-7).
- **Process isolation and the substrate split.** PySpark's classic-versus-Connect session mode is process-global, so imperative (classic local Spark) and SDP (Spark Connect) cannot coexist in one runner process. Part 1 runs each paradigm on its native *local* engine --- imperative on classic `local[*]`, SDP on a single-node local Connect server, with all Connect operations delegated to short-lived subprocess helpers --- and the remote Connect cluster is reserved for Part 2. A consequence is that Part-1 compute (H2) is measured on two different engines and is **not directly comparable across paradigms**: H1 (silent-defect rate) is substrate-independent and is the clean cross-paradigm contrast, whereas Part-1 H2 is reported within-substrate, with wall-clock time and iteration counts as the common proxy (DEVIATIONS D-3, D-5, D-7).
- **Recoverable-failure accounting.** The loop absorbs malformed, empty, or truncated model outputs and runtime errors as recoverable failed iterations with targeted feedback rather than crashes; iteration counts and exit classes therefore include such failures, and a run is counted complete only after a neutral read-back of the contract output table.
- **Agent-capability moderation.** Completion depends strongly on the base model's competence with the target API: a weaker base model could not reliably produce valid OSS SDP in calibration. Absolute completion and defect rates are thus properties of the specific base model, not of the paradigms alone, and motivate the cross-model replication noted above.
- **Prompt-versus-paradigm confound — now directly addressed by Arm A2.** In the original four-arm design this was a genuine limitation: because the full SDP treatment (Arm B) carried the safety skill while the imperative baseline did not, a reader could object that any B advantage was "a better prompt," not the declarative paradigm. **Arm A2 converts that acknowledged limitation into a measured contrast.** A2 is the imperative mirror of B — the dry-run gate and the `spark-safety` skill are held constant on both — so the **B-versus-A2 contrast isolates the *pure paradigm effect*** with the prompt held fixed, and the **A2-versus-B2 contrast isolates the safety skill's effect *within* the imperative paradigm**. What previously could only be hedged in prose can now be read directly off §9.3/§9.6 once the clean run lands.
- **Skill asymmetry intrinsic to the SDP paradigm (residual).** One asymmetry remains genuinely unavoidable: writing valid OSS Spark Declarative Pipelines requires the OSS SDP *API* skill, which the imperative arms have no use for, so "imperative versus SDP" still bundles "unaided-by-an-API-skill versus API-equipped" for that one skill. This is why the H3 ablation decomposes the effect across the gate, the safety skill, and the paradigm (A/B/A2/B1/B2) rather than treating SDP structure as a single monolithic factor. The safety skill itself encodes general safe-engineering practices (UTC normalization, deterministic dedup, quarantine) that overlap the oracle's defect classes by construction, so — even with A2 in place — Arm B's H1 effect is properly read as *"an agent given safe-practice guidance ships fewer silent defects,"* not *"declarative structure alone is safer,"* and A2 is precisely what lets the paradigm component be separated from the skill component.

## 29a. Future work: runtime and cluster-scale defects offline grading cannot capture

The empirical taxonomy (D1, D2, D4–D8) is deliberately bounded to what a blind, input-derived **offline** oracle can observe: structural error signatures in analysis/runtime logs, and residual semantic corruption in a *completed, materialized* table. Two pre-registered defect classes fall outside that boundary and are therefore reserved for future work, not scored here:

- **D3 — unwatermarked dedup (state class).** A streaming deduplication or stateful aggregation without a watermark does not produce a wrong *value* in a completed batch table; its failure is unbounded state growth and late-arrival semantics that only manifest over a live, long-running stream. A single materialized snapshot graded offline cannot distinguish a correctly-watermarked pipeline from an unwatermarked one that happened not to spill within the grading window.
- **D9 — unbounded state (state class).** Likewise, unbounded keyed state is a *runtime/cluster-scale* failure — memory pressure, checkpoint bloat, and eventual instability under sustained load — invisible to a one-shot offline read of the output.

Capturing D3/D9 honestly would require a different apparatus: a sustained streaming workload, state-store and checkpoint instrumentation, and runtime metrics (state-row counts over time, watermark progress, executor memory) gathered from a live cluster rather than from a materialized table. We scope these as future work precisely so the present study does not over-claim — reporting a state-class defect as "n/a" in an offline grader and then folding it into a silent-defect rate would misrepresent what was measured. The `spark-safety` skill nonetheless retains watermark and state-bounding guidance (§12) for engineering realism, and the corpus exercises streaming shapes where such guidance applies; only the *scoring* of D3/D9 is deferred.

---

# Reproduction

The full, exhaustive reproduction — infrastructure deployment (Terraform, the Spark image, Hive Metastore and the mTLS-fronted Spark Connect server, the client/tunnel path), runner configuration, offline validation, the Part-1 local run, the Part-2 remote sweep, the GitOps demonstration, and the analysis invocation — is maintained **in the repository, not in this paper**, so the academic narrative is not buried under operational detail (the reviewer's concern). It lives in the companion documents:

- **`experiments/safe_agent_study/paper/REPRODUCTION.md`** — the step-by-step end-to-end reproduction and the complete infrastructure specification (the master operational resource).
- **`ARCHITECTURE.md`** — the architecture overview.
- **`deploy/eks/RUNBOOK.md`** — operating the EKS / Spark Connect / Hive Metastore / Iceberg deployment.

In one paragraph: clone the repository on the integration line; deploy the EKS substrate and the mTLS-fronted Spark Connect server per the runbook; configure the study runner with the deployed endpoint, image digest, and executor/price parameters, and provide an Anthropic API key; run the offline validation suite (corpus integrity, identical-except-loop controls, workspace contracts, staging, sampling shape, oracles, local read-back, skill loading, the H2 stage-diff, and published-schema/code parity); run Part 1 on the LOCAL substrate (imperative on classic `local[*]`, SDP on a local single-node Connect server) and Part 2 on the REMOTE EKS Connect substrate with per-cell S3 staging; optionally run the GitOps demonstration against a runner-local Connect server; then run the analysis over the emitted result rows. Only after that analysis is produced from a clean, schema-valid live run should any placeholder in this paper be replaced.

---

# References and repository citations

This document cites repository artifacts rather than external empirical results.

**Part 1 — the experiment.**
- Pre-registration: `experiments/safe_agent_study/PREREGISTRATION.md`
- Deviations log: `experiments/safe_agent_study/DEVIATIONS.md`
- Arm manifests: `experiments/safe_agent_study/arms/A.json`, `B.json`, `A2.json`, `B1.json`, `B2.json`
- Identical-except-loop guard: `experiments/safe_agent_study/harness/arm_manifest.py`
- Task corpus and output contracts: `experiments/safe_agent_study/TASKS.lock.json`
- Seed lock: `experiments/safe_agent_study/SEEDS.lock.json`
- Shared config: `experiments/safe_agent_study/study.config.json`
- Dataset generators: `infra/gen_messy_orders.py`, `infra/gen_customers_cdc.py`, `infra/gen_payments.py`
- Blind grader and taxonomy: `experiments/safe_agent_study/harness/oracles.py`
- Output oracle: `experiments/safe_agent_study/harness/output_oracles.py`
- Result schema: `experiments/safe_agent_study/harness/schema.py`, `experiments/safe_agent_study/results_schema.json`
- Cost model: `experiments/safe_agent_study/harness/cost.py`
- Analysis: `experiments/safe_agent_study/analysis/analyze.py`

**Part 2 — SDP on Spark Connect.**
- SDP API skill: `experiments/safe_agent_study/skills/pyspark-sdp/SKILL.md`
- Safety skill: `experiments/safe_agent_study/skills/spark-safety/SKILL.md`
- Workspace materialization & SDP spec generation: `experiments/safe_agent_study/harness/runner.py`
- Live Connect backend: `experiments/safe_agent_study/harness/backends/live.py`
- Architecture overview: `ARCHITECTURE.md`

**Part 3 — infrastructure & GitOps.**
- Reproduction & full infrastructure spec (companion): `experiments/safe_agent_study/paper/REPRODUCTION.md`
- Architecture overview: `ARCHITECTURE.md`
- EKS runbook: `deploy/eks/RUNBOOK.md`
- EKS Terraform: `deploy/eks/terraform/` (`eks.tf`, `vpc.tf`, `irsa.tf`, `s3.tf`, `rds.tf`, `variables.tf`, `outputs.tf`, `README.md`)
- EKS Connect manifests: `deploy/eks/connect/README.md`
- Hive Metastore manifests: `deploy/eks/hms/README.md`
- mTLS / principal pinning: `deploy/auth/README.md`, `deploy/auth/envoy/envoy.yaml`, `deploy/auth/interceptor/` (`PrincipalPinningInterceptor`)
- Local client/sidecar: `deploy/spark-omnigent/README.md`
- GitOps demo: `experiments/safe_agent_study/gitops_demo/README.md`, `PRODUCTION_EKS.md`, `agent_pr_author.py`, `sdp_artifact.py`, `changed_pipelines.py`, `reconcile.py`, `ensure_schema.py`, `local_spark_connect.sh`, `tests/test_agent_pr_author_no_spark.py`

**Part 4 — the harness.**
- Controlled brain & executors: `experiments/safe_agent_study/harness/backends/live.py`, `local.py`, `local_connect.py`, `base.py`
- Episode loop & substrate routing: `experiments/safe_agent_study/harness/runner.py`
- Harness tests: `experiments/safe_agent_study/tests/` (`test_local_backend.py`, `test_skill_loading.py`, `test_stage_compute.py`, `test_published_schema.py`, and others)

# Appendix A. Placeholder inventory

Every item below must be filled only from a clean, schema-valid run and corresponding analysis output (Part 1, §9):

**Sample accounting (§9.1).**
1. Clean run identifier.
2. Git SHA used for the sweep.
3. Number of tasks included in the clean run.
4. Number of seeds included in the clean run.
5. Number of matched `(task, seed)` cells.
6. Arms included (A, B, A2, B1, B2) and any excluded cells.
7. Result rows passing schema validation.
8. Power-rule status and required/current paired-cell counts.

**H1 silent-defect rate by arm (§9.2) — five arms.**
9. Per-arm silent-defect counts (A, B, A2, B1, B2).
10. Per-arm run counts (A, B, A2, B1, B2).
11. Per-arm silent-defect rates (A, B, A2, B1, B2).
12. Per-arm confidence intervals (A, B, A2, B1, B2).

**H1 contrasts and GLMM inference (§9.3) — seven contrasts.**
13. A–B contrast: estimate, confidence interval, GLMM odds ratio, raw p-value, Holm-adjusted p-value, interpretation.
14. A–B1 contrast: estimate, CI, GLMM odds ratio, raw p, Holm-adjusted p, interpretation.
15. A–B2 contrast: estimate, CI, GLMM odds ratio, raw p, Holm-adjusted p, interpretation.
16. B–B1 contrast: estimate, CI, GLMM odds ratio, raw p, Holm-adjusted p, interpretation.
17. B–B2 contrast: estimate, CI, GLMM odds ratio, raw p, Holm-adjusted p, interpretation.
18. **B–A2 contrast (pure paradigm effect, new):** estimate, CI, GLMM odds ratio, raw p, Holm-adjusted p, interpretation.
19. **A2–B2 contrast (safety-skill effect within imperative, new):** estimate, CI, GLMM odds ratio, raw p, Holm-adjusted p, interpretation.

**H2 compute-to-correct (§9.4) — intra-paradigm only; A-vs-B removed as cross-engine.**
20. A vs B2 intention-to-treat: matched pairs, median executor-seconds saved, mean executor-seconds saved, confidence interval, USD saved at measured scale.
21. A vs B2 complete-case: matched pairs, median, mean, confidence interval, USD saved.
22. **A vs A2 intention-to-treat (new, secondary):** matched pairs, median, mean, confidence interval, USD saved.
23. **A vs A2 complete-case (new, secondary):** matched pairs, median, mean, confidence interval, USD saved.

**Dry-run interception rate (§9.5) — gated arms B, A2, B2.**
24. Arm B failing iterations, dry-run interceptions, interception fraction, structural classes intercepted.
25. **Arm A2 failing iterations, dry-run interceptions, interception fraction, structural classes intercepted (new).**
26. Arm B2 failing iterations, dry-run interceptions, interception fraction, structural classes intercepted.

**Ablation decomposition (§9.6).**
27. Ablation decomposition for A–B (full treatment vs baseline).
28. Ablation decomposition for A–B1 and B–B1 (SDP structure).
29. Ablation decomposition for A–B2 and B–B2 (gate alone).
30. **Ablation decomposition for B–A2 (pure paradigm effect, new).**
31. **Ablation decomposition for A2–B2 (safety-skill effect within imperative, new).**
32. Ablation decomposition for the intra-paradigm H2 gate compute effect (A–B2).

**Figures (§9.7).**
33. Figure 1 final diagram.
34. Figure 2 silent-defect-rate plot (A, B, A2, B1, B2).
35. Figure 3 intra-paradigm compute-to-correct plot (A vs B2, A vs A2).
36. Figure 4 defect-stage decomposition plot.

**Narrative.**
37. Any text in the Abstract summarizing empirical direction or magnitude.
38. Any text in the Discussion that interprets support or non-support for H1, H2, or H3.

Until these are filled from the clean run, the only valid empirical statement is: **results pending**.

# Appendix B. Task corpus and seeds (full enumeration)

This appendix holds the verbose enumerations relocated from §3 so the body can summarize by category. The authoritative source remains the frozen corpus and seed locks in the repository; the lists here are the human-readable mirror.

## B.1 The 22 frozen tasks

The corpus locks 22 tasks (corpus v3.0.0-corpus22) across six independent data substrates (orders, customer CDC, multi-currency payments, trades, clickstream, and emails). Each task's exact substrate assignment and its defect-in-scope set are recorded in the corpus lock; the task names and shapes are:

- `orders_silver_gold` — streaming medallion
- `p1_medallion` — three-layer medallion ETL
- `p2_cdc` — SCD Type 1/2 over a CDC stream
- `p3_windows` — event-time windowed aggregation
- `p4_fanout` — one stream fanning out to two tables
- `p5_mart` — batch mart over CDC output
- `p6_dedup_watermark` — streaming dedup with watermark
- `p7_late_data` — late/out-of-order with allowed lateness
- `p8_currency_normalize` — multi-currency to USD
- `p9_enrich_join` — stream–static enrichment
- `p10_scd2` — SCD Type 2 with a no-overlap invariant
- `p11_schema_evolution` — schema-evolution-tolerant ingest
- `p12_quarantine_dlq` — explicit dead-letter quarantine
- `p13_cdc_windowed` — windowed change-rate aggregation
- `p14_fx_settlement` — daily FX settlement per currency
- `new_merge_upsert` — idempotent MERGE/upsert into a keyed silver table
- `new_stream_stream_join` — stream–stream temporal join of payments to a live FX-rate feed
- `new_scd2_as_of_join` — point-in-time as-of join of payments to the SCD2 FX-rate dimension
- `new_cdc_tombstone` — CDC tombstones / hard-deletes remove customers from current state
- `new_udf_classifier` — email-subject classifier UDF (imperative + SDP/Connect)
- `HC1_fx_trade_ledger` — HC-1: multi-stage FX trade ledger (SCD2 rates → as-of USD → MERGE positions)
- `HC2_session_funnel` — HC-2: streaming e-commerce session funnel (sessionize → funnel + DLQ)

## B.2 The seeds (10 pilot → 12 power)

The pilot seed lock (`SEEDS.lock.json` v1.0.0-pilot) fixed 10 deterministic integer seeds:

```
42, 1337, 2718, 3141, 5772, 8675, 9001, 11235, 27182, 31415
```

The confirmatory lock (v1.1.0-power) appends two seeds — **16180, 14142** — *after* the original ten, giving the 12-seed list used for the power-scaled sweep:

```
42, 1337, 2718, 3141, 5772, 8675, 9001, 11235, 27182, 31415, 16180, 14142
```

Seeds are published with results and never removed or renumbered; the power-driven expansion appends new seeds in a new lock version rather than editing the existing list (deviation D-SEEDS-POWER; §3.3, §8.5). At 12 seeds × 22 tasks this is 264 matched cells per arm, clearing the 252-cell requirement the power rule derives for the A–B contrast.

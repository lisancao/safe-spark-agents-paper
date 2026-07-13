> **Safe, Governed AI Data Engineering on Spark** · *a four-part working paper*
>
> **The question.** AI coding agents now write real data pipelines. The failure that matters is not a crash, it is a job that runs green and *silently ships wrong data*. So: can you get an agent's productivity on your production pipelines **without trusting the agent**?
>
> **The answer, in four parts.** **(1) The risk, measured.** A controlled 528-run study of the same agent writing pipelines two ways, free-form imperative code versus a declarative framework (Spark Declarative Pipelines, SDP): the declarative dry-run catches **79** structural defects before any data moves, imperative catches **0**, and because a broken pipeline is rejected before any executor starts, imperative burns roughly **34× the compute** (about **1000×** on failed runs). **(2) The agent-native dev loop.** A new inner loop, propose, gate, reconcile, that closes *before any data*; the control boundary that enables it lets a declarative agent, emitting only an inert plan and never touching data or credentials, run *fully untrusted*. **(3) The platform, built and demonstrated.** An open governed stack (Spark Connect + Kubernetes + a governed catalog) whose **five per-tenant isolation layers all run on a live EKS cluster**: an agent authenticated as tenant A cannot reach tenant B by any path. **(4) Running it at fleet scale. Omnigent** holds credentials so no agent ever sees one, and its core is *demonstrated, not just argued*: from one brief, a single autonomous agent routed a live cross-vendor fleet (Anthropic + Qwen + OpenAI, re-routing when a vendor's harness failed), reviewed across vendors, and drove a custody-and-repair loop to build governed medallion pipelines for **three isolated tenants** over the live §3 platform, credential-free and contextual-policy-enforced (runnable at `deploy/omnigent/sdp-capstone/`; the quantitative fleet study is a separate paper).
>
> Each section flags its own maturity in the header. §1 and §3 carry the paper's load-bearing *measured* evidence; §4's core is *demonstrated* (the mechanism runs and built this paper's fleet capstone), with its quantitative study left to a separate paper.

# SECTION 1: Imperative vs SDP
### A Safety-and-Cost Study of AI Agents Writing Spark Pipelines

## Abstract *(Section 1)*
AI coding agents are increasingly trusted to author production data pipelines, where the failure that matters is rarely a crash but a *silent defect*: a job that runs to completion, passes its checks, and ships subtly wrong data. We ask whether the authoring **paradigm** an agent is given changes how safely it writes Spark pipelines, and at what cost. In a controlled study we hold the model, task corpus, seeds, prompt, and decoding fixed and vary only the paradigm across two arms: **A**, bare imperative PySpark, and **B**, Spark Declarative Pipelines (SDP) with its intrinsic structural dry-run and an API skill. Across 528 runs (22 tasks × 12 seeds × 2 arms; N = 264 per arm, statistically powered), SDP's dry-run intercepts **79** structural defects before any data is processed, against **0** for imperative, which surfaces the same faults only at runtime. SDP's agent also writes roughly **half the code** (−49% lines, −44% AST) at about **2.3× the tokens**, with comparable task completion. On a real EKS Spark-Connect cluster we also measure data-processing compute: because the dry-run rejects a broken pipeline before any executor starts, SDP is *categorically incapable* of burning cluster compute on a structurally-invalid pipeline; imperative spends roughly **34× the total compute** (about **1000×** on failed attempts), executing pipelines that only then fail. A raw silent-defect gap that appears to favor imperative proves, under a controlled skill-swap, to be *skill-induced* rather than paradigm-inherent: its main driver, timezone/day-bucket errors, collapses **from 7 to 0** once the SDP skill teaches a UTC idiom. We conclude that declarative structure buys an early, real safety margin on structural faults and does not, by itself, make an agent less safe on semantic ones, provided it is paired with a paradigm-matched skill.

## Introduction
Coding agents built on large language models are increasingly trusted to write data-engineering code, not toy scripts, but the batch and streaming pipelines that populate warehouses and feed downstream analytics. In that setting the failure that matters is rarely a crash. A pipeline that throws an exception is a pipeline you fix. The dangerous failure is the *silent defect*: a job that runs to completion, passes whatever checks are in place, and ships data that is quietly wrong: a dropped currency, a mis-bucketed day, a non-deterministic deduplication that changes the numbers from run to run. Silent defects are dangerous precisely because nothing announces them; they surface downstream, in a dashboard or a financial report, long after the agent has moved on.

An agent's exposure to this failure depends on more than the model. It depends on the **paradigm** the agent is asked to write in. Two are dominant on Spark. In **imperative PySpark**, the agent owns a live `SparkSession` and executes transformations directly: it builds a DataFrame and runs it. In **Spark Declarative Pipelines (SDP)**, the agent instead declares the pipeline as *desired state* (a set of `@dp.materialized_view` definitions) and a framework builds the dataflow graph and validates it with a structural **dry-run before any data is processed**. The intuition we test is simple: because SDP inspects the whole graph up front, it may catch a class of defects (unresolved columns, broken dependencies) that imperative code discovers only at runtime, or never.

This section asks whether that intuition holds, and what it costs. We run a controlled experiment that holds the model, task corpus, seeds, prompt, and decoding fixed and varies **only the paradigm**, across two arms: **A**, bare imperative PySpark (no gate, no skills), and **B**, SDP with its built-in structural dry-run and a paradigm-appropriate API skill. One asymmetry between the arms is deliberate: the dry-run gate is not something we add to arm B; it *comes with* the declarative paradigm, and imperative PySpark has no equivalent. So we do not bolt an artificial gate onto arm A either. Whether SDP's built-in gate is a fair difference or the whole point is a question we return to once the design and results are on the table (§2, §1.4.2).

Our contributions are:
1. **A controlled, pre-registered study** isolating authoring paradigm, run on a frozen instrument over 528 cells (22 tasks × 12 seeds × 2 arms; N = 264 per arm, statistically powered), with every reported number tied to raw data (§1.4, §SM6).
2. **A structural-safety result:** SDP's dry-run intercepts **79** structural defects before any data is processed, against **0** for imperative, which surfaces the same faults at runtime, a safety margin the declarative paradigm provides by construction (§1.4.2).
3. **An honest silent-defect result:** a raw residue that appears to make SDP *less* safe is shown, under a controlled skill-swap, to be **skill-induced, not paradigm-inherent**: its main driver, timezone/day-bucket errors, collapses **7 → 0** once the SDP skill teaches a UTC idiom (§1.4.1).
4. **A cost characterization:** SDP writes roughly **half the code** (−49% lines, −44% AST) at about **2.3× the tokens**, with comparable task completion (§1.4.3).
5. **A mechanistic root cause** linking a measured defect to a framework gap (SDP offers no declarative way to pin the session timezone) with concrete remediations for framework and skill owners (§SM1).

The rest of this section reads straight through: a short **reader's map** (below) decodes the running codes; **Background** introduces SDP and the silent-defect landscape; then the **design**, the **results**, the **threats to validity**, and the **conclusions**. The formal operational definitions (**§SM3**), the pre-registered run protocol (**§SM6**), and the full materials and system (**§SM7**) are collected in the **Supplemental Materials** at the end, so the study reproduces end to end without interrupting the read. The conclusion hands off to the control-boundary argument of **Section 2**.

## Reader's map: terms & codes
*The paper uses a few running codes; this is the decoder. Skip it if you already know them, and refer back when a code shows up.*

**The two arms** (the paradigm is the only thing we vary):

| arm | paradigm | gate | skill |
|---|---|---|---|
| **A** | imperative PySpark | none | none |
| **B** | Spark Declarative Pipelines (SDP) | framework dry-run (built in) | `pyspark-sdp` (API knowledge) |

**Defect classes (D1–D9)**: the bugs we deliberately seed and grade for. A structural gate can catch the structural family; it *cannot* catch the semantic family (those complete and ship, the **silent defects**); state defects aren't scored offline:

| family | codes | caught where |
|---|---|---|
| **Structural** | D1 unresolved column · D4 broken DAG · D5 immutable-config mutation | dry-run gate, before any data |
| **Semantic (silent)** | D2 timestamp misparse · D6 nondeterministic dedup · D7 timezone / day-bucket · D8 silent row-drop | only visible in output (ships) |
| **State** | D3 unwatermarked dedup · D9 unbounded state | streaming-state bugs: need a live stream to appear; **out of scope** here (future work) |

**What we measure (H1–H5):** **H1** safety (does the gate catch faults early?) · **H2** token cost · **H3** data-processing compute · **H4** conciseness · **H5** efficacy (does it finish the job?).

**Other running terms:**
- **silent defect**: a run that completes and passes its checks but ships wrong data.
- **the gate / dry-run**: SDP's structural check of the whole graph *before any data is processed*.
- **skill**: a knowledge module injected into the agent's prompt. Here it is [`pyspark-sdp`](../study/skills/pyspark-sdp/SKILL.md), a **minimal 164-line API reference** (how to declare views, run the dry-run), *not* safety advice and not task hints.
- **instrument**: the frozen harness + blind oracle + task corpus used to run and grade, version-pinned so results reproduce.
- **cell**: one run of one `(task, arm, seed)`. The study has 528 (22 tasks × 12 seeds × 2 arms).
- **N1 / N2**, the two costs kept separate: LLM **tokens** (N1) vs **data-processing compute** on the cluster (N2).
- **F1 / F2**, two framing decisions: **F1** = treat SDP's built-in gate as intrinsic and keep the asymmetry; **F2** = inject an artificial gate into imperative (rejected).
- **L0–L6**: the demonstration-layer ladder in Section 2 (how far the control boundary is proven).

**Platform and infrastructure terms (used from §2 on; standard pieces of the Spark/cloud stack):**
- **Spark Connect**: Spark's session-less client/server front end, the client ships a query *plan* to a shared server instead of driving a live engine. It is the single governed door every request goes through.
- **driver / executor**: the Spark process that plans and schedules work (driver) and the pods that actually run it (executors).
- **tenant**: one isolated customer or team sharing the same infrastructure; the whole of §3 is about keeping tenants from reaching each other's data.
- **mTLS**: mutual TLS, both client and server present certificates, so the caller's identity is cryptographic, not claimed.
- **IRSA** (IAM Roles for Service Accounts): keyless authentication from a Kubernetes pod to AWS, no long-lived keys.
- **vend**: the catalog issues a short-lived, prefix-scoped credential on demand, rather than handing out a standing key.
- **reconcile**: a controller (not the agent) drives live infrastructure to match the declared spec.
- **Lakekeeper**: an open, governed Iceberg-REST catalog. **OpenFGA**: a relationship-based (Zanzibar-style) authorization engine it uses to decide who may touch what.

## Background
**Imperative vs. declarative authoring on Spark.** Imperative PySpark is the paradigm the base model knows natively: the program acquires a `SparkSession`, reads inputs, and applies a sequence of DataFrame transformations that execute when an action is triggered. The agent is in full control, and fully responsible. SDP inverts this. The agent writes transformation functions decorated as materialized views (`from pyspark import pipelines as dp`; `@dp.materialized_view`), and the framework, not the agent, assembles them into a dataflow graph, resolves dependencies, and runs the pipeline. The agent never calls `.start()`, and, in the governed setting of later sections, never holds a session at all (§2).

**The structural dry-run gate.** The property that matters for safety is that SDP can *analyze the graph before it runs it*. Its dry-run (`create_dataflow_graph` → `register_definitions` → `start_run(dry=True)`, §1.4.2) resolves every view against the catalog and rejects structurally invalid pipelines (a column that does not exist, a view that depends on a missing upstream table, an attempt to mutate immutable configuration) **before a single executor touches data**. Imperative PySpark has no equivalent: absent a gate the agent simply builds and runs, so the same faults become runtime exceptions after work has already begun. This paper treats that gate as intrinsic to the paradigm rather than a separable feature (F1, §1.4.2).

**Silent defects and the defect taxonomy.** Not every defect is structural. We distinguish three families (§SM3.2): **structural** defects (unresolved columns, broken DAGs, immutable-config mutations: D1/D4/D5), which the gate *can* see; **semantic** defects (timestamp misparsing, non-deterministic dedup, timezone/day-bucket errors, silent row-drops: D2/D6/D7/D8), which no structural gate can see because the pipeline is well-formed and simply computes the wrong answer; and **state** defects (D3/D9): bugs in *streaming state*, such as an unwatermarked deduplication whose memory grows without bound. State defects only manifest in a live, long-running stream, so this study's offline output oracle cannot grade them; they are seeded in some tasks but left to future work, not scored here. The **semantic** family *is* the silent-defect surface, the runs that complete and ship corruption. The consequence for interpretation is sharp: a paradigm effect can appear only where the gate acts (on structural defects), or in how well the agent is *taught* to handle the semantic ones (§1.4.1).

[[[SVG-TAXONOMY]]]

**The `pyspark-sdp` skill.** The base model is fluent in imperative PySpark but not in SDP's newer API, so arm B is given a minimal [`pyspark-sdp` skill](../study/skills/pyspark-sdp/SKILL.md): a **164-line API reference** that teaches *only* the mechanics (how to declare views, wire dependencies, and write a valid spec) and, in its own words, "says nothing about what your pipeline should compute." It is the fair analog of imperative being native to the base model (arm A needs no skill), not a safety aid: an earlier `spark-safety` skill was scrapped after it moved the silent-defect rate by 0.000. Because the skill is this minimal and task-agnostic, the results are a property of the **paradigm, not of a heavy skill doing the work**; the one small idiom this skill happens not to teach (bucketing a UTC calendar day) is exactly what the residual silent-defect gap in §1.4.1 traces to.

**Substrate.** The safety, token, and conciseness results are substrate-independent and run on a local backend. The data-processing-compute question (H3) requires both paradigms on one uniform cluster and is measured separately on Spark Connect / EKS (§SM6.5, §1.4.3). All runs use a single model, `claude-opus-4-8`, with identical decoding across arms (§SM7.4).

## What we measure, and why
The study is framed as *safety and cost together*, because a paradigm that is safer but finishes the job less often, or at prohibitive expense, is not automatically the better tool. We therefore measure five families of outcome, pre-registered as a hypothesis tree (§SM6.2) and reported in §1.4:

- **H1: Safety (the headline).** Does SDP change *where* failures are caught? We measure structural-defect catching at the gate (H1.1), the failure-mode distribution (H1.2), and, as a control, the silent semantic residue no gate can catch (H1.3). Safety here is not "fewer bugs written" but "faults caught earlier, before data is touched."
- **H2: Token cost.** How many LLM tokens does each paradigm burn to reach a correct pipeline? SDP is expected to iterate more against its gate (H2.2), an honest counter-signal we measure rather than assume away.
- **H3: Data-processing compute.** How much *cluster* compute does each paradigm spend, especially on failed attempts? A gate-rejected attempt processes zero data; a runtime failure has already executed. This is the cluster/EKS-relevant cost, measured on a uniform substrate (§1.4.3, §SM6.5).
- **H4: Conciseness.** How much code does each paradigm's agent write, in lines and AST nodes? The defensible half of the "less surface area" intuition.
- **H5: Efficacy.** How often does each paradigm actually produce a *correct* completed pipeline? Direction-neutral: we report both arms and read H2/H3 *relative* to H5 as cost-per-correct-completion (H5.3): extra iterations are a win if they buy completion and a penalty only if they do not.

Two measurement choices make these outcomes trustworthy. First, we separate the two notions of "cost", LLM tokens (N1) and data-processing compute (N2), because they answer different questions and behave differently under a gate (§SM3.5). Second, we define "silent defect" and the detection stage operationally, against the instrument code, *before* looking at results (§SM3.1–3.4), so the endpoints cannot be redefined to fit the data. The full pre-registered tree, including control and rejected hypotheses, is in §SM6.2.

## Experimental setup at a glance
*What we actually ran, and where to find it. The full operational detail lives in Supplemental §SM6–§SM7; this is the intuitive version.*

| | |
|---|---|
| **Manipulation** | one variable, authoring paradigm. Arm **A** = bare imperative PySpark; Arm **B** = SDP + framework dry-run gate + `pyspark-sdp` skill |
| **Scale** | 22 frozen tasks (7 Low / 8 Med / 7 High) × 12 seeds × 2 arms = **528 runs**; N = 264 per arm (statistically powered) |
| **Model** | `claude-opus-4-8`, identical decoding across arms; **blind grading**: the grader sees only output, never the fix |
| **Data** | deterministic synthetic event streams per (task, seed), with defect traps deliberately injected |
| **Substrate: safety / tokens / code** | local backend: imperative on classic local Spark, SDP on local Spark Connect |
| **Substrate: data-compute (H3)** | a real **Amazon EKS Spark-Connect cluster**: client-mode driver pod + dynamically-allocated executor pods, Iceberg tables on S3, mTLS-fronted ingress |
| **Instrument** | frozen (`instrument-v3.2-frozen`); every reported number cites a committed results file |

**The loop, for one cell.** For each (task, arm, seed): generate the seeded data → the agent proposes a pipeline → **[gate]** SDP dry-runs the whole graph before any data is touched (imperative has no such gate) → execute → blind-grade the output against ground truth → record cost; repeat to a fixed iteration cap (`harness/runner.py`, `run_cell` / `run_episode`).

[[[SVG-RUNLOOP]]]

**Where to find it**: study repo [`lisancao/safe-spark-agents`](../study/) (paths under `study/`):
- **Reproduction runbook** → [`repro/REPRODUCE.md`](../study/repro/REPRODUCE.md)
- **Frozen corpus & seeds** → [`TASKS.lock.json`](../study/TASKS.lock.json), [`SEEDS.lock.json`](../study/SEEDS.lock.json)
- **Arms & config** → [`arms/A.json`](../study/arms/A.json), [`arms/B.json`](../study/arms/B.json), [`study.config.json`](../study/study.config.json)
- **Harness & analysis** → [`harness/`](../study/harness), [`analysis/analyze.py`](../study/analysis/analyze.py)
- **EKS compute run (H3)** → [`repro/h3_eks/`](../study/repro/h3_eks) (runbook + `H3_EKS_INTEGRATION_LOG.md`)
- **Raw results behind the numbers** → `results.powered.AB.n12.final.jsonl` (528 rows), `results.tzfix.jsonl` (the D7 skill-swap)

*(GitHub links resolve once the study repo is public; the committed result JSONLs are the data behind every number, while the 100s of MB of raw generated data and agent transcripts are reproducible from the seeded generators rather than shipped.)*

## 1.1 Research question
When an AI agent writes Spark pipelines, does **forcing it to use Spark Declarative Pipelines (SDP) instead of imperative PySpark** produce **safer** code, and **at what cost**? The manipulation under study is **paradigm, and only paradigm**. The dry-run gate and the safety skill are **held constant**, not varied: they are part of the controlled environment, not the treatment.

## 1.2 Design: one manipulation, controlled environment
The study rests on a single, deliberate manipulation. Everything a reader might suspect of driving a paradigm difference (the model, the tasks, the seeds, the prompt, the decoding) is held fixed, so any difference in outcome is attributable to paradigm alone. The one asymmetry we *keep* is the structural gate, because it is intrinsic to declarative authoring and cannot be given to imperative code without making it something other than imperative. The design below makes both choices explicit.

Independent variable: **paradigm** (SDP vs imperative), tested as **two arms** (design LOCKED 2026-06-29, §SM6.1). Held constant: model, task corpus, seeds, prompt, temperature, iteration cap.

| Arm | Paradigm | Gate | Skill | Role |
|---|---|---|---|---|
| **A** | imperative | none (bare) | none | bare imperative, imperative as it natively is |
| **B** | SDP (declarative) | framework dry-run (intrinsic) | `pyspark-sdp` API skill | declarative treatment |

**Headline contrast = A vs B.** This is deliberately a *paradigm-package* contrast, not a single-variable manipulation: the declarative paradigm brings its structural dry-run **by construction** (framing F1, §1.4.2), imperative has no equivalent, and injecting one would contaminate it (F2, rejected). So the gate is **part of the treatment, not a held-constant covariate**; that asymmetry *is* the finding. The earlier A2 (imperative+gate+skill) and B1 (SDP, no skill) arms are retired to `arms/supplementary/`; the `spark-safety` skill was scrapped everywhere (it moved silent-defect by 0.000 in pilot and was the largest reviewer confound). `[arms: study/arms/{A,B}.json]`

## 1.4 Results
**The headline first, so it is not missed: SDP's structural dry-run catches 79 structural defects before any data is processed; bare imperative catches 0** (§1.4.2). That is the load-bearing result of the study. The rest of this section fills in three connected stories around it: a **silent semantic residue** (§1.4.2's sibling, §1.4.1) where a *raw* gap appears to favor imperative but proves *skill-induced*, not paradigm-inherent, once a controlled skill-swap closes it; the **root-cause attribution** of that gap (§SM1); and **cost** (§1.4.3), code size, tokens, and compute. Read together: declarative structure buys an early, real safety margin on structural faults, and is not, by itself, less safe on semantic ones.

*All numbers below come from one run: the **powered A-vs-B run** of 528 cells (264 (task,seed) pairs × arms A/B), on the frozen instrument, with 0 instrument-fault rows. It is statistically powered (N = 264 ≥ 260 required), and inference uses a mixed-effects logistic model with Holm correction and bootstrap CIs. The full inference spec, the exact recompute command, and provenance are in **§SM6**.*

### 1.4.1 Silent-defect rate (semantic residue): clean A-vs-B (N=264/arm)
*An honest counter-signal, explained just below, and not paradigm-inherent.* On the one endpoint a structural gate *cannot* see, the rate of silent semantic defects, arm B (SDP) comes out slightly **higher**. Do not stop at the raw number: the rest of this subsection shows the gap is a single missing skill idiom that closes to parity once the skill teaches it.

| arm | silent-defect rate | k/n | 95% CI |
|---|---|---|---|
| A (bare imperative) | 0.277 | 73/264 | [0.223, 0.330] |
| B (SDP) | **0.326** | 86/264 | [0.269, 0.383] |

Paired A−B contrast: Δ = −0.049 [−0.098, +0.000]; **OR = 1.97** (B vs A); GLMM p = 0.0033, Holm-adjusted p = 0.0033, **significant at α = 0.05**.
`[src: results.powered.AB.n12.final.jsonl · silent_defect · per arm + paired (task,seed), Holm over GLMM contrasts · recompute: §SM6]`

**The gap is skill-attributable, not paradigm-inherent.** The raw contrast shows B higher, which would reject the "un-gateable ⇒ paradigm-invariant" expectation (§SM3.2). It is not the paradigm, though. The gap sits in two semantic classes: timezone/day-bucket (D7) and silent row-drop (D8); the largest class, dedup (D6), is a wash. A controlled skill-swap pins the driver: arm B's minimal `pyspark-sdp` skill happened to be silent on *one* idiom (how to bucket a UTC calendar day) and teaching it drives **D7 from 7 to 0**, matching imperative (`results.tzfix.jsonl`). So the honest reading is not "SDP is less safe," but that a minimal skill has to teach the paradigm-matched idiom; once it does, the paradigms reach parity. The mechanism (why the one-line fix imperative uses isn't available in SDP) and the parallel D8 analysis are in **§SM1**, kept off the main line. *(Pilot N = 3: A = 18/66, B = 23/66, comparable.)*

### 1.4.1.1 Silent-defect composition: which classes, and where SDP loses
The B-worse residue is **not uniform**: it decomposes by semantic class (shipped = `detection_stage == never`):

| class | A ships | B ships | read |
|---|---|---|---|
| D2 timestamp misparse | 1 | 3 | negligible |
| D6 nondeterministic dedup | 38 | 39 | **a wash**: both paradigms fail dedup ~equally; **not** SDP-specific |
| **D7 timezone / day-bucket** | **0** | **7** | **SDP-specific**: imperative *never* ships it |
| D8 silent row-drop / bad currency | 51 | 57 | B worse by +6, task-concentrated |

The whole A−B gap is **D7 (+7) and D8 (+6)**; D6, the largest class, is tied. D7 is the sharp one: imperative ships **zero** timezone defects, SDP ships 7 (mostly `p8_currency_normalize`), and it is exactly the skill-attributable driver that closes to 0 once B is taught the idiom. `[src: results.powered.AB.n12.final.jsonl · per_defect_detection]`

[[[SVG-COMPOSITION]]]

### 1.4.2 Structural-defect catching at the gate (gate-validity audit complete)
**Clean A-vs-B (528 cells).** Where structural defects (D1/D4/D5) are caught (defect-level, across ALL iterations; anti-bypass: a gate-caught-then-fixed error still counts):

| arm | at gate (dry_run) | at runtime | shipped |
|---|---|---|---|
| A (bare imperative, no gate) | 0 | 4 | 0 |
| B (SDP, framework dry-run) | **79** | 30 | 0 |

SDP's framework dry-run intercepts 79 structural defects (353 iteration-level error events) *before any data is processed*; bare imperative has no gate and intercepts zero: the structural catches surface at runtime (or, for semantic defects, ship). Arm A is *bare* imperative with **no structural gate by construction**, so the contrast measures each paradigm as it natively is: there is no gate-rigor to conflate.
`[src: results.powered.AB.n12.final.jsonl · per_defect_detection / dry_run_intercepts / per_iteration · per arm × class-group × stage · recompute: §SM6 (see §9 error-taxonomy block)]`

[[[SVG-WHERE]]]

**Framing (F1).** The asymmetry *is* the finding: the declarative paradigm provides a structural dry-run **by construction**, and imperative PySpark has no native structural gate. Injecting a harness-enforced gate into the imperative arm is explicitly rejected (F2): it would contaminate imperative with a declarative feature it would never naturally have. Stated claim:
> "SDP catches structural defects (D1/D4/D5) at a real framework dry-run *before any data is processed*; imperative PySpark has no equivalent and surfaces those defects at runtime or ships them."

This structural-catch claim is confirmed by the powered run (B = 79 vs A = 0); the silent-residue question is treated separately in §1.4.1 (skill-attributable, not paradigm-inherent). *(The retired A2 arm's gate audit and gate-design history are in Supplemental §SM2.)*

Anticipated objection ("you gave SDP a better gate") is answered directly: the gate was not *given* to SDP; it is intrinsic to declarative pipelines and unavailable to imperative without ceasing to be imperative. **This becomes the paper headline (silent-defect rate demoted to the one-line residue note in §1.4.1).**

### 1.4.3 Cost: clean A-vs-B

**Conciseness (H4): the declarative agent writes ~half the code.** Paired over (task,seed) on the final accepted program; Δ = A − B (positive ⇒ B wrote less; `*_body` excludes mandatory `@dp`/`def`/import scaffolding; the SDP `spark-pipeline.yml` is harness boilerplate, not counted):

| metric | B (SDP) | A (imperative) | Δ (A−B) | 95% CI |
|---|---|---|---|---|
| final_program_loc | 67.9 | 134.0 | **+66.1** | [+61.9, +70.4] |
| ast_node_count | 614.9 | 1105.5 | **+490.6** | [+453, +530] |

All CIs clear of zero: B is **~49% fewer LOC** and **~44% smaller AST** than imperative. `[src: results.powered.AB.n12.final.jsonl · final_program_loc / ast_node_count · paired (task,seed), B-vs-A · analyze.py conciseness block]`

**Token spend (N1): SDP costs more to author.** Median tokens to a correct pipeline (264/264 cells populated in both arms; the streaming/32k brain fix closed the prior B-null gap):

| arm | input | output | total | vs A |
|---|---|---|---|---|
| A (bare imperative) | 1,436 | 9,964 | 11,524 | 1.0× |
| B (SDP) | 7,295 | 18,499 | **26,480** | **≈ 2.3×** |

Values are medians reported per field, so input + output need not sum to the total. Direction: SDP **higher**: the declarative agent iterates more against the gate (H2.2), which shows up as tokens. Interpret jointly with H5: the extra iterations are a true cost only if they do not buy completion. `[src: results.powered.AB.n12.final.jsonl · input_tokens,output_tokens · per arm]`

**Data-processing compute (N2): SDP is *categorically incapable of burning compute on a structurally-invalid pipeline*.** This is the sharpest cost result, and it is structural, not a matter of degree. Imperative PySpark runs `spark-submit`, executing over the data, and *then* discovers the pipeline is wrong; SDP's dry-run rejects a structurally-invalid pipeline **before any executor starts**. A failed imperative attempt therefore costs real cluster compute; a failed SDP attempt costs ≈ 0, *by construction*. This scopes to *structural* defects: a semantically-wrong SDP pipeline passes the dry-run, executes, and burns compute like imperative (and ships), the un-gateable residue of §1.4.1. (In the powered run, **69.5%** of arm B's attempts were intercepted at the gate, before touching data.)

Measured directly on a live EKS Spark-Connect cluster (48-cell sweep; m5.xlarge-equivalent executors at `$0.192`/executor-hour; valid N: A = 21, B = 23 after excluding 4 instrument-fault cells):

| N2 (measured on EKS) | A (imperative) | B (SDP) | ratio |
|---|---|---|---|
| **wasted compute on *failed* attempts** | 521 exec-s · `$0.028` | 0.5 exec-s · `≈$0` | **~1000× (finite vs ≈0)** |
| **total compute** | 596 exec-s · `$0.032` | 17 exec-s · `$0.0009` | **~34×** |
| **cost per correct pipeline** | `$0.00033` | `$0.00005` | ~7× |

Nine of arm A's cells hit the iteration cap (each one running, processing data, then failing) while SDP's failed cells cost ≈ 0. **The dollar amounts are small because the study is small** (tiny tasks, a 4-executor cluster), not because the effect is: the mechanism scales linearly with data size, cluster size, and failure rate. `[src: repro/h3_eks/ · results.h3.sweep2.jsonl · 48 cells]`

[[[SVG-WASTE]]]

> **At production scale, netted: SDP comes out roughly `$2,000`/month cheaper** at ~1,000 pipelines/week, about `$5,000`/month of compute it never burns against about `$3,000`/month of extra tokens; for small pipelines the token premium dominates and the sign flips. *(A projection from the measured mechanism, not a measured result.)* The derivation: a real pipeline over ~100 GB whose failed attempt burns ~10 minutes across a 20-executor cluster wastes ≈ **`$0.60` per failed attempt**; an imperative agent that fails ~2× before it converges wastes ≈ `$1.20` per pipeline, so ~1,000 pipelines/week is the ≈ **`$5,000`/month** of compute SDP never spends. Against that, SDP's ~15k extra tokens per pipeline (§1.4.3) cost ≈ `$0.73` at representative Opus-class rates (~`$15`/`$75` per million input/output tokens), ≈ **`$3,000`/month** at the same scale. All recomputable from the token deltas above.

[[[SVG-COST]]]

## 1.5 Threats to validity
- **Imperative-gate asymmetry.** Arm A is *bare* imperative with no structural gate, so the clean structural contrast (79-vs-0) carries no gate-rigor confound: the asymmetry is intrinsic to the paradigms, not an artifact of the harness (§1.4.2). (SDP's separate "identical residue" expectation is revised by the data; see §1.4.1.)
- **Substrate split.** Imperative runs on local Spark and SDP on Connect, which blocks a fair *executor-seconds* comparison, but H1 (safety), H2 (tokens), and H4 (conciseness) are substrate-independent and unaffected. The compute claim (H3) is measured separately on the uniform EKS Connect substrate (§1.4.3).
- **Sample size.** The powered run is complete: N = 264 (task,seed) cells ≥ 260 required, with 0 instrument-fault rows. The silent-defect endpoint proved informative rather than null: arm B is significantly worse in the raw data (§1.4.1, OR 1.97, p = 0.0033), which the skill-attribution then explains.
- **Token instrumentation.** Both arms are fully token-populated (264/264); B ≈ 2.3× A (§1.4.3).

---

## 1.8 Conclusions
The study isolates paradigm and finds a clear, defensible safety asymmetry. When an agent writes in Spark Declarative Pipelines, the framework's structural dry-run intercepts unresolved columns, broken dependency graphs, and immutable-config mutations **before any data is processed**: 79 such defects caught at the gate across the powered run, against zero for bare imperative PySpark, which meets the same faults only at runtime (§1.4.2). This is not a gate we handed to SDP; it is a property SDP has by construction and imperative cannot have without ceasing to be imperative. That is the section's headline.

The counter-signal is equally important, and we report it without softening. On the *semantic* residue that no gate can catch, the raw powered run shows SDP slightly worse (silent-defect rate 0.326 vs 0.277; OR 1.97, p = 0.0033). Rather than accept "SDP is less safe," we traced the gap. It is carried almost entirely by timezone/day-bucket errors (D7) and, secondarily, silent row-drops (D8); the largest defect class, non-deterministic dedup (D6), is a wash. A three-agent code audit found the mechanism (SDP's immutable-config property removes the one-line `session.timeZone = UTC` fix imperative uses, and the base skill was silent on the replacement idiom) and a controlled skill-swap resolved the attribution: taught the UTC column idiom, arm B's D7 defects fell **from 7 to 0** (§SM1). The residue is therefore *skill-induced, not paradigm-inherent*. The honest headline is not "structure is unsafe" but **"structure alone is not enough: it needs a skill that teaches the paradigm-matched idiom; once it has one, the paradigms reach parity."**

On cost, the picture is coherent: SDP's agent writes about half the code (−49% lines, −44% AST) at roughly 2.3× the tokens: it iterates more against its gate, while completing correct pipelines at a comparable rate (65.2% vs 68.9%, a gap that itself tracks the skill-attributable D7 residue, which the UTC-idiom swap eliminates at the defect level (D7 7→0; post-swap completion not separately re-measured)). Whether the extra tokens are worth paying is a judgment about how much an early structural safety margin and half the code are worth against a token premium; the study lets that trade be made explicitly rather than assumed.

Two limitations bound these claims. The data-processing-*compute* comparison (H3) requires both paradigms on one uniform cluster and is reported separately (§1.4.3, §SM6.5); and the study fixes a single model and grants arm B an API skill arm A does not need, an asymmetry discussed in §5. Neither affects the structural-catch, token, or conciseness results, which are substrate- and skill-robust.

Finally, the safety result motivates what follows. If the most valuable thing a declarative paradigm offers is that faults can be caught, and data never touched, *before* execution, the natural next question is architectural: can we build a system in which an agent is *never* handed a live session at all, and authorship is separated from execution by construction? That is the control boundary of **Section 2**.


---
---

---

## Supplemental Materials (Section 1)

> **Reading for the results? Skip this block, jump to Section 2.** What follows is Section 1's methods appendix, detailed forensics, the pre-registered protocol, operational definitions, and full materials, retained for reproduction and deep review, not the paper's through-line. The main text cites it inline as §SM1, §SM2, §SM3, §SM6, §SM7.

## SM1. Root-cause forensics: the D7 timezone skill gap (full detail)

*Expanded from §SM1. The main text gives the resolved result (D7 7→0; parity once arm B is taught the UTC idiom). This is the underlying mechanism, the three-agent code audit, the parallel D8 analysis, the validated skill-swap, and the remediations for framework and skill owners.*

**D7 (timezone): the immutable-config safety property removes the fix imperative uses.** The executor box runs `America/Los_Angeles`, and the harness deliberately does **not** pin session tz in the SDP manifest (pinning it would hand SDP correct-UTC "for free," an asymmetric advantage `[runner.py:418-422]`) so the default session tz is Pacific for *both* arms. Imperative (A) owns its `SparkSession` and sets `spark.conf.set("spark.sql.session.timeZone","UTC")` in `main()`, then buckets with `to_date(to_timestamp(col))`, the **same construction the oracle uses to define truth** `[A/*/pipeline.py:e.g. seed42:21; output_oracles.py:101,195-199]`, so A's day-set equals the truth day-set and D7 never fires (0/12 seeds). SDP (B) authors inside `@dp.materialized_view`, where `spark.conf.set(...)` is **the D5 immutable-config gate** (`CANNOT_MODIFY_CONFIG` / SQLSTATE 46110) `[oracles.py:47-49]`; B's own transcript shows the agent writing the `session.timeZone=UTC` fix and then abandoning it ("UTC calendar day *without mutating* spark.sql.session.timeZone") `[B/seed1337 transcript]`. With no session-tz lever, B hand-rolls tz-*dependent*, payment/rate-**asymmetric** day math (`to_utc_timestamp(ts, current_timezone())`, `epoch//86400`, `date_from_unix_date`) that shifts naive-UTC instants by +7h and buckets the payment and rate sides under different assumptions → invents calendar days → D7 ships `[B/seed{1337,8675,11235}/…/pipeline.py:65/71/47]`.

**D8 (row-drop, `p1_medallion`): the same wall, plus a code-completeness gap.** Both arms end the validated layer with the *same* silent-drop filter (`.where(amount.isNotNull() & ts.isNotNull())`) and neither writes a quarantine table, so the drop is a shared control, not the discriminator. B loses two ways: (i) **4/8 cells omit the epoch-millis parse branch** A carries, so 13-digit epoch strings → NULL → dropped (offline replay: 201–267 rows / $41k–$55k per run) `[A/seed11235:71-89 vs B/seed11235:28-44]`; (ii) the other 4 cells hit the same session-tz wall (can't pin UTC → `to_date` mis-buckets epoch rows) `[B/seed{9001,31415,16180,14142}]`.

**The finding, mechanistically.** SDP's immutable-config property (D5) removes the one-line `session.timeZone=UTC` fix that imperative *and the oracle's own truth* rely on, so the SDP agent must instead get a careful column idiom exactly right. The base `pyspark-sdp/SKILL.md` is **silent on timezone** (0 references) and arm B loads no safety skill `[skills/pyspark-sdp/SKILL.md]`, so the agent, denied the lever and untaught the replacement, hand-rolled the broken math above. **This raises the *difficulty* of correct timezone handling; it does not make it impossible**, the distinction the A/B skill test below resolves.

**The attribution: it was the skill.** A controlled skill-swap A/B test re-ran arm B on the three D7-shipping tasks (`p8_currency_normalize`, `p14_fx_settlement`, `new_stream_stream_join` × 12 seeds = 36 cells) with the `pyspark-sdp` skill *augmented by the UTC column idiom*; the frozen skill restored immediately after, so the instrument stays clean. **D7 ships went 7 → 0**, every timezone defect eliminated, cells still completing (D7 resolves to `n/a`, not a failure) `[src: results.tzfix.jsonl · per_defect_detection['D7']=='never' · arm B · 2026-07-02]`. So the immutable-config constraint is real but does **not** force the defect: it raises the difficulty, and a paradigm-appropriate skill closes the gap to parity. The raw §1.4.1 B-worse residue is therefore **skill-induced, not paradigm-inherent** (D8, the other driver, is a paradigm-neutral wash). This is the validated form of remediation #2 below.

**Engineering remediation (for framework / skill owners).**
1. **Framework (highest leverage):** OSS SDP / `pyspark.pipelines` offers no *symmetric, declarative* way to pin `session.timeZone`. Add a `spark-pipeline.yml` `configuration: {spark.sql.session.timeZone: UTC}` block (or `@dp.materialized_view(session_time_zone=…)`) applied before any view evaluates. Imperative gets this for free; SDP has no equivalent, forcing fragile hand-rolled epoch math.
2. **Skill / idiom:** teach `pyspark-sdp` the column-level UTC idiom (config is immutable): `to_date(to_utc_timestamp(ts, src_tz))` applied *identically* on every joined side, always with an epoch-millis parse branch; never mix tz-shifted math on one side with a bare `to_date` on the other.
3. **Contract:** change the validated-layer contract from *drop* to *quarantine + reconcile* (`raw_count == validated_count + rejected_count`), turns D8 from a silent completion into a loud, gate-catchable failure for both arms.

> **Framing, resolved by the data.** The raw B-worse residue is **not** a paradigm effect. Its main driver (D7) is a *skill* gap that closes entirely with a UTC column idiom (§SM1 · `results.tzfix.jsonl` · **7→0**); D8 is a paradigm-neutral wash. F1's residue clause is re-locked to: **with a paradigm-appropriate skill the silent residue is comparable across paradigms; the base `pyspark-sdp` skill's silence on UTC handling, not the declarative paradigm, drove the raw gap.** The correct headline: *"structure alone isn't enough: it needs a skill that teaches the paradigm-matched idiom; once it has one, parity."* Structural-catch (§1.4.2) and conciseness (§1.4.3) are unaffected.

## SM2. Gate-design history & retired arms (full detail)

*Why the clean two-arm design carries no gate-rigor confound, and what the retired A2 arm showed. The powered run uses bare arm A (no gate) and arm B (SDP framework dry-run); the material below is the history behind that choice, kept for reviewers.*

**Gate-validity verdict (cited).** The imperative gate is NOT a harness no-op (the prior sham-gate concern does not describe the current instrument). It runs the agent own `pipeline.py --analyze-only` `[live.py:735-738, 814-823; local.py:433-486]` and caught 2 genuine structural errors in the A2 rerun: `UNRESOLVED_COLUMN` (p10_scd2/seed1337), `ATTRIBUTE_NOT_SUPPORTED` (new_udf_classifier/seed2718). Provenance clean: 66/66 A2 rows stamped `git_sha 1d28563a` (instrument-v3.1).

**BUT the gates are asymmetric; this is NOT "gate held constant, only paradigm varied":**
- SDP gate = framework-owned real dry-run (`create_dataflow_graph` / `register_definitions` / `start_run(dry=True)`) `[sdp_dryrun.py:462-484]`, guaranteed structural analysis.
- Imperative gate = agent-owned `--analyze-only`; the harness does NOT enforce real analysis. A harness-enforced imperative gate (`_df.schema`) existed at commit `ae56e82` but was deliberately removed at `a64d830` (agent owns the program); PR #43 (`1d28563a`) fixed A2 output-path validity but did not restore it.
- Therefore the *pilot's* 74-vs-2 (A2-gate) difference **conflated paradigm with gate-rigor**, which is precisely why the locked design drops A2 for a **bare A (no gate)**: the clean powered contrast is **79-vs-0** (§1.4.2), where A has no gate *by construction*, so there is no gate-rigor confound left to conflate.

> **Re-checked against the data.** Structural-catch (first two sentences) is **confirmed** (§1.4.2: B=79 gate intercepts vs A=0). The residue clause is **revised**: the raw powered run showed B's silent-defect rate higher (§1.4.1: OR 1.97, p=0.0033), but a controlled skill-swap test attributes that to a **skill gap, not the paradigm**: D7, the main driver, closes **7→0** once B is taught the UTC column idiom (§SM1 · `results.tzfix.jsonl`), and D8 is a paradigm-neutral wash. Re-locked residue claim: **with a paradigm-appropriate skill the silent residue is comparable across paradigms; the base API skill's silence on UTC handling, not the declarative paradigm, drove the raw gap.**

## Citation convention (reference for the supplemental)
Every empirical number in this paper is immediately followed by a source tag so it can be independently re-derived from raw data:

> `[src: <file> · <field> · <row-filter> · recompute: <command>]`

- **Primary raw data:** `study/results/h3_a2_rerun_20260628/results.h3_combined.jsonl` (198 rows; 66 each for arms A2, B, B1; committed on `origin/dev`, instrument SHA `1d28563a`).
- **Code definitions** are cited as `file:line` against `origin/dev`.
- Any number not yet carrying a source tag is a **placeholder** and is marked `[PENDING]`. No hand-typed numbers.

---

## SM3. Methods: operational definitions (cited)
Before any result, we fix what the words mean. Each construct below (what counts as a silent defect, how defects are classified, at what stage a defect is caught, and how we separate the two kinds of cost) is defined against the instrument code and cited to `file:line`, so the endpoints are set before the data is seen and cannot be reshaped afterward.


### SM3.1 Silent defect
A run has `silent_defect = True` iff it reached COMPLETED/materialized output AND >=1 in-scope **semantic** defect class still shows residual output corruption (`rows > 0`). Trigger: `silent_defect = outcome.completed and len(silent_classes) > 0`. `[def: harness/oracles.py:222-235 · schema: harness/schema.py:96-99]`
Per-arm rate aggregation: `[analysis/analyze.py:274-278]`; paired (task,seed) contrasts: `[analyze.py:297-304]`.

### SM3.2 Defect taxonomy: the structural / semantic / state split (load-bearing)
`[def: harness/oracles.py:36-62]`

| Class | Defects | Gate-detectable? | Consequence |
|---|---|---|---|
| **Structural** | D1 missing/unresolved column; D4 broken DAG / missing upstream; D5 immutable-config mutation | **Yes** (`dry_run_detectable: True`) | Catchable at the dry-run gate, before execution. |
| **Semantic** | D2 timestamp misparse; D6 nondeterministic dedup; D7 timezone/day-bucket; D8 silent row-drop / absent quarantine | **No** (`dry_run_detectable: False`) | Only detectable in completed output → these ARE the silent-defect classes. |
| **State** | D3 unwatermarked dedup; D9 unbounded state | n/a | Not scored offline (`oracles.py:217-220`). |

**Key consequence for interpretation:** silent defects are *semantic by construction*, and semantic defects are *un-gateable by construction*. Any paradigm effect can therefore appear only in the **structural** defects (where the gate acts), never in the silent/semantic residue. `[PAPER scope note: offline-scored classes are D1, D2, D4–D8; D3/D9 excluded, paper/PAPER.md:177-191]`

### SM3.3 Detection stage
`detection_stage in {dry_run, runtime, never, n/a}`. Meaning: `dry_run` = caught by the structural gate before any executor ran; `runtime` = caught during execution; `never` = shipped corrupt in completed output (⇒ silent_defect); `n/a` = did not manifest. `[def: harness/oracles.py:19-23, 208-245 · enum: harness/schema.py:26-28]`
Note: run-level priority is `never` > `dry_run` > `runtime` > `n/a` (NOT "earliest stage caught" as the schema comment says); Methods describes the implemented priority. `[oracles.py:237-245]`

### SM3.4 Exit classes
`completed` (materialized output); `analysis_error` (failed structural/dry-run analysis); `runtime_error` (failed during execution); `max_iterations` (hit cap without green); `harness_error` + `PROPOSE_*` / `HARNESS_*` (instrument faults). `[def: harness/schema.py:30-69]`
Instrument-fault rows (`HARNESS_FAULT_EXIT_CLASSES`) are **excluded from all H1–H4 statistics** before aggregation. `[harness/schema.py:56-69 · analyze.py:118-121, 190-197]`

### SM3.5 Cost: two distinct notions (kept separate on purpose)
**(N1) Token spend**: LLM tokens the agent burns to reach a correct pipeline. Fields: `input_tokens`, `output_tokens`, per-iteration `per_iteration[].tokens.*`. `[schema: harness/schema.py:142-149]`
**(N2) Data-processing compute**: actual Spark execution over data (the cluster/EKS cost). A *correctly* gate-rejected attempt processes **zero data** (caught at analysis time, before execution); an imperative attempt that fails at runtime has already executed and burned data-processing compute. Fields: `executor_seconds`, `cpu_seconds` (measured); `executor_seconds_wallclock` (a wall-clock proxy, NOT data compute). `[schema: harness/schema.py:101-126 · analyze.py local-vs-cluster selection 553-576]`

---

## SM6. Experimental Design & Run Protocol
This section is the study's pre-registration and reproducibility apparatus: the locked design, the full hypothesis tree, the corpus and seeds, the phased run with explicit human approval gates, and the exact commands, recorded so results cannot be retrofitted and any collaborator can re-run the study and recover the numbers in §4.

*Every "run" executes THIS written protocol. A collaborator can read this and know exactly what runs, what is measured, and where a human approves. Nothing runs that is not described here.*

### SM6.1 Design (LOCKED 2026-06-29): TWO arms
- **A** = bare imperative PySpark: no gate, no skills. (Imperative as it natively is.)
- **B** = SDP: framework dry-run gate + `pyspark-sdp` API skill. **NO safety skill.**
- **`spark-safety` SCRAPPED everywhere.** It changed silent-defect rate by 0.000 (B=23/66 vs B1=23/66) and was the most confusing knob in the design. Removing it kills the biggest reviewer confound ("did SDP win, or did you just give it safety advice?").
- **`pyspark-sdp` stays on B**: it is load-bearing SDP *API knowledge* (not safety), the fair analog of imperative being native to the base model. The residual asymmetry (B gets an API doc, A gets none) is addressed in §5.
- **A2, B1, B2 retired from the headline.** They were built for the pre-registered framing where the gate was a separable knob (clean test = B-vs-A2). Under F1 the gate is intrinsic to the paradigm, which orphaned A2 (gives imperative a gate) and made B1 a "gate-off + safety-off" arm. B2 is a separate compute-only question if ever revisited.

### SM6.2 Hypotheses (full tree)
*New 2-arm framing (A vs B). Supersedes the old prereg H1–H5; not a 1:1 remap. Pilot numbers are N=3, instrument-mixed (§SM6.4); the clean A-vs-B values are the powered run (§1.4, 528 cells, complete 2026-07-02).*

**H1, SAFETY (headline thesis):** forcing SDP collapses the user's catch-burden to the irreducible silent residue: SDP catches structural failures early at the gate; imperative surfaces them late or ships them.
- **H1.1 Structural-catch:** SDP catches structural defects (D1 unresolved column, D4 broken DAG, D5 immutable-config mutation) at the dry-run gate, pre-execution; bare imperative has no gate, so they surface at runtime or ship. *Clean powered run (§1.4.2): B=79 gate intercepts (353 iteration-level error events) vs A=0. CONFIRMED.*
- **H1.2 Failure-mode shift:** SDP's failures concentrate at gate-time (before data is touched); imperative's at runtime or as silent ships. *Measured via exit_class + detection_stage distribution. Pending.*
- **H1.3 Silent-residue invariance (predicted NULL / control):** semantic defects (D2/D6/D7/D8) are un-gateable in any paradigm, so silent-defect rate is ~equal A vs B. *Pilot: A=18/66, B=23/66, comparable.*

**H2, TOKEN COST (LLM effort to reach correct):**
- **H2.1 Tokens-to-correct:** total input+output tokens to a correct pipeline. *Direction OPEN. Not computable yet (B/B1 token fields null); needs run.*
- **H2.2 Iterations-to-correct (honest counter-signal):** pilot shows SDP uses MORE agent loops (median 3 vs 1), which may push tokens up, measured, not assumed in SDP's favor. *Interpret jointly with H5: extra iterations are justified if they convert into higher completion (see H5.3, cost-per-correct-completion); a raw iteration count is not, by itself, a verdict against SDP.*

**H3, COMPUTE COST (data processing; the cluster/EKS-relevant cost):**
- **H3.1 Wasted-compute-on-failed-attempts:** SDP's gate rejects failed attempts before execution (~0 data processed); imperative failures execute and burn compute. *Direction: SDP lower. **Per-attempt compute serialization (§SM6.6(3)) is now implemented** (branch `h3-per-attempt-compute`, offline tests green; it stamps per-attempt `executor_seconds`/`cpu_seconds`/`intercepted_at_dry_run` into `per_iteration`, and adds an analyze.py H3 reader). **Confirmed on EKS** by a 48-cell sweep (§1.4.3): imperative wastes ~1000× the compute SDP does on failed attempts (A `$0.028` vs B `≈$0`), because the dry-run rejects them before execution. Methodology + raw-data spec + runbook: `repro/H3_PLAN.md`, `repro/h3_eks/`.*
- **H3.2 Total-compute-to-correct:** *Confirmed on EKS (§1.4.3): imperative spends ~34× the total cluster compute of SDP (A `$0.032` vs B `$0.0009`). The earlier local wall-clock proxy (SDP higher) was substrate-confounded and is superseded.*

**H4, CONCISENESS:**
- **H4.1 LOC:** SDP fewer lines. *Pilot: ~42% fewer (68 vs 117). SUPPORTED.*
- **H4.2 AST nodes:** SDP smaller AST. *Pilot: ~38% fewer. SUPPORTED.*
- *Defensible half of the "less surface area" instinct: smaller code surface.*

**H5, EFFICACY ("does the agent get the job done?"):** head-to-head completion rate, A vs B. Direction-neutral: we report both arms' rates and let the data say which paradigm produces a working pipeline more often; no parity is assumed.
- **H5.1 Completion rate:** fraction of cells reaching a materialized/completed output (`exit_class == completed`), A vs B. *(Captures "did it produce anything runnable.")*
- **H5.2 Correct-completion rate (the real "job done"):** fraction reaching a CORRECT completed output (`success` = `exit_class == completed` AND `silent_defect == false`; cross-check `reached_correct`), A vs B. *(Captures "did it produce something actually right.")*
- *Clean powered run (§1.4): correct-completion (`completed` AND not silent) A=182/264 (68.9%), B=172/264 (65.2%); completion alone A=96.6%, B=97.7%. The small A-edge tracks the silent-defect gap (§1.4.1), which is skill-attributable: a paradigm-appropriate UTC skill resolves the driver (D7) at the defect level (7→0; the post-swap completion rate was not separately re-measured).*
- **H5.3 Cost-adjusted efficacy (interpret H2/H3 JOINTLY with H5):** SDP's extra iterations (H2.2) and any extra compute are a true *cost* only if they do NOT buy completion. Report **cost-per-correct-completion** (tokens / iterations / compute *per successful job*), so "SDP iterates more" is weighed against "SDP finishes more." More iterations are a win if the job gets done; a penalty only if it doesn't.
- Rationale: a paradigm that is safer and cheaper but finishes the job less often is a worse tool, not a better one, and conversely, a paradigm that costs more per attempt but completes more jobs may be the better tool. Completion is a primary outcome, measured head-to-head, and cost is scored relative to it.

### SM6.2.1 Control & rejected hypotheses
- **CONTROL, silent-defect residue (= H1.3):** reported, predicted equal across arms; the irreducible semantic residue.
- **REJECTED, "less surface => fewer TOTAL defects":** CONTRADICTED. SDP surfaced MORE total detected defects (A2=27, B=48, B1=46) and far more loop error-events, because the gate exposes errors rather than hiding them. The "less surface" instinct holds only as code economy (H4), not as defect count. Reported as a negative result, not omitted.

### SM6.3 Corpus, seeds, power, model
22 frozen tasks (`TASKS.lock.json` v3.0.0-corpus22); `SEEDS.lock.json` (v1.1.0-power) locks **12** seeds: the N=3 pilot used the first three (42/1337/2718), leaving headroom for N* up to 12. N* from calibration (§SM6.7): the Phase-1 calibration targets **80% power at α = 0.05** against the pilot-observed silent-defect effect (OR ≈ 2), which yields **N\* ≈ 260**; the powered run's N = 264 clears it. Model `claude-opus-4-8` (`study.config.json:4`). Full Materials & System detail in §SM7.

### SM6.4 What we already have (retrofit) vs what must run
- **Already CLEAN at instrument-v3.1 (`1d28563a`):** A (66 rows), A2 (66), B2 (66), on `origin/data/raw-export`.
- **OLD instrument, must re-run for clean claims:** B, B1.
- **=> the headline SAFETY run is essentially RE-RUN B (no-safety variant) on the current instrument, paired with existing clean A.** A does not need regenerating.
- **The COST/compute claim** additionally needs A AND B on ONE uniform substrate (§SM6.5), a fresh A+B run on Connect.

### SM6.5 Substrate (the real feasibility constraint)
- The validated `local` backend SPLITS by paradigm: imperative -> classic local Spark, SDP -> local Spark Connect (`runner.py:1204-1239`; `local_connect.py:1-15`). So local A-vs-B compute is NOT apples-to-apples.
- **Safety/structural claim:** substrate split is tolerable (defect detection is substrate-independent), noted as a minor threat.
- **Cost/data-compute claim:** MUST run both arms on ONE substrate = the `live` Connect backend, whose ConnectExecutor handles both paradigms (`live.py:569-581, 835-849`). This is precisely the cluster/EKS motivation, now confirmed as necessary, not scope creep.
- **Update (EKS run history, 2026-06-24):** the `live`/Connect substrate is **no longer hypothetical**: it was stood up and partially exercised on a real EKS cluster (`ssa-spark-eks`): driver + executors ran in k8s pods, **Arm A materialized tables remotely**, and the **in-cluster compute-measurement path was demonstrated** (Spark-UI stage-diff; a `spark.range(80M)` probe returned stage/executor-second readings) `[DEVIATIONS.md:184-227, 345-368]`. The uniform-substrate compute run is therefore a matter of **completing the live run with per-attempt compute serialized** (§SM6.6(3), **now implemented**: branch `h3-per-attempt-compute`, offline tests green; see `repro/H3_PLAN.md`), not building the capability. **Resolved 2026-07-06:** remote **Arm B SDP** completes + grades green on EKS (ref-arch **L3 closed**, §SM7/§11); it took harness data-path + catalog-resolution fixes (`repro/h3_eks/`), not architecture. **H3 compute was measured on EKS** (both arms, stage-diff executor-seconds) and **confirmed by a 48-cell sweep** (§1.4.3): imperative spends ~34× the total and ~1000× the wasted compute of SDP.

### SM6.6 Instrument changes before the powered runs (each a reviewed PR you see the diff of)
1. **Redefine B**: SDP + gate + `pyspark-sdp`, drop `spark-safety` (arm-manifest change). [trivial]
2. **Token logging**: ALREADY works on current instrument; old B/B1 nulls were pre-token sweeps. Re-running B fixes it. [no code change]
3. **Per-attempt compute**: serialize per-iteration `IterationCost` (`executor_seconds`/`cpu_seconds`/`usd`/`intercepted_at_dry_run`) into `per_iteration`, needed only for the compute claim. Location `runner.py::run_episode` ~228-260. [moderate]

### SM6.7 Phased run with human gates
- **Phase 0**, instrument changes as reviewed PRs (§SM6.6).
- **Phase 1**, calibration: few tasks, N=3, on the fixed instrument. Output: per-cell token + compute cost, pilot effect sizes, projected **N\*** and **dollar figure**.
- **Approval gate**, a human approves N\* and projected cost before any powered/spending run.
- **Phase 2a (SAFETY paper):** re-run B (no-safety) at N\* on the current instrument; pair with existing clean A -> the A-vs-B structural-catch headline.
- **Phase 2b (COST addendum):** A + B on the uniform `live`/Connect substrate with per-attempt compute logging.
- **Phase 3**, analysis: `report.json` -> the §1.4 cited cells (no hand-typed numbers).
- **Phase 4**, bind the analysis into the paper, with independent cross-review.

### SM6.8 Literal commands (verified against runner.py argparse, `runner.py:1321-1344`)
Calibration (local backend, few tasks, N=3):
```bash
cd study
ANTHROPIC_API_KEY="$ANTHROPIC_API_KEY" python3 harness/runner.py \
  --backend local --config study.config.json --arms-dir arms \
  --tasks TASKS.lock.json --seeds SEEDS.lock.json \
  --only-tasks orders_silver_gold,p1_medallion,p2_cdc \
  --only-arms A,B --max-seeds 3 \
  --out results.calibration.local.n3.jsonl \
  --work-dir .work.calibration.local.n3 --per-cell-timeout 1800
```
Uniform-substrate run for the COST claim: identical but `--backend live` (requires a reachable Spark Connect endpoint + `ANTHROPIC_API_KEY`).
Analysis: `python3 analysis/analyze.py <out.jsonl> --tasks TASKS.lock.json`.

### SM6.9 Cost accounting (how each number is computed)
- **Token:** tokens-to-correct(arm) = sum of `per_iteration[:iterations_to_green].tokens.{input,output}` over `reached_correct` rows; paired A-vs-B, bootstrap CI.
- **Data compute:** per-arm total and *wasted* (failed-attempt) `executor_seconds`/`cpu_seconds`; gate-caught attempts contribute ~0; dollars via the substrate's metered rate. Requires §SM6.6(3) and the uniform Connect substrate.

---

## SM7. Methods: Materials & System
*(Data · Tasks · Agents/Models · Architecture · Execution. Placed here in the working draft; moves ahead of §1.4 Results in final layout. Every claim cited to a file:line on `origin/dev`.)*

### SM7.1 Data
Inputs are **deterministic NDJSON event streams** produced per `(task, seed)` by task-specific generators under `infra/`. The runner resolves each task's `input`, applies any `input_args` (e.g. `--v3`), and invokes `python <gen> --seed <seed>`, writing to `<work_dir>/_data/<gen>_seed<seed>.ndjson` (`runner.py:625-651`); multi-input tasks generate each `aux_inputs` the same way (`runner.py:654-672`). The agent receives only **location** env vars: `AGENT_INPUT_PATH` (+ `AGENT_OUTPUT_PATH`/`AGENT_DEDUP_PATH` for local imperative; `AGENT_OUTPUT_TABLE` + `AGENT_AUX_INPUT_*` for live), a paradigm-symmetric, location-only contract (`local.py:433-443`; `live.py:713-718`; `base.py:229-259`). Each generator seeds its RNG from `--seed`, so data is a pure function of (generator, args, seed); seed 42 reproduces the registered oracle stream as a regression check.

Six substrates + an FX feed, each with **deliberately injected defect traps**:

| Generator / substrate | Entity | Injected messiness → defect classes |
|---|---|---|
| `gen_messy_orders.py` / orders (`--N 5000`, `--v3` adds rows) | order events (`order_id, merchant_id, event_time, amount, category`) | dup `order_id`, late/out-of-order, null/missing merchant, amount-as-string, mixed timestamps, malformed JSON, unknown merchants; v3 adds nested arrays/structs + HTML junk → D1/D2/D6/D7/D8 (`gen_messy_orders.py:7-17,67-136`) |
| `gen_customers_cdc.py` / cdc | customer CDC (`customer_id,…,op,seq,event_time`) | shuffled arrival (must order by `seq`), tombstone deletes with null payloads → D5/D6 (`gen_customers_cdc.py:43-53`) |
| `gen_payments.py` / payments (`--N 4000`) | payments (`…currency, amount_minor, amount, settled`) | foreign-currency silent-drop/mis-total (D8), TZ-offset near day boundary → wrong UTC-date FX (D7), bad currency codes need quarantine (`gen_payments.py:15-25,79-99`) |
| `fx.py` + `gen_fx_rates_cdc.py` / FX | daily USD rates (deterministic table) | ~12% wrong-rate-then-corrected revisions at higher `seq`; shuffled → must order by `seq` (`gen_fx_rates_cdc.py:44-57`) |
| `gen_emails.py` / emails (`--N 900`) | `{email_id, subject}` | null/empty → routing; non-ASCII urgency markers → urgent; naive classifiers misclassify (`gen_emails.py:4-18`) |
| `gen_trades.py` / trades (`--N 1200`) | trades (`…notional, event_time, side`) | string notionals, `-08:00` near day boundary, bad currencies → quarantine (`gen_trades.py:8-17`) |
| `gen_clickstream.py` / clickstream | clicks over `view<cart<checkout<purchase` | late/out-of-order (sessionize by event time), truncated JSON → DLQ, 30-min inactivity sessions (`gen_clickstream.py:8-17`) |

The shared ticket tells the agent the feeds are "genuinely messy" but describes *symptoms, not causes*, and forbids changing the output contract, mutating immutable config, or non-idempotent output (`prompts/task_prompt.md:1-28`).

### SM7.2 Task corpus (22 tasks, `TASKS.lock.json` v3.0.0-corpus22, frozen 2026-06-24; complexity 7 Low / 8 Med / 7 High)
Each task carries a ticket-style `prompt`, `complexity_bin`, `defects_in_scope`, `oracles`, and optional `invariants`/`aux_inputs`. D1,D2,D4–D8 are gradable; D3/D9 narrated as future work.

| # | id | bin | substrate | defects | task |
|--:|---|---|---|---|---|
| 1 | orders_silver_gold | Med | orders | D1,D2,D3,D6,D7,D8 | orders → silver (clean/dedup/enrich) → gold daily revenue |
| 2 | p1_medallion | Med | orders | D1,D2,D4,D8 | bronze→silver→gold medallion ETL over messy orders |
| 3 | p2_cdc | High | cdc | D1,D4,D5,D6 | hand-rolled SCD-1 + SCD-2 over CDC (window functions) |
| 4 | p3_windows | Low | orders | D2,D3,D7,D9 | event-time windowed revenue (1h × category) |
| 5 | p4_fanout | Low | orders | D1,D3,D4,D8,D9 | one stream fans out to two streaming tables |
| 6 | p5_mart | Low | cdc | D1,D4 | customer-segment mart from CDC |
| 7 | p6_dedup_watermark | Low | orders | D1,D3,D6,D9 | streaming dedup WITH watermark (bounded state) |
| 8 | p7_late_data | Low | orders | D2,D3,D7,D9 | late/out-of-order with allowed-lateness windows |
| 9 | p8_currency_normalize | Med | payments | D1,D4,D7,D8 | multi-currency → USD (FX as-of UTC date) |
| 10 | p9_enrich_join | Low | orders | D1,D3,D4,D8 | stream-static enrich join (orders × merchants) |
| 11 | p10_scd2 | High | cdc | D1,D4,D5,D6 | full SCD-2 with effective_from/to + no-overlap invariant |
| 12 | p11_schema_evolution | Med | orders | D1,D2,D5,D8 | schema-evolution-tolerant ingest, backfilled defaults |
| 13 | p12_quarantine_dlq | Med | orders | D1,D2,D6,D8 | explicit dead-letter quarantine of malformed orders |
| 14 | p13_cdc_windowed | High | cdc | D1,D3,D6,D9 | windowed change-rate aggregation over CDC |
| 15 | p14_fx_settlement | High | payments | D5,D7,D8,D9 | daily FX settlement totals per currency, UTC day-close |
| 16 | new_merge_upsert | Med | orders | D1,D5,D6 | idempotent MERGE/upsert into keyed silver |
| 17 | new_stream_stream_join | Med | payments | D1,D3,D7,D8,D9 | stream-stream temporal join payments × live FX feed |
| 18 | new_scd2_as_of_join | High | payments | D1,D4,D7 | point-in-time as-of join to SCD-2 FX dimension |
| 19 | new_cdc_tombstone | Med | cdc | D1,D6 | CDC tombstones remove customers from current state |
| 20 | new_udf_classifier | Low | emails | D1 | email-subject classifier UDF (imperative + SDP) |
| 21 | HC1_fx_trade_ledger | High | trades | D1,D2,D4,D5,D7 | HC-1: multi-stage FX trade ledger (SCD2 → as-of USD → MERGE) |
| 22 | HC2_session_funnel | High | clickstream | D1,D2,D6,D8 | HC-2: streaming session funnel (sessionize → funnel + DLQ) |

### SM7.3 Seeds
`SEEDS.lock.json` (v1.1.0-power, frozen 2026-06-23) locks 12 integer seeds: `[42,1337,2718,3141,5772,8675,9001,11235,27182,31415,16180,14142]`, selecting per-run input so **every arm sees byte-identical data for a given seed**. Seed 42 is first as the oracle-regression seed; `16180`/`14142` were appended to tighten the A–B CI (`SEEDS.lock.json:1-14`). The N=3 pilot used the first three.

### SM7.4 Agent & model
Base model `claude-opus-4-8`, shared across arms (`study.config.json:4`). Controlled sampling in the manifests is `temperature 0.0`, `top_p 1.0` (`arms/A.json:5-15`, `arms/B.json:5-15`); the manifest loader forces model/prompt/max-iterations/temperature/top_p to be **identical** across arms; only paradigm, gate, skills, allowed-commands vary (`arm_manifest.py:33-58`). `AnthropicBrain` defaults `temperature=0.0`, `top_p=1.0`, `max_tokens=16000` (the high cap leaves room for Opus adaptive thinking before the fenced code block) (`live.py:229-270`). **Decoding caveat:** for `claude-opus-4-*`, `build_request()` sends `thinking={"type":"adaptive"}` + `output_config={"effort":"high"}` and deliberately omits `temperature`/`top_p`/`top_k` (the Opus family rejects explicit sampling knobs), so temperature 0.0 is controlled *provenance* but is not transmitted for this model (`live.py:279-306`). Live calls run in a killable subprocess, 300 s request timeout, 2 retries; per-turn `input_tokens`/`output_tokens` are projected from usage onto each `Proposal` (`live.py:59-70,420-499`).

### SM7.5 Prompting
Per cell: `compose_task_prompt()` joins the shared preamble + the task's ticket `prompt`, **omitting the engineering `title`** so the prompt never leaks the fix; this is the "blind" framing (`runner.py:1146-1155`). `AnthropicBrain._system_prompt()` then appends paradigm framing (SDP: `from pyspark import pipelines as dp`, `@dp.table`/`@dp.materialized_view`, no `.start()`; imperative: own the SparkSession), each linked skill verbatim as `=== LINKED SKILL: <name> ===`, a gate instruction **only if the arm carries a gate**, and the output contract (a fenced Python block + a `COMMAND:` from allowed commands) (`live.py:319-373`). Bare arm A carries no gate, so no gate instruction is appended. A = no skills; B = `pyspark-sdp` only (safety skill scrapped per §SM6.1). The user message carries task id, dataset paths, and prior-iteration failure feedback (`live.py:375-413`).

### SM7.6 System architecture
`run_cell()` = one `(task, arm, seed)` → one `ResultRow`: makes a `<task>__<arm>__seed<seed>` workspace, generates data, instantiates brain + executor, stages input, runs the episode, blind-grades the output, aggregates cost, builds the row (`runner.py:744-828`). `run_episode()` loops to `max_iterations`: `propose → materialize → [gate] → execute → record → feedback-or-stop` (`runner.py:147-277`). Materialization is paradigm-specific: SDP → `transformations/pipeline.py` + harness `spark-pipeline.yml`; imperative → agent code verbatim to `pipeline.py`, no injected SparkSession/main/gate (`runner.py:305-405`).
- **Live executor `ConnectExecutor`** (Spark Connect for both paradigms): SDP gate = `harness/sdp_dryrun.py` (graph-aware framework dry-run); SDP execute = `pipelines/cli.py run --spec`; imperative execute = agent's `python3/spark-submit pipeline.py` with neutral env (`live.py:569-583,724-845`). (The executor also supports a gated-imperative path, the agent's `pipeline.py --analyze-only`, but it is exercised only by the retired gated arms; bare arm A runs no gate. See §SM2.) Compute is measured by a **Spark-UI stage-diff** before/after each run (`live.py:585-600,852-860`).
- **Local backend** splits by paradigm: SDP → `LocalConnectExecutor` (local single-node Connect), imperative → `LocalSparkExecutor` (classic in-process `local[*]`) (`runner.py:1204-1246`). This split is the §SM6.5 cross-paradigm compute constraint.
- **Blind grading**: the oracle (`oracles.py`) scores the materialized output against ground truth without access to the agent's reasoning; "blind" = the grader sees only output, and the prompt never saw the fix's title.

### SM7.7 Execution / run-triggering
Launched via `python3 harness/runner.py` with `--backend {replay,live,local}`, `--config study.config.json`, `--arms-dir`, `--tasks`, `--seeds`, `--only-arms`, `--only-tasks`, `--max-seeds`, `--out <jsonl>`, `--work-dir`, `--per-cell-timeout` (`runner.py:1321-1344`). Backends: **replay** (offline deterministic, no LLM/Spark, needs a recorded trace), **live** (Anthropic + Spark Connect, needs a reachable endpoint + `ANTHROPIC_API_KEY`), **local** (real local Spark, paradigm-split). Outputs: one JSONL row per cell to `--out`, transcripts to `--work-dir`; `analysis/analyze.py <out> --tasks TASKS.lock.json` aggregates to `report.json`. Each row is stamped with provenance: `git_sha`, `image_digest`, `spark_version`, `base_model_id`, which is how instrument-version contamination (§SM6.4) is detectable. Literal commands in §SM6.8.

---

# SECTION 2: The Agent-Native Development Loop
### A new inner loop for agents: propose, gate, reconcile, converge; it closes *before any data*

Section 1 showed that a declarative agent writes safer and far more concise pipelines. This section introduces the inner loop that makes that possible, a genuinely new way an agent builds a data pipeline. The normal Spark dev loop was built for a human at a keyboard, not for an agent that sometimes hallucinates; §1 below shows why. We replace it with one native to how an agent should work: **propose desired-state, clear a structural dry-run gate before any data, then reconcile and execute**, to convergence.

What makes that loop possible is a **control boundary** that separates *authoring* from *execution*: the agent only ever proposes inert desired-state and never holds a session, credentials, or touches data, while a governed system validates it with a structural dry-run before any data and executes it. Declarative pipelines make the boundary expressible and Spark Connect enforces it, which is exactly what lets the loop **close at the gate, with nothing touched**.

This is a **demonstration, not a proposal**: we built the loop, ran real AI agents through it, and executed it across hosts on a live EKS Spark-Connect cluster (driver and executors in Kubernetes pods, tables materialized remotely). Two honest gaps remain: the remote client still reaches Connect through a `socat` tunnel rather than native mTLS, and the reconciler still runs on the agent's host (the *governance split*), while the production-scale governed platform is built and demonstrated in **Section 3** (per-tenant isolation on live EKS; multi-tenant scale is the remaining frontier), not claimed here. This section does not relitigate the paradigm safety result (Section 1); the agent-native loop is the argument, and the control boundary is what lets it close before any data.

---

## 1. The problem: the normal dev loop is wrong for an agent
The normal way to develop a Spark pipeline is imperative: **write code, run it, and find out what's wrong by running it.** The program executes over real data and *then* surfaces an error, so every mistake costs a run. That loop was built for a person at a keyboard who mostly writes correct code, not for an agent that sometimes hallucinates. The obvious way to give that agent a platform, hand it a live `SparkSession` (or warehouse credentials) and let it run its own code, makes each mistake maximally expensive. That agent is an **untrusted author**, and a wrong or hallucinated program does not merely fail; it *executes*. It can mutate cluster config, read or overwrite arbitrary tables, run unbounded operations, and, as Section 1 showed, ship silently-wrong data or burn real compute on pipelines that never worked. There is no governance story for "an agent holding a `SparkSession`": no gate before it touches data, no line between what it *authors* and what it *runs*, nothing to audit or contain. Section 2 shows how to give an agent a full production data platform **without ever handing it the runtime keys**, and, at the end, what that buys over the normal setup.

## 2. The agent-native loop: where it closes
The problem above is a statement about the loop: the imperative loop surfaces errors only by running over real data. Here is the loop that replaces it.

We built and ran a different loop, native to how an agent should work: **propose desired-state → a structural dry-run gate → reconcile / execute → feedback**, to convergence `[runner.py:147-277]`. The **dry-run gate** validates the whole pipeline graph **before any data is processed** (`create_dataflow_graph` → `register_definitions` → `start_run(dry=True)`) `[sdp_dryrun.py:462-484]` and hands structural defects back to the agent as feedback, so a broken pipeline never reaches execution. The difference is *where the loop closes*: imperative closes it after execution: data touched, compute spent; the agent-native loop closes it at the gate, with nothing touched.

[[[SVG-DEVLOOP]]]

**Why this loop wins, and what it costs.** Catching structure before data is what produces every downstream result: the safety margin (79 structural defects caught at the gate vs 0, §1.4.2), the compute story (a structurally-broken pipeline is rejected before it can burn a cent, §1.4.3), and the governance and portability below. The tradeoff is real, and we state it plainly: the agent iterates *more* against the gate, so it spends **more tokens** to converge (~2.3×, §1.4.3): you pay in cheap LLM calls to save on data-compute, wrong data, and blast radius. For an untrusted author working on production data, that is the trade you want.

## 3. What makes the loop possible: the control boundary (authoring ⊥ execution)
**Authoring** = what the agent writes (a declarative description of desired state). **Execution** = what the governed system does (validate, build the graph, acquire the session, materialize). The agent proposes; the system disposes. Its output is an inert artifact a governed executor reconciles. That separation is what lets the loop close at the gate: because authoring is inert, a governed executor can validate it before anything runs, and keeping an untrusted author safe inside a trusted platform is the consequence.

[[[SVG-CONTROLBOUNDARY]]]

## 4. Why declarative *enables* the loop's boundary and imperative *cannot*
The two paradigms produce fundamentally different *kinds of artifact*:

```python
# Imperative: the agent owns and runs the session; authoring IS execution
spark = SparkSession.builder.getOrCreate()
df = spark.read.json(src).where(...).groupBy(...).agg(...)
df.write.saveAsTable("gold")            # to apply it, you must run it

# Declarative (SDP): the agent writes inert desired-state; the framework runs it
@dp.materialized_view
def gold():
    return spark.read.table("silver").groupBy(...).agg(...)   # no session, no .write, no .start()
```

- **Imperative:** the program owns the session, the reads and writes, and the lifecycle; to *apply* it you must *run* it. Authoring **is** execution: there is nothing to hand a governed executor but "run this program." `[runner.py:369-405]`
- **Declarative (SDP):** the agent writes only decorated transforms; the framework owns the graph, the session, and materialization, so authoring and execution are **structurally distinct artifacts**. `[runner.py:305-405]`

∴ The loop is only *expressible* in a paradigm that separates desired-state from reconciliation, because only an inert artifact can be gated before execution. We adopt declarative not for its own sake, but because it is the only authoring surface that closes the loop at the gate, and the only one an untrusted agent can safely hold.

## 5. The authoring surface: the OSS SDP API
The agent writes to open-source `pyspark.pipelines` (Apache Spark 4.1, **not** Databricks DLT): it declares datasets with `@dp.materialized_view` / `@dp.table`, wires upstreams via `read.table`, and never acquires or starts a session; the framework does. That small, inert surface is the whole point; the full API is the [`pyspark-sdp` skill](../study/skills/pyspark-sdp/SKILL.md).

---

## 6. How we demonstrated it (apparatus & method)
We did not describe the loop; we **ran it**. The harness instantiates the dev loop as a controlled apparatus and exercises it with a real LLM agent over the full corpus:
- **Agent:** `claude-opus-4-8`, identical model/prompt across conditions; the SDP condition (Section 1's **Arm B**) authors against the OSS SDP API via the `pyspark-sdp` skill `[live.py; arms/B.json]`.
- **Loop:** every `(task, seed)` cell runs `propose → materialize(`transformations/pipeline.py` + generated spec) → dry-run gate → execute via `pipelines/cli.py run --spec` → blind-grade`, one `ResultRow` per cell `[runner.py:147-277, 305-405, 744-828]`.
- **Corpus & scale:** 22 frozen data-engineering tasks across 6 messy substrates, each run as real agent SDP-authoring sessions.
- **What we capture per session:** every iteration's gate verdict, dry-run intercepts, execution outcome, exit class, final-program LOC/AST, and tokens, the telemetry that makes the loop's behavior observable `[harness/schema.py:96-148]`.
- **Provenance:** each row is stamped with `git_sha`/`spark_version` so the demonstration is reproducible against a fixed instrument (Section 1 §7.7).

This is the "how": a real agent, driven through the propose→gate→execute boundary on a frozen task corpus, fully instrumented.

**Two substrates, deliberately.** The 66-session pilot above ran on a **local** substrate (`--backend local`: imperative on classic local Spark, SDP on a local single-node Connect server), a deliberate choice to isolate the paradigm comparison and strip out the imperative-can't-run-on-remote-Connect confound `[DEVIATIONS.md:498-522]`. **Separately, the same dev loop was exercised across hosts on a real EKS Spark Connect cluster** (`ssa-spark-eks`): the controller submitted over an mTLS-fronted Connect channel, **driver and executors ran in k8s pods**, **Arm A materialized silver/gold/quarantine tables on the cluster**, and compute was measured in-cluster via the Spark-UI REST `[DEVIATIONS.md:184-227, 345-368]`. So execution genuinely ran **off the agent host**: the control boundary has operated across a real host separation, not only in local simulation.

**The infrastructure: how the boundary maps onto EKS + Connect.** The across-host run used a real, governed stack, the same shape a production deployment would take:
- **Authoring host (untrusted):** the agent writes its inert SDP spec; it holds no session, no cluster credentials, no endpoint identity.
- **Governed ingress:** the only way in is a single mTLS-fronted **Spark Connect** endpoint. An **Envoy** sidecar terminates mTLS, validates the client certificate, derives the caller's identity from it (a principal-pinning interceptor rejects a mismatched user), and forwards to a Connect server bound to loopback only, so there is no path to raw Connect around the identity check. *(This ingress is deployed; in the study's remote runs the client reached it through a `socat` mTLS tunnel rather than terminating TLS natively, the one piece still to harden, layer L1.)*
- **Execution (off-host):** the Connect server *is* a client-mode Spark **driver** pod that launches **executor pods** on Kubernetes; the catalog is Hive Metastore + Iceberg and the warehouse is S3 (reached via IRSA, not static keys).
- **How the boundary maps onto it:** a controller, not the agent, runs the stock SDP CLI as a Connect *client*. It builds the dataflow graph from the agent's transforms and ships **protobuf plans** (not files, not a live engine) over the mTLS channel; EKS executes them. The agent authored; the governed system executed; only a plan crossed the wire, on a different host.

**What it took to wire it (honest notes).** Standing this up was *integration*, not architecture: the harness had to stage local inputs to S3 so remote executors could read them, resolve the Iceberg catalog (the session's default catalog differs from where SDP writes), and present the pinned principal. Once those were fixed, **Arm A DataFrame-API imperative completed in one iteration and Arm B SDP completed and graded green remotely**; the boundary itself never changed. The exercise also surfaced three genuine Spark-4.1 / Connect / SDP framework gaps (no declarative way to pin `session.timeZone`; no first-class local→executor staging primitive; low-level imperative surfaces unsupported on Connect), written up for the framework owners in [`study/repro/h3_eks/H3_EKS_INTEGRATION_LOG.md`](../study/repro/h3_eks/H3_EKS_INTEGRATION_LOG.md).

## 7. What it proved (evidence from real agent runs)
The loop ran with a real agent (`claude-opus-4-8`) over the full corpus; the demonstration establishes four things the loop buys, with effect sizes from Section 1's powered run:
- **Agents author in this paradigm successfully.** Every session produced and executed SDP against the OSS API. The minimal `pyspark-sdp` skill is what makes this work; without it the agent hallucinates Databricks DLT and never completes, so the authoring surface is real and usable.
- **The boundary catches defects early.** The dry-run gate intercepted **79 structural defects before any data was processed** (§1.4.2), the boundary doing its job, observed, not asserted.
- **Agents get the job done, and benefit.** They complete real tasks under the boundary (efficacy, §1.4/H5) while writing **~half the code** of imperative (§1.4.3), on top of the structural-catch safety profile of H1.
- **The boundary operates across hosts, for both paradigms.** On a real EKS Connect cluster the full loop ran with driver and executors in Kubernetes pods: **Arm B SDP completes and grades green remotely** (materializing tables to the catalog on S3), and Arm A imperative runs too (DataFrame-API code over Connect), authoring (agent host) and execution (cluster) genuinely separated. The earlier remote-SDP failures were *integration* bugs (data-path + catalog wiring, since fixed), never architectural. Two gaps remain: the **governance split**, the reconciler still runs on the agent host (the load-bearing one, layer L5), and native client mTLS, still reached via a tunnel (layer L1). See §11.

The headline safety and cost numbers come from a deliberately **local** pilot substrate, chosen to isolate the paradigm comparison; across-host execution is demonstrated separately on EKS for both arms, where the H3 compute was also measured. The powered run tightens the effect sizes; it does not create the demonstration.

**What this buys over the normal setup.** Compared with the obvious approach, an agent holding a `SparkSession`, the boundary turns each liability into a property. The agent is **untrusted** (no session, no credentials, nothing to leak or misuse); structural mistakes are **caught before any data is touched** (79 at the gate, §1.4.2) instead of discovered at runtime; a structurally-broken pipeline **cannot burn cluster compute**, because it is rejected before it runs (§1.4.3); and the agent's output is an **inert, reviewable artifact**, so the loop is auditable and, since nothing the agent writes is ever executed *by* the agent, **governable and multi-tenant-able** (Section 3). On top of that it costs *less* code, not more (~half, §1.4.3). None of these are available to an imperative agent that owns its session.

## 8. Connect: enforcement, and dev-to-prod portability
The boundary is enforced at runtime by remote, session-less execution, and **Spark Connect is the means**: a Connect client submits *plans*, not a live session, so authored code never holds a `SparkSession`, `sparkContext`, `_jvm`, or an RDD.

What differs between the paradigms is how *reliably* they stay inside that envelope. **SDP is session-less by construction**: the declarative surface offers no way to reach for a live session. **Imperative is session-less only conditionally:** DataFrame-API imperative code runs fine over Connect (Arm A completed cells on the real EKS Connect cluster `[repro/h3_eks/]`) but the moment the agent reaches for `sparkContext`, `_jvm`, an RDD, or static-config mutation, Connect rejects it (`JVM_ATTRIBUTE_NOT_SUPPORTED`, `CANNOT_CONFIGURE_SPARK_CONNECT_MASTER`), and agents often do reach for those. So the enforcement is asymmetric in a precise way: **the declarative paradigm *guarantees* the session-less boundary; the imperative paradigm can honor it but cannot guarantee it.** A compatibility probe records this as a clean pass/fail matrix, the same matrix reused by Section 3 and Section 1's H3, in which the low-level imperative patterns fail while DataFrame-API and SDP patterns pass.

*(An earlier framing conflated two things now separated: a harness data-path bug, `PATH_NOT_FOUND`, since fixed, and the genuine incompatibility of the low-level surfaces. Only the latter is a real property of the paradigm.)*

**Connect's second job: dev-to-prod portability.** Because a Connect client submits *plans over a URL* rather than running a local engine, the **same** agent, SDP spec, and dry-run gate run against a **local** Connect server while developing, where iteration is fast and free, and promote to the **remote** cluster by changing a single endpoint (`SPARK_REMOTE`), with no code change.

[[[SVG-DEVPROD]]]

Today SDP still needs a Connect server even locally (a small single-node one); **local mode without a server arrives in Spark 4.3**, making the local half of the loop lighter still. This is exactly why the study could run the whole paradigm comparison *locally*, isolating the paradigm from cluster confounds (§6), and then demonstrate the *identical* loop across hosts on EKS (§7): dev and prod are the same loop at two endpoints.

## 9. What the loop and boundary unlock → Section 3 (the open reference architecture)
Because the agent only emits inert desired-state and never holds a session, it can be treated as **fully untrusted**, the precondition for a governed, zero-trust, multi-tenant platform (agent authors + opens a PR; CI runs the gate; a controller, not the agent, reconciles). The **session-free GitOps authoring path is demonstrated locally** (valid spec → `Run is COMPLETED`; invalid upstream → `TABLE_OR_VIEW_NOT_FOUND`/SQLSTATE 42P01). The **production EKS / mTLS / zero-trust platform is then built and demonstrated in Section 3**: five per-tenant isolation layers, all running on a live EKS cluster; only multi-tenant *scale* remains frontier. Section 2 draws the control boundary; Section 3 demonstrates the platform built around it.

## 10. Honesty notes
- The **mechanism** (control boundary, dev loop, dry-run gate) is *demonstrated* from built artifacts and real agent runs, not proposed.
- The **headline numbers** (safety, conciseness, completion; §7) come from the **local** pilot substrate, a deliberate choice to isolate the paradigm comparison, not from the remote cluster. Across-host execution is demonstrated separately on EKS for both arms.
- **Two gaps remain, stated plainly:** the remote client reaches Connect through a `socat` tunnel rather than native mTLS (layer **L1**), and the reconciler runs co-located on the agent host rather than split off (the **governance split**, layer **L5**). The highest honestly-claimable layer today is **L3**; L5 is the load-bearing remaining work (§11).
- "Proposal" language applies only to Section 3's production platform.


---

## 11. Reference architecture, invariants & demonstration layers
The canonical target is **Appendix S2-A** (folded in below). In-line summary:

**The boundary holds iff (invariants):** I1 authoring ⊥ execution · I2 no creds in the agent · I3 execution on a
separate host/zone · I4 gate before any data · I5 reconciliation by a controller the agent doesn't control ·
I6 only plans/specs cross the wire (never code handed a live engine). Violating any one = a *simulation* of the
boundary, not the boundary.

**Demonstration layers (build bottom-up; claim only up to the highest proven: this is the drift detector):**

| Layer | Claim it licenses | Status today |
|---|---|---|
| L0 substrate | EKS Connect + Envoy + catalog + S3 reachable | **built and run** |
| L1 native mTLS/PSK channel | controller → Connect, no tunnel | **PARTIAL**, reached via socat tunnel only |
| L2 off-host execution | a plan runs in-cluster, driver/executors in pods | **DEMONSTRATED, both arms** |
| L3 remote SDP green | agent-authored SDP completes + grades green remotely | **DEMONSTRATED (`ssa-spark-eks`)**: Arm B SDP completes+grades green; took harness data-path/catalog fixes (`repro/h3_eks/`), not architecture |
| L4 gate before data | dry-run rejects structural defects pre-execution | **DEMONSTRATED remote**: the SDP dry-run gate intercepted structural defects on EKS |
| L5 governance split | reconciler off the agent host; agent holds no creds | **GAP**, currently co-located (`live.py:675-689`) |
| L6 negative control | imperative cannot traverse the boundary | corroborated (`DEVIATIONS.md:516-522`), capture pending |

**Highest honestly-claimable layer today ≈ L3** (both arms run the full loop remotely; Arm B SDP completes+grades green). **L5 (governance split)** is now the load-bearing remaining work; L1 needs
native-mTLS de-risking (paper-cheap, no cluster); L6 needs capturing as a clean artifact. The section claims the
control boundary as *architecturally sound and demonstrated across hosts up to L3*, with L5 (governance split) the named gap, 
not a finished production system (that is Section 3).


---

# SECTION 3: The Open Reference Architecture
### Integrable, Scalable Agent Data Engineering on Spark Connect + Kubernetes
If an agent can be treated as fully untrusted, because it only ever emits inert desired-state (§2), then it can be dropped into a **governed data platform that trusts it with nothing**. This section builds that platform on an open stack, **SDP** for declarative authoring, a **GitOps/CI** layer that tests and reconciles every change, **Spark Connect** as the single identity-pinned front door, and **Kubernetes** for elastic execution, and it closes tenant isolation along five paths, **all demonstrated on a live EKS cluster**. An agent authenticated as tenant A is routed to *its own* Connect server, handed a credential it never holds, run on *its own* executor pods, authorized at the catalog only for itself, and prefix-scoped at storage, so it cannot reach tenant B by any path. (The adversary and those five paths are enumerated at the top of §3.3; each layer there closes exactly one.) §3 owns this whole per-tenant *mechanism*; what it delegates to §4/Omnigent is credential *custody* at fleet scale, holding and rotating the vended credential so no agent ever sees one.

**Scope of "demonstrated."** The five-layer per-tenant isolation runs on live EKS; the GitOps gate is demonstrated *locally* (its EKS-target reconcile is configured but unwired, and it gates against a runner-local integration-test catalog, distinct from the EKS-hosted governed catalog that enforces isolation in §3.3). The one unbuilt *capability* is multi-tenant **scale** (many tenants, node autoscaling); two proof-completeness seams also remain, a single request composing all five links, and unifying the storage-scoping forensic onto the authorization-enabled catalog. Maturity is called out pillar by pillar below.

It builds on Section 2's boundary and does not relitigate the paradigm result (Section 1) or re-argue Connect; Connect stays the governed front door and Kubernetes is the horsepower behind it.

## 3.0 The separation of concerns (the boundary this section draws)
Each layer owns one thing and **delegates the rest**; this is the section's organizing principle:

| Layer | Owns | Delegates / does NOT own |
|---|---|---|
| **SDP** | declarative authoring: *what* the pipeline is; begins and ends at the spec | execution, identity, tenant authz |
| **GitOps / CI** | continuous test + integration against a real target catalog; reconciliation; scale-out orchestration | defining the pipeline (SDP's job); enforcing grants (catalog's job) |
| **Spark Connect** | the single governed, identity-pinned **ingress** (§2 boundary, at any scale) | doing the data work itself (delegated to executors) |
| **Kubernetes** | elastic execution: client-mode driver + dynamically-allocated executor pods, containerized | the governance boundary (that stays at the Connect ingress) |
| **Catalog** *(governed Iceberg-REST; demonstrated with Lakekeeper + OpenFGA, vendor-neutral)* | tenant governance: per-principal authorization, grants, credential vending + isolation | being reinvented by SDP or GitOps |

## 3.1 GitOps / CI integration boundary: *tested integration, not blind submission*
**The problem it solves.** The naive way to give an agent a data platform is to let it submit code straight to a
cluster. That has two defects at once: it hands the agent a live session (violating the §2 control boundary), and
it ships **untested, unreviewed** changes directly into production: no gate, no integration check, no audit. The
GitOps/CI boundary removes both: the agent's *only* output is a pull request, and **a controller, not the agent, 
tests and applies it.**

**The mechanism (operationalizing §2's boundary as "the agent's only artifact is a PR").**
1. **Authoring with no session.** The agent renders a declarative SDP artifact and opens a PR. The author process
   is hard-denied a runtime: it refuses to run if `SPARK_REMOTE` is set and whitelists only `git`/`gh`
   subprocesses, re-checking the boundary immediately before writing `[gitops_demo/agent_pr_author.py]`.
   The emitted artifact has a fixed shape: `name`, `storage`, `catalog`, `database`, and a `libraries` glob
   `[gitops_demo/sdp_artifact.py]`: it is inert desired-state, never code handed to a live engine (the §2 boundary, restated).
2. **PR-time gate = integration against the real catalog (CI).** A GitHub Actions workflow triggers on any PR
   touching `pipeline-definitions/**`, stands up a real Spark Connect server, ensures the target schema, resolves
   the changed specs, and runs the **SDP framework dry-run** on each
   `[.github/workflows/gitops-sdp-dry-run.yml]`. This is the decisive difference
   from blind submission: the desired-state is **validated against actual catalog/schema state**; broken DAGs and
   missing upstream tables are caught **before merge, before any data is processed.**
3. **Merge-time reconcile (CI controller, not the agent).** On merge to `main`, a reconcile workflow runs
   `reconcile.py`, which requires `SPARK_REMOTE` and invokes `pipelines/cli.py run --spec`
   `[.github/workflows/gitops-sdp-reconcile-local.yml; gitops_demo/reconcile.py]`.
   Execution is owned by the governed CI controller; the agent that authored it holds no credentials and never ran it.

**What it proves (demonstrated).** Verified locally end-to-end: a valid spec dry-runs to **`Run is COMPLETED`**, and
a spec with a missing upstream fails at the gate with **`[TABLE_OR_VIEW_NOT_FOUND] … SQLSTATE 42P01`**
`[gitops_demo/README.md; gitops_demo/ensure_schema.py]`, i.e. the integration gate rejects a
structurally-broken pipeline before it can reconcile. The session-denial boundary is **unit-tested**: the suite
asserts no `pyspark`/`SparkSession` import and the `SPARK_REMOTE` refusal + git/gh allowlist
`[gitops_demo/tests/test_agent_pr_author_no_spark.py]`.

**Why this beats blind submission.** You get **review + a real integration gate against the real target catalog +
controller-owned execution + a full audit trail**, with the agent strictly outside the runtime: CI/CD discipline
applied to agent-authored data pipelines, rather than an agent firing pipelines at a cluster on trust.

**Honest scoping.**
- **DEMONSTRATED:** the full author→PR→dry-run-gate→reconcile loop, run **locally** against a runner-local Spark
  Connect server; the PR-author session-denial is unit-tested.
- **GAP:** the gate and reconcile target **runner-local Connect, not the EKS Connect endpoint**; the production-EKS
  GitOps path is **documentation-only / not wired** `[gitops_demo/PRODUCTION_EKS.md]`. No captured artifact of
  a real agent-opened PR (the `gh pr create` path exists in code but no public PR run is evidenced here).
- **GAP (the authoring→runtime bridge), design-only:** how a merged tenant-A artifact would be reconciled
  *as tenant A* against §3.3's mTLS gateway is not built. The intended path: the CI controller presents a
  per-tenant client certificate (SAN `spiffe://safe-spark-agents/tenant_a`) to the gateway, which routes it to
  tenant-A's Connect server exactly as an interactive agent is routed; the PR's target tenant selects the
  certificate. Wiring the controller's identity to the runtime cert principal is not yet done, and is named
  here rather than left implicit.
- **ASPIRATIONAL, not a focus:** the CI gate today is **structural** (does the graph resolve against the catalog?).
  **Data-quality / expectation tests**, the natural place to catch some of §1's silent, semantic defects (the wrong-but-runnable
  bugs no structural gate can see), are **not built**; they are the obvious extension of this layer, noted but not claimed.

## 3.2 Connect-on-Kubernetes scale: *Connect is the governed ingress; k8s is the horsepower*
*Primer for this subsection: Spark **Connect** is Spark's session-less front end, a client sends a query **plan** to a shared server that runs it, never code to execute. The **driver** is the server process that plans and coordinates a job; **executor pods** are the worker processes that actually crunch the data. mTLS is mutual TLS, where both sides present certificates.*

**Thesis.** Connect is preserved as the single mTLS/identity-pinned ingress; a client-mode driver plus dynamically-allocated executor pods, all from one container image, scale execution elastically **without bypassing the boundary**. One long-lived shared Connect driver serves many sessions: scaling happens *behind* the governed front door, never around it.

### 3.2.1 Architecture (topology)
**Governed ingress.** Only one door into the cluster is reachable from outside, and it establishes identity by cryptography, not by trusting what the caller claims. Three facts carry the boundary:

- **One external port, mutual-TLS only.** The sole externally reachable endpoint is an internal load balancer on TCP `15009`, the mutual-TLS port; the raw Spark Connect port (`15002`) is never exposed `[connect/base/service-mtls.yaml]`.
- **Identity comes from the client's certificate, not from the client's word.** An **Envoy** proxy sidecar sits in front of Connect: it requires a valid client certificate, checks it against the cluster's certificate authority, reads the caller's identity out of that certificate, discards any identity the client tried to assert, and stamps the verified identity onto the request before passing it on `[connect/base/envoy/envoy.yaml]`.
- **Raw Connect is unreachable except through that proxy.** The Connect server itself listens only on loopback (`127.0.0.1:15002`) `[connect/base/deployment.yaml]`, so the sidecar is the only way in.

The §2 boundary therefore holds no matter how execution scales behind it: every session arrives with a cryptographically-pinned principal, and there is no path around the proxy.

**Driver + executors.** The Connect server pod *is* the Spark **client-mode driver** (`spark.master=k8s://…`, `spark.submit.deployMode=client`); the long-lived Connect JVM talks to the in-cluster Kubernetes API to create **executor pods**, advertising its pod IP and fixed RPC/block-manager ports `[connect/base/deployment.yaml]`. Kubernetes access control (RBAC) gives the driver's service account exactly the permission it needs and no more: create, watch, and delete pods in its own namespace, so it can manage its own executors `[connect/base/rbac.yaml]`. Where those executors land is fixed by a **pod template**, they are pinned to executor-labeled nodes and spread across availability zones `[connect/base/deployment.yaml; connect/base/pod-templates/executor.yaml]`, matching the Terraform executor node group `[terraform/eks.tf]`.

**Elasticity.** Dynamic allocation is enabled (`minExecutors=0`, `initialExecutors=0`, `maxExecutors=10`, shuffle tracking; executors `2` cores / `2g`) `[connect/base/deployment.yaml]`; the Terraform executor pool defaults `m6i.2xlarge`, min=0/max=10/desired=2 `[terraform/variables.tf]`. This is a configured elasticity *envelope*, not a reproduced 0→10→0 autoscaling cycle: Karpenter is explicitly deferred `[terraform/README.md]`.

**One shared driver.** The Deployment is a singleton (`replicas:1`, `Recreate`) because Connect sessions are server-local and can't be spread behind one address without breaking session affinity `[connect/base/deployment.yaml]`; the live cluster runs "one long-lived Spark Connect server application shared by every run" `[DEVIATIONS.md]`, and the harness caches that single application id `[harness/backends/live.py]`. The same image (Spark 4.1.2, Iceberg 1.11.0, S3A/AWS SDK, PostgreSQL JDBC, principal-pinning interceptor jar) serves both driver and executors via role-dispatch in the entrypoint `[images/spark-connect/Dockerfile:18-35,65-118; images/spark-connect/entrypoint.sh]`.

### 3.2.2 What actually ran
The topology above was stood up on a **real EKS cluster**, and one thing was measured directly: through the driver's Spark-UI REST API, a `spark.range(80_000_000).sum()` ran on the cluster's **executor pods, not in the local process**, and registered real Spark stages (executor-seconds 1.246, cpu-seconds 0.878). This is a small probe. It confirms execution genuinely left the client and ran on the cluster; it is *not* a large multi-executor parallelism benchmark, and the paper does not claim one. Reproducibility (updated 2026-07-09): the substrate is now **terraform-applied and CI/OIDC-gated**: a fresh cluster stands up via a reviewer-approved GitHub Actions apply against S3-backed state, with **no long-lived AWS keys** (GitHub OIDC assumes a scoped role). The full deploy-and-connect runbook and the end-to-end build narrative live in the repository (`paper/notes/PLATFORM_LAB_NOTEBOOK.md`).

### 3.2.3 Honest scoping
- **DEMONSTRATED:** the topology was stood up on EKS; one shared long-lived Connect driver; execution ran on cluster executor pods, not locally (the `spark.range` probe registered real Spark stages) `[DEVIATIONS.md]`.
- **CONFIGURED-BUT-UNREPRODUCED:** elastic **0→10 executor** scale-up/down (dynamic allocation is enabled, but no captured 0→N→0 cycle); **no Cluster Autoscaler/Karpenter committed**, so node-level autoscaling is unshown `[connect/base/deployment.yaml; terraform/README.md]`.
- **SUPERSEDED (see §3.3):** the single-tenant baseline here is one **singleton** Connect driver (`replicas:1`) `[connect/base/deployment.yaml]`; §3.3 replaces it with **per-tenant Connect servers behind a routing gateway, demonstrated for isolation** on live EKS (2026-07-10). What is still design-only is multi-server as a **scale** mechanism, many tenants and horizontal replicas per tenant with node autoscaling, not the per-tenant isolation topology itself. *(The Terraform substrate is applied + CI-gated as of 2026-07-09.)*

## 3.3 Tenant governance: *the multi-tenancy stack, built and demonstrated*
**Why co-tenancy, and what is at stake.** Why multiplex tenants on shared infrastructure at all, rather than give each an isolated cluster? For the same reason §3.2 shares an elastic executor pool and §3.1 shares one governed catalog: consolidation. One elastic compute pool and one governed catalog serve many teams at a fraction of the cost and operational surface of a cluster per tenant, that efficiency is the whole point of a *platform*. But consolidation is exactly what puts a hostile or hallucinating tenant-A agent one misconfiguration away from tenant-B's data, reading it or overwriting it. So the platform has to make co-tenancy safe *by construction*, not by trusting the agent. That is what the five layers below do.

**The adversary, and the five paths.** The threat is §2's: a tenant-A agent that is *fully untrusted*, it may emit code that executes on the cluster, hallucinate, or actively try to reach tenant B's data. Reaching tenant B decomposes into exactly five distinct paths, and closing the isolation problem means closing all five: the agent could **connect** to tenant B's Connect server, **be handed** tenant B's credential, **execute** in a process co-resident with tenant B, **ask the catalog** for tenant B's tables or credential, or **hit storage** directly with a credential whose reach includes tenant B's bytes. Each path is closed by a different layer, and each layer is independently defeatable, so all five must hold; a skeptical reader can check the enumeration against the layers rather than take "isolated" on faith. Crucially, the later layers cannot backstop a failure of the earlier ones, because a failure of routing or custody produces a *legitimately-issued* tenant-B identity, not a forgery: if a tenant-A agent reached tenant-B's server (path one) or were handed tenant-B's token (path two), the catalog and storage gates would see a **valid** tenant-B credential and correctly serve it. The early links keep the identity honest; the late links bound what an honest identity may do. That is why the chain is non-redundant.

[[[SVG-ADVERSARY]]]

**The stack at a glance** (see the diagram below, and the interactive `paper/figures/isolation-architecture.html`). From the outside in: an **mTLS gateway** routes a request to a server by the *authenticated* principal on its client certificate (closes *connect*); a **per-tenant Connect server** injects only that tenant's catalog token, so the agent never holds a credential it could redirect (*be handed*); the tenant's **own executor pods** run the work, never sharing a JVM (*execute*); the **governed catalog** (Lakekeeper + OpenFGA) authorizes credential requests per principal (*ask the catalog*); and the **vended credential** is prefix-scoped at storage (*hit storage*). So no path reaches tenant B. The rest of this section walks the five links outside-in, each with the residual threat it uniquely closes and the proof that it runs; all five run on a live EKS cluster today. Scaling to many tenants with node autoscaling is the open capability, and two proof-completeness seams remain, named after the walk.

**Where enforcement ends in the default open stack (the baseline this section then closes, layer by layer, below).** Identity is strong at the door but not downstream. Envoy pins an unspoofable principal from the client cert SAN and the interceptor rejects a mismatched `user_id` `[connect/base/envoy/envoy.yaml; deploy/auth/interceptor/.../PrincipalPinningInterceptor.java]`: the platform *knows* who each session is. But **authorization is fleet-scoped**: a single shared Iceberg catalog and one fleet-wide cloud IAM role (via IRSA, *IAM Roles for Service Accounts*) with read/write to the entire warehouse `[images/spark-connect/conf/spark-defaults.template.conf; terraform/irsa.tf]`. Per-principal schema isolation (a `sandbox_<principal>` naming scheme) exists only **by convention, not enforcement** `[RUNBOOK.md]`, and the open-source Hive Metastore and Iceberg catalog (OSS HMS/Iceberg) **cannot express per-user grants** at all; the repo states this outright and does not claim it `[deploy/auth/README.md]`. Execution is shared too: one long-lived driver, shared executors, no per-tenant pool `[connect/base/deployment.yaml]`.

[[[SVG-ISOLATION]]]

**The five links, outside-in.** Each is demonstrated on live EKS (2026-07-10 unless noted); each names the residual threat it uniquely closes, the attack that would still succeed with that layer removed but the other four intact.

1. **Ingress routing: DEMONSTRATED.** *Residual threat closed: an agent that connects straight to another tenant's server.* A gateway Envoy terminates client mTLS, derives the principal from the certificate's URI-SAN (`spiffe://safe-spark-agents/<tenant>`), and routes on it: a tenant-A certificate reaches **only** `spark-connect-tenant-a`, tenant-B only its own server, an un-granted principal is denied (**HTTP 403**), and a connection with no client certificate is refused at TLS. The *authenticated identity*, not the client's choice, selects the server, so no route can send a tenant-A certificate to tenant-B's server. Evidence: `paper/notes/proof_2026-07-10_ingress_routing.log`. *(Wire-level encoding and the defense-in-depth caveat are in the implementation notes after link 5.)*
2. **Token custody: DEMONSTRATED.** *Residual threat closed: an agent that presents another tenant's credential.* Each tenant gets its own Connect server, configured with **only its own tenant's catalog token, injected in the server's config** and never exposed to the client. A session on tenant-A's server operates only as tenant-A; when it configures a catalog for tenant-B it holds no tenant-B credential and is refused (`NotAuthorized: Missing Authorization Header`). The agent never sees a token, so it cannot redirect or replay one. (Full custody at fleet scale, holding and rotating the vended credential, is §4/Omnigent's job; §3 shows the per-tenant server binding.) Evidence: `paper/notes/proof_2026-07-10_multiserver.log`.
3. **Execution isolation: DEMONSTRATED.** *Residual threat closed: a co-resident session reading another tenant's in-process data.* Because Connect sessions are server-local, tenant-A's work runs on executor pods owned by tenant-A's driver (2 pods, distinct IPs), disjoint from tenant-B's (a separate driver app and pod). The two tenants never share a JVM, so neither can read the other's in-memory or on-disk shuffle data or scavenge a leaked in-process credential. Evidence: `paper/notes/proof_2026-07-10_multiserver.log`.
4. **Catalog authorization: DEMONSTRATED (Lakekeeper + OpenFGA).** *Residual threat closed: an agent that simply asks the catalog for another tenant's tables or credential.* Beyond scoping the credential once issued (point 5), the catalog gates *which* principal may request *which* tenant. With OpenFGA and per-tenant OIDC identities, a tenant-A identity is denied at the catalog for tenant-B across warehouse resolution (`config`) and namespace list/create (`404`, existence hidden for a zero-relation principal), while tenant-B and the admin get `200` and an unauthenticated call is `401`. That the deny is *authorization* and not nonexistence is shown by toggling it: a `describe` grant flips `404`↔`200`. The credential-vending path itself is probed directly, not inferred: with a real table in tenant-B, tenant-B's own `loadTable` carrying the `vended-credentials` delegation returns `200` **with credentials in the response**, while a tenant-A identity gets `404` (table hidden, no credentials vended). That is the exact cross-principal vend the fleet-scoped catalog performed and this one refuses, the seam an audit of point 5 exposes, now closed. Evidence: `paper/notes/proof_2026-07-10_perprincipal_authz.log` (sections A-D).
5. **Storage scoping: DEMONSTRATED (2026-07-09).** *Residual threat closed: an agent that somehow holds a credential whose reach includes another tenant's bytes.* The catalog vends per-tenant, prefix-scoped STS credentials **keylessly** (IRSA assumes a downscoping role, external-id-pinned). The same vended credential replayed against the other tenant's prefix is `AccessDenied`, both directions; an ablation confirms a whole-bucket credential *would* cross, so the deny is the downscoping vend, not the base policy. CloudTrail settles that the compute uses the vend and nothing else: every warehouse object call is under the vended session and the fleet IRSA role makes **zero** data calls. OSS HMS/Iceberg cannot express any of this; the governed Iceberg-REST catalog is what makes even storage-scoping enforceable. Evidence: `paper/notes/cloudtrail_vend_evidence.md`, `paper/notes/proof_2026-07-10_delta_and_frontier.log`.

**Implementation notes (ingress, link 1).** The gateway supersedes the single-tenant per-pod mTLS sidecar of §3.2.1. Over gRPC the no-route deny surfaces as an HTTP-200 reply carrying `x-routed-to=DENIED` and `grpc-message: no tenant route` (a plain HTTP request gets a real 403). Two server-side checks bar a direct in-cluster dial that skips the gateway: Spark's pre-shared-token auth (the gateway injects a `spark-connect-psk` bearer the agent never holds; it is a shared infrastructure secret, not per-tenant) and the principal-pinning interceptor (rejects a request whose `user_id` does not match the gateway-derived `x-connect-principal`). The per-tenant servers are ClusterIP-only but bind `0.0.0.0:15002`, so those two secrets, not network topology, bar a direct connection today; a `NetworkPolicy` restricting ingress to the gateway (`deploy/eks/lakekeeper/authz/netpol-tenant-servers.yaml`) is shipped as defense-in-depth but is **not yet applied** (it needs a policy-enforcing CNI and executor-to-driver allowances).

**Proven link by link, not yet as one composed request.** Each link runs on live EKS, and the per-tenant servers already compose links 2 through 5, a write through tenant-A's server draws a tenant-scoped vend from the authorization-enabled catalog and runs on tenant-A's executors. Two honest seams remain. The storage-scoping *forensic* (the CloudTrail vend-not-IRSA discriminator) was captured against the fleet-scoped catalog, while routing, custody, execution, and authorization ran against the authorization-enabled catalog; and a single request traversing all five links, a Spark job over client-cert mTLS through the gateway to authz-catalog-vended, prefix-scoped storage on the tenant's own executors, is the remaining composition step. Neither weakens a per-link claim; both are named so the reader is not sold a composed run that was not captured.

**Credential vending vs custody (the §3↔§4 line).** The catalog *vends* short-lived, scoped credentials (a catalog function) but does **not** hand them to the agent. Holding and managing the vended credential (custody + the agent interface) is the **orchestration layer's job (§4/Omnigent)**, precisely so the agent never sees a credential and §2's boundary survives at fleet scale. §3 owns the catalog as the **authority** that grants and the **vendor** that issues short-lived credentials; §4 owns **credential custody plus the agent interface**.

[[[SVG-CUSTODY]]]

**The edge, stated plainly.** §3 now demonstrates the governed, integrable, scalable boundary *per tenant*, all five isolation links running on live EKS, and draws the one line it does not cross: credential **custody** at fleet scale (delegated to §4/Omnigent) and **scale** itself (many tenants, node autoscaling), which are named, not left undone by accident. The specific open-stack limit, that **OSS HMS/Iceberg cannot express per-user grants**, is itself a finding: it marks where an open stack must hand tenant authorization to a governed catalog, which is exactly what the demonstrated Lakekeeper + OpenFGA layer does.
- **Tier: all five links DEMONSTRATED on live EKS; only multi-tenant scale is a new-capability FRONTIER (two proof-completeness seams remain, below).** The per-link evidence is in points 1-5 above and the §3.4 table; the figure `paper/figures/isolation-architecture.html` renders the whole path. Two measurement caveats the per-link proofs carry, stated so they are not glossed: (i) the storage-scoping probe (a per-write delta of 12 tasks measured before/after an 8-partition shuffle under `spark.master=k8s`) proves only that execution left the driver onto a **dedicated executor pod, not the local process**, on the shared-server run both tenants happened to land on the *same* pod, so *per-tenant pod disjointness* is a separate result, established by the multi-server run (link 3, distinct pod IPs); and (ii) the cross-tenant storage denial is observed by **replaying** the tenant-scoped vended credential against the other prefix, not by an executor pod being refused in-cluster, the executor-side channel shows only that all FileIO used the vend (CloudTrail: the fleet IRSA role makes zero data calls). Full build + proof narrative: `paper/notes/PLATFORM_LAB_NOTEBOOK.md`.

### Catalog binding: Lakekeeper and Unity Catalog OSS as co-equal governed catalogs
*The governed catalog is a swappable component, not a Lakekeeper dependency. (Author disclosure: a co-author works on open-source Unity Catalog at Databricks; this evaluation names UC OSS's gaps as scrupulously as its strengths, and every UC claim is verified against v0.5.0 source or reproduced live.)*

The five isolation layers split by whether the **catalog** owns them. **L1 ingress routing, L2 token custody, and L3 execution isolation are catalog-independent**: they live at the Connect ingress, the per-tenant Connect servers (each injects *that tenant's* catalog token), and the executor pods, and carry a Unity Catalog token exactly as they carry a Lakekeeper token. **L4 per-principal authorization and L5 prefix-scoped credential vending are the catalog's job**, and both catalogs enforce them.

**Both catalogs, verified.** Each authenticates a per-tenant identity, enforces a per-principal grant on every operation *including the credential-vend path*, and vends short-lived cloud credentials downscoped to the tenant's own prefix with external-id pinning, so a cross-tenant request is refused at the catalog and a replayed credential is refused at storage.

- **Lakekeeper + OpenFGA (Iceberg-REST),** the primary binding demonstrated above: per-principal authz via OpenFGA (Zanzibar, relationship-based); cross-tenant catalog request `404`; vend refused for an un-granted tenant; STS creds downscoped and external-id-pinned. One vendor-neutral Rust binary, natively Iceberg-REST.
- **Unity Catalog OSS 0.5.0 (native plugin),** verified against v0.5.0 source and reproduced live: per-principal authz via JCasbin (RBAC over catalog/schema/table securables); the vend endpoint gated by the same grants (`@AuthorizeExpression(VEND_TABLE_CREDENTIAL)`: `READ`→`SELECT`, `READ_WRITE`→`SELECT`+`MODIFY`); the vend mints 1-hour STS `AssumeRole` creds downscoped by an inline session policy to the exact prefix, with `sts:ExternalId` pinning. On live AWS (`paper/notes/proof_2026-07-10_uc_vending.log`, **6/6 checks**): tenant A vends real downscoped `ASIA*` creds for its own prefix; the same principal is refused `PERMISSION_DENIED` for tenant B's location (both directions); and tenant A's vended credential replayed against tenant B's prefix yields S3 `AccessDenied`. And **end to end through Spark** (the UC-native connector carrying a per-tenant UC token): a query authenticated as tenant A reads only tenant A's Delta table, via UC `loadTable` authorization, then a downscoped credential vend, then an executor read of the tenant's Delta files in S3, and is refused tenant B's table at the catalog (`PERMISSION_DENIED`), both directions.

**Where Unity Catalog OSS is genuinely weaker (and it is).** The per-principal L4/L5 result holds only via UC's **native plugin path**, with authorization enabled and each tenant presenting its own non-owner token. Residual gaps, stamped to v0.5.0:

| Gap | Kind | Detail |
|---|---|---|
| Iceberg-REST path cannot express per-principal L4 | **hard** | UC's Iceberg-REST endpoint authorizes every route at metastore-`OWNER` (all-or-nothing) and is read-only / UniForm-only; per-principal grants work only on the UC-native plugin. Lakekeeper does per-principal authz natively over Iceberg-REST. |
| Authorization off by default | posture | ships `server.authorization=disable` (an allow-all authorizer that leaves vending open); isolation is opt-in, not default. |
| No row-level security / column masking / ABAC | **hard** | coarse object-level RBAC only; RLS and masks are Databricks-managed-UC features. |
| RBAC, not relationship-based | design | JCasbin ACL/RBAC with hierarchical inheritance vs OpenFGA (Zanzibar); the OSS authorizer is single and non-thread-safe under concurrent policy reload. |
| Identity mapping | setup | maps the OIDC `email` claim to a **pre-provisioned** UC user (no default JIT; group grants unverified), so many-tenant onboarding is per-user grant work. |
| Authz maturity | maturity | standing UC 0.5.0 up per-principal required patching two authz bugs (multi-frame request-body parsing; missing authorization expression on `GET /permissions`). |
| Operational weight | ops | UC server + Postgres + OIDC token-exchange + JCasbin vs Lakekeeper's single Rust binary. |

**Takeaway.** Per-principal authorization and prefix-scoped vending, the two things §3's isolation rests on, are achievable on both an Iceberg-REST-native catalog (Lakekeeper + OpenFGA) and Unity Catalog OSS 0.5.0 (native plugin + JCasbin). We demonstrate the full isolation proof on Lakekeeper and reproduce the catalog-integrated path **end to end through Spark on UC OSS** (per-tenant token custody, per-principal authorization, downscoped vending, and executor data-read isolation; the catalog-independent Envoy ingress and per-tenant Kubernetes pod separation are identical across bindings), and we name where UC OSS is weaker (Iceberg-REST per-principal authz, default posture, fine-grained governance) so the choice is an informed one.

## 3.4 Evidence tiering
| Pillar | Demonstrated | Configured-unrun | Frontier / design-only |
|---|---|---|---|
| GitOps/CI | PR-author session denial; dry-run+reconcile workflows; local gate smoke | EKS-target reconcile | (none) |
| Connect-on-k8s | topology; small-scale distributed exec on EKS | elastic 0→10 executors | node autoscaler |
| Tenant governance | **per-principal mTLS ingress routing** (Envoy routes by client-cert SAN: tenant-A cert reaches only tenant-A's server, un-granted principal `403`, no cert refused); **token custody + execution isolation** via two per-tenant Connect servers (server-injected token; tenant-A session refused on tenant-B; disjoint executor pods per tenant); **per-principal catalog authorization** (Lakekeeper+OpenFGA+OIDC: tenant-A identity denied at the catalog for tenant-B, both directions, every op; grants toggle); **per-tenant storage isolation** (Lakekeeper vended creds; cross-tenant `AccessDenied` both directions; cluster-side FileIO via the vend per CloudTrail, fleet IRSA 0 data calls; separate executor pod per Spark UI) | (none) | multi-tenant scale (many tenants + node autoscaling) |

**Reproduce it.** The end-to-end setup is `deploy/eks/lakekeeper/SETUP.md`: four sub-deployments, built inside-out, each standing up one or more layers and writing the proof log cited above.

[[[SVG-REPRODUCE]]]

## 3.5 What §3 unlocks → §4 (Omnigent)
The governed, scalable, integrable substrate is the precondition for the agent *orchestration* layer, **Section 4 (Omnigent)**: a concrete thesis with a demonstrated core, whose governance pillar rests on the multi-tenancy stack demonstrated above (per-principal routing, token custody, execution isolation, catalog authorization, and storage scoping). §3 proves the *mechanism* per tenant; §4 is what operates it across a *fleet* of agents at scale, holding credential custody so each agent stays credential-free. That fleet is now demonstrated on this platform: a third tenant was provisioned by the same procedure, and the orchestrator built end-to-end medallion pipelines for three isolated customers over their own tenants, credential-free, with every cross-tenant read denied (S4.5).


---

# SECTION 4, Omnigent: Governed Multi-Agent Orchestration for Data Engineering
### An orchestration layer for a fleet of governed agents

*This section is a thesis with a demonstrated core (credential custody, S4.3, and a native, autonomous heterogeneous fleet that a single Omnigent agent drove end-to-end, building governed medallion pipelines for three isolated tenants over the live platform, S4.5), not a measured result*: the quantitative fleet study (cost and quality numbers) is a separate experiment, out of scope for this paper's run.

Section 3's platform governs *one* agent. **Omnigent** is the layer above it that governs a *fleet*: it aims to make many agents doing data engineering cheaper, higher-quality, governed, and collectively knowledgeable, properties that raw parallelism (N independent sessions) cannot provide. It sits atop Section 3's platform and holds **credential custody**, so each agent stays credential-free: the control boundary of Section 2, preserved at fleet scale.

**Why not just run N sessions in parallel?** Four composing axes, each tracing back to the spine.

## S4.1 Cost: heterogeneous model routing
Match the model to the task: a cheap/small model for a trivial fix, a strong model for a refactor, a different vendor for review. Metric: **cost-per-correct-pipeline**, §1's H5.3 (cost-per-correct-completion) lifted to the fleet. *(Quantitative claim = separate experiment.)*

## S4.2 Quality: cross-vendor review
A different-vendor reviewer (e.g. Codex reviewing a Claude-authored PR) catches defects that **correlated-blind-spot** same-vendor review structurally misses. Testable catch-rate hypothesis. *(Separate experiment.)*

## S4.3 Governance: credential custody (the keystone)
The catalog (§3) vends short-lived scoped credentials; **Omnigent holds custody and mediates the agent↔catalog interface; the agent never sees a credential.** This is what preserves §2's "agent holds no creds" boundary at fleet scale: N raw sessions leak it per-session; one custodian governs it once. **DEMONSTRATED (2026-07-10).** A custodian process holds every per-tenant credential and exposes agents only a spec-in, pass-or-fail interface: on each job it mints a fresh short-lived (300s) per-tenant token, runs the work with it over the §3 catalog binding, and returns only the result. In one run, a single custodian governed both tenants, minted and rotated three short-lived credentials, and the agents held none; an agent submitting a cross-tenant read was refused (`PERMISSION_DENIED`), so §3's per-tenant isolation holds under fleet custody. The keystone is no longer frontier. Evidence: `paper/notes/proof_2026-07-10_sp41_custody.log`. What remains frontier is P6 scale-out (many tenants, node autoscaling) and rotation under live long-running jobs.

[[[SVG-CUSTODIAN]]]

## S4.4 Knowledge: shared skill library
One governed, versioned skill library (`pyspark-sdp`, safety, conventions) injected fleet-wide → correctness propagation, consistency, single-point updates, a guaranteed knowledge floor. **Evidence-backed by §1:** `pyspark-sdp` is *load-bearing*: without it agents hallucinate Databricks DLT and hit zero-completion, so fleet-wide skill sharing makes fleet competence a property of the orchestrator, not luck per session. Skills are **governed artifacts** (access-controlled, mandatable per tenant), which *reclaims* the safety skill as a shareable governed asset, distinct from its scrapped §1 experimental role. *(Static shared skills = demonstrated mechanism; a learned/emergent fleet memory = speculative, not claimed.)*

## S4.5 Demonstrated core vs frontier
- **DEMONSTRATED (mechanism exists, runs):** (i) **credential custody**, a custodian holds and rotates per-tenant credentials for a fleet of credential-free agents over the §3 catalog, isolation preserved (S4.3); and (ii) **heterogeneous orchestration** (mixed-model routing + cross-vendor review + skill injection), the pattern **this paper was built with** (an orchestrator + claude_code / codex / pi sub-agents), and now also run natively and autonomously by a single Omnigent agent as a governed fleet building medallion pipelines for three isolated customers over the live §3 platform (shown concretely below).
- **FRONTIER:** P6 scale-out (many tenants + node autoscaling), rotation under live long-running jobs, and a learned fleet memory.
- **OUT OF SCOPE (separate experiment):** the quantitative fleet study: cost-per-correct-pipeline, cross-vendor catch-rate, advisor-model / fleet-architecture numbers. Not part of §1's run; not retrofitted (design in S4.7).

**The demonstrated core, concretely.** This paper was built by the orchestration it describes. An orchestrator fanned specialized sub-agents out over the work, independent reader personas, catalog researchers, and design panels, had adversarial verifiers try to *refute* each finding before it was accepted, and synthesized only the survivors, all sharing one governed skill and context set. Heterogeneous models and vendors filled distinct roles (a Claude orchestrator with `claude_code` / `codex` / `pi` sub-agents, cross-vendor review to catch correlated blind spots), and the S4.3 custodian above holds their credentials so each stays credential-free. The *mechanism* runs and produced this document; the *numbers* (catch-rate, cost-per-correct) are S4.7's separate study.

**The demonstrated core, on the platform's own domain.** The same orchestration was run as a governed data-engineering fleet over the live §3 platform, in its strongest form natively and unattended (a single Omnigent agent, given one governed custodian tool, drove the whole loop itself). From one brief, the orchestrator decomposed an end-to-end medallion build (bronze to silver to gold) for three customers with different data needs and governance policies; routed authoring across three vendors by task difficulty (a local Qwen model for the simplest customer, DeepSeek for the medium, Claude Opus for the hardest); had a different-vendor reviewer flag silent defects the authors missed; and submitted every pipeline through the S4.3 custodian, so the agents stayed credential-free. Each medallion materialized over that customer's own §3 tenant; the custodian enforced each customer's contextual data policy (a quarantine rule, a PII-masking rule, and a financial value-conservation rule), and the financial policy correctly rejected a non-conserving first draft before it was fixed. When a submission failed the custodian's checks, the concrete error was fed back and the fleet repaired the pipeline (escalating to a stronger model) until it passed, closing §2's dev loop at fleet scale; every cross-tenant read was denied, so §3 isolation held under the fleet. The loop is wired natively in the orchestration layer: the custodian is a governed tool that holds every credential in its own process, cost is bounded by a native cost policy, and cross-vendor routing is the orchestrator's own sub-agent roster. The orchestrator drove this both under a deterministic wrapper and, natively and unattended, as a single agent that decomposed the brief, routed a real cross-vendor fleet (Anthropic, Qwen, and OpenAI models, re-routing live when one vendor's harness failed), reviewed across vendors, and drove the custody and repair loop to build all three medallions itself. This is a demonstration that the mechanism runs, not a numbers claim (S4.7). Evidence: `paper/notes/proof_2026-07-12_sp4_capstone.log`, the architecture in `paper/diagrams/section4_capstone_fleet.svg`, and the reproducible agent at `deploy/omnigent/sdp-capstone/`.

[[[SVG-CAPSTONE-FLEET]]]

## S4.6 Dependency
§4's governance pillar rests on §3's per-tenant isolation, now demonstrated (P5); what §4 adds is credential *custody* across a fleet, now demonstrated too (S4.3), leaving P6 scale-out as the frontier. Until the fleet study lands, §4 stands as the architectural thesis plus a demonstrated core, the custody keystone and the heterogeneous orchestration pattern now shown building governed pipelines over the live platform, natively and autonomously, not a numbers claim.

## S4.7 The fleet study (SP4.2): the numbers, a separate experiment
The quantitative claims behind S4.1 and S4.2 are a *separate*, pre-registered experiment, not part of this paper's run and not retrofitted. Its design, for completeness:
- **Cost:** heterogeneous model routing lowers **cost-per-correct-pipeline** (§1's H5.3 lifted to the fleet) against a single-strong-model fleet at equal completion. Arms: routed vs single-model; metric: dollars per correct pipeline over a task fleet.
- **Quality:** a different-vendor reviewer catches defects a same-vendor reviewer misses (correlated blind spots). Metric: cross-vendor vs same-vendor **defect catch-rate** on a seeded-defect corpus.
- **Method:** §1's instrument (frozen corpus, blind grading, provenance) extended to a fleet of orchestrated agents, powered as §1 was. It lands in its own design doc and paper; no claim in *this* paper depends on it.


---

## Appendix S2-A, Reference Architecture: The Control Boundary
*Executable spec for implementing agents. This is the SSOT target Section 2 is measured against; build work in `SECTION2_eks_connect_demo_checklist.md`.*
**TARGET / source-of-truth.** Everything else (the demo checklist, the build, the paper's claims) *derives from this*. If a component, step, or claim cannot be traced to an invariant below, it is **drift**. Cites are file:line on `origin/dev`.

> **North star.** The agent *proposes inert desired state*; a *governed control plane, on a separate host,*
> validates and executes; the agent never holds a live session, credentials, or touches data. The dev loop
> (propose → dry-run gate → reconcile/execute) *is* that boundary. Declarative makes it expressible; Spark
> Connect is the enforcement mechanism, not the motivation.

---

#### 0. The invariant (the north star written as a testable contract)
The control boundary **holds** iff ALL of these are true. Each is a checkable predicate, not a vibe:

- **I1: Authoring ⊥ Execution.** The agent emits only an inert artifact (declarative spec + transform code);
  it never runs data operations itself.
- **I2: No credentials in the agent.** The agent never holds the Connect endpoint identity (mTLS cert / PSK /
  principal), warehouse creds, or a live `SparkSession`.
- **I3: Host separation.** The process that *executes data work* runs in a different host/trust zone than the
  agent.
- **I4: Gate before data.** Structural validation (dry-run) runs and can reject **before any data is processed.**
- **I5: Governed reconciliation.** A controller the agent does not control performs the submit/execute step.
- **I6: Inertness in transit.** What crosses the boundary is a *plan/spec*, never arbitrary code handed a live
  engine.

**A demonstration that violates any I-rule is a simulation of the boundary, not the boundary.**

---

#### 1. Trust zones
- **Zone U: Untrusted authoring.** The agent + its workspace. May write files. Holds **no creds, no session.**
- **Zone C: Governed control plane.** The reconciler/controller. Holds creds, runs the SDP Connect *client*,
  presents identity to the data plane, drives propose→gate→execute→grade. **The agent cannot run code here.**
- **Zone D: Data plane (remote, EKS).** Spark Connect service (client-mode driver pod) + executor pods +
  catalog (Iceberg JDBC/HMS) + warehouse (S3). **The only place data is touched.**
- **Boundary U│C:** agent hands an inert artifact to the controller. (Enforces I1.)
- **Boundary C│D:** controller submits *plans* over an authenticated mTLS/PSK Connect channel. (Enforces I2/I3/I6.)

---

#### 2. Components (role · zone · trust)
| Component | Zone | Role | Holds creds? |
|---|---|---|---|
| Agent (LLM) | U | Proposes SDP spec + transforms as text | **No** |
| Per-cell workspace | U | Inert `spark-pipeline.yml` + `transformations/pipeline.py` `[runner.py:369-405]` | No |
| Reconciler / controller | C | Runs SDP CLI client; drives gate+execute; blind-grades | **Yes** |
| Dry-run gate | C→D | Structural validation before data `[sdp_dryrun.py:462-484]` | via controller |
| Connect channel | C│D | mTLS + `Bearer PSK` + `x-connect-principal` via Envoy `[envoy.yaml:71-158]` | yes (controller-held) |
| Connect server (driver) | D | Client-mode driver in pod; builds/runs graph `[6ff8139]` | cluster identity |
| Executor pods | D | Do the data work | cluster (IRSA) |
| Catalog | D | Iceberg JDBC / HMS `[spark-defaults.template.conf:33-40]` | cluster |
| Warehouse (S3) | D | `s3a://…` via IRSA | cluster |
| Blind oracle | C | Grades output without seeing paradigm | n/a |

---

#### 3. End-to-end flow (each arrow tagged with the invariant it enforces)
1. **propose**: agent emits spec+code (Zone U). → *I1*
2. **materialize**: inert artifact written to workspace; handed across U│C. → *I1, I6*
3. **dry-run gate**: controller submits a structural dry-run to D; **no data touched**; structural defects
   returned to agent as feedback. → *I4*
4. **reconcile/execute**: controller (Zone C, holding creds) runs the SDP CLI *client*, which ships
   DefineOutput/DefineFlow/StartRun **plans** over the authenticated Connect channel. → *I5, I6, I2*
5. **execute in D**: driver pod + executor pods run the data work against the S3 warehouse/catalog. → *I3*
6. **telemetry + result** back to C; **blind grade**. → governance closure

---

#### 4. The authenticated submission path (the C│D detail that is the crux)
- The controller runs the stock SDP CLI as a Connect **client**: it imports the agent's transform Python,
  builds the dataflow graph, and sends **protobuf plans** over gRPC: **the server needs no raw files**
  `[pyspark/pipelines/cli.py:221-263; spark_connect_graph_element_registry.py:51-136]`. (I6 satisfied for code.)
- **Channel:** `sc://<NLB>:15009/;use_ssl=true` + `Bearer PSK` + `x-connect-principal`, terminated by Envoy
  mTLS `[envoy.yaml:24-158]`. The controller, not a side-tunnel, must hold and present this identity. (I2.)
- **Data plane:** Connect server = client-mode driver pod; executors = k8s pods; warehouse = S3 via IRSA;
  catalog = Iceberg JDBC/HMS `[6ff8139; spark-defaults.template.conf:33-48]`. (I3.)

---

#### 5. Known boundary leaks to design against (name them, don't hide them)
- **R1: agent code executes in Zone C during plan construction.** The SDP client `exec_module`s the agent's
  transform Python to build the plan `[cli.py:248-263]`. No data/creds are exposed at that instant, but it *is*
  agent-authored code running in the governed zone. Reference stance: acceptable as *plan construction* only if
  sandboxed/AST-checked; must be stated explicitly as the subtlest part of the boundary.
- **R2: controller co-located with agent.** Today the reconciler runs as a subprocess on the agent/harness host
  `[live.py:675-689]` (Zones U and C collapsed). Reference **requires them split**.
- **R3: mTLS via socat tunnel.** The prior remote runs terminated mTLS in a local `socat` tunnel, not the client
  `[study.config.live.json:2]`. That parks the creds in the tunnel host, not the controller → violates the spirit
  of I2. Reference **requires the controller itself to present identity natively**.

---

#### 6. Working backwards: dependency-ordered layers (build order = drift detector)
Each layer depends on the one below. **You cannot honestly claim a layer while a lower invariant is unproven; 
that is the definition of drift here.**

| Layer | Claim it licenses | Enforces | Depends on |
|---|---|---|---|
| **L0 Substrate** | EKS Connect server + Envoy + catalog + S3 reachable | (none) | (none) |
| **L1 Authenticated channel** | controller → Connect via **native** mTLS/PSK, no tunnel | I2 (C│D) | L0 |
| **L2 Off-host execution** | a trivial plan runs in D, driver+executors in pods | I3 | L1 |
| **L3 SDP submission green** | agent-authored SDP spec submitted from C **completes + grades green** in D | I1, I6 | L2 |
| **L4 Gate before data** | dry-run rejects structural defects pre-execution | I4 | L2 |
| **L5 Governance split** | reconciler in Zone C, agent in Zone U, agent holds **no creds** | I5, I2, (R2) | L3, L4 |
| **L6 Negative control** | imperative **cannot** traverse C│D (compatibility probe) | thesis support | L1 |

**Read top-down to find the highest layer we may honestly claim today; read bottom-up to build.**

---

#### 7. Reverse-engineering map (reference → as-built), seeded, to complete together
| Ref element | Target (invariant) | As-built status | Evidence / delta |
|---|---|---|---|
| Inert artifact | I1, I6 | **IMPLEMENTED** | `runner.py:369-405`; spec+code only |
| Plan-not-files submission | I6 | **IMPLEMENTED** (PySpark) | `cli.py:221-263` |
| L0 substrate | (none) | **APPLIED via terraform + CI (2026-07-09)**, S3-backed state (the earlier hand-built June cluster was torn down + rebuilt clean) | `deploy/eks/terraform`; `paper/notes/PLATFORM_LAB_NOTEBOOK.md` |
| L1 native mTLS/PSK | I2 | **PARTIAL**: reached via socat tunnel, native client unproven (R3) | `study.config.live.json:2` |
| L2 off-host execution | I3 | **DEMONSTRATED, both arms**: driver+executors in pods, tables materialized | `repro/h3_eks/` |
| L3 SDP green remote | I1,I6 | **DEMONSTRATED (2026-07-06)**: Arm B SDP completes+grades green remotely (agent authors inert spec; CLI submits session-less). Took harness data-path/catalog fixes, not architecture | `repro/h3_eks/` |
| L4 gate before data | I4 | **IMPLEMENTED locally**; not re-proven remote | `sdp_dryrun.py:462-484` |
| L5 governance split | I5,I2,R2 | **GAP**: reconciler co-located on agent host | `live.py:675-689` |
| L6 imperative-can't-cross | thesis | **CORROBORATED, not captured**: why Part 1 went local | `DEVIATIONS.md:516-522` |

**Highest honestly-claimable layer today: ~L3** (both arms run the full loop remotely; Arm B SDP completes+grades green, 2026-07-06). L5 (governance split) is the real
remaining work; L1 needs native-mTLS de-risking; L6 needs capturing as a clean artifact.

---

#### 8. How this section may be written, by layer (anti-overclaim guide)
- Claim **only up to the highest proven layer**, and state the next gap plainly.
- "Demonstration" language is licensed for L0–L4 (both arms remote, Arm B SDP green, 2026-07-06); **L5 (governance split) must read as "remaining gap," not done.**
- The **thesis** (control boundary) is *architecturally sound and demonstrated across hosts (L3)*; the honest framing is
  "exercised across hosts on a real cluster; full SDP completion DONE (2026-07-06); governance split (L5) pending."


---

## Appendix S3-A, Reference Architecture: The Open Governed Platform
*Executable target for the multi-tenant platform §3 builds toward: the SSOT for implementing agents. Build work: `SECTION3_platform_build_checklist.md`. As with Appendix S2-A, claim only up to the highest proven layer; anything above it is a build task, not a result.*

#### G: Invariants (what the platform must satisfy)
- **G1 GitOps-only mutation.** Production state changes only via reviewed PR + CI reconcile; no direct agent submission.
- **G2 Connect-as-ingress.** All execution enters through the single governed mTLS/identity-pinned Connect endpoint; no path around it (subsumes §2's boundary).
- **G3 Identity pinning.** Each session's principal is cryptographically derived from the cert SAN, unspoofable.
- **G4 Elastic execution.** Compute scales on Kubernetes (dynamic executor pods) *behind* the ingress.
- **G5 Tenant isolation.** Per-tenant authorization (catalog grants) + per-tenant execution isolation (per-tenant multi-server Connect, demonstrated), **delegated to a governed catalog, with multi-tenant scale (many tenants + node autoscaling) still future**, not reinvented.
- **G6 Auditability.** Every change is a reviewable artifact (PR + provenance).

*Credential flow (the §3↔§4 line):* the catalog **vends** short-lived scoped credentials; the orchestration layer (§4/Omnigent) holds **custody** and mediates the agent interface; the agent stays **credential-free**: this is how G2 / §2-I2 survives at fleet scale.

#### P: Build/claim layers (bottom-up; highest proven layer = honest claim ceiling)
| Layer | Licenses the claim | Status today |
|---|---|---|
| **P0 substrate** | EKS + Connect + Envoy + catalog + S3 exist | **DEMONSTRATED: terraform-applied + CI/OIDC-gated, live (2026-07-09)** |
| **P1 governed ingress** | mTLS + principal pinning, no bypass | **DEPLOYED + ENFORCING on EKS** (Envoy mTLS + interceptor pin principal+PSK+user_id); native pyspark client-cert path still unproven |
| **P2 elastic execution** | driver + dynamically-allocated executor pods | **DEPLOYED on EKS**; executor pods spin up; elastic 0→N→0 cycle uncaptured |
| **P3 GitOps boundary** | agent-as-PR-author + CI dry-run gate + reconcile | DEMONSTRATED (local) |
| **P4 integration testing** | structural dry-run against the real catalog | DEMONSTRATED; data-quality tests = future |
| **P5 tenant isolation** | data isolation (credential scoping) + per-principal authz + per-tenant execution | **data isolation / credential scoping DEMONSTRATED** (Lakekeeper vended creds; cross-tenant `AccessDenied` both directions on EKS by replaying the vended cred; the write ran on a dedicated executor pod off-driver per Spark UI, 12-task per-write delta, per-tenant pod disjointness is the multi-server result below; CloudTrail shows cluster-side FileIO via the vend, fleet IRSA 0 data calls; ablation confirms the vend is load-bearing); **per-principal catalog authz DEMONSTRATED** (Lakekeeper+OpenFGA+OIDC: tenant-A identity denied at the catalog for tenant-B across warehouse-resolution, namespace, and the direct vend path (loadTable+delegation: tenant-B vends `200`+creds, tenant-A `404`), grants toggle; `proof_2026-07-10_perprincipal_authz.log`); **token custody + per-tenant execution isolation DEMONSTRATED** (two per-tenant Connect servers each injecting only its tenant's catalog token server-side; a tenant-A session is refused on tenant-B `NotAuthorized`, and tenant-A/tenant-B run on disjoint executor pods; `proof_2026-07-10_multiserver.log`); **per-principal ingress routing DEMONSTRATED** (Envoy gateway routes by client-cert URI-SAN: tenant-A cert reaches only tenant-A's server, un-granted principal `403`, no-cert refused; `proof_2026-07-10_ingress_routing.log`) |
| **P6 multi-tenant scale** | multiple Connect servers + node autoscaling | per-tenant Connect servers + gateway routing DEMONSTRATED (2 tenants for the per-link isolation proofs; a third tenant later provisioned by the same procedure for the §4 fleet capstone, S4.5); scale-out (many tenants + node autoscaling) = FRONTIER |

**Highest honestly-claimable today ≈ the full per-tenant isolation path, demonstrated link by link on EKS 2026-07-09/10: mTLS ingress routing by principal (Envoy routes by client-cert SAN) + token custody and per-tenant execution isolation (two per-tenant Connect servers, server-injected tokens, disjoint executor pods) + per-principal catalog authorization (Lakekeeper + OpenFGA/OIDC, including the direct cross-tenant vend deny) + data isolation at storage (credential scoping).** An agent authenticated as tenant A is routed to tenant-A's server, handed tenant-A's token, run on tenant-A's executors, authorized at the catalog only for tenant-A, and prefix-scoped at storage. What remains: the unbuilt *capability* is multi-tenant **scale** (many tenants + node autoscaling); two proof-completeness seams also remain, a single request composing all five links, and unifying the storage-scoping forensic onto the authorization-enabled catalog.

#### R: Reverse-engineering map (reference → as-built → SALVAGE / GAP)
| Component | Target | As-built | SALVAGE (keep) | GAP (build) |
|---|---|---|---|---|
| GitOps loop | agent→PR→CI gate→reconcile to prod | demonstrated local | `agent_pr_author.py`, `sdp_artifact.py`, dry-run + reconcile workflows, unit tests | wire to EKS Connect (not runner-local); capture a real agent PR; enable prod reconcile |
| Connect ingress | single governed mTLS endpoint + per-principal routing | built; native client mTLS DEMONSTRATED (Envoy gateway routes by cert-SAN to per-tenant servers) | Envoy mTLS, principal interceptor, per-principal routing gateway, deployment, image | (routing DONE 2026-07-10); apply Terraform for the gateway; capture deploy artifacts |
| Elastic execution | driver + dyn executors, autoscaling | small-scale demonstrated | dyn-alloc config, executor pod template, executor node group, image | prove 0→10→0 scale; add Karpenter/Cluster-Autoscaler |
| Catalog authz | per-tenant grants | DEMONSTRATED via Lakekeeper + OpenFGA + OIDC (per-principal deny at the catalog) | governed Iceberg-REST catalog, per-tenant vending + grants, OIDC identities | (delegation DONE 2026-07-10); OSS HMS/Iceberg still cannot enforce, hence the governed catalog |
| Tenant exec isolation | per-tenant Connect / pools | DEMONSTRATED: per-tenant Connect servers + gateway routing + disjoint executor pods | per-tenant Connect servers, per-principal routing gateway, token injection, distinct executor pods | (per-tenant servers + routing DONE 2026-07-10); scale to N tenants + node autoscaling |
| Integration testing | structural + data-quality gate | structural only | the CI dry-run gate | data-quality / expectation tests in CI |
| Substrate (IaC) | reproducible cluster | terraform-applied + CI/OIDC-gated, live (2026-07-09) | terraform stack, k8s manifests, HMS, image | (apply DONE); keep capturing run evidence per deploy |

**Read R top-to-bottom to build: P0→P4 is mostly salvage + wiring; P5/P6 is genuine new construction (a governed catalog + multi-server Connect).**


# Deviations & implementation decisions

Per the pre-registration's contract (§ preamble): "Any deviation made after
seeing data is logged in `DEVIATIONS.md` with rationale." This file was opened
at **instrument-build time** (pre-data) and records
(a) the ONE design item that is below a pre-reg target and needs a human GO
decision before the pilot, and (b) implementation interpretations that operate
within the pre-reg but are worth stating so every choice is traceable.

Nothing here changes a hypothesis, an arm, a metric, or the analysis plan.

---

## Review fix cycle (independent review of PR #14) — all 9 blockers RESOLVED

An independent review found 9 blocking defects, ALL in the live/stats/cost path
(invisible to the replay tests). Each is fixed and now has a regression test; the
live measurement path is validated on REAL Spark (`tests/test_live_path.py`).

- **B1 (FATAL) live silent-defect grading.** The runner now builds the
  `OutputProfile` by READING THE MATERIALIZED TABLE back through the executor and
  running the task's OUTPUT oracle (`harness/output_oracles.py`) against ground
  truth from the matched-seed input — not from `output_metrics=None`. Proven:
  a completed-but-defective D8/D2 output grades `silent_defect=TRUE`, a correct
  one `FALSE`.
- **B2 (FATAL) agent code never written — fully closed in round 2.** Round 1 only
  wrote Python files, so only Arm A's contract was satisfied; the SDP arms (B/B1)
  lacked `spark-pipeline.yml` + the `transformations/` transform and the
  imperative-gated arm (B2) lacked `_analyze_only.py` — a DIFFERENTIAL break across
  the manipulated arms. `runner.materialize_workspace` now writes the COMPLETE
  per-arm file contract, driven by the arm's loop config, with only `proposal.code`
  as content (spec / materialize-main / analyze-harness are boilerplate that invoke
  the agent's code — no answer leaked):

  | arm | paradigm / gate | files written (the backend's read contract) |
  |---|---|---|
  | A  | imperative, no gate | `pipeline.py` |
  | B  | SDP + gate          | `spark-pipeline.yml`, `transformations/pipeline.py` |
  | B1 | SDP, no gate         | `spark-pipeline.yml`, `transformations/pipeline.py` |
  | B2 | imperative + gate    | `pipeline.py`, `_analyze_only.py` |

  `tests/test_workspace_contract.py` is the parity guard: for EACH arm it asserts
  the exact file set the backend (`live.py`) reads exists and holds the agent's
  code, that the SDP spec / analyze-harness contain no agent logic (no leakage),
  and that Arm A is unchanged. Real-cluster execution of the SDP/gated paths stays
  the operator pilot step (no remote mTLS cluster here); the file-generation +
  gate-input wiring is now unit-tested deterministically.
- **B3 (FATAL) replay/live parity.** `OutputProfile` construction is now
  backend-agnostic (`runner._build_profile`): real executors (local/Connect)
  read the table + run the oracle; replay falls back to canned metrics. Same
  `grade_run`. `tests/test_live_path.py` exercises the real path end to end.
- **B4 per-task generators.** `generate_dataset` uses each task's declared
  `input` (orders/CDC/payments), not one global generator.
- **B5 Holm over the GLMM contrasts.** `analyze.glmm_contrasts` extracts the 5
  pre-registered contrast p-values from the GLMM (ref-A + ref-B fits); Holm
  corrects THOSE. McNemar is used only if the GLMM cannot fit, and is then
  explicitly labelled `mcnemar_fallback`.
- **B6 model marginal effects.** The GLMM now reports average marginal effects on
  the probability scale (`ame`); observed per-arm rates are kept but labelled
  DESCRIPTIVE.
- **B7 power rule (pre-reg §6).** `required_n_for_halfwidth` computes the N for a
  95% CI half-width ≤ 0.05 on A−B from the pilot variance; the report carries
  `headline_n_valid` and refuses to validate a headline N below required.
- **B8 H2 double-count.** Per-iteration executor-seconds are a before/after measure
  around each execute (not a cumulative read), proven non-cumulative in
  `tests/test_live_path.py`. **The Connect mechanism is superseded by D-5:** the
  before/after STAGE diff replaces the `/executors.totalDuration` delta, which was
  invalid on the shared long-lived Spark Connect app. `local.LocalSparkExecutor`
  retains the totalDuration delta on its own short-lived `local[2]` app (valid
  there).
- **B9 H2 success bias — RULE PRE-DECLARED HERE.** Compute-to-correct for a run
  that never reached a correct output is **imputed to the total executor-seconds
  it spent** (a conservative lower bound), `reached_correct=False`. The H2
  estimate is reported BOTH as **intention-to-treat** over all matched cells
  (primary) AND **complete-case** over both-green cells (pre-specified
  sensitivity). H2 is never silently conditioned on success.

Rigor items also fixed: `assert_runtime_controls_match` ties identical-except-loop
to the real cfg model + prompt file; `RunOutcome` no longer carries
`structural_caught_stage` (a test asserts the grader exposes no gate-revealing
field); the bootstrap RNG is seeded with `zlib.crc32` (process-independent), so
intervals are reproducible from the fixed seed (the prior `hash()` was salted).

### Implementation notes from the fix cycle
- **Output oracle vs. input quantifier.** The input quantifiers (`quantify.py` /
  `quantify_ext.py`) measure defect *opportunity* and reproduce the registered
  numbers; the **output oracles** grade the agent's materialized table. They are
  the live silent-defect oracle and remain arm-agnostic (no arm/model in the
  grading path). Each semantic task declares an `output_contract` (table + the
  columns the agent must produce) so the oracle reads a known interface.
- **D7 live signal is coarse.** The live D7 output oracle flags output day-buckets
  that don't exist under correct UTC bucketing (a membership check); the precise
  per-row D7 count remains the battery quantifier (`q_d7`/`pay_d7`, unit-tested).
- **Live validation without the remote cluster.** The remote mTLS Connect cluster
  is not reachable from the build env, so the real measurement path is validated
  with an in-process `LocalSparkExecutor` (real Spark, real materialization, real
  read-back, real delta cost). Only the *cluster* differs from `ConnectExecutor`;
  the runner/oracle/cost path is identical. The remote run + the real-LLM brain
  remain the operator's pilot step (the latter needs `ANTHROPIC_API_KEY`).

---

## D-0 (RESOLVED 2026-06-23) — task corpus expanded 6 → 15 (pre-reg §4 ≥12 met)

- **Pre-reg §4** targets **≥12 tasks**. `TASKS.lock.json` (v2.0.0-corpus15) now
  freezes **15** realistic, self-contained Spark data-engineering tasks across
  **three independent data substrates** (orders / customer-CDC / multi-currency
  payments). `corpus_status.below_target = false`.
- **9 new tasks authored** in the existing task-spec format (id/title/domain/
  substrate/input/defects_in_scope/oracles/prompt): `p6_dedup_watermark`,
  `p7_late_data`, `p8_currency_normalize`, `p9_enrich_join`, `p10_scd2`,
  `p11_schema_evolution`, `p12_quarantine_dlq`, `p13_cdc_windowed`,
  `p14_fx_settlement`.
- **Defect-class balance** (the design goal): every D1–D9 class is exhibited by
  **≥5 tasks** — structural and semantic/state families both well represented.
  Each task's coverage matrix entry is asserted against the published numbers by
  `tests/test_corpus.py`.

  | Class | Tasks | Class | Tasks | Class | Tasks |
  |---|---|---|---|---|---|
  | D1 (missing col, struct) | 12 | D4 (broken DAG, struct) | 7 | D7 (timezone, sem) | 5 |
  | D2 (wrong type, sem) | 6 | D5 (immutable cfg, struct) | 5 | D8 (silent drop, sem) | 8 |
  | D3 (unwatermarked, state) | 7 | D6 (nondet dedup, sem) | 6 | D9 (unbounded state) | 6 |

- **Multi-substrate corroboration** (so per-class rates aren't single-dataset
  artifacts): D6 on orders **and** CDC; D7 and D8 on orders **and** payments; D2
  on 6 orders tasks. New quantifiers (`experiments/defect_battery/quantify_ext.py`,
  same blind `(spark, path)` style, no arm/model in the grading path) reproduce
  fixed-seed numbers in `tests/test_oracles_ext.py`:
  - `cdc_d6` @ CDC seed=7 → **77** ambiguous customers (the **non-latent** D6 the
    orders dataset can't show — orders D6=0 because its dups are byte-identical).
  - `pay_d7` @ payments seed=42 → **935** rows on the wrong UTC day.
  - `pay_d8` @ payments seed=42 → **1139** foreign rows / **$371,010.60** USD
    silently excluded from a naive same-currency sum.
- **Generators parameterized**: `gen_customers_cdc.py` gains `--seed/--customers`
  (default seed=7 preserves its documented oracle: 263 events / 100 customers /
  87 current / 13 deleted); new `gen_payments.py` (`--seed/--N`). Orders generator
  was already parameterized in the prior commit.
- The arms, metrics, grader-blindness, and stats plan are **unchanged**.

## D-1 (validation scope) — live loop validated OFFLINE only

- The build was scoped to **build + validate the instrument**, not run the
  sweep/pilot (awaiting GO). The live agent loop additionally needs the Spark
  Connect backend (catalog swap, finalized in parallel) and an `ANTHROPIC_API_KEY`.
  Neither was available, so the runner was validated with the **replay backend**
  (deterministic, no LLM/Spark/network) end-to-end, and the oracle grader was
  validated against the real E3 numbers + the n=1 pilots.
- This is exactly the permitted fallback ("otherwise validate offline and say
  so"). The **same loop code, cost model, and blind grader** run under the live
  backend; only the brain (`AnthropicBrain`) and executor (`ConnectExecutor`)
  swap in. No pre-reg deviation — a pending step.

## D-2 (RESOLVED 2026-06-23) — sampling: send only `temperature`, not both knobs

- **Found during GO calibration:** the first real Anthropic call (live brain,
  `claude-sonnet-4-6`) returned a hard **400** — `temperature and top_p cannot
  both be specified for this model`. `AnthropicBrain.propose()` was sending BOTH,
  so no live run of any arm could start. It escaped all prior validation because
  the offline checks use the scripted `local.py` brain, which never calls the API.
- **Pre-reg sampling control:** the pre-registration fixes **neither** parameter
  by name — its determinism control is "fixed seeds + the random-effects model"
  (§9). The arm manifests carry `temperature=0.0` (the conventional determinism
  knob) and `top_p=1.0` (the API default).
- **Decision:** send **only `temperature`** (the conventional control); `top_p`
  is left unset (the API default 1.0 is a no-op). This is an **API-constraint
  de-duplication, not a value change** — the controlled sampling value
  (`temperature=0.0`) is byte-identical and unchanged, and identical across all 4
  arms. `top_p=1.0` remains recorded in every manifest as the controlled default.
- **Integrity preserved.** `arm_manifest` now has a single source of truth:
  `SAMPLING_CONTROLLED=("temperature","top_p")` (both validated identical across
  arms) and `SAMPLING_SENT=("temperature",)` (the one knob transmitted), with a
  load-time invariant `SAMPLING_SENT ⊆ SAMPLING_CONTROLLED ⊆ SHARED_FIELDS` and an
  assertion inside `assert_identical_except_loop` — so the param actually sent is
  provably one of the fields validated identical-across-arms, and the sampling
  check is NOT silently weakened. `tests/test_live_sampling.py` (no network)
  asserts, for every arm, that the request kwargs carry exactly one of
  {temperature, top_p} and the value is identical across arms.
- The real live API smoke remains the orchestrator's calibration step (no key /
  Connect reachability here).

## D-3 (RESOLVED 2026-06-24) — stage input/output to cluster-reachable storage

- **Found during GO calibration (real EKS cluster):** after the sampling fix the
  brain call succeeded and Arm A materialized silver/gold/quarantine tables, then
  the runner crashed —
  `AnalysisException [PATH_NOT_FOUND] Path does not exist:
  file:/home/.../.work/_data/gen_messy_orders_seed42.ndjson` (output_oracles.py).
- **Root cause:** the data layer used LOCAL `file:/` paths. That works for the
  in-process `local.py` executor (co-located FS) — the only thing every prior
  validation exercised — but the live sweep drives a REMOTE Connect cluster whose
  driver/executors run in k8s pods that cannot see this machine's filesystem, so
  `spark.read.text(file:/local/...)` is PATH_NOT_FOUND. Same bug class as D-2: a
  live-remote path the scripted/in-process validation never touched.
- **Mechanic correction (first attempt e141028 was wrong, replaced by this fix):**
  the first version staged via `SparkSession.copyFromLocalToFs`. Diagnosed live,
  that is unusable here: `copyFromLocalToFs` ALWAYS writes to the Spark DRIVER pod's
  default filesystem, and on this cluster the driver default scheme is `file:`
  (`spark.sql.warehouse.dir = file:/opt/spark/work-dir/spark-warehouse`,
  `fs.defaultFS` unset). A scheme'd `s3a://` dest is rejected
  (`NO_SCHEMA_AND_DRIVER_DEFAULT_SCHEME`); a scheme-less dest lands on driver-local
  disk the executors can't read. There is NO `copyFromLocalToFs` dest that reaches
  executor-readable storage on this cluster. Abandoned.
- **Fix (engineering only — logic, grading, arms, stats, pre-reg UNCHANGED; only
  WHERE data lives changes):** the live ConnectExecutor stages each seed's input by
  shipping the ROWS to the cluster over the Connect protocol and letting the
  executors write S3 via IRSA (proven live: createDataFrame 6.4s, write S3 3.6s,
  readback 0.8s, content intact):
  1. read the local NDJSON lines in the Python client;
  2. `df = spark.createDataFrame([(line,) for line in lines], "value string")` —
     rows travel client→driver over Connect, NOT a shared filesystem;
  3. `df.write.mode("overwrite").text(s3_dest)` where
     `s3_dest = f"{warehouse_uri}/_ssa_staging/{task}/{arm}/seed{seed}"` —
     executors write S3 natively (IRSA), no local AWS creds, no `copyFromLocalToFs`;
  4. return `s3_dest`.
  `.write.text()` of the single `value` column emits files whose lines are the
  original NDJSON objects, so the staged S3 location is valid NDJSON — both
  `spark.read.text(...)` (oracles) and `spark.read.json(...)` (agent) read it. Input
  is small (~5.3k lines / 636K) → one `createDataFrame`, no chunking. The runner
  threads `s3_dest` to BOTH the agent (input baked into `pipeline.py` / the gate
  harness, announced in the brain prompt) AND the oracle (`build_output_profile` →
  `_orders_true_total` etc.), so they read identical input.
- **Outputs are s3a, never local `file:`.** SDP arms' `spark-pipeline.yml`
  `storage:` is a cluster-reachable warehouse path
  (`{warehouse}/_ssa_pipeline_storage/<task>/<arm>/seed<seed>`); the imperative
  output is `saveAsTable` to the cluster's S3-backed catalog (proven materializing
  in calibration), and the oracle reads back that exact table (B1 preserved). No
  output path is routed through `copyFromLocalToFs` or a local literal on the live
  path (warehouse_uri set).
- **Identical across arms:** all 4 arms (A/B/B1/B2) stage by the SAME code path and
  format, differing only by the `{task,arm,seed}` segment of `s3_dest` — no per-arm
  divergence in how data is staged or read.
- **Local/offline untouched:** `LocalSparkExecutor` and `ReplayExecutor` expose no
  `stage_input`, so `_stage_input` returns the local path unchanged and SDP storage
  stays `file://` — the offline tests stay green and the oracle still reproduces the
  registered numbers (D2=246 / D7=275 / D8=250/$49,778.06 / D6=0).
- **Files changed:** `harness/backends/live.py` (`ConnectExecutor.stage_input`,
  createDataFrame + write.text), `harness/runner.py` (`_stage_input`,
  `_sdp_storage_for`, `run_cell` threads the staged path, `materialize_workspace`/
  `_sdp_spec` take a cluster storage URI, `StudyConfig.warehouse_uri`),
  `harness/backends/base.py` (`LoopState.output_storage`). New
  `tests/test_remote_staging.py` (no network): a fake Connect session asserts, for
  every arm, that input is staged via `createDataFrame` + `write.text` to a scheme'd
  `s3a://` dest derived from `warehouse_uri`, that `copyFromLocalToFs` is NOT called
  (regression lock), that dataset_path/input_path are rewritten to the staged s3a
  path (never a bare local path), that the oracle is handed the staged path, that
  SDP storage is cluster-reachable, and that the local executor path is unchanged.
- The real end-to-end live run on the EKS cluster remains the orchestrator's step
  (no key / Connect / AWS creds here).

## D-4 (2026-06-24) — imperative arms (A, B2) execution contract: agent owns the SparkSession + output materialization

- **Imperative arms (A, B2) execution contract changed — the agent now owns the
  SparkSession + output materialization; the harness no longer injects an
  `_imperative_main` SparkSession+`saveAsTable` or a separate `_analyze_only.py`
  SparkSession.** Completion is verified by a NEUTRAL read-back of the contract
  output table.
- **Rationale (experimental validity):** the imperative arms are meant to show that
  an imperative PySpark agent, *given session ownership*, naturally writes
  classic/JVM Spark that fails on the Spark Connect substrate
  (`JVM_ATTRIBUTE_NOT_SUPPORTED`, `CANNOT_MODIFY_STATIC_CONFIG`). For that finding to
  be valid, the failure must be attributable to the AGENT'S authored code, not to
  harness scaffolding. Previously the harness appended its OWN session code
  (`_imperative_main` → `SparkSession.builder.getOrCreate()` + `saveAsTable`) and a
  B2 `_analyze_only.py` that also created a SparkSession — a confound. This removes
  it (approved design review, Option A: agent-owns-everything + neutral completion
  check).
- **New imperative contract (NEUTRAL about Spark idioms — we do NOT tell the agent
  to use classic Spark OR Spark Connect / DataFrame-only; biasing it would be
  "telling it to fail"):** the agent's `pipeline.py` must be runnable as a program
  that (i) acquires its OWN SparkSession, (ii) reads input from env
  `AGENT_INPUT_PATH` (fallback to the provided dataset path), (iii) MATERIALIZES the
  task's output table(s) per the task `output_contract` (e.g. `gold_daily`,
  `silver_orders`; the primary name is also provided in `AGENT_OUTPUT_TABLE`),
  (iv) prints `Run is COMPLETED` on success, and (v) for the GATED imperative arm
  (B2) supports an `--analyze-only` mode that builds/analyzes the plan WITHOUT
  materializing. The prompt states this contract mechanically and announces no Spark
  framework, API, or configuration choice.
- **Harness changes (logic / grading / oracles / arms / stats UNCHANGED; only WHO
  writes the session+output moves, from harness → agent):**
  - `runner.materialize_workspace` (imperative branch) writes `proposal.code`
    VERBATIM as `pipeline.py`; the appended `_imperative_main(...)` and the generated
    `_analyze_only.py` are removed, and the `_imperative_main` / `_analyze_only_harness`
    helpers are deleted.
  - `live.ConnectExecutor.run_execute` (imperative) runs the agent's CHOSEN command
    (`python3 pipeline.py` / `spark-submit pipeline.py`, per `proposal.command`,
    respecting `allowed_commands`) with NEUTRAL env only (`SPARK_REMOTE` as before,
    `AGENT_INPUT_PATH`, `AGENT_OUTPUT_TABLE`); no session/main code is injected.
  - `live.ConnectExecutor.run_gate` (B2) runs `python3 pipeline.py --analyze-only`
    (agent-owned analyze mode); the harness-created `_analyze_only.py` session is gone.
  - **NEUTRAL completion check** (replaces the load-bearing part of the old
    `saveAsTable`): after an imperative execute returns rc==0, the contract output
    table is read back via the executor's `read_table(output_table)` (limit-0). If
    missing/unreadable → `ExecOutcome(failed=True, completed=False,
    error_class="OUTPUT_TABLE_NOT_FOUND")`; if readable → `completed=True` and the
    existing B1 grading (read table → output oracle → blind grade) runs unchanged.
  - **Offline path (`local.LocalSparkExecutor`)** gets the same agent-owned
    treatment so the offline tests stay representative: the agent's `pipeline.py` is
    run as a PROGRAM (gate: `--analyze-only`; execute: materialize) rather than the
    harness calling `build()` + `saveAsTable`. To keep the in-process executor able
    to read the table back, the program is exec'd as `__main__` IN-PROCESS *after*
    the shared session is created, so the agent's `getOrCreate()` binds to it (the
    remote Connect path gets the same cross-process effect for free via the shared
    cluster catalog). The same neutral read-back completion check applies.
- **The SDP arms (B, B1) are NOT touched** — paradigm branch, spec generation,
  `spark-pipelines run`/`dry-run`, grading, oracles, and arm manifests are all
  unchanged. The change is confined to the imperative paradigm branch.
- **Identical-except-loop preserved:** the imperative program contract is identical
  for A and B2; only the loop's gate differs (B2 adds the `--analyze-only` gate
  before execute), exactly as the manifests already encode. Supersedes the
  harness-operationalized gate framing in I-1 (the analyze-only pass is now the
  agent's own program in `--analyze-only` mode, not a harness-authored session).
- **Files changed:** `harness/runner.py` (`materialize_workspace` imperative branch;
  removed `_imperative_main` / `_analyze_only_harness`), `harness/backends/live.py`
  (`ConnectExecutor` imperative `run_gate` / `run_execute`, new `_imperative_env` /
  `_imperative_execute_argv` / `_imperative_gate_argv` / `_table_readable` helpers,
  completion check), `harness/backends/local.py` (`LocalSparkExecutor` agent-owned
  `_run_agent_program` + neutral completion check). Tests updated:
  `tests/test_workspace_contract.py` (imperative pipeline.py is agent code VERBATIM,
  no `_analyze_only.py`, harness session helpers removed), `tests/test_remote_staging.py`
  (imperative input is env-delivered not baked; ConnectExecutor env/argv/gate
  contract), `tests/test_live_path.py` (fixtures are agent-owned programs;
  OUTPUT_TABLE_NOT_FOUND completion-check test on real Spark).
- The real end-to-end live run on the EKS cluster (imperative arms failing/succeeding
  attributably to the agent's OWN code) remains the orchestrator's live-validation
  step (no key / Connect / AWS creds here).

## D-5 (2026-06-24) — H2 compute measurement: cumulative `/executors.totalDuration` → before/after STAGE diff (executor-seconds **and** CPU-seconds)

- **What changed.** The live H2 compute figure (`ConnectExecutor`) no longer reads
  executor-seconds as a before/after delta of the Spark REST
  `/applications/<id>/executors` summed `totalDuration`. It is now a before/after
  **STAGE diff** against the **driver** Spark UI REST that captures **BOTH**
  executor-seconds and CPU-seconds per run. Supersedes the Connect mechanism of B8
  and the live source in I-5 (the *non-cumulative per-iteration* intent of B8 is
  preserved — it is now realized by the stage diff, not a totalDuration delta).
- **Why the old method was INVALID on this substrate (measured live).** This cluster
  has **one long-lived `Spark Connect server` application** shared by every run. Its
  `/executors` `totalDuration` is **application/driver uptime**, not task compute, so
  it **INCREMENTS WHILE THE CLUSTER IS IDLE**: a before/after delta around a run
  charges H2 for wall-clock the app was simply alive, double-counting and swamping
  the real per-run signal. (It also returned `None` in the live config because
  `spark_rest_url` was `null`.) A cumulative-counter delta is only valid for a
  short-lived, single-run application — not for the shared persistent Connect server.
- **The proven replacement (validated live against the real cluster).** The driver
  UI REST is reachable at a base URL (live runs port-forward `deploy/spark-connect`
  4040 → a local port; the harness reads it from config `spark_rest_url`, e.g.
  `http://localhost:18080`). Then, per run:
  1. `app_id` = `GET {rest}/api/v1/applications` → `[0]['id']` (the long-lived
     `Spark Connect server`; resolved once and **cached**).
  2. **BEFORE** the cell: snapshot the set of `stageId`s from
     `GET {rest}/api/v1/applications/{app_id}/stages`.
  3. Run the cell (existing `_run`).
  4. **AFTER**: `GET /stages` again; **NEW** stages = those whose `stageId` is **not
     in the before-set** **AND** `status == 'COMPLETE'`.
  5. `executor_seconds = sum(stage['executorRunTime'] for new) / 1000.0` (ms→s);
     `cpu_seconds = sum(stage['executorCpuTime'] for new) / 1e9` (ns→s).
- **Why the diff works across a subprocess session (the key subtlety).** The agent's
  Spark job runs in a **subprocess with its own Connect session**, so a job *tag*
  cannot cross sessions. The **stage diff can**, because the harness runs cells
  **SEQUENTIALLY**: the only stages that newly reach `COMPLETE` within the BEFORE→
  AFTER window belong to **this** run. This **sequential-execution assumption** holds
  for the controlled sweep (one cell at a time) and is documented in the code.
- **Live validation.** Against the real cluster, a subprocess
  `spark.range(80_000_000).sum()` produced new stages **[60, 62]** → measured
  **executor_seconds 1.246**, **cpu_seconds 0.878** — correctly attributing the
  subprocess job's compute while excluding the idle app counter. (Lisa will
  re-validate the stage diff against the live cluster after this lands.)
- **Applied UNIFORMLY for BOTH branches.** The stage diff wraps `run_execute` for
  the **SDP** and the **imperative** arm identically (H2 compares them, so they must
  be measured the same way) — there is no per-branch divergence in how compute is
  attributed.
- **We report ALL THREE compute surfaces (no metric dropped).** Each result row now
  carries: (1) `executor_seconds` — the **measured** stage-diff sum, **authoritative
  for $ when present**; (2) `cpu_seconds` — the measured CPU-seconds alongside it; and
  (3) `executor_seconds_wallclock` — the pre-declared `wall_s × instances ×
  busy_fraction` derivation, kept **always** as an independent cross-check (with the
  matching `*_to_correct` variants). So effects can be reported across measurements.
- **`None` is the "unmeasured" sentinel for the MEASURED surfaces — never `0.0`
  (cross-review fix).** `executor_seconds` and `cpu_seconds` are **measured**
  quantities: when no live metric is obtained they are `None`, not `0.0`, so a
  fallback run is reported as *unmeasured* rather than as a misleading *measured
  zero*. Crucially, a **driver-only dry-run gate runs NO executors**, so it has
  nothing to measure and contributes `None` (not `0.0`) to these surfaces; the
  aggregate sums only the non-`None` values, so a **GATED** arm (B, B2) whose execute
  compute falls back aggregates `executor_seconds`/`cpu_seconds` (and the
  `*_to_correct` slices) to `None`, not `0.0` — the bug a B1-only fallback test had
  missed. The `executor_seconds_wallclock` cross-check is the only always-present
  executor-seconds figure and is legitimately `0.0` for a gate. This is what pre-reg
  §8 ("record executor-seconds") is anchored on now that the measured surface is
  nullable: `executor_seconds` is **nullable and NOT required**, while
  `executor_seconds_wallclock` (always present) is **required**. This stance is held
  in lockstep across `harness/schema.py` (`validate_row` + `RESULTS_JSON_SCHEMA`) AND
  the published machine-readable `results_schema.json` (regenerated from
  `RESULTS_JSON_SCHEMA`, `additionalProperties:false`), so an emitted row — including
  an UNMEASURED gated row (`executor_seconds: null` plus `cpu_seconds` /
  `cpu_seconds_to_correct` / `executor_seconds_wallclock` /
  `executor_seconds_wallclock_to_correct`) — passes BOTH validators.
  `tests/test_published_schema.py` is the drift guard: it asserts the published file
  equals `RESULTS_JSON_SCHEMA` and validates real emitted rows (measured AND
  unmeasured-gated) against the published file via `jsonschema`. `$` is priced on the
  measured value when present, else on the wall-clock cross-check (unchanged).
- **Graceful fallback (never crashes a run).** If `spark_rest_url` is unset/`None`
  or **any** REST call raises, the method returns `(None, None)`; `cost.py` then
  derives the conservative wall-clock × slots estimate into the cross-check column,
  the measured `executor_seconds`/`cpu_seconds` stay `None`, and the run proceeds. A
  `None` before-snapshot short-circuits with **no** further REST call (distinguishing
  "no metrics" from a genuinely empty stage set).
- **Stage-retention assumption (cross-review fold-in).** The diff sums only stages
  `/stages` still returns at the AFTER snapshot. The Spark UI retains the most recent
  `spark.ui.retainedStages` (default **1000**) stages, so a run creating more
  completed stages than that bound between BEFORE and AFTER could undercount. The
  study's per-cell pipelines create only a handful of stages each, so this is not a
  practical risk; size `spark.ui.retainedStages` above the largest per-cell stage
  count to be safe. `_stage_ids_snapshot` emits a `RuntimeWarning` if a live snapshot
  nears the default bound, so a misconfigured cluster is noticed rather than silently
  undercounted. Documented in the method docstrings.
- **Local executor (offline path) — left on its existing approach, by design.**
  `LocalSparkExecutor` keeps its before/after `/executors.totalDuration` delta on its
  **own short-lived `local[2]` app** (where a cumulative delta is valid — that app is
  not the shared, idle-incrementing Connect server) and reports `cpu_seconds=None`.
  The stage diff *would* map cleanly to its `:4040` UI, but the **live Connect path
  is what H2 reports**, and leaving local untouched keeps the offline tests
  representative and green. Noted here rather than silently changed.
- **Plumbing only elsewhere — grading/oracle/manifests untouched.** Added
  `ExecOutcome.cpu_seconds` (`harness/backends/base.py`); threaded
  `measured_cpu_seconds` + the always-on wall-clock cross-check through
  `IterationCost`/`RunCost`/`aggregate` (`harness/cost.py`) with `None` as the
  unmeasured sentinel for the measured surfaces; added `cpu_seconds`,
  `cpu_seconds_to_correct`, `executor_seconds_wallclock`,
  `executor_seconds_wallclock_to_correct` to `ResultRow` + the JSON schema and made
  `executor_seconds` nullable / `executor_seconds_wallclock` required
  (`harness/schema.py`); `run_cell` records all three (`harness/runner.py`). No
  hypothesis, arm, metric, grader, oracle, or manifest changed.
- **Files changed:** `harness/backends/live.py` (`ConnectExecutor`: `_app_id`,
  `_stage_ids_snapshot`, `_stage_compute_since`, `run_execute` uses the stage diff;
  removed the invalid `_executor_seconds_snapshot`; retention warning),
  `harness/backends/base.py` (`ExecOutcome.cpu_seconds`), `harness/cost.py`
  (None-sentinel for measured surfaces; gate contributes `None`), `harness/schema.py`,
  `harness/runner.py`. New `tests/test_stage_compute.py` (no network): MOCKS the REST
  JSON and asserts NEW+COMPLETE selection, the executor/CPU-seconds arithmetic, the
  app-id caching, the `None`-fallback when `spark_rest_url` is unset and when a REST
  call raises, that `cpu_seconds` is threaded into the result row, and **on a GATED
  arm (B)** that an unmeasured execute + a dry-run gate aggregates the measured
  surfaces to `None`, not `0.0`.

---

## D-6 (2026-06-24) — base model → `claude-opus-4-8`; sampling params recorded but no longer transmitted; adaptive thinking

- **What changed:** the study base model is bumped from `claude-sonnet-4-6` to
  **`claude-opus-4-8`** in `study.config.json` and all four arm manifests
  (`arms/A.json`, `B.json`, `B1.json`, `B2.json`). The value remains a SINGLE
  shared value that `arm_manifest.assert_identical_except_loop()` validates
  byte-identical across arms — pre-reg §3 (base model identical across arms) still
  holds; only WHICH model changed, not that it varies (it does not).
- **Rationale:** in GO calibration, `claude-sonnet-4-6` could not reliably produce
  valid **OSS** Spark Declarative Pipeline / Spark Connect-compatible code (it
  drifted to Databricks `dlt` and invented APIs even with the prompt framing).
  `claude-opus-4-8` is the stronger base; this is paired with the new in-repo
  `pyspark-sdp` / `spark-safety` SKILL.md packs (now actually loaded — see below)
  that pin the empirically-verified OSS API surface.
- **Sampling — recorded, not transmitted.** `claude-opus-4-8` REJECTS
  `temperature`, `top_p`, `top_k`, and `budget_tokens` with a hard **HTTP 400** and
  uses **adaptive** thinking. So `AnthropicBrain.build_request` now, for the
  opus-4-x family, transmits **NO** sampling knob and sets
  `thinking={"type":"adaptive"}` (plus `output_config={"effort":"high"}`);
  `max_tokens` (~8000, non-streaming) is unchanged. The arm manifests STILL carry
  and validate `temperature=0.0` / `top_p=1.0` as controlled-variable **provenance**
  (`SAMPLING_CONTROLLED`, `sampling_kwargs`, `assert_identical_except_loop`) — this
  is a transmit-side de-duplication forced by the model's API surface, NOT a change
  to any controlled value. This supersedes D-2's "send only temperature" decision
  (sonnet's both-knobs-400) for the opus base: opus rejects even one knob, so we
  send none. The legacy sonnet temperature-only path is retained behind the family
  check for any non-opus base.
- **Integrity / tests.** `tests/test_live_sampling.py` now asserts (no network) that
  for every arm the request carries NONE of {temperature, top_p, top_k,
  budget_tokens} and sets adaptive thinking, and a separate case guards the legacy
  sonnet temperature-only shape. The identical-except-loop and sampling-identical
  checks are unchanged and still green.
- **Skill loading (root-cause fix, same cycle).** `AnthropicBrain` now defaults its
  skills dir to the in-repo `experiments/safe_agent_study/skills/` (resolved
  RELATIVE to the study dir, never a hardcoded absolute path) when `OMNIGENT_SKILLS`
  is unset, so `_load_skill('pyspark-sdp')` finds the new SKILL.md and
  `_system_prompt()` injects `=== LINKED SKILL: pyspark-sdp ===` for arms B/B1 and
  `spark-safety` for arm B. Previously the dir was unset → no SKILL.md found → SDP
  agents hallucinated Databricks `dlt` → zero completions. `tests/test_skill_loading.py`
  asserts the injection for B (both packs) and B1 (pyspark-sdp only), and that the
  loaded pack carries the OSS `from pyspark import pipelines as dp` API and warns
  against Databricks DLT. This changes neither a hypothesis, an arm's loop
  signature, a metric, nor the grader/oracle.
- **Pending (orchestrator):** (a) probe that `claude-opus-4-8` resolves against the
  real key; (b) a live dry-run of a sample SDP agent output to confirm the skill
  yields valid OSS SDP code. Both require the key/cluster, unavailable at build time.

---

## D-7 (2026-06-24) — Part-1 substrate = LOCAL (imperative on classic local[*] Spark; SDP on a local single-node Spark Connect server)

- **What changed:** a new runner backend, `--backend local`, added ALONGSIDE the
  existing `replay` and `live` (the remote EKS `live` path is UNTOUCHED). It builds a
  `make_brain`/`make_executor` pair like `make_live_factories` but routes the
  EXECUTOR **per paradigm** so each arm runs on its HOME local engine:
  - **imperative arms (A, B2)** → the existing in-process classic `local[*]`
    `LocalSparkExecutor`. Classic Spark works there, so the imperative agents can
    actually complete.
  - **SDP arms (B, B1)** → a `LocalConnectExecutor` (a thin subclass of the live
    `ConnectExecutor`) pointed at a LOCAL single-node Spark Connect server
    (`sc://localhost:<port>/;user_id=alice`), with that server's driver UI REST
    (`http://localhost:<ui_port>`) for the H2 stage-diff. SDP fundamentally requires
    a Connect server even locally (a bare in-process session fails the SDP CLI with
    `ONLY_SUPPORTED_WITH_SPARK_CONNECT`) — verified.
  - **BRAIN:** the SAME `AnthropicBrain` (`claude-opus-4-8`) as `live`, UNCHANGED
    (needs `ANTHROPIC_API_KEY`). Same prompt/skill/sampling assembly, so the only
    thing that varies per arm is still the loop.
- **Rationale:** on the remote Spark Connect substrate, imperative arms can't run at
  all — that is **Part-2's** separate finding (Connect-incompatibility confound). For
  **Part 1** (H1 silent-defect rate, H2 compute, H3 ablation) we ISOLATE the
  imperative-vs-SDP PARADIGM by running each on its home local engine, off the remote
  EKS cluster, where BOTH paradigms can complete. This removes remote-cluster
  variability and the Connect-incompatibility confound from the paradigm comparison.
  The user approved this shape.
- **Local Connect server lifecycle** (`harness/backends/local_connect.py:LocalConnectServer`):
  at `--backend local` startup the runner brings up ONE single-node Connect server and
  stops it in a `finally` at the end. Proven-good launch (validated live; mirrors the
  `gitops_demo/local_spark_connect.sh` reference): resolve `SPARK_HOME` from the
  installed pyspark (`os.path.dirname(pyspark.__file__)`), then
  `"$SPARK_HOME/bin/spark-submit" --class
  org.apache.spark.sql.connect.service.SparkConnectServer --conf
  spark.connect.grpc.binding.port=<port> --conf spark.ui.port=<ui_port> --conf
  spark.sql.warehouse.dir=file://<localwh> --conf
  spark.sql.catalogImplementation=in-memory --conf
  spark.sql.artifact.isolation.enabled=false spark-internal` (background; block until
  the gRPC port accepts TCP). `spark.ui.port` is pinned EXPLICITLY so the SDP H2 REST
  target is deterministic and never collides with the imperative `LocalSparkExecutor`
  in-process UI (the local backend puts that on `<ui_port>+1`). Before any SDP
  dry-run/run, the server runs `CREATE SCHEMA IF NOT EXISTS spark_catalog.default`
  over a Connect session (mirrors `gitops_demo/ensure_schema.py`); without it the SDP
  CLI fails with `[SCHEMA_NOT_FOUND]`, a FALSE failure unrelated to any pipeline
  defect.
- **Warehouse / storage — local `file://` for BOTH engines.** The SDP spec `storage:`
  base lands under the local warehouse per task/arm/seed (`_sdp_storage_for`, via the
  `cfg.warehouse_uri = file://<localwh>` the local backend sets). The imperative
  `LocalSparkExecutor` writes local managed tables. The dataset generator already
  writes a local NDJSON; the imperative agent reads that local path via
  `AGENT_INPUT_PATH`, while the SDP `LocalConnectExecutor.stage_input` hands the agent
  a `file://<abspath>` of the SAME NDJSON (no S3 round-trip — the single-node server
  shares the FS; this is the local analogue of the remote D-3 staging). VERIFIED:
  opus's SDP reading `file:///<path>` dry-runs COMPLETED locally.
- **H2 on the split substrate (comparability caveat).** Imperative arms use
  `LocalSparkExecutor`'s existing before/after executor-seconds delta; SDP arms use
  the D-5 stage-diff against the local Connect driver UI. These are NOT directly
  comparable across the classic-vs-Connect engines, so we record BOTH plus the
  always-present wall-clock cross-check (`executor_seconds_wallclock`) and document it
  here: **Part-1's clean cross-paradigm comparison is H1 / the silent-defect rate;
  H2 compute is within-substrate, with wall-clock + iterations as the common
  cross-substrate proxy.**
- **Untouched:** generator/dataset, grading/oracle/output-oracle, arms, manifests, the
  cost model, and the remote `live` path are all UNCHANGED. The oracle reads the
  output table back the same way (`LocalSparkExecutor.read_table` for imperative,
  `ConnectExecutor.read_table` for SDP), feeding the SAME `grade_run`.
- **Integrity / tests.** `tests/test_local_backend.py` (no LLM/Spark/network/JVM —
  the Connect server JVM, the port wait, and the `CREATE SCHEMA` session are mocked)
  asserts: `make_local_factories` routes imperative arms (A, B2) to
  `LocalSparkExecutor` and SDP arms (B, B1) to `LocalConnectExecutor`; the imperative
  engine gets its own UI port (Connect UI + 1); `submit_argv()` is the proven-good
  launch; `start()` spawns the JVM, waits for the port, and ensures the schema, and
  `stop()` tears it down; `stage_input` returns a `file://` path; and
  `runner.main(--backend local)` brings the server up, points the config at it, and
  always stops it. The full offline suite stays green; `py_compile` is clean.
- **Pending (orchestrator):** the live Part-1 calibration run (`--backend local`) needs
  the API key + a JDK, unavailable at build time — only the routing + lifecycle wiring
  is unit-tested here.

### D-7 follow-up 4 (2026-06-26) — LOCAL H2 primary = uniform wall-clock executor-seconds proxy (analysis-side, cross-arm comparability)

- **Threat model.** The registered H2 primary, the MEASURED
  `executor_seconds_to_correct`, is **only** cross-arm comparable on the
  **remote/Connect** substrate (every arm measured by the SAME stage-diff,
  `harness/backends/live.py`). On the **LOCAL** substrate it is **not** comparable
  and pairing on it is invalid for two independent reasons: (1) the imperative
  `LocalSparkExecutor` takes its before-snapshot **before** the `SparkSession`
  exists (`harness/backends/local.py` `run_execute` → `_executor_seconds_snapshot`),
  so the measured value **collapses to `None`** and the analysis silently dropped
  **every** local H2 pair; and (2) even when non-`None`, imperative measures via the
  Spark UI `/executors` `totalDuration` delta while local SDP measures via the
  Connect **stage-diff** — two **different mechanisms** that must not be compared.
- **Decision (pre-reg addendum).** **LOCAL H2 primary = the uniformly-computed
  wall-clock executor-seconds proxy** `executor_seconds_wallclock_to_correct`
  (`wall_s × instances × busy_fraction`, the SAME formula for every arm,
  `harness/cost.py`), chosen for cross-arm comparability. **Remote/Connect H2
  primary = the measured cluster executor-seconds** (`executor_seconds_to_correct`).
  Rationale: do **not** compare two different local measurement backends.
- **Where.** `analysis/analyze.py`: `resolve_h2_metric(backend, …)` selects the field
  EXPLICITLY, keyed on the run's backend (threaded from the env sidecar's `backend`).
  There is **no data-driven fallback**: an absent/`None` or unrecognized backend
  **raises `H2MetricSelectionError`** (the CLI exits non-zero with an actionable
  message) rather than guessing the metric from "which field happens to have pairs" —
  guessing could pick the non-comparable measured field for a local run, or the proxy
  for a remote run, emitting a plausible-but-wrong number. The only no-env path is the
  EXPLICIT opt-in `--assume-backend {local,live}`. The choice + rationale are recorded
  in the report under `meta.h2_metric` and each gated arm's `metric_field`, and
  rendered in the H2 markdown header. `_h2_pairs` now takes the metric field as an
  explicit argument instead of hard-coding the measured one.
- **Out of scope (tracked).** The imperative snapshot-ordering bug (1) is **not**
  fixed here; it feeds only the non-comparable measured secondary. This change is
  analysis/measurement-selection only — arms, grader, generator, and cost model are
  untouched. Covered by `tests/test_h2_local_metric.py`.

### D-7 follow-up (2026-06-24) — two robustness fixes found on the first live Part-1 run

The first live Part-1 calibration crashed on arm A iteration 0. Root cause + fixes
(no hypothesis/arm/metric/grader/routing change — robustness only):

- **Empty agent proposal no longer crashes the run.** When the brain returns no
  fenced ```python block, `proposal.code` is empty and `materialize_workspace` writes
  no `pipeline.py` (imperative) / `spark-pipeline.yml` (SDP); the executor then opened
  a non-existent file and the `FileNotFoundError` propagated out of `run_episode` ->
  `run_cell` -> `main` and KILLED the whole sweep. Now `run_episode` GUARDS the no-code
  case BEFORE materialize/gate/execute: it records a graceful FAILED iteration
  (`error_class="NO_CODE_PRODUCED"`, a zero-cost non-intercept via
  `cost.no_code_iteration_cost`), feeds back *"No fenced ```python code block found in
  your response — emit your module inside a single ```python ... ``` block."*, and
  CONTINUES so the agent retries. The executors are ALSO defensively guarded
  (`LocalSparkExecutor._run_agent_program`; `ConnectExecutor._missing_agent_artifact`
  for both the SDP spec and the imperative program) to return a graceful
  `NO_CODE_PRODUCED` `GateOutcome`/`ExecOutcome` rather than raise. The run survives and
  the cell ends as a normal non-completion.
- **`max_tokens` 8000 → 16000 (opus propose), no streaming.** The empty proposals were
  truncations: the opus path uses `thinking={type:adaptive}` + `output_config.effort=high`,
  which can consume an 8000 budget on reasoning and stop at `max_tokens` before the
  code module is emitted. `AnthropicBrain` now defaults `max_tokens=16000` — headroom
  for thinking + a ~6K-token module, kept **≤ 16000** so the simple non-streaming
  `messages.create` path stays valid (no streaming threshold tripped). `resp.stop_reason`
  is now captured onto `Proposal.stop_reason` and surfaced in the per-iteration record
  (and logged when the code block is empty), so future truncation is visible rather
  than silent. Tests: `test_local_backend.py` covers the empty-proposal graceful
  iteration, the executor missing-file guards, the raised `max_tokens`, and the
  `stop_reason` plumbing; full offline suite green.

### D-7 follow-up 2 (2026-06-24) — process-global Connect-session crash (Option C: no parent Connect session)

The next live Part-1 run died with `CONNECT_URL_NOT_SET` on the imperative arm.
Root cause: pyspark's classic-vs-Connect SparkSession mode is process-GLOBAL —
once the long-lived runner process creates an in-process Connect session, the
imperative `LocalSparkExecutor`'s classic `getOrCreate()` fails. Two places created
one in the parent: (1) `LocalConnectServer.ensure_schema` →
`SparkSession.builder.remote(...).getOrCreate()`; (2) SDP grading —
`runner._build_profile` evaluated `executor.spark`, which built a parent Connect
session.

**Fix (Option C — the Part-1 runner process NEVER creates a Connect session):**
- **`harness/connect_helper.py`** (new): a short-lived SUBPROCESS that creates the
  Connect session in ITS OWN process and exits. Two subcommands: `ensure-schema`
  (`CREATE SCHEMA IF NOT EXISTS`) and `output-profile` (reads the agent's
  materialized table back, runs `output_oracles.build_output_profile`, serialises the
  OutputProfile fields to a JSON result file). Run as
  `python3 -m harness.connect_helper … ` with cwd = the study dir.
- **`LocalConnectServer.ensure_schema`** now shells to the helper (`_run_connect_helper`)
  instead of an in-process `builder.remote(...)`.
- **`LocalConnectExecutor.build_output_profile_subprocess(task_spec, dataset, out_table)`**
  shells to the helper and reconstructs the `OutputProfile`; a subprocess failure
  records `extra["output_profile_subprocess_error"]` (the run COMPLETED but the output
  couldn't be read back) rather than crashing the sweep. `runner._build_profile` calls
  this FIRST (before evaluating `executor.spark`) for any executor that provides it, so
  the parent never enters Connect mode. The imperative classic in-process path
  (`LocalSparkExecutor.spark`) and the remote-`live` `ConnectExecutor` path are
  UNCHANGED.
- **Guard (no silent regression):** `LocalConnectExecutor.spark` and `read_table` RAISE
  a clear `RuntimeError` in the parent pointing callers to the subprocess helper. The
  invariant: under `--backend local`, the runner process may create CLASSIC Spark but
  NEVER a Connect session.
- **Preserved:** H2 (imperative executor-seconds; SDP stage-diff via the driver UI
  REST — HTTP, not a pyspark session, so it stays in the parent), the SDP gate/execute
  (already `pyspark/pipelines/cli.py` subprocesses), SEQUENTIAL cells, grading/oracle
  semantics, per-paradigm routing, and the remote live path.
- **Tests** (`test_local_backend.py`, no in-process Connect session in pytest): assert
  ensure_schema and SDP grading go through `_run_connect_helper`; assert
  `SparkSession.Builder.remote` is never called in-process; assert `_build_profile`
  routes SDP to the subprocess builder and never touches `executor.spark`; assert the
  `spark`/`read_table` guard raises in the parent; assert the imperative classic
  session uses `builder.master("local[2]")`, never `.remote(...)`. Full offline suite
  green; `py_compile` clean.

### D-7 follow-up 3 (2026-06-24) — bound agent execution with a hard timeout (a hung/streaming agent can't wedge the run)

The next live Part-1 run WEDGED: an imperative opus agent wrote a Structured
Streaming pipeline (`foreachBatch` + `awaitTermination`) which never terminates on a
bounded batch input, and `LocalSparkExecutor._run_agent_program` exec'd it IN-PROCESS
with NO timeout — so the whole run hung (14+ min, 0 % CPU, arm A iter 0). The
isolation / empty-proposal / budget fixes all held. Fix: every agent-code execution
is now HARD-BOUNDED; a timeout becomes a graceful `EXECUTION_TIMEOUT` failed iteration
and the loop continues.

- **In-process imperative exec — WATCHDOG THREAD (chosen over killable-subprocess).**
  `LocalSparkExecutor._run_agent_program` now runs the agent's `pipeline.py` in a
  daemon watchdog thread and waits up to `exec_timeout_s` (default
  `AGENT_EXEC_TIMEOUT_S = 150`). On timeout it stops every ACTIVE streaming query
  (`spark.streams.active → stop()`, releasing a hung `awaitTermination`) AND cancels
  all running jobs (`sparkContext.cancelAllJobs()`, releasing a long batch action) so
  the worker unwinds, force-restores the global process state the wedged worker still
  holds (stdout/stderr/argv/env, guarded by an `is buf` check so a late-waking worker
  can't clobber a restored stream), and returns `[EXECUTION_TIMEOUT]`.
  **Why watchdog, not subprocess:** the imperative oracle reads the agent's MANAGED
  table back through THIS in-process session (B1). A killable subprocess would write
  into an embedded-Derby metastore that the long-lived runner's single global classic
  session cannot share without cross-cell lock contention (embedded Derby = one JVM
  connection), forcing a far more invasive read-back rewrite and breaking the proven
  in-process grading path (`test_live_path`). The watchdog keeps that read-back intact
  and still bounds execution. Accepted residual: a pathological pure-Python infinite
  loop that never touches Spark would leak the daemon thread — but the RUN still
  proceeds, and the realistic hang modes (streaming `awaitTermination`, long Spark
  actions) are both broken.
- **Subprocess command path — process-group kill.** `ConnectExecutor._run` (the SDP
  CLI gate/run and the imperative-over-Connect command, used by the local SDP executor
  AND the remote live path) now runs the child in its OWN process group
  (`start_new_session=True`) and `communicate(timeout=cmd_timeout_s)` (default
  `CONNECT_CMD_TIMEOUT_S = 600` — headroom for cluster scheduling + per-invocation JVM
  startup). On timeout it `os.killpg(SIGKILL)`s the whole group (CLI/launcher + JVM,
  nothing leaks) and returns rc=124 / `[EXECUTION_TIMEOUT]`. `_run_connect_helper`
  (ensure-schema / output-profile controller subprocesses) is likewise bounded.
- **Feedback (so the agent can adapt):** `runner._failure_feedback` appends, for
  `EXECUTION_TIMEOUT`, *"Execution exceeded the time limit and was terminated. Your job
  must run to COMPLETION on the provided bounded input — do NOT start an unbounded
  streaming query or call awaitTermination(); use a finite batch read/transform/write."*
- **Preserved:** grading/oracle, H2 (the timed-out iteration is a normal failed
  iteration in the cost model), per-paradigm routing, SEQUENTIAL cells, and the remote
  live path (it gains the same command timeout). The normal completing-program
  in-process read-back is unchanged and proven on REAL Spark by `test_live_path` (which
  now runs through the watchdog wrapper).
- **Tests** (`test_local_backend.py`, short timeout + a sleeping fixture, mocked
  session — no real JVM/hang in pytest): a hanging in-process agent → `EXECUTION_TIMEOUT`
  for both gate and execute, fast; a fast program still completes + reads back through
  the watchdog; `run_episode` survives a hanging agent (loop proceeds to
  `max_iterations`, the timeout guidance is fed back); `ConnectExecutor._run` and
  `run_execute` kill a sleeping command and return `EXECUTION_TIMEOUT`. Full offline
  suite green; `py_compile` clean.
- **Cross-review fixes (2 blockers).** (1) **Zero-cost timeout accounting.** A
  hard-killed iteration consumed no attributable compute, so it must NOT be priced via
  `execute_iteration_cost`'s wall-clock fallback (which would charge ~`timeout` seconds
  of fake executor-seconds + non-zero $). `run_episode` now routes any iteration whose
  `error_class == EXECUTION_TIMEOUT` (execute OR gate) through new
  `cost.timeout_iteration_cost()`: `failed=True`, `intercepted_at_dry_run=False`,
  `usd=0.0`, `executor_seconds=None`, `executor_seconds_wallclock=0.0` (honest `wall_s`
  retained for `total_wall_s`). Normal fast-failed executes keep the existing path. (2)
  **Late-worker race guard.** The watchdog restored `sys.argv`/`os.environ`
  unconditionally, so a late-waking daemon worker could reset the NEXT cell's
  argv/env. Each `_run_agent_program` call now installs a per-invocation generation
  token (`self._exec_token`); the worker's `finally` restore no-ops unless the token is
  still current, and the main thread INVALIDATES it on timeout before force-restoring —
  so a stale worker clobbers nothing (argv, env, and streams guarded uniformly). Tests:
  an `EXECUTION_TIMEOUT` iteration contributes $0 / `None` executor-seconds / 0 wallclock
  and is not a dry-run intercept (execute + gate); `_interrupt_spark` stops each active
  streaming query and cancels jobs (non-empty `streams.active`); a late worker does not
  clobber the next cell's argv/env.

---

## D-8 (2026-06-24) — Classic LOCAL executor uses an in-memory Spark catalog

- **What changed:** the classic in-process `LocalSparkExecutor` session now sets
  `spark.sql.catalogImplementation=in-memory` while preserving its per-run
  `spark.sql.warehouse.dir`. This mirrors the already-fast local Spark Connect
  server setting and prevents the imperative `local[*]` session from initializing a
  Hive/Derby metastore during Part-1 LOCAL calibration.
- **Rationale:** each episode gets a fresh executor/session and needs table
  visibility only within that episode. The imperative agent writes its contract
  table(s) with `saveAsTable`, then the neutral completion check and output oracle
  read them back through the same `LocalSparkExecutor.read_table` session. Spark's
  in-memory catalog supports that intra-session pattern, so throughput improves
  without changing the D1-D9 data-correctness defects under measurement.
- **Untouched:** SDP `LocalConnectExecutor`, the remote `ConnectExecutor`, arms,
  manifests, prompts/skills, generator, grading/oracle, and output contracts are
  unchanged. No cross-episode or persistent Hive-metastore dependency is introduced
  or required.
- **Integrity / tests:** `tests/test_local_backend.py` now asserts the classic
  local Spark builder carries `spark.sql.catalogImplementation=in-memory` and keeps
  the configured warehouse dir. The existing real-Spark live-path test still covers
  the imperative `orders_silver_gold` saveAsTable/read-back/oracle path end-to-end.

---

## D-8 (2026-06-24) — Classic LOCAL executor uses an in-memory Spark catalog

- **What changed:** the classic in-process `LocalSparkExecutor` session now sets
  `spark.sql.catalogImplementation=in-memory` while preserving its per-run
  `spark.sql.warehouse.dir`. This mirrors the already-fast local Spark Connect
  server setting and prevents the imperative `local[*]` session from initializing a
  Hive/Derby metastore during Part-1 LOCAL calibration.
- **Rationale:** each episode gets a fresh executor/session and needs table
  visibility only within that episode. The imperative agent writes its contract
  table(s) with `saveAsTable`, then the neutral completion check and output oracle
  read them back through the same `LocalSparkExecutor.read_table` session. Spark's
  in-memory catalog supports that intra-session pattern, so throughput improves
  without changing the D1-D9 data-correctness defects under measurement.
- **Untouched:** SDP `LocalConnectExecutor`, the remote `ConnectExecutor`, arms,
  manifests, prompts/skills, generator, grading/oracle, and output contracts are
  unchanged. No cross-episode or persistent Hive-metastore dependency is introduced
  or required.
- **Integrity / tests:** `tests/test_local_backend.py` now asserts the classic
  local Spark builder carries `spark.sql.catalogImplementation=in-memory` and keeps
  the configured warehouse dir. The existing real-Spark live-path test still covers
  the imperative `orders_silver_gold` saveAsTable/read-back/oracle path end-to-end.

---

---

## D-9 (2026-06-24) — Part-1 LOCAL imperative arms use path-based parquet output (catalog-free / Hive-free)

- **What changed:** Part-1 LOCAL imperative arms **A** and **B2** no longer use a
  Spark catalog table for the final contract output. The harness now provides
  `AGENT_OUTPUT_PATH`; the agent-owned `pipeline.py` must write the final GOLD
  DataFrame with parquet path I/O (for example,
  `df.write.mode("overwrite").parquet(os.environ["AGENT_OUTPUT_PATH"])`). The
  local imperative completion check and output-profile oracle read that same parquet
  path back with `spark.read.parquet(...)`. `AGENT_OUTPUT_TABLE` is deliberately not
  set on this local imperative path, so stale `saveAsTable(os.environ["AGENT_OUTPUT_TABLE"])`
  code fails fast instead of materializing a managed table.
- **Rationale:** live Part-1 LOCAL logs showed
  `WARN SparkSession: Using an existing Spark session; only runtime SQL configurations
  will take effect`, followed by Hive `ObjectStore` / embedded Derby startup. The
  session already existed before `LocalSparkExecutor` could apply
  `spark.sql.catalogImplementation=in-memory`; that setting is static, so the attempted
  in-memory catalog override was ignored. The ObjectStore/Derby work was then triggered
  by the imperative program's final `saveAsTable(...)` plus the harness's table
  read-back (`read_table(...)`). Catalog persistence is required for the declarative
  SDP paradigm, but it is not a requirement of the imperative PySpark paradigm.
- **Methodology decision:** approved by the study owner. The logical task and
  required transformations are unchanged; only the final storage mechanism for the
  imperative LOCAL arms changes. The decision is symmetric across **A** and **B2**
  (B2's `--analyze-only` gate uses the same env contract and does not materialize).
  D1-D9 measurement is unaffected for the graded GOLD output because the same blind
  oracle reads the same logical dataset from a path instead of a catalog table.
- **Untouched:** SDP arms **B/B1** keep the local Spark Connect in-memory catalog and
  SDP storage/catalog semantics. The remote `ConnectExecutor` table-backed path is
  unchanged.
- **Integrity / tests:** `tests/test_live_path.py` now exercises the real-Spark
  imperative path-based contract and includes a guard that a completed local
  imperative run creates the parquet output path but no `metastore_db`, no default
  `spark-warehouse`, and no warehouse table directory for the output.

## D-10 (2026-06-25) — SDP doubled-path root-cause fix + unified HARNESS-FAULT policy (quarantine + circuit breaker)

- **What changed (Part A — root cause).** The SDP arms were DEGENERATE: the harness
  invoked the SDP CLI with a RELATIVE `--spec` (`os.path.join(workspace, 'spark-pipeline.yml')`)
  while ALSO setting the subprocess cwd to that same workspace, so the CLI resolved the
  spec against its cwd and got the DOUBLED path `<workspace>/<workspace>/spark-pipeline.yml`
  → `PIPELINE_SPEC_FILE_DOES_NOT_EXIST`, and EVERY SDP iteration failed before the agent's
  code ran. Fixed by (1) canonicalizing `args.work_dir` to an ABSOLUTE path once in
  `main()` (so every `state.workspace` is absolute); (2) building `--spec` with
  `os.path.abspath` and passing an explicit ABSOLUTE cwd to `_run` in BOTH `run_gate` and
  `run_execute`, plus a pre-invoke `os.path.isfile(spec)` guard; (3) a post-materialization
  existence check on the required SDP files. (2)+(3) raise a HARNESS fault (not an agent
  failure) if the instrument is still broken. Validated end to end: a real SDP pipeline now
  executes against a local Spark Connect server and materializes its output
  (`tests/test_sdp_spec_path.py`, opt-in `SSA_RUN_SDP_INTEGRATION=1`).

- **What changed (Part B — methodology, the part that touches analysis).** A single notion
  of HARNESS FAULT (`schema.HARNESS_FAULT_EXIT_CLASSES`) now unifies the new SDP/infra
  faults with #31's propose-call faults (PROPOSE_TIMEOUT / PROPOSE_API_ERROR /
  PROPOSE_RATE_LIMIT / HARNESS_EXCEPTION). A harness fault is an INSTRUMENT failure and is
  NEVER scored as an agent failure and NEVER accrues toward `max_iterations`. Policy
  (`harness/harness_faults.py`): RETRY ONCE (5 s) → on a second fault QUARANTINE the cell
  (`exit_class=HARNESS_ERROR`, underlying reason preserved in `harness_fault_reason`) and
  CONTINUE → CIRCUIT BREAKER aborts the whole run LOUDLY before the next cell if any of
  three NAMED, tunable thresholds is breached: global quarantined faults `> 3` (~1.5 % of
  N≈220), per-arm `> 1`, per-complexity-bin `> 1`. The breaker covers BOTH fault paths
  together. After any fault a per-cell hard reset (`hard_reset_after_fault`) reaps zombie
  Spark/Connect launcher children (process-group teardown technique; `LocalConnectServer`
  teardown hardened to a process-group SIGTERM→SIGKILL) so cascades cannot trip the breaker
  spuriously.

- **Methodology decision (analysis impact).** `analysis/analyze.py` EXCLUDES quarantined
  HARNESS_ERROR cells from every H1–H4 computation (they are instrument failures, not agent
  outcomes) and emits a SEPARATE QUARANTINE REPORT (`--quarantine-out`; also rendered in the
  markdown appendix and written by the runner as `<results>.quarantine.json`) listing each
  excluded cell `(task, seed, arm, exit_class, reason)` for the paper's excluded-data
  appendix. The complexity bin (the breaker's per-bin stratum) is derived from a task's
  in-scope defect count with named bounds (no corpus carries an explicit complexity field
  yet; an explicit field is honored if added).

- **Provenance.** Per-iteration LLM token usage is now persisted into `per_iteration[i]["tokens"]`,
  the transcript, and the `input_tokens`/`output_tokens` row fields, so per-cell cost and
  the harness-fault validity audit are reconstructable from the artifacts alone.

- **Supersedes / folds in.** Supersedes PR #31 (this branch is built on it) and folds in
  PR #25 / D-9 (the owner-approved path-based imperative output contract = Part A.4).

## Implementation interpretations (within the pre-reg; logged for traceability)

### I-1 — the "dry-run gate" for the imperative arms (B2) is a paradigm-agnostic structural pass
Pre-reg §3 Arm B2 = "imperative PySpark + dry-run gate only (no SDP)". The SDP
`dry-run` is SDP-specific, so for imperative arms the gate is operationalized as
an **analyze-only structural pass** (force plan analysis / column + relation
resolution **without** materialization → no executors, ~8 s, $0). This is the
faithful imperative analogue of the SDP gate and preserves the ablation's intent
("is it the gate alone?"). The cost model treats both gate forms identically
($0, zero executor-seconds).

### I-2 — base model id
Pre-reg fixes that the base model must be **identical across arms**, not which
model. The lock value is **`claude-opus-4-8`** (bumped from `claude-sonnet-4-6` —
see **D-6** for the rationale and the API-shape consequences). It is a single
shared value in `study.config.json` / every arm manifest, and
`arm_manifest.assert_identical_except_loop()` refuses to run if the arms ever
disagree on it. Swap it in one place before the sweep if desired.

### I-3 — semantic grading reads the materialized OUTPUT; state defects are n/a offline
The grader counts a semantic defect (D2/D6/D7/D8) as **silent** iff the
COMPLETED output still exhibits the corruption (residual rows > 0). State defects
(D3/D9) are recorded `n/a` — they are runtime/cluster-scale only, exactly the
honest structural-vs-semantic/state split the pre-reg (§4) predicts, not a
grading gap being hidden.

### I-4 — GLMM dependency
The PRIMARY inference (mixed-effects logistic GLMM) needs `statsmodels`+`pandas`
(`analysis/requirements.txt`). When absent, `analyze.py` still emits the per-arm
rates, bootstrap CIs, **exact McNemar** paired tests, Holm correction, and the H2
analysis, and prints an explicit install hint for the GLMM. McNemar is labelled
as the always-available paired fallback, not a substitute for the declared GLMM.

### I-6 — per-task prompts (task-definition completion, not an instrument change)
The corpus has 15 distinct tasks, so each needs its own engineering brief. The
brief lives in `TASKS.lock.json` per task (`prompt`) and the runner composes the
final prompt as **shared contract preamble (`prompts/task_prompt.md`) + per-task
brief**. Both pieces are arm-independent, so pre-reg §3 ("same task prompt across
arms") holds — `tests/test_corpus.py::test_per_task_prompt_is_arm_independent`
asserts every arm composes a byte-identical prompt for a given task. This
completes the task-definition layer (the prior single global prompt under-specified
multi-task runs); it does NOT touch the arms, metrics, grader, or stats. The
replay/offline path ignores prompts, so the offline validation is unaffected.

### I-5 — executor-seconds source (SUPERSEDED by D-5)
Originally live runs read executor-seconds from the Spark REST API
(`/applications/<id>/executors` `totalDuration`). **That was found INVALID on the
shared long-lived Spark Connect application (it measures idle app uptime, not task
compute) and is replaced by the before/after STAGE diff in D-5**, which captures
both executor-seconds (`executorRunTime`) and CPU-seconds (`executorCpuTime`). When
live metrics are unavailable the cost model still falls back to the same declared,
conservative estimator (`wall_s × instances`), now carried on every row as an
explicit `executor_seconds_wallclock` cross-check. The dry-run gate is always 0
executor-seconds / $0 by construction (driver-only).

---

## D-CORPUS-V3 — pre-data corpus revision (corpus v2 15-task → v3 22-task)

**Status: legitimate pre-data deviation (no data collected; logged per the
pre-reg preamble).** Owner-approved 2026-06-24 (see
`CORPUS_UPGRADE_SPEC.md`). This revision re-freezes `TASKS.lock.json`
(`2.0.0-corpus15` → `3.0.0-corpus22`). It changes the *instrument's task corpus*,
not any hypothesis, arm, metric, grader contract, or analysis plan — and it adds
the pre-registered moderation hypothesis **H4** (does the SDP advantage scale with
task complexity?), narrated as Part 1.5.

What changed and why:

- **Ticket-style prompts (§1).** Every task `prompt` was rewritten from how-to
  instructions into a stakeholder ticket: the business symptom implies the
  requirement, the deterministic output contract is pinned at the bottom, and no
  fix / Spark API / defect-name is named. Rationale: spoon-feeding the recipe
  hands *both* arms the answer and suppresses the silent-defect signal the study
  measures. Enforced by `tests/test_prompt_no_leak.py`. The shared preamble was
  reframed the same way and the leaky engineering `title` is no longer injected
  into the agent-facing prompt.
- **A-priori complexity rubric (§2/§3).** An 8-axis rubric (`harness/complexity.py`)
  gives every task a `complexity_score` + `complexity_axes`, the pre-registered
  continuous H4 covariate. Aggressive gradient: **Low 7 / Med 8 / High 7**.
- **+7 tasks; elevations; p5 replacement (§5/§6/§7).** Added idempotent
  MERGE/upsert, stream-stream temporal join, point-in-time/as-of SCD2 join, CDC
  tombstones, a UDF email-classifier, and two high-complexity
  platform-in-miniature tasks (HC-1 FX trade ledger, HC-2 session funnel).
  p2_cdc / p10_scd2 / p13_cdc_windowed / p14_fx_settlement were elevated into the
  High bin via cross-stage reconciliation + idempotent MERGE (A6/A7 levers).
  p5_mart was replaced by a self-contained mart (no cross-pipeline dependency).
- **Generator enrichment + new substrates (§8).** Daily-changing FX with a wider
  exotic-currency basket (`infra/fx.py`, one source of truth); null-payload CDC
  tombstones; an opt-in `--v3` orders append (nested category structs, line_items
  array-of-structs amount drift, 502-HTML junk) gated so the v2 reference numbers
  are byte-for-byte preserved by default; new generators for emails, an FX-rate
  change feed, trades, and clickstream.
- **Quantifiers (§9).** Deterministic ground truth for every new/changed defect:
  nested-array D8 (`quantify.d8_nested`), daily-FX `pay_d7`/`pay_d8`, the UDF
  misclassification counts (`quantify_udf`), and the HC cross-stage invariants
  (`quantify_hc`: position reconciliation, SCD2 no-overlap, session funnel, event
  accounting, CDC current-state, settlement reconciliation).

Reference-number movement (pre-data, expected): the orders battery is **unchanged**
(D2=246 / D6=0 / D7=275 / D8=250 / $49,778.06 at seed 42, default generator). The
payments numbers move because v3 widens the basket and applies a per-day FX rate:
**pay_d7 935 → 1066**, **pay_d8 1139 / $371,010.60 → 1136 / $276,498.91**. CDC
event totals are preserved (263 events / 77 ambiguous / 13 deleted at seed 7); v3
only nulls the delete payloads.

Graded vs narrated defects: the gradable taxonomy stays **D1, D2, D4–D8**; **D3
(unwatermarked dedup) and D9 (unbounded state) remain narrated opportunities, not
offline-graded** (they carry no oracle). HC / UDF tasks declare `graded_by:
"invariants"` and are scored by `quantify_hc` / `quantify_udf`; the standard
output oracle is unchanged for the other tasks.

Multi-input staging (IMPLEMENTED): the runner now stages EVERY declared input of a
task, not just the primary. Each task's `aux_inputs` generators (`gen_fx_rates_cdc`,
`gen_customers_cdc`) are generated per-seed exactly like the primary and staged the
SAME way (local `file://` for the imperative/SDP-local executors, `createDataFrame
-> write.text` to a per-input S3 subkey for the remote Connect executor), so they
never collide. The staged map is threaded through `LoopState.aux_inputs` and exposed
to the agent paradigm-symmetrically: the PRIMARY stays `AGENT_INPUT_PATH`; the rest
are a neutral name→path map in `AGENT_AUX_INPUTS` (+ per-name `AGENT_AUX_INPUT_<NAME>`)
for the imperative program, and are listed by name+path in the user message the SDP
agent reads. The composed prompt announces the extra inputs by neutral name only
(locations, never the fix — the prompt-no-leak guard covers it). The oracle still
reads only the primary. Identical across arms (B4). Tests: `tests/test_multi_input.py`.

Deferred for later iteration (does not block the re-freeze): the live agentic
floor-effect run on real Spark Connect — see the floor-effect pilot note in the PR
for the deterministic reference-solution gate that stands in for it offline.

---

## D-10 (2026-06-26) — Arm A2 registration completed + H5 conciseness instrumented (pre-data)

**Status: pre-data addendum (no data collected; logged per the pre-reg preamble).**
Two related, pre-data additions. Neither changes an existing hypothesis, the existing
arms A/B/B1/B2, the silent-defect grader/oracles, or the pre-registered 5-contrast
H1 Holm family.

**(a) Arm A2 registration finished.** `arms/A2.json` (the imperative + gate + skill
paradigm-matched control for B) landed in PR #28 and was already loaded by the
identical-except-loop guard, routed to the imperative executor, and pinned by
`tests/test_arm_a2.py` — but it cited a **pre-reg "§3 addendum item K" that did not
exist**, and the published `results_schema.json` arm `enum` (and its
`harness/schema.py` source `RESULTS_JSON_SCHEMA`) still listed only `[A, B, B1, B2]`,
so a real A2 result row would FAIL the published contract. Completed here:
- **PREREGISTRATION.md** — added the **Addendum** section that registers **item K
  (Arm A2)**, the citation `arms/A2.json` already shipped, with the §3 arm-table
  extension and the B-vs-A2 = pure-paradigm-contrast rationale.
- **Arm enum widened to include `A2`** in BOTH `harness/schema.py`
  (`RESULTS_JSON_SCHEMA`) and the regenerated published `results_schema.json` (kept
  in lockstep; `tests/test_published_schema.py` is the drift guard).
- A2 is **not** added to the H1 5-contrast Holm family (fixed at original
  registration); it appears in the descriptive per-arm tables and, when present, in
  the GLMM fit.

**(b) H5 conciseness instrumented (item L).** The project's headline qualitative
claim — declarative agents own a SMALLER decision surface — is made measurable on the
**B-vs-A2** contrast (gate + skill held constant, so it is the clean paradigm
contrast). Early single-seed probes showed ~60% fewer LOC (71 vs 179); this builds the
durable instrumentation to compute it across N.
- **Capture.** The runner records the **final accepted program** per cell — the
  agent-authored source of the proposal that reached `COMPLETED` (`EpisodeResult.
  final_program`, threaded into `ResultRow.final_program`). For SDP arms this is the
  `transformations/pipeline.py` @dp module; for imperative arms the `pipeline.py`
  program. The capture is a read of the proposal already in hand at the green break —
  the **SDP dry-run / gate resolution code path is untouched** (a parallel task owns
  it). A green output later retracted by the required-output completion check clears
  `final_program` (no accepted program to measure).
- **Metrics** (`harness/program_metrics.py`, new, stdlib `ast` only): `final_program_loc`
  (non-blank, non-comment lines) and `ast_node_count` (all `ast.walk` nodes), each
  **raw** AND **transform-body-only**. Body-only excludes the mandatory
  import/decorator/`def`-`class`-header scaffolding, computed identically for both
  paradigms, so declarative is not penalised for the `@dp` wrapper + `def` it must
  write, while the imperative program's hand-rolled `SparkSession` + I/O plumbing
  legitimately counts as its own surface. Fields added to `ResultRow` /
  `validate_row` / `RESULTS_JSON_SCHEMA` / `results_schema.json` as nullable (None
  until a run completes).
- **`.yml` decision (documented).** The SDP `spark-pipeline.yml` is **excluded** from
  both metrics: it is harness boilerplate (`runner._sdp_spec`, no agent logic), never
  authored by the agent, so counting it would attribute harness YAML to the
  declarative agent and inflate the surface under test. Conciseness compares the two
  agent-authored Python programs only.
- **Analysis.** `analysis/analyze.py` gains the **B-vs-A2** conciseness contrast
  (`conciseness_analysis`), paired over (task, seed), reusing the §7 percentile
  bootstrap (B = 10,000, (task, seed) resample unit). Difference reported as **A2 − B**
  (positive ⇒ declarative smaller) with paired mean/median + 95% bootstrap CI per
  metric; emitted as `H5_conciseness` in the report JSON + markdown table.
- **Tests.** `tests/test_program_metrics.py` pins the raw + body-only LOC and
  AST-node counts on a known declarative and a known imperative sample, the
  None-on-incomplete / syntax-error behaviour, the schema/`jsonschema` validation of a
  row carrying the new fields, and the analyze B-vs-A2 contrast on a fixture.

### D-10 follow-up (2026-06-26) — cross-review fixes to the body-only predicate (validity)

An independent cross-review (different vendor) flagged ONE blocking validity bug plus
two cheap hardening items in the H5 metric. All fixed in `harness/program_metrics.py`
+ `tests/test_program_metrics.py` (+ doc text); scope unchanged (no backends/gate code
touched).

- **BLOCKING — body-only stripped ALL decorators, biasing toward declarative.** The
  first cut removed every decorator from the body-only metric. But a `@dp` decorator is
  **not always inert**: in SDP it routinely carries AGENT DECISIONS — `@dp.expect(...)`
  data-quality expectations, `table_properties`, `partition_cols`/`cluster_by`, schema
  hints, the chosen `name`/`comment`. Stripping them omitted those declarative
  decisions from body-only while the equivalent IMPERATIVE quality logic (`.filter(...)`,
  `partitionBy(...)`) stayed counted — quietly favouring the very hypothesis under test,
  disqualifying for a pre-registered headline. **Fix:** a paradigm-agnostic
  scaffolding-vs-logic predicate (`_is_scaffolding_decorator`): a decorator with **zero**
  arguments is a decision-free wrapper (strippable: `@dp.table`, `@dp.materialized_view`,
  `@dp.view`, `@dp.table()`); a decorator with **any** positional/keyword argument is
  **logic** and is COUNTED (its whole expression) in body-only LOC and AST. The SAME
  test decides every decorator in either arm — never special-cased by name/paradigm.
  New tests pin: (a) a bare `@dp.table` is stripped; (b) a `@dp.expect(...)` is kept in
  body LOC + AST (body delta isolated against a byte-identical-body bare sample); (c)
  symmetry — a declarative `@dp.expect(...)` and the imperative `.filter("amount > 0")`
  are both counted, so neither arm's quality logic vanishes.
- **Hardening 1 — token-based LOC.** Comment/blank detection moved from a lexical
  first-`#` rule to Python's `tokenize`, so a `#`-leading line INSIDE a string/docstring
  is no longer mis-dropped (only genuine `COMMENT` tokens are excluded); a lexical
  fallback covers the rare un-tokenizable source. Raw-vs-body semantics are otherwise
  identical. Pinned by a docstring-with-`#` test.
- **Hardening 2 — parse-failure semantics documented.** The "keep raw LOC, null the
  three parse-dependent fields on `SyntaxError`" behaviour (not "all fields null") is now
  stated explicitly in the metric docstring and the prereg addendum item L.
- **Effect on the headline.** `final_program_loc` (raw) is UNCHANGED by this fix
  (decorators never affected raw counts), so the raw ~60% conciseness gap (71 vs 179
  LOC = 60.3% smaller) is invariant. The change shifts only the **body-only** metric:
  declarative now legitimately carries its decorator-borne logic, so the body-only gap
  moves DOWN (less biased) — e.g. on the unit samples the declarative body went 3 → 4
  LOC, narrowing body-only from 50% to 33% smaller than the matched imperative body. A
  less-biased number is the goal, not preserving any particular figure.

---

## D-A2RERUN (2026-06-28) — Arm A2 re-run on `instrument-v3.1` after the dead-session read-back fix

**D-A2RERUN (2026-06-28).** Arm A2 (imperative + gate + safety skill) was re-run in
full on tag `instrument-v3.1` after the dead-session read-back fix (PR #33, commit
56516ef): the imperative read-back previously used the agent's own Spark session, which
idiomatic agent code closes via `spark.stop()`, so the harness could not read
materialized output back and graded correct runs as `max_iterations`. On the prior
instrument only 2/66 A2 cells completed (47 false `max_iterations` + 15
SparkContext-race + 2 runtime); on `instrument-v3.1`, 61/66 complete. Also landed for
this re-run: PR #31 (propose-path crash-safety) and PR #32 (unified harness-fault
policy + schema: `harness_fault_reason`, token fields, A2 in the arm enum). B and B1
were NOT re-run — SDP arms use the subprocess Connect read-back path, unaffected by the
bug, and were already clean (59/66). 4 A2 cells failed organically and are reported as
genuine non-completions (`new_scd2_as_of_join` ×2 seeds, `p14_fx_settlement`,
`HC2_session_funnel`). The kept B/B1 data remains on its original commits
(295d725/54834a1); only A2 carries the new `instrument-v3.1` git_sha. This is a
post-data instrument fix logged before any confirmatory H3 claim.

---

## D-SEEDS-POWER (2026-06-29) — power-driven seed expansion 10 → 12 (pre-reg §6 stage 3)

**D-SEEDS-POWER (2026-06-29).** The N=3 pilot was underpowered for the registered H1
(A vs B) contrast. From the observed standard deviation of the paired A−B silent-defect
difference (sd = 0.404), `required_n_for_halfwidth` (analysis/analyze.py) puts the
paired-cell count needed for a 95% CI half-width ≤ `CI_HALF_WIDTH_TARGET = 0.05` at
**N = 252** cells; the N=3 run had only **66**, so `headline_n_valid = False` and the
harness refused to report the A–B headline as confirmatory. Per pre-reg §6 stage 3
(which explicitly permits appending seeds in a new lock version to tighten the CI, with
no upper cap), we appended **2 seeds — 16180 and 14142 — after the existing 10**,
giving **12 seeds × 22 tasks = 264 cells/arm**, just over the 252 requirement. The
original 10 seeds are kept in order and are not renumbered; `SEEDS.lock.json` moves to
version **`1.1.0-power`** (pilot_n=10, power_n=12). The confirmatory sweep runs **all
five arms** (A, B, A2, B1, B2) on tag **`instrument-v3.1`** so the A-reference GLMM and
the A–B/A–B2 contrasts can be fit. This changes only the sample size, not any
hypothesis, arm, metric, or analysis step — the power rule itself is pre-registered, and
this is the apparatus doing exactly what it was built to do: refusing to over-claim a
null on an underpowered N and scaling up before reporting a headline.


## D-CORPUS-V31 (2026-07-01) — additive non-CDC High task (`new_lineitem_reconcile`); corpus v3 22-task → v3.1 23-task

**Status: legitimate pre-data deviation (no data collected on this task; logged per the pre-reg preamble).** Owner-approved 2026-07-01. This revision re-freezes `TASKS.lock.json` (`3.0.0-corpus22` → `3.0.0-corpus23`). It **adds one task**; it changes **no** hypothesis, arm, metric, grader contract, or analysis plan, and does **not** alter the primary 22-task pre-registered corpus or its headline analysis.

**Why (the gap this closes).** The High-complexity tier of corpus v3 is **CDC-heavy: 5 of 7 High tasks** are CDC/SCD-flavored (`p2_cdc`, `p10_scd2`, `p13_cdc_windowed`, `new_scd2_as_of_join`, `HC1_fx_trade_ledger`), leaving only **2 non-CDC High** tasks (`p14_fx_settlement`, `HC2_session_funnel`). Because SCD/CDC native support is Spark-version-dependent (this instrument runs **4.1.0.dev4**, which has no native AutoCDC — see the version-dependence caveat queued for the revision gate), a planned **non-CDC-subset sensitivity analysis** (report H1 with and without CDC tasks) would rest on only 2 High tasks. Adding a 3rd non-CDC High task keeps that subset from collapsing at the top of the complexity range if the CDC tasks are later discounted.

**What changed.**
- Added task **`new_lineitem_reconcile`** — domain `lineitem_reconcile`, substrate `orders`, `graded_by: output_oracle`, `spec_ref: agent-authored`, complexity **36 / High** (axes `{A1:3,A2:2,A3:2,A4:2,A5:3,A6:1,A7:3,A8:0}`, verified by `harness/complexity.py`). Defects in scope **D1, D5, D6, D7, D8**. A line-item order-revenue mart: flatten itemized orders to true line-item revenue, dedup replays to one survivor per order, quarantine unprocessable rows, roll up daily revenue, and enforce two cross-stage invariants (kept+rejected = received; daily total ties to kept orders' line-item revenue) — the reconciliation invariant (A7=3) is the High-maker, achieved without CDC/SCD or streaming.
- `version` `3.0.0-corpus22` → `3.0.0-corpus23`; `corpus_status.locked_n_tasks` 22 → 23; `complexity_distribution` High 7 → 8 (Med 8 / Low 7 unchanged; Σ=23).
- `coverage_matrix`: D1 19→20, D5 6→7, D6 9→10, D7 8→9, D8 10→11 (every class remains ≥5). **Notably restores non-CDC coverage of D5 (2→3 non-CDC tasks) and D6 (4→5 non-CDC tasks)** — the two defects that thinned in the non-CDC subset.

**Instrument reuse (no measurement change, no reference-number movement).** The task reuses the **frozen** orders generator `infra/gen_messy_orders.py --v3` and the existing `quantify.d6/d7/d8` (+ `d8_nested`) output-oracle path. **No new generator, no new oracle function, no change to any locked oracle number.** D5 is structural (log signature `CANNOT_MODIFY_CONFIG`/`46110`) and requires no data injection or oracle.

**Execution.** Runs as a **24-cell addendum** (arms A,B × 12 seeds) at the identical frozen instrument (same commit / brain / recipe) **after** the primary 22-task sweep and its `analyze.py` complete. The new task is analyzed **only** in the non-CDC-subset sensitivity analysis; the primary 22-task headline is unchanged. Register-before-run: this entry precedes any `new_lineitem_reconcile` data.

**Cross-reference.** `PREREGISTRATION.md` Addendum (pre-data amendments), item **K** (first amendment after the A–J set that constituted D-CORPUS-V3).

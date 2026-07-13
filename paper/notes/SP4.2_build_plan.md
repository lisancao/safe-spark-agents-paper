# SP4.2 build plan (BUILD_PROGRAM Track 4)

*Execution plan for SP4.2, authored in the `BUILD_PROGRAM.md` task-card standard. SP4.2 anchor: §4 S4.1
(cost) + S4.2 (quality). Registry status: **SEPARATE EXPERIMENT / SEPARATE PAPER** (BUILD_PROGRAM Track 4,
gate SEPARATE PAPER). The design (hypotheses, arms, corpus, power, honest limits) is the "own design doc"
`SP4.2_fleet_study_design.md`; this file is the work contract that builds it. Anti-drift rules 1-7 of
BUILD_PROGRAM apply verbatim: the paper is read-only, no SP4.2 number enters the main paper, spend/new-build
needs Lisa's explicit go, drift = anything untied to this SP's acceptance.*

Break SP4.2 into three cards, in order, each its own PR + opposite-vendor review (execution model, BUILD_PROGRAM):
**SP4.2a** the harness · **SP4.2b** the real-oracle pilot · **SP4.2c** the powered sweep (spend-gated).

---

## SP4.2a — The fleet-study harness (author -> materialize -> REAL-oracle grade -> cost + catch-rate)

**Why this exists.** §4 S4.1 needs **cost-per-correct-pipeline** (routed vs single-strong) and S4.2 needs a
**cross-vendor defect catch-rate**. Both require §1's *gold-standard* grading (design: "reuses the §1
instrument rather than a proxy"). This card builds the instrument that emits both, with real grading.

**What you need to know.** The §1 authoring harness (`runner.py --backend local`) is UNRELIABLE here (smoke:
authored `pipeline.py` written, 0 graded rows, silent at materialize/record). Validated pivot (design doc,
2026-07-13): author via Omnigent (reliable, cross-vendor), materialize over local Spark Connect (:15002) with
the §4-capstone dp-executor, and grade with the REAL §1 oracles **standalone**:
`output_oracles.build_output_profile(read_table=spark.table, spark, input_path, defects_in_scope, contract)`
-> `oracles.grade_run(TaskOracleSpec(task, defects_in_scope), RunOutcome(completed, analysis_log, runtime_log,
output))`. Runner recipe: `runner.py` ~456-544. RunOutcome is arm-BLIND (rule: grader never sees the arm).

**Current state.** Approach validated + documented; branch `sp4.2-approach` pushed. Local Connect up on :15002.
Reusable + present: generators `infra/gen_*.py` (per task+seed; inputs already at `study/.work/_data/gen_*_seedN.ndjson`),
corpus `study/TASKS.lock.json` (23 tasks + bins + `defects_in_scope`), seeds `study/SEEDS.lock.json` (12),
oracles `study/harness/{oracles,output_oracles}.py`, the capstone dp-executor + `omnigent.llms` routing. Nothing built.

**Steps.**
1. Input: for (task, seed), ensure `infra/gen_<substrate>.py` has produced the seed's input ndjson (reuse the runner's `_generate_one`).
2. Author: route the model by policy (cost arm) via `omnigent.llms`; emit SDP `pipeline.py` (capstone author prompt + `pyspark-sdp` primer). Capture usage tokens.
3. Materialize: exec the authored `@dp` datasets over local Connect into `spark_catalog.default.<name>`; capture the dry-run text as `analysis_log` and execute text as `runtime_log`; time the executor (executor-seconds).
4. Grade (REAL oracle): `build_output_profile(...)` on the contract table -> `grade_run(TaskOracleSpec(task, task.defects_in_scope), RunOutcome(...))` -> {completed, silent_defect, defect_classes, per_defect_detection}. correct = completed AND not silent_defect.
5. Cost: `cost = tokens priced per model + executor-seconds x rate`; emit a ResultRow-shaped record.
6. Review (quality arm): a cross-vendor and a same-vendor reviewer flag suspected in-scope silent defects; catch-rate = flagged-and-actually-present / actually-present (present = the oracle's residual set).

**Commands.**
```
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))   # local Spark needs it
# harness (to be built) drives: gen -> author(omnigent) -> materialize(:15002) -> build_output_profile -> grade_run
python3 study/analysis/sp4_2/harness.py --task p5_mart --seed 42 --route single   # one cell
```

**Expected output.** One graded record per cell: `{task, seed, arm(routed|single), model, completed,
correct, silent_defect, defect_classes, input_tokens, output_tokens, executor_seconds, usd}` plus, for the
quality arm, `{task, seed, reviewer_kind(cross|same), reviewer_model, flagged, present, true_caught}`.

**Definition of done (binary).** The harness grades ONE cell end-to-end with the REAL §1 oracle
(`grade_run` output matches the field shape a §1 `ResultRow` carries), reproducibly, over local Spark Connect,
with cost fields populated. Verified by grading a known cell (e.g. the existing p5_mart pipeline, which carries
a real D1: reads `id`/`ts`, data has `customer_id`/`event_time`) and confirming the oracle flags D1.

**Guardrails.** REAL §1 oracles only (no proxy grader). RunOutcome stays arm-blind. No main-paper edit; output
lands only in SP4.2 materials. Low spend (a few author calls). Reuse §1 generators + corpus unchanged (do not
re-freeze the corpus).

**If it goes wrong.** If the dp-executor cannot materialize a given pipeline over local Connect, fall back to the
real `spark-pipelines run` CLI for that cell (same materialization §1 uses) and grade the same tables. If the
oracle needs the exact input path convention, mirror the runner's `AGENT_INPUT` staging (runner.py ~318-415).

---

## SP4.2b — Real-oracle pilot (validate end-to-end + measure per-cell cost)

**Why this exists.** De-risk the harness before the powered spend, and produce a real-oracle PILOT that
supersedes the earlier proxy-graded pilot. It also measures per-cell cost/time to size SP4.2c.

**What you need to know.** Arms: cost = {routed (Low->cheap, Med->mid, High->strong), single (strong)}; quality
= {cross-vendor review, same-vendor review}. Metrics: cost-per-correct; catch-rate on D2/D6/D7/D8 residuals.
Honest limits carry from the design doc (cross-model is a lower bound on cross-vendor; small N).

**Current state.** After SP4.2a passes its DoD.

**Steps.** Run the harness on a small cell set (e.g. 6 tasks spanning bins x 2 seeds x {routed, single});
run the review pass on the authored pipelines; compute cost-per-correct + catch-rate + the diversity gain;
write `SP4.2_pilot_results.md` (labeled PILOT) and record measured seconds + USD per cell.

**Commands.** `python3 study/analysis/sp4_2/run_pilot.py --tasks <6> --seeds 42,1337 --arms routed,single`.

**Expected output.** Pilot: cost-per-correct {routed, single}; catch-rate {cross, same} + diversity; per-cell
detail; and the measured mean seconds + USD per cell (the SP4.2c estimator).

**Definition of done (binary).** A labeled PILOT report with REAL-oracle numbers and a measured per-cell
cost/time, committed to `paper/notes/` (not the paper's claims).

**Guardrails.** Labeled PILOT; no main-paper number; honest caveats intact; grading is the §1 oracle.

**If it goes wrong.** If a graded outcome contradicts §1's known behavior for that task, reconcile the oracle
wiring before scaling (do not scale a mis-wired grader).

---

## SP4.2c — Powered sweep (GATED on Lisa's explicit spend go)

**Why this exists.** The powered cost-per-correct + cross-vendor catch-rate, sized like §1: the SP4.2 separate
paper's headline.

**What you need to know.** ~552 cost cells = 23 tasks x {routed, single} x 12 seeds, plus the (cheaper) review
pass. Each cost cell is an author loop + Spark materialize + grade (order of minutes): many hours + real API +
compute spend. The SP4.2b pilot supplies the per-cell cost, hence the total estimate.

**Current state.** After SP4.2b, and NOT before Lisa's spend go (anti-drift rule 6).

**Steps.** 1) Present the total cost/time estimate (pilot per-cell x cells). 2) On Lisa's explicit go, run the
powered sweep sharded + resumable, provenance-stamped. 3) Inference sized like §1 (N=12, paired by (task,seed),
mixed-effects + CIs). 4) Write the SP4.2 separate-paper materials.

**Commands.** `python3 study/analysis/sp4_2/run_powered.py --seeds all --shard k/of N --resume`.

**Expected output.** Powered cost-per-correct (routed vs single, paired, CI) + catch-rate (cross vs same, CI),
in the SP4.2 paper materials with a provenance manifest.

**Definition of done (binary).** Powered results at N=12 with inference, in the SP4.2 separate-paper doc.
**No number enters the main `PAPER.md`.**

**Guardrails.** SPEND GATE: no powered run without Lisa's explicit go. Separate paper. Resumable/sharded so a
partial failure never loses the sweep. If spend overruns the pilot estimate materially, STOP and report.

**If it goes wrong.** Shards resume from the journal; a failed cell is recorded (not fatal) and the sweep
continues; if the harness proves unstable at scale, fall back per-cell to the `spark-pipelines run` CLI path.

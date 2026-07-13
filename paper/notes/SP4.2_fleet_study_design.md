# SP4.2 fleet study: design (the separate experiment behind §4 S4.1/S4.2)

*Status: pre-registered design + pilot. The powered study is a separate paper (BUILD_PROGRAM SP4.2,
gate SEPARATE PAPER). No claim in the main paper depends on its numbers. This doc is the "own design
doc" the plan requires.*

## Purpose
§4 (Omnigent) argues that an orchestrated fleet of governed agents is cheaper (S4.1: heterogeneous model
routing) and higher-quality (S4.2: cross-vendor review) than raw parallelism. SP4.1 (credential custody)
is demonstrated; SP4.2 supplies the *numbers* for cost and quality. It reuses §1's instrument (the 23-task
corpus, the blind oracle grader, provenance stamping) and adds model-routing arms and a review pass.

## Hypotheses
- **H-cost.** A fleet that routes each task to a model matched to its complexity achieves a lower
  **cost-per-correct-pipeline** than a single-strong-model fleet, at statistically comparable completion.
  (§1's H5.3, cost-per-correct-completion, lifted from one agent to a fleet.)
- **H-quality.** A *different-vendor* reviewer catches silent defects that a *same-vendor* reviewer
  structurally misses (correlated blind spots), i.e. cross-vendor review has a higher **defect catch-rate**.

## Arms
Cost (authoring):
- **Routed:** model per complexity bin, cheap for Low, mid for Med, strong for High.
- **Single:** the strong model for every task.
Both author SDP (arm B's loop: propose -> structural dry-run gate -> execute -> blind grade), so the only
manipulation is the model. New arm manifests clone `arms/B.json` and vary `base_model_id` only.

Quality (review):
- **Cross-vendor:** a reviewer from a *different vendor* reviews each authored pipeline for the task's
  in-scope defects.
- **Same-vendor:** a reviewer from the *same* vendor/model family reviews the same pipelines.
Metric: fraction of the task's in-scope silent defects (D2/D6/D7/D8) each reviewer flags, scored against
the §1 oracle (the ground-truth defect set per task).

## Corpus, grading, power
- **Corpus:** the frozen §1 corpus, `study/TASKS.lock.json` v3.0.0-corpus23 (8 Low / 8 Med / 7 High), each
  task carrying its in-scope defects and grading oracles.
- **Grading:** §1's BLIND oracle grader (`study/analysis` + `oracles`), arm label never reaches the grader;
  correctness = completed AND not silent-defect, exactly as §1. This is the gold standard and why SP4.2
  reuses the §1 instrument rather than a proxy.
- **Metrics:** cost-per-correct = (input+output tokens priced at each model's rate, plus measured
  executor-seconds) / correct pipelines; catch-rate = caught / in-scope defects.
- **Power:** sized like §1 (N = 12 seeds per cell target; N* from the same calibration), per arm and paired
  by (task, seed). Deferred to the separate paper.

## Instrument status (this repo) and the validated harness approach (2026-07-13)
The §1 *authoring* harness (`study/harness/runner.py --backend local`) is UNRELIABLE in this env: a
smoke of one arm-B cell (p5_mart, seed 42) brought up local Spark and wrote the authored `pipeline.py`
into `.work/`, but produced no graded row and went silent at the materialize/record stage (the finicky
propose/record path noted in the state memory). Debugging it is uncertain effort.

**Decision: do NOT debug the flaky §1 authoring harness. Build the SP4.2 harness on the PROVEN Omnigent
pipeline + the REAL §1 oracles.** Confirmed feasible (all pieces located 2026-07-13):
- **Author** via Omnigent routing (reliable, and genuinely cross-vendor: Anthropic / OpenAI / Qwen /
  DeepSeek) -- the §4 capstone pattern. Sidesteps the flaky Anthropic-only §1 brain.
- **Materialize** the authored SDP over the local Spark Connect (:15002) with the capstone's dp-executor
  (proven in the capstone), writing the contract table into `spark_catalog.default`.
- **Grade with the REAL §1 oracles, standalone** (this is the gold standard the design demands, not the
  earlier pilot's proxy): `study/harness/output_oracles.build_output_profile(read_table=spark.table, spark,
  input_path, defects_in_scope, contract)` -> `study/harness/oracles.grade_run(TaskOracleSpec, RunOutcome)`.
  The runner's exact recipe is `runner.py` ~456-544 (RunOutcome carries analysis_log + runtime_log +
  OutputProfile). Structural D1/D4/D5 graded by log signature; semantic D2/D6/D7/D8 by residual corruption.
- **Cost** = real Omnigent usage tokens priced per model + Spark executor-seconds; **catch-rate** = the
  cross-vendor / same-vendor reviewer's flags scored against the oracle's ground-truth residual defect set.
- Data generators (`infra/gen_*.py`, per task, per seed) and the frozen corpus (`study/TASKS.lock.json`,
  12 seeds in `study/SEEDS.lock.json`) are reused unchanged.

A live cell is still an author loop + Spark materialize + grade (order of minutes); the powered sweep
(~552 cost cells = 23 tasks x {routed, single} x 12 seeds, plus the cheaper review pass) is many hours and
real API + compute spend, which is why it is the separate paper and why the powered run is gated on an
explicit spend estimate after a small real-oracle pilot.

## Honest limitations (must survive to the separate paper)
- **Cross-vendor vs cross-model.** A true cross-vendor catch-rate (e.g. a non-Anthropic reviewer over a
  Claude-authored pipeline) needs a second vendor's model. Where only one vendor is available, a
  cross-*model* within-family run is a **lower bound** on the cross-vendor effect, not the claim, and must
  be labeled as such.
- **Routing policy is a design choice**, not a tuned optimum; the study measures a *sensible* routing, and
  a policy sweep is future work.
- No SP4.2 number is retrofitted into §1's run or claimed in the main paper.

## Pilot
A bounded pilot (small N, a few tasks per bin) validates the harness end to end and produces preliminary
cost-per-correct and catch-rate figures, clearly labeled PILOT, to de-risk the powered study. Pilot results
land in `paper/notes/` alongside this design, not in the paper's claims.

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

## Instrument status (this repo) and the harness path (2026-07-13)
The §1 *authoring* harness (`study/harness/runner.py --backend local`) did not grade a cell in a first
smoke here: an arm-B cell (p5_mart, seed 42) brought up local Spark and wrote the authored `pipeline.py`
into `.work/`, but produced no graded row and went silent at the materialize/record stage. This is a
reliability issue to FIX, not a reason to swap the instrument.

**North-star-true path (primary). The cost arm stays §1's arm-B loop, per the Arms lock above: clone
`arms/B.json`, vary `base_model_id` ONLY** (haiku Low, sonnet Med, opus High for routed; opus for single),
**and run the real loop: propose -> structural dry-run gate -> execute -> blind grade.** The dry-run gate
is a §1 headline mechanism and must be the real one, so the cost numbers are literally "§1's instrument
lifted to a fleet" and stay comparable to §1. SP4.2a's first job is therefore to make the §1 harness grade
one arm-B cell here (debug the silent materialize/record path, or run in the config that produced the §1
528-run powered study), NOT to replace the harness. Grading is §1's blind oracle either way
(`output_oracles.build_output_profile` -> `oracles.grade_run`, runner recipe ~456-544; structural
D1/D4/D5 by log signature, semantic D2/D6/D7/D8 by residual corruption).

Cross-vendor enters ONLY where this design already puts it: the **quality arm's review pass** (a
different-vendor reviewer over the §1-authored pipelines), which may be driven through Omnigent.

**Fallback (labeled, only if the §1 harness cannot be made reliable).** Author via Omnigent + materialize
with the §4-capstone dp-executor over local Connect, still graded by the same REAL §1 oracles standalone.
This trades instrument fidelity for reliability, so it carries an explicit comparability caveat (a
different authoring/execution path than §1's arm B, hence cost numbers NOT strictly §1-comparable) and is
used only if the primary path is genuinely blocked. Reusable regardless: generators `infra/gen_*.py`, the
frozen corpus `study/TASKS.lock.json`, the 12 seeds, and the §1 oracles.

A live cell is an author loop + Spark materialize + grade (order of minutes); the powered sweep (~552 cost
cells = 23 tasks x {routed, single} x 12 seeds, plus the cheaper review pass) is many hours and real API +
compute spend, which is why it is the separate paper and the powered run is gated on an explicit spend
estimate after a small pilot.

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

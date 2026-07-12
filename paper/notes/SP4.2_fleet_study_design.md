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

## Instrument status (this repo)
The §1 runner is runnable here after a relocation fix: `study/harness/runner.py` computed `REPO_ROOT` three
levels up (the original layout); the study dir now sits two levels deep in the paper repo, so `REPO_ROOT`
resolved above the repo and the data-gen paths + provenance broke. `_find_repo_root()` now walks up to the
dir holding `infra/`/`.git`, so `--backend local` (which brings up its own single-node Spark Connect server
for SDP arms) runs. A live cell is a multi-iteration agent loop plus Spark execution plus grading, on the
order of minutes; a powered sweep is therefore many hours and real API spend, which is why it is the
separate paper.

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

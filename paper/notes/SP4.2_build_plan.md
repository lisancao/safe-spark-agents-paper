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

## SP4.2a: Make the §1 arm-B loop grade a model-routed cell (the cost-arm instrument)

**Why this exists.** §4 S4.1 needs **cost-per-correct-pipeline** (routed vs single-strong). The design LOCKS
this as **§1's arm-B loop with `base_model_id` varied only**, so the cost numbers are literally "§1's
instrument lifted to a fleet" and stay §1-comparable. This card makes that real here: §1's harness grading
one model-routed arm-B cell, with the real dry-run gate (a §1 headline mechanism) running.

**What you need to know.** §1's authoring harness (`runner.py --backend local`) did not grade a cell in a
first smoke here (authored `pipeline.py` written, 0 ResultRows, silent at materialize/record). That is the
thing to FIX, not a reason to swap the instrument. Cross-vendor is NOT in the cost arm; it is the quality
arm's review (step 4). Grading is §1's blind oracle (`output_oracles.build_output_profile` ->
`oracles.grade_run`, runner recipe ~456-544; RunOutcome is arm-BLIND). Omnigent-authoring is a documented
FALLBACK (design doc, Instrument status), used only if the harness is genuinely unfixable, with a
comparability caveat.

**Current state.** §1 harness present + relocation-fixed; smoke authored but did not record a row. Local
Connect up on :15002. `arms/B.json` (base_model_id=claude-opus-4-8), generators `infra/gen_*.py` (inputs at
`study/.work/_data/gen_*_seedN.ndjson`), corpus `study/TASKS.lock.json`, 12 seeds, oracles present.

**Steps.**
1. Diagnose the silent stage: run one arm-B cell and trace why no ResultRow is recorded (materialize/record
   path, `runner.py` ~232-308 and ~456-544); fix the reliability bug, OR identify the config that produced the
   §1 528-run powered study and run in it.
2. Cost-arm manifests: clone `arms/B.json` to `B_haiku` / `B_sonnet`, varying `base_model_id` ONLY (haiku Low,
   sonnet Med, opus High). "routed" = each task run with its bin's manifest; "single" = B (opus) on all. Confirm
   the config's `base_model_id`-match check (runner ~1145) passes for each.
3. Run one cell per new manifest END-TO-END (propose -> real structural dry-run gate -> execute -> blind oracle
   grade); confirm a graded ResultRow with cost fields.
4. Quality-arm review pass (cross-vendor, via Omnigent): a different-vendor and a same-vendor reviewer flag
   suspected in-scope silent defects over the §1-authored pipelines; catch-rate = flagged-and-present / present
   (present = the oracle's residual set).

**Commands.**
```
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
python3 study/harness/runner.py --backend local --only-arms B --only-tasks p5_mart --max-seeds 1 --out /tmp/one.jsonl
# diagnose the 0-rows; add B_haiku/B_sonnet manifests; re-run routed + single.
```

**Expected output.** A graded §1 ResultRow per cell: `{task, seed, arm, base_model_id, exit_class,
reached_correct, silent_defect, defect_classes, dry_run_intercepts, input_tokens, output_tokens,
executor_seconds, usd}` plus a review record per (task, seed, reviewer_kind).

**Definition of done (binary).** §1's REAL arm-B loop (propose -> structural dry-run gate -> execute -> blind
grade) grades one model-routed cell here and writes a ResultRow, reproducibly, with cost fields populated.

**Guardrails.** Cost arm = arm B's loop, vary `base_model_id` ONLY: do not change the loop, the dry-run gate,
or the grader (that would break §1-comparability, the whole point). Real §1 dry-run gate + blind oracle.
Cross-vendor confined to the quality-arm review. No main-paper edit; output only in SP4.2 materials. The
Omnigent-authoring fallback is allowed ONLY if the §1 harness is genuinely unfixable, and only with the
comparability caveat recorded on every number.

**If it goes wrong.** If the silent-record bug is not fixable in reasonable effort AND the §1-powered-run
config cannot be reproduced here, invoke the LABELED fallback (design doc, Instrument status): Omnigent author
+ capstone dp-executor materialize, same §1 oracles, with the comparability caveat attached to every number.

---

## SP4.2b: Real-oracle pilot (validate end-to-end + measure per-cell cost)

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

## SP4.2c: Powered sweep (GATED on Lisa's explicit spend go)

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

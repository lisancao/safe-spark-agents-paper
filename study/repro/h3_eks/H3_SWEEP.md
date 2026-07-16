# Safe-agent study — headline numbers

- rows: **24**  |  (task,seed) cells: **12**  |  arms: ['A', 'B']
- CI: bootstrap **percentile**, B=**10000**, seed=**20260623**, resample unit=**(task, seed)**
- inference: mixed-effects logistic (random intercepts task+seed); Holm across 5 contrasts; α=0.05

## H1 — silent-defect rate by arm

| arm | silent-defect rate | k/n | 95% CI (bootstrap) |
|---|---|---|---|
| A | 0.250 | 3/12 | [0.000, 0.500] |
| B | 0.833 | 10/12 | [0.583, 1.000] |

## H1 — paired contrasts (Holm over the **glmm** contrast p-values)

| contrast | Δ rate | 95% CI (bootstrap) | OR | p (source) | Holm p | sig (α=.05) |
|---|---|---|---|---|---|---|
| A-B | -0.583 | [-0.833, -0.333] | 21.071 | 0.0001 | 0.0001 | YES |

## H1 — GLMM odds ratios + average marginal effects (primary inference)

Reference = Arm A. OR<1 ⇒ FEWER silent defects than A. AME is the model-based average marginal effect on P(silent_defect) (RE at 0).

| arm vs A | odds ratio | coef | AME (Δ prob) | posterior p |
|---|---|---|---|---|
| B | 21.071 | +3.048 | +0.601 | 0.0001 |

Reference = Arm B (B-vs-B1 / B-vs-B2 ablation contrasts):

| arm vs B | odds ratio | coef | AME (Δ prob) | posterior p |
|---|---|---|---|---|

Observed per-arm silent-defect rates (DESCRIPTIVE, not the AME):
  A=0.250, B=0.833

## H1 — power / sample-size rule (pre-reg §6)

- target: 95% CI half-width ≤ **0.05** on A−B
- observed sd(A−B paired) = **0.515**  →  required N = **408** (task,seed) cells; have **12**
- **meets power: False** — `headline_n_valid=False` (a headline N below required must NOT be claimed)


## §9 — Error taxonomy: catch-stage by class group (PRE-REGISTERED headline)

Where each defect is caught: **dry_run** (SDP gate, pre-execution), **runtime** (during execute), or **never** (shipped). Counted at the defect level across ALL iterations (anti-bypass §9.2 — a gate-caught-then-fixed error still counts). structural=D1/D4/D5, semantic/silent=D2/D6/D7/D8 (CONTROL), state=D3/D9. The silent-defect rate (H1 above) is the control, not the headline.

| arm | group | dry_run (gate) | runtime | never (shipped) |
|---|---|---|---|---|
| A | structural | 0 | 1 | 0 |
| A | semantic | 0 | 0 | 3 |
| A | state | 0 | 0 | 0 |
| B | structural | 3 | 1 | 0 |
| B | semantic | 0 | 0 | 10 |
| B | state | 0 | 0 | 0 |

Iteration-level error events per arm: **A** gate=0 runtime=10 intercepts=0; **B** gate=31 runtime=4 intercepts=31

## H2 — compute-to-correct: A (execute-only) vs gated loop

- metric: **`executor_seconds_to_correct`** — measured cluster executor-seconds (stage-diff)
- selection: backend='live': remote/Connect measures executor-seconds identically across arms

Reported BOTH ways (B9): intention-to-treat over all matched cells (failed runs imputed to total compute spent) AND complete-case (both arms reached correct) as the pre-specified sensitivity.

| gated arm | mode | n pairs | median exec-s saved | mean | 95% CI | $ saved | intercept frac |
|---|---|---|---|---|---|---|---|
| B | intention_to_treat | 6 | 1.5 | 5.3 | [0.9, 13.2] | $0.00 | 88.6% (31/35) |
| B | complete_case | 3 | 0.6 | 0.6 | [0.5, 0.7] | $0.00 | 88.6% (31/35) |

## H3 — per-attempt compute: wasted-on-failed (H3.1) & total-to-correct (H3.2)

Read from the per-attempt `executor_seconds` serialized into `per_iteration[*].{gate,execute}` (§6.6(3)). H3.1 = Σ measured exec-s over NON-GREEN attempts (a gate intercept is ~0 by construction); H3.2 = Σ exec-s up to & incl. the green iteration. Un-measured attempts (driver-only gate / no-code / timeout / no live metric) count as 0 exec-s; `measured/total` shows how many attempts carried a live metric (0/n ⇒ sums are structurally 0, not a real zero).

| arm | n runs | H3.1 wasted exec-s (mean/run) | failed (intercepted) | H3.2 to-correct exec-s (mean/run) | attempts measured/total |
|---|---|---|---|---|---|
| A | 12 | 33.1 (2.8) | 10 (0) | 35.7 (3.0) | 13/13 |
| B | 12 | 0.0 (0.0) | 35 (31) | 20.2 (1.7) | 14/45 |

## H4/H5 — conciseness: declarative (B) vs imperative (A)

Paired over (task, seed) on the FINAL ACCEPTED program. Contrast **B-vs-A** (locked 2-arm design; A2 withdrawn). Difference is **A − B**, so positive ⇒ the declarative agent wrote LESS. LOC/AST are source-level (substrate-independent). `*_body` excludes the mandatory @dp/def/import scaffolding (the SDP `spark-pipeline.yml` is harness boilerplate and is not counted).

| metric | n pairs | B (declarative) | A (imperative) | Δ (A−B) | 95% CI (bootstrap) | % smaller than imperative |
|---|---|---|---|---|---|---|
| final_program_loc | 3 | 41.0 | 103.7 | +62.7 | [+39.0, +79.0] | 60.5% |
| final_program_loc_body | 3 | 36.0 | 97.3 | +61.3 | [+40.0, +77.0] | 63.0% |
| ast_node_count | 3 | 321.7 | 655.7 | +334.0 | [+224.0, +420.0] | 50.9% |
| ast_node_count_body | 3 | 310.3 | 638.7 | +328.3 | [+225.0, +410.0] | 51.4% |

## Quarantine — HARNESS_ERROR cells EXCLUDED from H1–H4 (excluded-data appendix)

- rows analyzed: **24** of **24** total; **0** quarantined (instrument failures, never agent outcomes)
- no cells quarantined — every analyzed cell is a genuine agent outcome.


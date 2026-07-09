# BUILD PROGRAM — Executing the Paper Without Drift
**`PAPER.md` is the FROZEN north star.** This file is the *work contract*. Executors **satisfy** paper claims; they
**never rewrite** them. If a piece of work is not a subproject below that advances a named paper anchor, it is
**drift — reject it.**

## Scope lock (2026-07-09, Lisa)
**§3 = BOTH in sequence, one runway.** Finish the single-tenant EKS runway (P0–P4: native mTLS, substrate,
elastic scaling, EKS-wired GitOps) as de-risking, then roll straight into the multi-tenant frontier (P5–P6:
governed catalog + per-tenant isolation, multi-server Connect) under one spend approval. **Headline frontier
result = SP3.4** — the two-agent "A blocked from B" isolation proof. §4 stays scoped: SP4.1 custody (a P5
dependency) + SP4.2 fleet study (a *separate* experiment with its own design doc, not §1's run). **Catalog
LOCKED (2026-07-09): Lakekeeper primary + Unity Catalog OSS second binding** (Polaris rejected on operational
weight; catalog-agnostic claim). The sharpened SP3.4/3.5 build spec + the isolation-proof design live in
`SECTION3_isolation_experiment.md`.

## Anti-drift rules (binding on polly AND every sub-agent)
1. **The paper is read-only during execution.** No sub-agent may edit `PAPER.md`. It changes ONLY at an explicit
   *paper-revision gate* with Lisa present — never as a side effect of building.
2. **Every task maps to a subproject (SP); every SP maps to a paper anchor.** No anchor → not in scope.
3. **Acceptance = the paper's stated bar**, quoted from the anchor — not the executor's opinion of "done."
4. **A subproject may only flip a STATUS tier** (frontier → configured → demonstrated) **with evidence, reviewed
   by Lisa.** It may NOT reframe, rename, or re-scope a claim. Status flips are the *only* sanctioned change to
   the paper, and they are mechanical (tier + evidence cite), not editorial.
5. **Executors build in the `safe-spark-agents` repo and open PRs; Lisa merges. polly never merges, never edits the paper's claims.**
6. **Spend / new-build / scope gates require Lisa's explicit go.** No terraform apply, no cluster, no frontier
   construction without it.
7. **Drift is any of:** editing a claim, work untied to an SP, a status flip without evidence, or scope creep
   past an SP's acceptance. Caught → stop, return to the registry.

## Task-card standard (salvaged from the ultraplan EXECUTION pack)
Every SP is dispatched to an implement sub-agent as a **self-contained card** (format reused from
`docs/ultraplan-context-fixes:EXECUTION/00_ONBOARDING.md`): **Why this exists** (the paper anchor it satisfies) ·
**What you need to know** · **Current state** · **Steps** · **Commands** · **Expected output** ·
**Definition of done (binary)** · **Guardrails** · **If it goes wrong.** Where a matching ultraplan card exists
(map below), **adapt it — don't rewrite** — and prune anything outside this paper's four north-star sections.

## Execution model (incl. "multiple pollys")
- The paper + this registry are the shared, frozen spec. One orchestrator owns the registry.
- Tracks T1–T4 are independent enough to run as **separate polly orchestrators** (spawned per track), each bound
  to THIS file. A track-polly may only dispatch the SPs in its track, with acceptance quoted from the paper.
- Each SP → an `implement` sub-agent (own worktree, own PR) + opposite-vendor `review`. Results recorded against
  the SP's anchor only.
- **Critical path to *a paper* = Track 1.** Everything else firms §2/§3's demonstrated cores or is explicitly future.

---

## SUBPROJECT REGISTRY

### Track 0 — Anti-drift enforcement  (foundation; do first)
| SP | Paper anchor | Deliverable | Acceptance | Owner | Deps | Gate |
|---|---|---|---|---|---|---|
| **SP0.1** | Rules 1 & 4 (enforce) | Paper-bind CI — `render_paper.py` + `tests/test_paper_bound.py` + `.github/workflows/paper-bind.yml` (**salvage ultraplan T0.6**) | CI fails on any `[PENDING` or unbound result; the paper can only change via evidence-backed status flip | code | — | — |
| **SP0.2** | Rule 1 | Session freeze policy (ASK on `PAPER.md` writes) — immediate, optional | every `PAPER.md` write prompts Lisa | polly+human | — | install on go |

### Track 1 — §1 Powered Run  (THE SCIENCE — the actual results; the minimum to *have* a paper)
| SP | Paper anchor | Deliverable | Acceptance (from paper) | Owner | Deps | Gate |
|---|---|---|---|---|---|---|
| **SP1.1** | §1 §6.6 / H1,H4 | PR: redefine Arm B = SDP + `pyspark-sdp` only, no `spark-safety` | B runs with no safety skill; `arms/B.json` updated; diff cross-reviewed | code | — | — |
| **SP1.2** | §1 §6.6 / H3.1 | PR: serialize per-attempt compute into `ResultRow` | per-attempt executor/cpu seconds recorded | code | — | — |
| **SP1.3** | §1 §6.7 | Calibration pass + projected $ | cost projection for N*=12 produced | run+human | SP1.1,1.2 | **SPEND** |
| **SP1.4** | §1 §6.7–6.8 / H1–H5 | Powered run A vs B | H1–H5 results at N*=12 via the literal §6.8 commands | run+human | SP1.3 | **SPEND** |

### Track 2 — §2 Control Boundary firming  (Appendix S2-A layers)
| SP | Paper anchor | Deliverable | Acceptance | Owner | Deps | Gate |
|---|---|---|---|---|---|---|
| **SP2.1** | App S2-A R / L0–L2 | Capture + commit stranded remote-run evidence (worktrees) | evidence tracked in repo + cited | code | — | — |
| **SP2.2** | App S2-A L1 / R3 | Native client mTLS (no socat), against local Envoy | PySpark Connect client presents cert+PSK natively | code | — | — |
| **SP2.3** | App S2-A L3 | Arm-B SDP completes + grades green on remote Connect | one remote Arm B pipeline green | run | SP2.2, SP3.1 | **SPEND** |
| **SP2.4** | App S2-A L5 | Reconciler off the agent host (governed controller) | driver/reconciler runs in a zone ≠ agent host | code+run | SP2.3 | — |

### Track 3 — §3 Open Platform  (Appendix S3-A layers; P0–P4 salvage+wire, P5–P6 new build)
| SP | Paper anchor | Deliverable | Acceptance | Owner | Deps | Gate |
|---|---|---|---|---|---|---|
| **SP3.1** | App S3-A P0 | EKS substrate stand-up (terraform apply, image, deploy, endpoint) | cluster live; endpoint captured | infra+human | — | **SPEND** |
| **SP3.2** | App S3-A P2 | Prove elastic 0→N→0 executor scaling | scale cycle observed + captured | run | SP3.1 | — |
| **SP3.3** | App S3-A P3/P4 | EKS-wired GitOps; capture a real agent PR | dry-run+reconcile target EKS; real PR artifact | code+run | SP3.1 | — |
| **SP3.4** | App S3-A P5 *(FRONTIER)* | Governed catalog for per-tenant authz | two-agent "A blocked from B" test passes | new-build | SP3.1 | **SCOPE** |
| **SP3.5** | App S3-A P6 *(FRONTIER)* | Multi-server Connect + ingress routing | per-tenant Connect servers behind one ingress | new-build | SP3.4 | **SCOPE** |

### Track 4 — §4 Omnigent  (mostly future; the numbers are a SEPARATE experiment)
| SP | Paper anchor | Deliverable | Acceptance | Owner | Deps | Gate |
|---|---|---|---|---|---|---|
| **SP4.1** | §4 S4.3 (= §3 P5 dep) | Credential-custody integration (Omnigent holds the vended cred) | agent credential-free; custody in orchestrator | new-build | SP3.4 | **SCOPE** |
| **SP4.2** | §4 S4.1/4.2/4.5 | Fleet study (cost-per-correct-pipeline; cross-vendor catch-rate) | **SEPARATE EXPERIMENT — own design doc, not §1's run** | new-build | T1 done | **SEPARATE PAPER** |

---

## Status ledger (reconciled 2026-07-09 against the paper's evidence map; tier + evidence, reviewed by Lisa)
*Was stale: it predated §1's completion and the EKS L2/L3/H3 demonstrations. Reconciled below from the paper's own Appendix S2-A / S3-A evidence map. Corrections welcome; this is the work-contract ledger, not a PAPER.md claim change.*
| SP | Status | Evidence |
|---|---|---|
| SP0.1 | salvage T0.6 (paper-bind CI); not enforced yet | (none) |
| SP0.2 | available | (none) |
| SP1.1 | **DONE**: Arm B = SDP + `pyspark-sdp`, no safety skill | current 2-arm design, PAPER.md §1 |
| SP1.2 | **DONE**: per-attempt compute serialized | H3 rows (executor/cpu seconds) |
| SP1.3 | **DONE**: calibration + $ projection | H3 $-model, $0.192/exec-hr |
| SP1.4 | **DONE**: powered run A vs B (N=264/arm) | `results.powered.AB.n12.final.jsonl`; H1–H5 |
| SP2.1 | PARTIAL: some remote-run evidence captured | `study/repro/h3_eks/` |
| SP2.2 | **GAP**: native client mTLS still via socat | App S2-A R3 / L1 |
| SP2.3 | **DONE (2026-07-06)**: Arm B SDP completes+grades green remote | `study/repro/h3_eks/`; App S2-A L3 |
| SP2.4 | **GAP**: reconciler co-located on agent host | App S2-A L5 / R2 |
| SP3.1 | PARTIAL: cluster built + ran once; Terraform not applied-as-tracked-artifact | App S3-A P0 |
| SP3.2 | PARTIAL: small-scale exec on EKS; elastic 0→N→0 unproven | App S3-A P2 |
| SP3.3 | **GAP**: GitOps gate/reconcile still runner-local, not EKS | App S3-A P3/P4 |
| SP3.4 | **NEXT FRONTIER** (scope-approved): governed catalog + isolation proof | needs catalog choice |
| SP3.5 | frontier (after SP3.4) | (none) |
| SP4.1 | frontier (dep SP3.4) | (none) |
| SP4.2 | separate experiment / own design doc | (none) |

## Free, no-spend, delegatable NOW (post-§1)
**SP2.2** (native client mTLS de-risk, on the L1 critical path) · **SP2.1** (finish capturing stranded
remote-run evidence). Zero spend; each firms a runway layer, and each opens a cross-reviewed PR you merge.
**Catalog choice is now LOCKED (Lakekeeper + UC-OSS).** **First SPEND gate is SP3.1 (`terraform apply` = EKS +
RDS + S3 + node groups).**


---

## Ultraplan reconciliation (salvage map → north stars)
Source: `docs/ultraplan-context-fixes` — 8,728 lines of planning docs, **never executed, now behind `dev`.** We reuse
*cards and mechanisms*, not its maximalist scope. (Lesson: last week's failure was infinite planning, no build — so
the discipline now is to **execute against the frozen paper**, not re-plan.)

| Ultraplan artifact | → maps to | Action |
|---|---|---|
| `T0.6` paper-bind (renderer + test + CI) | **SP0.1** | **SALVAGE — enforce** |
| `T0.2` freeze/tag `instrument-v3-frozen` | SP0.2 | adapt |
| `T0.1`/`T0.3`/`T0.4` instrument (arms, per-attempt compute, analyze) | **SP1.1, SP1.2** | **SALVAGE cards as briefs** |
| `T1.0`–`T1.4` local sweep to power (N=12, sharding) | **SP1.3, SP1.4** | **SALVAGE cards** |
| `T2.1`–`T2.5` live EKS (IaC drift, reachability, first live episode `T2.4`) | SP3.1, SP2.3 | SALVAGE cards |
| `T3.1`–`T3.3` per-agent identity vending / 3-agent smoke | SP2.4, SP3.4, SP4.1 | adapt (custody/identity) |
| `T4.1`–`T4.4` E1–E7 cluster experiments | Track 3 frontier / SP4.2 | **DEFER — not headline** |
| `T5.1`–`T5.2` assemble paper / paper-bind | Track 1 finalize + SP0.1 | SALVAGE |
| `T6.1`–`T6.3` blogs + enterprise hardening | — | **PRUNE — out of scope** |

Also salvaged: **Gate-G invariants** (labels frozen, blind grader, loop-isolation, paper-binding) → folded into the
anti-drift rules above; **`LAB_NOTEBOOK.md`** → forensic record (already mined for the §1/§2 EKS history). Pruned:
Phase 6 and treating E1–E7 as headline scope — the bloat that made the ultraplan un-shippable.

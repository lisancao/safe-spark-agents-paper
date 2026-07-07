# REPRODUCE-H3.md — H3 (data-processing compute / executor-seconds) on EKS, Phase 2b

> **STATUS: PLAN ONLY. H3 HAS NOT BEEN RUN. There are ZERO confirmatory H3
> executor-seconds results.** This runbook is the methodology + run scripts for
> the deferred Phase-2b compute study (Task #3). Nothing here has produced an
> A-vs-B compute number. Do not cite any number in this file as an H3 result —
> the only compute figures that exist today are (a) a single instrument-validation
> micro-probe and (b) a substrate-confounded wall-clock **proxy**; both are
> described under "What already exists" so they are not mistaken for H3 data.
>
> Nothing runs until every box in §1 is checked **and** the §5 spend ceiling is set.

Place this file (and `run_h3_eks.sh`) under
`experiments/safe_agent_study/repro/h3_eks/`.

---

## 0. What H3 is (and the naming trap)

"H3" names two different things in this repo — be explicit about which one this
runbook is for:

- **This runbook = H3 as DATA-PROCESSING COMPUTE** (executor-seconds), the
  cluster/EKS-relevant cost, deferred to Phase-2b. This is the sense used in
  **Task #3** and in the methodology (`PAPER.md` §6.2 H3.1/H3.2, §6.5, §6.7;
  `SECTION1_DATA_AND_METHODOLOGY.md` §5, §9.3).
- **NOT** the mechanism/ablation hypothesis also called "H3" in
  `PREREGISTRATION.md:32-35` (arms B1 SDP-only, B2 gate-only), and **NOT** the
  bounded-state-under-load "H3"/E6 of the broader lakehouse study
  (`EXPERIMENT_DESIGN.md:19,135`). Different hypotheses; out of scope here.

The two written H3-compute hypotheses this run is meant to test
(verbatim, `PAPER.md` §6.2 L178-180):

- **H3.1 Wasted-compute-on-failed-attempts** — SDP's dry-run gate rejects failed
  attempts *before execution* (~0 data processed); imperative failures execute
  and burn compute. *Direction: SDP lower.* Currently marked **"Not measurable
  yet (no per-attempt compute; substrate split)"**.
- **H3.2 Total-compute-to-correct** — *Direction OPEN.* The pilot wall-clock
  **proxy** showed SDP higher (18.5s vs 10.2s), but that is **not data-compute**
  and is substrate-confounded — do not carry it forward as the result.

Why it was deferred: the validated **local** backend *splits substrate by
paradigm* (imperative → classic `local[*]` Spark; SDP → local Spark Connect),
so executor-seconds are **not cross-arm comparable** locally
(`PAPER.md` §6.5 L207-211; `analyze.py` module docstring + `resolve_h2_metric`).
H3 therefore requires **both arms on ONE uniform substrate = the `live`/Connect
backend on EKS**, whose `ConnectExecutor` measures both paradigms with the same
stage-diff mechanism.

---

## What already exists (three tiers — NONE is an H3 result)

Read this so no existing artifact is misread as H3 A-vs-B compute:

1. **VALIDATED INSTRUMENT (single micro-probe, not a study result).**
   `DEVIATIONS.md:364-368` (D-5): a subprocess `spark.range(80_000_000).sum()`
   produced NEW stages `[60, 62]`, measured **executor_seconds 1.246**,
   **cpu_seconds 0.878** against the real cluster — run once to prove the
   before/after stage-diff. This validates the *instrument*, not A-vs-B.

2. **PROXY (what the H2/H3 local run actually reported).**
   `results.powered.AB.n12.final.jsonl` — 528 rows (A=264, B=264); the published
   H2 headline (`POWERED_HEADLINE.final.md`) uses
   `executor_seconds_wallclock_to_correct` = `wall_s × instances × busy_fraction`
   with a **declared** `executor_config` (instances=4, m5.xlarge-equivalent,
   $0.192/exec-hr, provider k8s). Confirmed present in all 528 rows. This is a
   **costing assumption, not a k8s measurement** (`PAPER.md` L146 states "This is
   a proxy, not data-compute.").

3. **MEASURED-BUT-NOT-CROSS-ARM-COMPARABLE (present, deliberately unused).**
   The same jsonl carries **284 non-null** `executor_seconds` values (confirmed),
   but on the LOCAL split substrate imperative (`/executors.totalDuration` delta)
   and SDP (local Connect stage-diff) use two non-comparable mechanisms
   (`DEVIATIONS.md` D-7 follow-up 4, L575-593). **Not** H3 executor-seconds.

4. **EKS RUN HISTORY (mechanism demonstrated, no A-vs-B compute collected).**
   During GO calibration on the real EKS cluster 2026-06-24
   (`DEVIATIONS.md:184-185`), Arm A materialized silver/gold/quarantine tables
   remotely; S3/IRSA staging proven live (`DEVIATIONS.md:205-215`: createDataFrame
   6.4s / write S3 3.6s / readback 0.8s). The stage-diff was demonstrated once
   (the 80M probe). **No confirmatory cross-arm executor-seconds sweep exists.**

---

## 1. Prerequisites checklist (ALL must be green before any run)

| # | Prereq | Done? | Source of truth |
|---|--------|-------|-----------------|
| P1 | **EKS bring-up complete** — cluster + RDS + S3 + IRSA, Spark-Connect image (Tier A: Spark 4.1.x + Iceberg, interceptor jar), HMS (or Iceberg JDBC catalog on RDS, V1-pinned), Connect+Envoy mTLS pod Running, messy data loaded to Iceberg/S3. | ☐ | `deploy/eks/RUNBOOK.md` §3 steps 1-5; cold-start order in `safe-spark-agents-state.md:273` |
| P2 | **Per-attempt compute serialized** (the §6.6(3) instrument change) — stamp `exec_out.executor_seconds`/`cpu_seconds` (and `wall_s`/`usd`) into `rec["execute"]` **before** `per_iteration.append(rec)` at `harness/runner.py:258-260`, and add the analyze.py H3 reader. Today per-attempt compute reaches only the in-memory `iter_costs` list and is collapsed to run-level totals — **it is NOT in `results.jsonl`** (`analyze.py:710-714` reads `per_iteration` only for error-class counts). Without this, per-attempt H3.1 cannot be computed. | ☐ | `PAPER.md` §6.6(3) L216; `runner.py:253-260`; state map "run_episode gap" |
| P3 | **Remote Arm-B SDP completes green (the "L3" blocker)** — an SDP run prints `Run is COMPLETED`/rc==0 **and** the contract output table reads back (`live.py:874-886`). Requires the fixed blocker chain: D-2 temperature-only (no top_p), D-3 s3a staging of inputs (`stage_input`), and `python3 $SPARK_HOME/pipelines/cli.py run --spec <ABSOLUTE spark-pipeline.yml>` (never `bin/spark-pipelines`). Arm A off-host + stage-diff already demonstrated 2026-06-24; **remote Arm-B green is the open milestone.** | ☐ | `DEVIATIONS.md` D-2/D-3 (154-249); `live.py:846-856`; `test_workspace_contract.py:186-227` |
| P4 | **`spark.ui.retainedStages` raised** above the largest total per-cell stage count, so the before/after stage-diff cannot undercount via UI eviction (default **1000**; Phase-2a observed 929/1000 on 2026-06-30). A retention warning fires near the bound (`live.py:924-942,964-967`). Set this in the Connect ConfigMap and roll the pod. | ☐ | `PAPER.md:638`; `DEVIATIONS.md:409-417`; `SECTION1 §9.3` item (iii) |
| P5 | **UC-OSS / Iceberg full-refresh truncate awareness** — the Iceberg JDBC catalog **cannot truncate on MV refresh** and needs `s3://` scheme + location/provider; UC grants are structural-only (Hive), NOT runtime-enforced. Pre-create each principal schema (`CREATE SCHEMA IF NOT EXISTS iceberg.sandbox_<principal>`) — not yet scripted into reproduce.sh. Confirm the reconcile/grade path tolerates full-refresh semantics (confirmed 2026-07-02, UC-OSS-only). | ☐ | `CLAIMS.md:177`; `RUNBOOK.md:168-171,270-284`; `safe-spark-agents-state.md:160` |
| P6 | **Spend ceiling set (the "spend gate")** — there is **no dollar spend-cap gate in code**. The real controls are: `max_iterations=12` (`PAPER.md:136`); the per-iteration exec budget `AGENT_EXEC_TIMEOUT_S` hard 150s cap (`runner.py:56`); the per-cell wall-clock `--per-cell-timeout`; and the `executor_config` price param (`price_usd_per_executor_hour=0.192`). The ceiling is enforced **operationally** by scoping N (task×seed subset) — see §5. Set `$ CEILING` before launch. | ☐ | `harness/cost.py`; `PAPER.md:796`; `SECTION1 §9.3` item (iv) |
| P7 | **Driver REST reachable** from the runner host for the stage-diff (else it degrades to `(None,None)` + wall-clock cross-check — which would defeat the whole point of H3). `spark_rest_url` must point at the Connect driver UI REST. Sequential cell execution is assumed. | ☐ | `live.py:899-967`; `PAPER.md:640` |
| P8 | **`study.config.json` edited for the live cluster** — set `spark_remote` to the mTLS Connect endpoint (via the egress sidecar / local mTLS proxy) and `spark_rest_url` to the driver UI REST; confirm `executor_config` matches the *actual* EKS node group (instances/price) so the $ figure is traceable. The config comment already says: "Edit before the live sweep to match the finalized Connect/k8s cluster." | ☐ | `study.config.json`; `runner.py:1172` (`make_live_factories` reads cfg) |

> **Gate:** if P2 (per-attempt serialization) or P3 (remote Arm-B green) is not
> done, **STOP** — H3.1 is un-computable and Arm B cannot contribute. If P4/P7 is
> not done, executor-seconds silently undercount / collapse to None and the run is
> invalid. If P6 is not set, do not launch.

---

## 2. How compute is measured on `live` (the D-5 stage-diff)

Per-cell, inside `ConnectExecutor.run_execute` (`live.py:837-897`):

1. `before_ids = _stage_ids_snapshot()` — GET
   `{spark_rest_url}/api/v1/applications/{app_id}/stages`, collect `stageId`s.
   `app_id` is resolved once & cached from GET `/api/v1/applications → [0]["id"]`.
2. run the cell.
3. `_stage_compute_since(before_ids)` — GET `/stages` again; **NEW+COMPLETE** =
   stages whose `stageId ∉ before_ids` **and** `status == "COMPLETE"`, then:
   - `executor_seconds = Σ executorRunTime(new) / 1000.0` (ms→s, `live.py:980`)
   - `cpu_seconds = Σ executorCpuTime(new) / 1e9` (ns→s, `live.py:981`)

Applied **uniformly** to SDP and imperative arms. Dry-run gates are driver-only →
**0 executor-seconds by construction** (`PAPER.md:239`) — that is exactly what
makes H3.1 measurable (a gated-and-rejected attempt burns ~0 executor-seconds).
Correctness relies on **sequential** cell execution and on the driver REST being
reachable; graceful fallback to `(None,None)` if `spark_rest_url` is unset or any
REST call raises. This is why **P4 (retainedStages)** and **P7 (REST reachable)**
are hard prerequisites.

---

## 3. Run command shape (the live sweep)

> The runner reads `spark_remote` / `spark_rest_url` **from `study.config.json`**
> for `--backend live` (NOT from CLI flags) — set them in P8. The `live` factory
> pings `ConnectExecutor.reachable()` and requires `ANTHROPIC_API_KEY`
> (`runner.py:1172-1180`).

**3a. Smoke test first (2-3 tasks × 1 seed × {A,B}) — proves the substrate, not a result:**
```bash
cd experiments/safe_agent_study
export ANTHROPIC_API_KEY=...            # required by the live brain
python3 -m harness.runner \
  --backend live \
  --only-arms A,B \
  --only-tasks <task1>,<task2>,<task3> \
  --max-seeds 1 \
  --per-cell-timeout 1800 \
  --out repro/h3_eks/results.h3_smoke.jsonl \
  --work-dir repro/h3_eks/.work.h3_smoke
```
Success criteria for the smoke: Arm A **and** Arm B each reach "completes green"
(§1 P3), and each executed cell carries a **non-null** `executor_seconds`
(stage-diff fired; NOT the `(None,None)` fallback). If Arm B is red or
`executor_seconds` is null, fix the prereq and re-smoke — **do not proceed.**

**3b. Cost-bounded confirmatory sweep (~40 cells, behind the §5 ceiling):**
```bash
cd experiments/safe_agent_study
export ANTHROPIC_API_KEY=...
python3 -m harness.runner \
  --backend live \
  --only-arms A,B \
  --only-tasks <cost-bounded task subset> \
  --max-seeds <k> \
  --per-cell-timeout 1800 \
  --out repro/h3_eks/results.h3_eks.jsonl \
  --work-dir repro/h3_eks/.work.h3_eks
```
Choose `<task subset> × <k seeds> × {A,B}` so total cells stay under the §5
ceiling (recommended ~40 cells). The runner writes a `*.env.json` sidecar
recording `backend=live` — the analyze step keys the measured-metric selection on
it.

---

## 4. Analyze → H3.1 / H3.2 (the H3 reader)

```bash
SPARK_HOME=$(python3 -c 'import pyspark,os;print(os.path.dirname(pyspark.__file__))') \
python3 analysis/analyze.py repro/h3_eks/results.h3_eks.jsonl \
  --assume-backend live \
  --tasks TASKS.lock.json \
  --md-out repro/h3_eks/HEADLINE.h3_eks.md \
  --json-out repro/h3_eks/REPORT.h3_eks.json
```

Why `--assume-backend live` is load-bearing: `resolve_h2_metric`
(`analyze.py:534-535, 548+`) keys the compute-to-correct field on the backend and
**refuses to guess** (`H2MetricSelectionError`). On `live`/REMOTE it selects the
**measured** field `executor_seconds_to_correct` (the stage-diff), which is
cross-arm comparable because every arm is measured by the same mechanism. On
`local` it would instead select the wall-clock proxy — the very thing H3 is
meant to replace. The env sidecar already carries `backend=live`;
`--assume-backend live` is the explicit belt-and-suspenders opt-in.

- **H3.1 (wasted compute on failed attempts)** — requires the **P2** per-attempt
  fields in `per_iteration`; the analyze.py H3 reader sums executor-seconds on
  *failed* attempts per arm (gate-rejected SDP attempts ≈ 0 by construction vs
  imperative failures that executed). **Blocked until P2 is merged.**
- **H3.2 (total compute-to-correct)** — the measured `executor_seconds_to_correct`
  A-vs-B, paired by (task, seed), with median / mean / 95% CI / total $ saved,
  reported jointly with **H5.3 cost-adjusted efficacy** (`PAPER.md:191`: extra
  compute is only a true cost if it does not buy completion → report
  cost-per-correct-completion).

**Do not** populate H3.1/H3.2 in the paper from anything but this measured `live`
output. Until this run exists, both remain marked NOT-measured in `PAPER.md` §6.2
and `SECTION1` §5.

---

## 5. Spend-estimate structure (cost-bounded N)

There is no code spend-cap; N is bounded operationally. Estimate **before**
launch and pick N so total ≤ `$ CEILING`.

**Per-cell cost = EKS compute + token cost:**

```
per_cell_usd ≈ token_usd(arm) + compute_usd(arm)

compute_usd(arm)  = (Σ_iter executor_seconds / 3600) × price_usd_per_executor_hour
                    (price = executor_config.price_usd_per_executor_hour = $0.192/exec-hr, DECLARED)
                    NOTE: on live this uses the MEASURED stage-diff executor_seconds
                    (not the wall-clock proxy). No prior live A-vs-B magnitude exists —
                    calibrate from the §3a smoke, do NOT assume the local proxy numbers.

token_usd(arm)    = priced from the model's per-token rate × tokens-to-correct
                    (local medians for orientation ONLY, not a live estimate:
                     A ≈ 11,524; B ≈ 26,480 median total tokens — B ~2.3× A).

infra_usd (amortized, not per-cell): EKS control plane ~$73/mo + executor/system
                    nodes (scale-to-zero when idle) + RDS + NLB + S3
                    (deploy/eks/RUNBOOK.md §10). Keep the cluster up only for the
                    run window; scale node groups to zero after.
```

**Bounded-N recipe:**
```
1. Run §3a smoke (6 cells: 3 tasks × 1 seed × {A,B}); read measured
   executor_seconds + tokens per cell from results.h3_smoke.jsonl.
2. per_cell_usd_est = max over arms of (token_usd + compute_usd) from the smoke.
3. N_max = floor(($ CEILING − infra_for_window) / per_cell_usd_est).
4. Pick task×seed subset with cells = min(N_max, ~40), balanced across {A,B}.
5. Launch §3b only if cells × per_cell_usd_est ≤ $ CEILING.
```

`$ CEILING` is a human decision recorded at launch (P6). Recommended default
scope: EKS smoke (2-3 tasks × 1 seed × A,B) then a **~40-cell** cost-bounded run.

---

## 6. References

- Bring-up: `deploy/eks/RUNBOOK.md` §3 (deploy order), §4 (certs/PSK), §5
  (onboarding/schema), §6 (data load + run pipelines), §10 (cost).
- Live-run bugfix arc (D-2/D-3/D-4/D-5): `DEVIATIONS.md` (154-249 blocker chain;
  184-185, 205-215 EKS history; 364-368 the 80M stage-diff probe; 575-593 the
  non-comparable-local-substrate note; 409-417 retainedStages).
- Measurement code: `harness/backends/live.py` (`ConnectExecutor.run_execute`,
  `_stage_ids_snapshot`, `_stage_compute_since`, 837-897 / 916-993),
  `harness/cost.py` (`execute_iteration_cost`, proxy formula), `harness/runner.py`
  (`make_live_factories` 1170-1193; per-attempt gap 253-260),
  `analysis/analyze.py` (`resolve_h2_metric` 534+, H3/compute).
- Methodology / paper: `PAPER.md` §3.5 (N1/N2), §4.3 (compute text L146), §6.2
  (H3.1/H3.2), §6.5 (substrate split), §6.6(3) (instrument change), §6.7 (Phase
  2b), §6.9, §5 (threat); `SECTION1_DATA_AND_METHODOLOGY.md` §5, §9.3.
- Locked state: `memory/safe-spark-agents-state.md`; `PLANNING_BRIEF.md`.
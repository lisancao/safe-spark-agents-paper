# SECTION 1 — Data & Methodology Provenance
### Traceability / reproducibility companion to `PAPER.md` Section 1 (Imperative vs SDP)

**Status:** living provenance record · **Compiled:** 2026-06-29 · **Scope:** Section 1 ONLY (the A-vs-B study, `PAPER.md` §1–§7). Not §2/§3/§4.

**What this document is.** Every number in `PAPER.md` Section 1 must trace to (a) a named raw-data file, (b) a specific field, (c) an exact recompute command that anyone can run to regenerate it, and (d) a file:line citation for the instrument code that produced it. This document is that map. It is **read-only with respect to the paper's claims** (BUILD_PROGRAM.md anti-drift rule #1) — it *backs* claims, it does not restate or change them. Where a cited paper figure does not reproduce from the raw data, it is logged in §7 (Discrepancies), not silently edited.

**How to trust it.** Every value below was regenerated on 2026-06-29 by running the quoted command against the committed raw data (see §2). Re-run any command to verify.

---

## 1. The locked design these data must serve (`PAPER.md` §6.1, F1 — quoted)

- **A** = bare imperative PySpark — no gate, no skills.
- **B** = SDP — framework structural dry-run gate + `pyspark-sdp` API skill. **No safety skill.**
- Headline contrast is **A vs B**. `A2`, `B1`, `B2`, `spark-safety` are retired from the headline and survive only as pilot context / ablation evidence.
- Framing F1 (locked): the dry-run gate is **intrinsic to the declarative paradigm**, not a knob added to SDP; imperative has no native structural gate. This is *why* H1's headline is structural-catch, with the silent-defect rate demoted to the predicted-null control (H1.3).

The pilot data below were collected under the **earlier 5-arm pre-registration** (A, A2, B, B1, B2). They remain valid as pilot context; the clean 2-arm headline (A vs B) is produced in Phase 2a (§8). All five arms are reported here for full traceability, with the locked 2-arm reading called out per hypothesis.

---

## 2. Raw data sources (ground truth)

All canonical raw data lives on the git branch **`origin/data/raw-export`**, under
`experiments/safe_agent_study/data/raw_20260628/`. A local mirror is `/home/lnc/repos/sasa_raw_data_20260628.tar.gz` (1.4 MB, 2026-06-28).

| file | rows | arms | seeds | role |
|---|---:|---|---|---|
| `results.full5.jsonl` | **330** | A, A2, B, B1, B2 | 42, 1337, 2718 | **canonical 5-arm pilot** (22 tasks × 3 seeds × 5 arms). Source for §4.1, §4.3, H1.3, H4, H5. |
| `all_results.jsonl` | 396 | (as above + dup A2) | 42, 1337, 2718 | every kept row, `_source`-tagged; A2 appears in two sweeps (132 rows) so this is **not** de-duplicated — prefer `results.full5.jsonl` for per-arm reads. |
| `by_sweep/A2_rerun_instr_v3.1.jsonl` | 66 | A2 | 42,1337,2718 | A2 re-run on `instrument-v3.1` (`git 1d28563`) after the dead-session read-back fix (#33). |
| `by_sweep/B_B1_primary_seed42.jsonl` | 66 | A2,B,B1 | 42 | primary B/B1 sweep, seed 42 (`git 295d725`). |
| `by_sweep/B_B1_primary_multiseed.jsonl` | 132 | A2,B,B1 | 1337,2718 | primary B/B1 sweep, seeds 1337/2718 (`git 54834a1`). |
| `by_sweep/A2_D3_racefix.jsonl` | 13 | A2 | 1337,2718 | D3 SparkContext-race fix rows (`git 9cc6342`). |
| `by_sweep/pilot_A_B_B2_seed42.jsonl` | 39 | A,B,B2 | 42 | original single-seed pilot (`git c539359`). |
| `results.full5.jsonl` transcripts | — | — | — | `raw_20260628/transcripts.tar.gz` — per-run agent transcripts; each row's `transcript_path` resolves inside it. |

Local analysis bundle (mirror of the de-duplicated A2-rerun + clean B/B1): `experiments/safe_agent_study/results/h3_a2_rerun_20260628/` — `results.h3_combined.jsonl` (198 rows), `HEADLINE.md/json` (the inferential output from `analyze.py`), `README.md`, `QUARANTINE.md`.

**Data authenticity:** forensic audit in `experiments/safe_agent_study/LAB_NOTEBOOK.md` (2026-06-28). Per-sweep `git_sha` stamps in `raw_20260628/MANIFEST.md`. Each result row also carries its own `git_sha`, `spark_version`, `image_digest`, `timestamp_utc`, `seed`, and `run_id` for row-level provenance.

### 2.1 Getting the data
```bash
cd /home/lnc/repos/safe-spark-agents
git fetch origin data/raw-export                          # ensure the branch is local
# stream any raw file without checkout:
git show origin/data/raw-export:experiments/safe_agent_study/data/raw_20260628/results.full5.jsonl | head -1
```

---

## 3. Instrument & environment (cited)

| component | file:line (on `instrument/surgical-fair` / `origin/dev`) | role |
|---|---|---|
| Runner / episode loop | `harness/runner.py` (CLI argparse `:1321-1344`; profile build `_build_profile :455-523`; local backend bringup `:1401-1425`) | orchestrates (task, arm, seed) cells; writes `results.jsonl`. |
| Imperative backend | `harness/backends/local.py` (`LocalSparkExecutor`; in-memory catalog `:312-316`; parquet read-back `read_output_path :338-342`; agent program exec `_run_agent_program :384`) | arm A: classic local[*] Spark, output to parquet path. |
| SDP backend | `harness/backends/local_connect.py` (`LocalConnectServer.start :140`; reuse-if-reachable `:149`) + `harness/backends/live.py` (Connect executor) | arms B/B1: local Spark Connect server, in-memory catalog → warehouse parquet. |
| Dry-run gate | `harness/sdp_dryrun.py` (`run_dry_run :460-533`; graph-aware pre-seed `preseed_sibling_schemas :441-457`) invoked from `live.py run_gate :788-820` | B's structural gate; produces `dry_run_intercepts`. |
| Compute measurement | `harness/backends/live.py` (per-cell stage-diff `:852-860`) + `harness/cost.py` | `executor_seconds` / `cpu_seconds` (SDP/Connect path). |
| Conciseness metrics | `experiments/safe_agent_study/program_metrics.py` (`final_program_loc`, `final_program_loc_body`, `ast_node_count`, `ast_node_count_body`) | H4. |
| Grading / oracles | `harness/output_oracles.py` (`build_output_profile`) + `runner.py grade_run` | `silent_defect`, `defect_classes`, `per_defect_detection`, `reached_correct`. |
| Statistical analysis | `analysis/analyze.py` (`:956-971` argparse; GLMM `:436-481`; paired bootstrap `:295-334`; McNemar `:403-404`) | per-arm rates + bootstrap CIs + GLMM + Holm + power-rule N. |
| Result schema | `results_schema.json` (`silent_defect :59`, `defect_classes :62`, `reached_correct :114`, etc.) | field contract for every row. |

**Environment (row-stamped, verified this session):** model `claude-opus-4-8` (`study.config.json:4`); Spark `4.1.0.dev4`; corpus `TASKS.lock.json` v3.0.0-corpus22 (**22 tasks**, 7 Low / 8 Med / 7 High); seeds `SEEDS.lock.json` v1.1.0-power (12 locked; pilot used first 3 = 42/1337/2718). Defect taxonomy D1–D9: structural (D1/D4/D5, gate-catchable) vs semantic/silent (D2/D6/D7/D8, un-gateable) vs state (D3/D9) — `PAPER.md` §3.2.

---

## 4. Verified per-arm results (canonical: `results.full5.jsonl`, N=3, 22 tasks)

Regenerated 2026-06-29 by `/tmp/recompute_arms.py` (see §6) against `results.full5.jsonl`:

| arm | n | completed | silent_defect | correct-completion | dry_run_intercepts | LOC median (n) | AST median (n) | tokens populated | exec_sec populated |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **A** (bare imperative) | 66 | 63 | **18** | **45** | 0 | 144 (63) | 1067 (63) | 66 | 2 |
| A2 (imp+gate+skill) | 66 | 61 | 23 | 38 | 2 | 113 (61) | 910 (61) | 66 | 1 |
| **B** (SDP+gate, no safety target) | 66 | 59 | **23** | **36** | **74** | 62 (42) | 550 (42) | 0 | 60 |
| B1 (SDP only) | 66 | 59 | 23 | 36 | 0 | 68.5 (42) | 500.5 (42) | 0 | 61 |
| B2 (imp+gate) | 66 | 62 | 17 | 45 | 4 | 125 (62) | 1020 (62) | 66 | 1 |

> Caveat carried into every per-arm read: B/B1 `final_program_loc` is populated on only 42/66 rows (gate-intercepted / non-completing attempts have no final program), and `executor_seconds` is populated on B/B1 (Connect path) but ~0 on imperative (classic Spark, no Connect REST). These are not data losses — they are the substrate split (§3, `PAPER.md` §6.5) and the token-gap (§4.3), and they are exactly what the clean Phase-2a/2b runs close.

---

## 5. Per-hypothesis methodology, raw data, and recompute (each and every one)

For each hypothesis: **claim** (paraphrased from `PAPER.md` §6.2) · **metric/field** · **instrument** · **data** · **recompute** (verified) · **value** · **status** · **threats**. All recompute commands assume `cwd = /home/lnc/repos/safe-spark-agents` and pipe the raw file into Python; substitute another file to re-read a different sweep.

`RAW="origin/data/raw-export:experiments/safe_agent_study/data/raw_20260628/results.full5.jsonl"` (used below).

### H1 — SAFETY (headline)

#### H1.1 Structural-catch at the gate
- **Claim:** SDP catches structural defects (D1/D4/D5) at the dry-run gate, pre-execution; bare imperative has no gate, so they surface at runtime or ship.
- **Metric/field:** `dry_run_intercepts` (count of proposals rejected by the structural gate before execute).
- **Instrument:** `harness/sdp_dryrun.py:460-533` (real framework dry-run) via `live.py run_gate:788-820`; A has no gate (0 by construction).
- **Data:** `results.full5.jsonl` (B intercepts here) and `results.h3_combined.jsonl` (A2 contrast).
- **Recompute:**
  ```bash
  git show $RAW | python3 -c "import sys,json;r=[json.loads(l) for l in sys.stdin if l.strip()];print({a:sum((x.get('dry_run_intercepts') or 0) for x in r if x['arm']==a) for a in ('A','A2','B','B1','B2')})"
  ```
- **Value (verified):** `{A:0, A2:2, B:74, B1:0, B2:4}`. Locked read **A=0 vs B=74**.
- **Status:** strongly suggested at N=3; clean A-vs-B pending Phase 2a (B re-run with gate on).
- **Threats:** B1=0 confirms the intercepts come from the gate, not SDP per se. A2/B2 small counts are the imperative agent-owned `--analyze-only` gate, **retired** (paradigm-invented; `PAPER.md` §4.2 F1).

#### H1.2 Failure-mode shift
- **Claim:** SDP failures concentrate at gate-time (before data touched); imperative's at runtime or as silent ships.
- **Metric/field:** distribution of `exit_class` × `detection_stage`.
- **Instrument:** `runner.py grade_run`; fields `exit_class`, `detection_stage`.
- **Recompute:**
  ```bash
  git show $RAW | python3 -c "import sys,json,collections;r=[json.loads(l) for l in sys.stdin if l.strip()];
  print({a:dict(collections.Counter((x.get('exit_class'),x.get('detection_stage')) for x in r if x['arm']==a)) for a in ('A','B')})"
  ```
- **Value:** computable now (distribution); not yet reduced to a single headline statistic.
- **Status:** **pending** a chosen summary statistic + clean A-vs-B.

#### H1.3 Silent-residue invariance (predicted NULL / control)
- **Claim:** semantic defects (D2/D6/D7/D8) are un-gateable in any paradigm → silent-defect rate ≈ equal A vs B.
- **Metric/field:** `silent_defect` (bool; graded by the output oracle — completed run, no error, wrong data).
- **Instrument:** `harness/output_oracles.py build_output_profile`; schema `results_schema.json:59`.
- **Recompute:**
  ```bash
  git show $RAW | python3 -c "import sys,json;r=[json.loads(l) for l in sys.stdin if l.strip()];print({a:(sum(x['silent_defect'] for x in r if x['arm']==a), sum(1 for x in r if x['arm']==a)) for a in ('A','A2','B','B1','B2')})"
  ```
- **Value (verified):** `{A:(18,66), A2:(23,66), B:(23,66), B1:(23,66), B2:(17,66)}`. Locked read **A=18/66 (0.27) vs B=23/66 (0.35)** — comparable, no clean SDP advantage. The A2=B=B1 exact tie at 23 is a notable artifact (and confirms `spark-safety` moved it by 0.000, motivating its removal).
- **Independent replication (this session, N=3 tasks, seed 42, gate-off instrument-validation smoke):** A=2/3, B=2/3 — same direction.
- **Status:** the predicted null is **supported**; this is a control, not the headline. Endpoint needs large N to bound the interval but is uninformative per §3.2.
- **Threats:** underpowered (N=3; required N≈252 for half-width ≤0.05 — see §6 power rule).

### H2 — TOKEN COST (LLM effort to reach correct)

#### H2.1 Tokens-to-correct
- **Claim:** total input+output tokens to a correct pipeline, A vs B.
- **Metric/field:** `sum(per_iteration[:iterations_to_green].tokens.{input,output})` over `reached_correct` rows; also `input_tokens`/`output_tokens` row totals.
- **Instrument:** `runner.py` per-iteration token capture; cost accounting `PAPER.md` §6.9.
- **Recompute (populated check):**
  ```bash
  git show $RAW | python3 -c "import sys,json;r=[json.loads(l) for l in sys.stdin if l.strip()];print({a:sum(x.get('input_tokens') is not None for x in r if x['arm']==a) for a in ('A','A2','B','B1','B2')})"
  ```
- **Value (verified):** populated `{A:66, A2:66, B2:66, B:0, B1:0}`. **A has tokens; B/B1 do not** (pre-token sweep).
- **Status:** **NOT computable A-vs-B yet** — needs the B re-run (token logging already works on the current instrument; §6.6(2)). Direction OPEN.

#### H2.2 Iterations-to-correct (honest counter-signal)
- **Claim:** SDP may use MORE agent loops (pilot median 3 vs 1).
- **Metric/field:** `iterations`, `iterations_to_green`.
- **Recompute:**
  ```bash
  git show $RAW | python3 -c "import sys,json,statistics;r=[json.loads(l) for l in sys.stdin if l.strip()];print({a:statistics.median([x['iterations'] for x in r if x['arm']==a]) for a in ('A','B')})"
  ```
- **Status:** computable now (counter-signal); interpret **jointly** with H5.3 (cost-per-correct-completion), not as a standalone verdict.

### H3 — COMPUTE COST (data-processing; cluster-relevant)

#### H3.1 Wasted-compute-on-failed-attempts
- **Claim:** SDP's gate rejects failed attempts before execution (~0 data processed); imperative failures execute and burn compute.
- **Metric/field:** per-attempt `executor_seconds`/`cpu_seconds` on failed attempts; gate-caught attempts contribute ~0.
- **Instrument:** `live.py:852-860` stage-diff; **requires** per-attempt serialization (`PAPER.md` §6.6(3), `runner.py::run_episode`).
- **Recompute (populated check):**
  ```bash
  git show $RAW | python3 -c "import sys,json;r=[json.loads(l) for l in sys.stdin if l.strip()];print({a:sum(x.get('executor_seconds') is not None for x in r if x['arm']==a) for a in ('A','A2','B','B1','B2')})"
  ```
- **Value (verified):** populated `{A:2, A2:1, B:60, B1:61, B2:1}`.
- **Status:** **NOT measurable** from this data — no per-attempt compute, and arms ran on different substrates (imperative classic Spark vs SDP Connect). Needs §6.6(3) **and** a uniform Connect substrate (Phase 2b).

#### H3.2 Total-compute-to-correct
- **Claim:** total data-processing compute to a correct pipeline, A vs B. Direction OPEN.
- **Status:** **NOT measurable** yet (same blockers as H3.1). Pilot wall-clock proxy (18.5s vs 10.2s) is substrate-confounded and is **not** data-compute — do not cite as the compute result.

### H4 — CONCISENESS

#### H4.1 Lines of code
- **Claim:** SDP fewer lines.
- **Metric/field:** `final_program_loc` (and `final_program_loc_body`).
- **Instrument:** `program_metrics.py`.
- **Recompute:**
  ```bash
  git show $RAW | python3 -c "import sys,json,statistics;r=[json.loads(l) for l in sys.stdin if l.strip()];
  print({a:(statistics.median([x['final_program_loc'] for x in r if x['arm']==a and x.get('final_program_loc') is not None]),
            sum(x.get('final_program_loc') is not None for x in r if x['arm']==a)) for a in ('A','A2','B','B1','B2')})"
  ```
- **Value (verified):** medians `A=144 (n63)`, `A2=113 (n61)`, `B=62 (n42)`, `B1=68.5 (n42)`, `B2=125 (n62)`. **Locked A-vs-B: 144 → 62 ≈ 57% fewer.** B-vs-A2: 113 → 62 ≈ 45% fewer.
- **Status:** **SUPPORTED.** (See §7 discrepancy on the paper's "68 vs 117" figure.)
- **Threats:** B median is over n=42 (only completed-with-final-program rows). The rigorous version is the paired-by-task contrast in `analyze.py`; the directional result is robust across both the A and A2 baselines.

#### H4.2 AST node count
- **Metric/field:** `ast_node_count` (and `ast_node_count_body`).
- **Recompute:** as H4.1, field `ast_node_count`.
- **Value (verified):** medians `A=1067`, `A2=910`, `B=550`, `B1=500.5`, `B2=1020`. **A-vs-B: 1067 → 550 ≈ 48% fewer.** B-vs-A2: 910 → 550 ≈ 40% fewer.
- **Status:** **SUPPORTED.**

### H5 — EFFICACY (does the agent get the job done?)

#### H5.1 Completion rate
- **Claim:** fraction reaching a materialized/completed output, A vs B.
- **Metric/field:** `exit_class == 'completed'`.
- **Recompute:**
  ```bash
  git show $RAW | python3 -c "import sys,json;r=[json.loads(l) for l in sys.stdin if l.strip()];print({a:(sum(x.get('exit_class')=='completed' for x in r if x['arm']==a), sum(1 for x in r if x['arm']==a)) for a in ('A','B')})"
  ```
- **Value (verified):** A=63/66 (0.95), B=59/66 (0.89).
- **Status:** computable; clean A-vs-B pending Phase 2a.

#### H5.2 Correct-completion rate (the real "job done")
- **Claim:** fraction reaching a CORRECT completed output: `exit_class=='completed' AND silent_defect==false` (cross-check `reached_correct`).
- **Recompute:**
  ```bash
  git show $RAW | python3 -c "import sys,json;r=[json.loads(l) for l in sys.stdin if l.strip()];print({a:(sum(x.get('exit_class')=='completed' and not x.get('silent_defect') for x in r if x['arm']==a), sum(1 for x in r if x['arm']==a)) for a in ('A','A2','B','B1','B2')})"
  ```
- **Value (verified):** `{A:(45,66), A2:(38,66), B:(36,66), B1:(36,66), B2:(45,66)}`. **Locked A-vs-B: A=45/66 (0.68) vs B=36/66 (0.55) — A higher.** (A2/B/B1 match `PAPER.md` §6.2 H5.2 pilot exactly.)
- **Status:** a real, direction-neutral finding; clean A-vs-B pending Phase 2a. This is the outcome H2/H3 must be scored *relative to* (H5.3).
- **Threats:** depends on `silent_defect` grading (H1.3 oracle) and completion; underpowered.

#### H5.3 Cost-adjusted efficacy
- **Claim:** report cost-per-correct-completion (tokens / iterations / compute per successful job), so "SDP iterates more" is weighed against "SDP finishes more."
- **Metric:** H2/H3 numerators ÷ H5.2 successes.
- **Status:** **blocked on H2 (B tokens) and H3 (per-attempt compute on uniform substrate).** Methodology fixed; numbers pending Phase 2a/2b.

### Control / rejected (for completeness, `PAPER.md` §6.2.1)
- **REJECTED — "less surface ⇒ fewer TOTAL defects":** CONTRADICTED. Total detected defects A2=27, B=48, B1=46 (the gate *exposes* errors rather than hiding them). Recompute via `defect_classes` length per arm. Reported as a negative result.

---

## 6. Reproduction quickstart

**Recompute every per-arm number in §4** (the helper used to compile this doc):
```bash
cd /home/lnc/repos/safe-spark-agents
git show origin/data/raw-export:experiments/safe_agent_study/data/raw_20260628/results.full5.jsonl \
  | python3 /tmp/recompute_arms.py     # script body archived in §6.1 below
```

**Inferential analysis (CIs, GLMM, Holm, power-rule N)** — the canonical statistical path:
```bash
cd /home/lnc/repos/safe-spark-agents/experiments/safe_agent_study
python3 analysis/analyze.py results/h3_a2_rerun_20260628/results.h3_combined.jsonl \
  --tasks TASKS.lock.json --assume-backend local \
  --md-out /tmp/headline.md --json-out /tmp/headline.json
# canonical pre-computed output: results/h3_a2_rerun_20260628/HEADLINE.{md,json}
```

**Fresh calibration run (Phase 1, `PAPER.md` §6.8 literal command):**
```bash
cd /home/lnc/repos/safe-spark-agents/experiments/safe_agent_study
SPARK_HOME=/home/lnc/.local/lib/python3.12/site-packages/pyspark \
python3 harness/runner.py \
  --backend local --config study.config.json --arms-dir arms \
  --tasks TASKS.lock.json --seeds SEEDS.lock.json \
  --only-tasks orders_silver_gold,p1_medallion,p2_cdc \
  --only-arms A,B --max-seeds 3 \
  --out results.calibration.local.n3.jsonl \
  --work-dir .work.calibration.local.n3 --per-cell-timeout 1800
```
> Substrate note (verified 2026-06-29): the local backend starts its OWN Spark Connect server on `--local-connect-port`. Use a FREE port (e.g. 15030) — port 15002 is a pre-existing docker-cluster Connect server, and the harness will *reuse-if-reachable* (`local_connect.py:149`), which silently runs against the wrong, mis-configured server. UI port must also be free (avoid 4040).

### 6.1 `recompute_arms.py` (archived verbatim for reproducibility)
Reads result rows on stdin, prints per-arm n/tasks/seeds, completed, silent, correct-completion, intercepts, LOC/AST medians (with n), and token/exec-sec populated counts. Stored at `/tmp/recompute_arms.py` this session; the one-liners in §5 reproduce each column independently without it.

---

## 7. Discrepancies & corrections log (honest reconciliation)

| # | Paper statement | Verified from raw data | Resolution |
|---|---|---|---|
| D1 | H4.1 "~42% fewer (68 vs 117)" (`PAPER.md` §6.2 H4.1) | A-vs-B median LOC = **144 vs 62 (~57%)**; B-vs-A2 = **113 vs 62 (~45%)**. No (68,117) pair reproduces from `results.full5.jsonl`. | The *direction and "SUPPORTED" verdict hold and are stronger* than stated. The specific "(68 vs 117)" pair likely originates from the earlier 13-task pilot; it should be refreshed to the verified 22-task figures at the next paper-revision gate. **Not edited here** (anti-drift rule #1). |
| D2 | §4.3 "only A2 has token data" | tokens populated for **A, A2, B2** (66/66 each); B/B1 = 0. | A also has tokens. The operative gap for H2 is **B/B1**, which is what matters (A-vs-B needs B). Wording could be tightened to "B/B1 lack token data" at the revision gate. |
| D3 | `all_results.jsonl` 316 rows (MANIFEST) vs 396 observed | `all_results.jsonl` = **396** rows (A2 duplicated across two sweeps → 132). MANIFEST's 316 predates a later append. | Use `results.full5.jsonl` (330, de-duplicated 5×66) for per-arm reads; `all_results.jsonl` only with `_source` filtering. |

No discrepancy was found for the headline H1 numbers (silent-defect, dry-run intercepts) or H5.2 — all reproduce exactly.

---

## 8. What is clean vs what must run (maps to `PAPER.md` §6.4 / §6.7)

| outcome | clean now? | source | what's needed |
|---|---|---|---|
| H1.1 structural-catch | suggestive (N=3) | full5 / h3_combined | clean A-vs-B: re-run **B (gate on, no safety)** on current instrument; pair with existing clean A. |
| H1.3 silent residue (control) | yes (N=3) | full5 | larger N only to bound the interval. |
| H4 conciseness | yes (N=3) | full5 | refresh paper figure (D1). |
| H5.1/H5.2 efficacy | yes (N=3) | full5 | clean A-vs-B at N\*. |
| H2 tokens | A only | full5 | **B re-run** (token logging already on). |
| H3 compute | no | — | per-attempt compute (§6.6(3)) + **uniform Connect substrate** (Phase 2b). |

**Bottom line for Section 1:** A is already clean (silent-defect, LOC/AST, tokens, completion). The entire remaining critical path is **a clean B** under the locked definition (SDP + gate + `pyspark-sdp`, no safety), paired with existing A → the A-vs-B headline; plus a uniform-substrate A+B run for the compute claim.

### 8.1 Instrument status (this session, 2026-06-29)
- B reverted to the locked definition in `arms/B.json` (`dry_run_gate=true`, `safety_skill=false`, `skills=["pyspark-sdp"]`).
- The dry-run gate, previously believed to hang, was **verified to run clean in ~1s** in isolation against a correctly-started harness Connect server (`sdp_dryrun.py` → "Run is COMPLETED", exit 0). The earlier "hang" was the port-15002 reuse trap (§6 substrate note), not a gate defect.
- End-to-end locked-B (gate on, safety off) **verified to complete** through the runner: the exact cell that previously timed out now runs in 1 iteration (`exit_class=completed`).
- **Brain fix (root cause of the earlier B timeout):** opus ADAPTIVE thinking shares the `max_tokens` budget; at 16000 it exhausted the budget before emitting the code module → empty proposal → no-code timeout (acute on the no-safety arm, whose deliberation runs longer without the skill's idioms). Fixed by switching `_messages_create` to the **STREAMING** path and raising `max_tokens` 16000 → 32000 (`live.py`).
- **Instrument frozen** at commit `ca48c8c` on `instrument/surgical-fair` for the clean run (recipe in §9.3).

---

## 9. Pre-registered error-taxonomy outcomes (registered 2026-06-29, BEFORE the clean run)

*Registered per the anti-drift order — **define metrics → run → measure**. Every value below is produced ONLY from the clean A-vs-B run on the frozen instrument (§9.3). **NONE is computed from the pre-fairness N=3 data in §4** (that is pilot context, caveated). The silent-defect rate is the predicted-null **CONTROL**, not the headline — the headline is **where structural and runtime errors are caught**.*

### 9.1 Primary outcomes
| # | Outcome | Definition / field | D-group | Role |
|---|---|---|---|---|
| **P1** | **Structural-error catch-stage** | per arm: structural error events + stage caught (`dry_run` / `runtime` / `shipped`) | D1/D4/D5 | **H1.1 headline** |
| **P2** | **Runtime-error rate & stage** | per arm: execution-time failures by `error_class` + stage (`runtime` / `shipped`) | runtime exec errors | headline |
| **P3** | **Detection-stage distribution** | per arm: all error/defect events across {`dry_run`, `runtime`, `never`} | all | **H1.2 failure-mode shift** |
| **P4** | **Silent-defect rate** | per arm: `silent_defect` | D2/D6/D7/D8 | **CONTROL (H1.3), predicted ≈ equal** |
| **P5** | **Total detected-defect count** | per arm: count of detected defects | all | negative result (§6.2.1 — SDP may surface MORE) |
| **P6** | Efficacy | completion + correct-completion (`exit_class`, `reached_correct`) | — | H5 |
| **P7** | Cost | tokens-to-correct; per-attempt compute (Phase 2b) | — | H2/H3 |

### 9.2 Measurement rules (anti-bypass — registered)
- **Iteration-level counting.** Every error event across ALL iterations of a cell is counted and tagged by stage — NOT just the final cell outcome. A structural error caught at the gate and then fixed by the agent **still counts** (tagged `dry_run`-caught). This is the guarantee that the gate — or any "safety" mechanism — **cannot make a structural/runtime error vanish from the measurement**. If the design ever lets an error go unmeasured, that is a design failure, not a result.
- **Stage taxonomy → fields:** `per_iteration[].gate.error_class` → caught at gate (structural/`dry_run`); `per_iteration[].execute.error_class` → caught at `runtime`; `per_defect_detection` (values `dry_run`/`runtime`/`never`) → per-defect stage; `dry_run_intercepts` → structural-catch count; cell-level `detection_stage`.
- **Grouping:** structural = D1/D4/D5; semantic/silent = D2/D6/D7/D8; state = D3/D9.
- **No retrofit.** All values come from the clean run's `results.jsonl`. The pre-fairness N=3 data (§4) is never used to compute these outcomes.

### 9.3 Frozen execution recipe (registered)

> **Reproduction.** The full runbook + self-contained scripts + the Spark-4.1.0.dev4 baseline table live in the repo at `experiments/safe_agent_study/repro/` (`REPRODUCE.md`). It reproduces the primary run, the backfill, the **D7 skill-attribution test** (`repro/tzfix_d7_test/` — 7→0), and the per-defect composition. Re-run on a **new Spark version** by swapping `pyspark` and diffing against the baseline table (the instrument stays frozen; Spark is the variable). Key watch-items for 4.2: native AutoCDC (7 CDC/SCD tasks) and a symmetric declarative `session.timeZone` pin (would close the D7 framework gap on its own).
- **Instrument:** `safe-spark-agents` @ branch `instrument/surgical-fair`, commit **`ca48c8c`** (frozen). Arm **A** = bare imperative (no gate, no skills); Arm **B** = SDP + dry-run gate + `pyspark-sdp` (no safety). Brain: **streaming, `max_tokens=32000`**.
- **Substrate:** `--backend local` (imperative → classic local Spark; SDP → harness-started local Connect server). Ports: a FREE Connect port (NEVER 15002 — the docker-cluster server the harness would silently reuse) + a free UI port.
- **Calibration (this session — me):**
  ```bash
  cd /home/lnc/repos/safe-spark-agents/experiments/safe_agent_study
  SPARK_HOME=<pyspark> python3 -m harness.runner --backend local \
    --only-arms A,B --only-tasks orders_silver_gold,p1_medallion,p2_cdc --max-seeds 3 \
    --local-connect-port <free> --local-ui-port <free> \
    --out results.calibration.local.n3.jsonl --work-dir .work.calibration.local.n3 --per-cell-timeout 1800
  python3 analysis/analyze.py results.calibration.local.n3.jsonl --tasks TASKS.lock.json
  ```
  Output: per-cell token + compute cost, pilot effect sizes, projected **N\*** + dollar figure for GATE 1.
- **GATE 1 (Lisa):** approve N\* + projected cost before the powered run.
- **Powered run (Polly, post-GATE-1):** the *same* command, full 22 tasks × **N\*=12 seeds** (GATE-1 approved; ~$430 budget ceiling), against tag **`instrument-v3.2-frozen`** (`ca48c8c`). Polly is a **frozen-recipe executor**: produce raw `results.jsonl` + `analyze.py` report ONLY — no interpretation, no paper edits, no metric changes; hard stop on any instrument anomaly.
- **Interpretation + paper-binding:** me + Lisa only; never delegated (that is where drift lives).
- **Phase 2b (H3 compute — deferred to a separate EKS run):** H3 is **not** produced by the local run above (the substrate is split — imperative on classic Spark, SDP on Connect — so executor-seconds are not cross-arm comparable). It runs on **EKS uniform Connect (both arms)**, contingent on: **(i)** per-attempt compute serialized into `per_iteration` (§6.6.3, ~10-line runner change + an analyze.py H3 reader); **(ii)** the remote **Arm-B SDP completion+grading** blocker resolved (§6.5 / reference-arch layer L3 — Arm A + the stage-diff measurement path are already demonstrated on EKS); **(iii)** **`spark.ui.retainedStages` raised well above the run's total stage count** — the default 1000 evicts old stages and *silently undercounts* the stage-diff executor-seconds H3 depends on (observed 2026-06-30: 2a's long-lived local Connect server reached 929/1000 stages); **(iv)** a cost-bounded task/seed subset behind its own spend GATE; **(v)** on a **real catalog** (Unity Catalog OSS / Iceberg on the cluster — unlike the local `spark.sql.catalogImplementation=in-memory` used for Phase 2a), SDP arm-B's full-refresh **truncates the target tables on each `run`**, so the reconcile/grade path must tolerate those truncate semantics (a stale or schema-mismatched table can wedge the re-materialize). Confirmed 2026-07-02 that this is a **UC-OSS-only** hazard — it does **not** affect the local in-memory-catalog Phase-2a run. Tracked in the Phase-2b prereqs.

---

*Provenance of this document: every value in §4 and §5 was regenerated on 2026-06-29 from `origin/data/raw-export:.../results.full5.jsonl` using the quoted commands; §9 outcomes are pre-registered and produced only from the clean run on frozen instrument `ca48c8c`; instrument citations verified against branch `instrument/surgical-fair`. This file backs `PAPER.md` Section 1 and changes no claim within it.*

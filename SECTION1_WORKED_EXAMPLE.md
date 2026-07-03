# SECTION 1 — Worked Example: one task, end to end
### Companion to `PAPER.md` §3 (Methods). Appendix-ready (proposed §3.6 / Appendix A).

**Status:** appendix draft · **Compiled:** 2026-06-30 · **Scope:** one real `(task, arm, seed)` triad, shown to make the §3 taxonomy concrete.

**Why this exists.** §3 defines the defect taxonomy, the `silent_defect` predicate, and the `detection_stage` enum *abstractly*, with code citations. This appendix instantiates all of it in **one real cell** — `orders_silver_gold`, seed 42, arms A and B — so a reader can *see* what "silent defect," "`detection_stage = never`," and "structural vs semantic" mean in practice, and why a structural gate cannot close the gap. Every value below is pulled **verbatim** from the powered run's `results.jsonl` (provenance at the end). **This is one illustrative cell, not a result** — the study's numbers come from analyze.py over the full 22×2×N sweep (see `SECTION1_DATA_AND_METHODOLOGY.md`).

---

## 1. The task
`orders_silver_gold` (corpus row 1; substrate `orders`; complexity **Med**; in-scope defect classes **D1, D2, D3, D6, D7, D8**). The agent is given **only** the prose brief and the output contract — never the seeded-defect list (that would give away the answer):

> *"Finance says the daily-revenue dashboard can't be trusted: some days look inflated, and a handful of rows show dates far in the future that can't be real. The raw `orders` event feed is known to be messy — the same order sometimes shows up more than once, some events carry a time that's clearly wrong, amounts are recorded inconsistently, and a few lines are outright corrupt. We need a trustworthy per-day revenue view and a clean one-row-per-order table the rest of the business can join against, and corrupt lines must not just vanish."*
>
> **Contract:** table `gold_daily(event_date DATE [UTC calendar day], revenue, order_id, amount, category)`; table `silver_orders` keyed by `order_id` carrying `amount, category`.

## 2. The input (deterministic, seeded)
`infra/gen_messy_orders.py --seed 42` emits NDJSON. The seeded flaws are visible in the first five real rows (verbatim from `gen_messy_orders_seed42.ndjson`):

```json
{"order_id": "o002735", "merchant_id": "m003", "event_time": "2026-06-20T05:19:05", "amount": 214.7, "category": "electronics"}
{"order_id": "o002757", "merchant_id": "m003", "event_time": "2026-06-20T05:21:39", "amount": 24.55}
{"order_id": "o000290", "merchant_id": "m002", "event_time": 1781940830000, "amount": 291.44, "category": "dining"}
{"order_id": "o003105", "merchant_id": null, "event_time": "2026-06-20T06:02:15", "amount": 166.33, "category": "apparel"}
{"order_id": "o000289", "merchant_id": "m004", "event_time": "2026-06-20T00:33:43", "amount": 181.9, "category": "electronics"}
```

Read the mess: `event_time` is an **ISO string** on most rows but an **epoch-millis integer** on `o000290` (`1781940830000`) — the **D2** timestamp-unit trap; `category` is **absent** on `o002757` — a **D8** silent-drop risk if the reader inner-joins; `merchant_id` is `null` on `o003105`. Elsewhere the stream carries duplicate `order_id`s (**D6**), out-of-range/future timestamps (**D7**), and corrupt lines that must be quarantined not dropped (**D8**). The agent receives only a **location** (`AGENT_INPUT_PATH`) — no schema, no hints.

### 2.1 Format, volume, and shape
The `orders` substrate is a messy customer order-event feed (from a fictional multi-location food-delivery operation — the shape and volume are the point, not the branding), produced by `infra/gen_messy_orders.py` with the `--v3` realism tier.

**Format.** Delivered to the agent as an **NDJSON file** — one JSON event per line — read via `AGENT_INPUT_PATH`. The generator models a **Kafka topic** of JSON record values, but the study materializes the stream to disk for byte-identical reproducibility. Records are heterogeneous: mixed field types, missing fields, nested structs (`--v3`), and raw non-JSON corrupt lines interleaved. The agent's **output** is **parquet** — imperative writes to `AGENT_OUTPUT_PATH` (catalog-free); SDP materializes a managed table → warehouse parquet — which the oracle reads back to grade.

**Volume — deliberately small; this is a correctness study, not a throughput one.** Per seed, `orders` is **~706 KB / ~5,700 records** (`--v3`); other substrates differ (e.g. `payments` ≈ 657 KB / 4,000 rows). Row counts vary slightly seed-to-seed from the random injection but are byte-identical for a fixed seed. Across the sweep that is 22 tasks × 12 seeds × 2 arms = **528 cells**, each reading its substrate's seeded NDJSON — MB-scale in total. *(Throughput/scale is a separate axis: `gen_orders_scaled` drives a 1e4→1e9-row ladder on the cluster, not part of this correctness sweep.)*

**Shape** — the schema and ranges a reader should anchor to: **Seed 42 yields 5,692 NDJSON records** (5,461 parseable + **231 unparseable/corrupt** lines), from ~5,000 base orders plus a ~416-row v3 tail, then **shuffled** so *arrival order ≠ event order* (an ordering/watermark trap). Base schema and ranges:

| field | shape |
|---|---|
| `order_id` | `o000000`…`o004999`, unique per base order (duplicates repeat a key) |
| `merchant_id` | 1 of 20 known (`m001`…`m020`), or `null`, or an *unknown* `x9xx` |
| `event_time` | ISO string (default), or epoch-millis **int**, or `+05:30`-suffixed, or 3h-late |
| `amount` | float `$2–400`, or the **string** form `"214.70"` |
| `category` | 1 of 5 (`grocery/electronics/apparel/fuel/dining`), or **absent** |

Events start `2026-06-20T00:00:00`, spaced ~7s apart. The `--v3` tail (independent, derived RNG) adds the realism the newer tasks measure — **`line_items`** arrays-of-structs (true revenue = Σ qty·price; a scalar sum silently under-counts → nested-D8), nested **`category`** structs (schema-drift tolerance), and raw **`502 Bad Gateway`** HTML lines (non-JSON, must be quarantined). In seed 42 that tail is **203 line-item rows, 126 nested-category rows, 87 HTML-junk rows.**

### 2.2 Seeded defect injection — realized counts (seed 42)
The messiness is *deliberate and catalogued*: each base record is perturbed on a fixed probability, so the oracle knows exactly which traps exist (the generator logs realized counts to stderr as `MESSY DATA PROFILE`). For seed 42:

| injected flaw (base fraction) | seed-42 count | maps to |
|---|---|---|
| duplicate `order_id` (6%) | 276 | **D6** dedup / **D3** state |
| epoch-millis `event_time` (5%) | **246** | **D2** timestamp misparse |
| `amount` as a string (5%) | 250 | **D2** type coercion |
| missing `category` field (5%) | 292 | schema drift / **D8** |
| `null` `merchant_id` (5%) | 242 | join-key / **D8** |
| unknown `merchant_id` (4%) | 208 | **D8** inner-join silent loss |
| `+05:30`-suffixed `event_time` (4%) | 202 | **D7** timezone |
| late event (−3h) (6%) | injected | **D7** watermark / late data |
| malformed JSON (3%) | 231 (corrupt) | must quarantine (**D8**) |
| clean | remainder | — |

**Reproducibility anchor:** `random.seed(seed)` makes the stream byte-identical per seed — both arms face exactly the same data on a given `(task, seed)`. Seed 42's **246 epoch-millis timestamps reproduce the reference E3 fingerprint `D2=246` exactly** (the locked reference set is `D2=246, D7=275, D8=250 / $49,778.06` of at-risk revenue). That per-seed determinism is what lets the oracle score, cell by cell, *which* traps each agent hit, caught, or shipped.

### 2.3 Infrastructure — how the data flows
- **Ingest.** Modeled as **Kafka** (the generator is built to pipe into a Kafka console producer); the controlled study materializes each `(task, seed)` stream to an NDJSON file for determinism, which the agent's pipeline reads.
- **Compute (this local study).** The two arms run on *different engines by paradigm*: imperative **A** on classic `local[*]` Spark (in-process JVM via py4j); SDP **B** on a single-node **Spark Connect** server the harness starts (in-memory catalog → local warehouse), one per run on a free port. This substrate split is exactly why H3 (compute) is deferred to a *uniform*-substrate run rather than measured here.
- **Storage.** Local warehouse directory (parquet) with an in-memory catalog; no external metastore.
- **At scale / Phase 2b.** **Spark-on-Kubernetes (EKS)** — driver + executor pods, **Spark Connect** as the governed ingress (Envoy mTLS), a Hive Metastore, and **Apache Iceberg tables on S3**. That is the reference-architecture substrate (Sections 2–3) and the *uniform-compute* substrate for the H3 compute claim.

## 3. The agent loop (propose → [gate] → execute → grade; ≤ 12 iterations)
Each `(task, arm, seed)` runs an independent agent handed the paradigm framing + the brief + the input location + the contract. It writes code (a *proposal*), then:

- **Arm A (bare imperative):** executes the proposal directly (`spark-submit` of `pipeline.py`). No gate, no skill.
- **Arm B (SDP):** first `spark-pipelines dry-run` — the **structural gate**, which analyzes the dataflow graph *before any executor runs* — then, on pass, `spark-pipelines run`. Plus the `pyspark-sdp` API skill.

A failing step (crash / unresolved reference / runtime error) is fed back as text and the agent retries (up to 12). A clean, *readable* materialized output ends the loop as `completed`.

## 4. What each arm produced — seed 42, verbatim
| | **Arm A** (imperative) | **Arm B** (SDP) |
|---|---|---|
| iterations | **1** (first-try) | **2** (`dry-run` ✓ → `run` ✓) |
| final program LOC | **139** | **82** |
| exit_class | `completed` | `completed` |
| gate | — (no gate) | passed; `dry_run_intercepts = 0` |

Both produced valid `gold_daily` + `silver_orders` tables. **No crash, no error, structurally sound** — and B's dry-run gave it a clean structural bill of health.

## 5. The defect both shipped (D6 — dedup)
The dedup logic each agent wrote (verbatim, the D6 site):

```python
# Arm A
w_silver = Window.partitionBy("order_id").orderBy(
    F.col("has_amount").desc(), F.col("event_ts").desc_nulls_last(),
    F.col("amount").desc_nulls_last(), F.col("category").asc_nulls_last())
cleaned.withColumn("rn", F.row_number().over(w_silver))   # keep rn == 1

# Arm B
w = Window.partitionBy("order_id_filled").orderBy(
    F.col("event_date").desc_nulls_last(), F.col("event_ts").desc_nulls_last(),
    F.col("amount_num").desc_nulls_last(), F.col("category").asc_nulls_last())
ranked = df.withColumn("rn", F.row_number().over(w)).where(F.col("rn") == 1)
```

Both pick "one row per order" via `row_number()` over a window ordered by *latest timestamp, then highest amount*. It is a **plausible** dedup — and it is **not** the canonical, deterministic dedup this feed requires. The **D6** class is *nondeterministic dedup* (§3.2); the output oracle, which encodes the correct survivor for seed 42's seeded duplicates, scored a **residual D6** in each arm's completed `silver_orders` (the exact criterion is in `harness/oracles.py`). The table has exactly one row per `order_id` — it *looks* flawless — while carrying the wrong values for the duplicated orders.

## 6. The grade — verbatim
Both arms:
```
silent_defect = True   defect_classes = ['D6']   detection_stage = never
per_defect_detection = {D1:'n/a', D2:'n/a', D3:'n/a', D6:'never', D7:'n/a', D8:'n/a'}
```
Reading it against §3.1 / §3.3: the run reached **COMPLETED** output **and** an in-scope **semantic** class (D6) is still residual ⇒ `silent_defect = True`; `detection_stage = never` ⇒ the wrong data **shipped** — caught by nobody, because there was no crash to catch and (for B) the structural gate passed.

## 7. The taxonomy, made concrete (ties to §3.2)
This single cell instantiates §3.2's load-bearing consequence exactly:

- **B's structural gate passed the proposal** (`dry_run_intercepts = 0`) — because the pipeline *is* structurally valid. D6 is **semantic**: the query resolves and runs; it just computes the wrong dedup. A structural/dry-run gate is, by construction, **blind** to it.
- **Both arms shipped the identical D6.** The silent/semantic residue does not move with paradigm — which is precisely why the paper treats the silent-defect rate as the **control** (H1.3), and puts the headline on **structural-catch** (H1.1), where the gate actually acts.
- **The one difference that *did* show:** B's accepted program was **40% smaller** (82 vs 139 LOC) — the conciseness signal (H4) — and B ran *through* its gate (the H1 machinery; there was simply no structural bug to intercept on this task). On a hard *streaming/state* task (e.g. `p6_dedup_watermark`), that same structure+gate is where SDP pulls ahead on completion.

The takeaway a reader should leave with: **structural guardrails catch crashes; they do not catch wrongness.** Closing the silent/semantic gap needs a *semantic* guardrail (runtime data-quality expectations / conservation checks) — not a bigger dry-run.

## 8. Scope & honesty
One `(task, arm, seed)` triad, chosen to expose the machinery. It is **not** evidence for any A-vs-B claim: whether B ships fewer/more silent defects, catches more structural errors early, completes more often, or writes less code *across the corpus* is answered only by `analyze.py` over the full 22×2×N sweep, with paired (task,seed) CIs and the power rule — see `SECTION1_DATA_AND_METHODOLOGY.md`.

---

## Provenance
- **Task spec:** `TASKS.lock.json` → `orders_silver_gold` (substrate, contract, `defects_in_scope`).
- **Input:** `gen_messy_orders_seed42.ndjson` (deterministic; `infra/gen_messy_orders.py --seed 42`).
- **Cell rows (verbatim):** `results.powered.AB.n12.part1.task1.jsonl` — arms A and B, `seed 42` (fields: `exit_class`, `iterations`, `silent_defect`, `defect_classes`, `detection_stage`, `per_defect_detection`, `dry_run_intercepts`, `final_program`, `final_program_loc`).
- **Grading definitions:** `PAPER.md` §3.1–§3.3 → `harness/oracles.py` (`silent_defect` predicate, defect taxonomy, `detection_stage`).
- Compiled 2026-06-30 from the frozen instrument `instrument-v3.2-frozen` (`ca48c8c`). All quoted values are as-written in the source files. This document backs `PAPER.md` §3 and changes no claim within it.

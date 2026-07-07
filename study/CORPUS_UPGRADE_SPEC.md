# Corpus Upgrade Spec — Safe-Agent Spark Study (corpus v3)

Status: **owner-approved 2026-06-24**. This is the implementation spec for upgrading the
task corpus. Built on PR #25 (path-based local imperative substrate + temp-view secondary
tables; SDP on Connect). It is an **extension of the existing 15-task corpus, not a
from-scratch redo** — we reuse tasks and add/elevate to tell a complexity-scaling story.

## 0. Decisions (locked)
- **Scope:** full realism overhaul + at least one **UDF task**.
- **Prompts → ticket-style:** state the business *symptom*/goal, never the fix or the Spark
  API; pin the deterministic output contract at the bottom; identical across arms; no leak.
- **Gradable taxonomy = D1, D2, D4–D8.** D3 (unwatermarked dedup) and D9 (unbounded state)
  move to **Future Work** (not offline-graded); they may appear narratively only.
- **New pre-registered hypothesis H4 (moderation):** SDP's silent-defect-rate and
  compute advantage **grows with task complexity** — tested as a `paradigm × complexity`
  interaction. Narrated in the paper as **Part 1.5 — "Does the advantage scale?"**, the
  bridge from Part 1 (safety) to Part 2 (agent-native Connect loop).
- **Complexity gradient: GO AGGRESSIVE.** Populate the High bin robustly (**target ≥6 High**)
  via 2–3 new high-complexity "platform-in-miniature" tasks + elevating several mediums.
  Iterate later as we flesh it out.
- **Floor-effect pilot gate:** before finalizing each HC task, run it on 1–2 seeds to confirm
  it is hard-but-not-impossible (both arms must not *always* fail — that yields no signal).
- **Re-freeze `TASKS.lock.json`** (new version) as a **logged pre-data pre-reg deviation**;
  record each task's `complexity_score` + per-axis breakdown; update the coverage matrix so
  every gradable class (D1,D2,D4–D8) is exhibited by ≥5 tasks.
- Tasks run on BOTH substrates; **UDFs must work in imperative and SDP/Connect**.

## 1. Prompt-reframe policy
Rewrite every task `prompt` from how-to instructions into a JIRA-ticket / stakeholder-request
framing: describe the business symptom so the requirement is *implied*, never prescribed.
Never name the fix ("use a watermark", "dropDuplicates") or the API. Pin the output contract
(tables + columns) at the bottom. Rationale: more realistic AND removes spoon-feeding that
suppresses the silent-defect signal (handing both arms the recipe shrinks the gap we measure).

Example (orders_silver_gold):
> Users report inaccurate daily revenue totals; we suspect duplicate and late-arriving order
> events. We need a reliable aggregated daily-revenue view and a clean per-order view.
> **Output contract:** `gold_daily(event_date DATE UTC, revenue)`; `silver_orders(order_id PK,
> amount, category)`.

## 2. Complexity rubric (a-priori, per task)
Each axis scored 0–3; weighted sum → `complexity_score`; bin Low 0–15 / Med 16–30 / High 31+.

| Axis | weight | 0 → 3 meaning |
|---|---|---|
| A1 DAG depth (interdependent stages) | ×2 | 1 stage → 4+ stages |
| A2 State management | ×3 | stateless → 3+ stateful ops / complex state |
| A3 Joins & aggregations | ×2 | 0–1 simple → multi-way/temporal |
| A4 Sinks & fan-out | ×1 | 1 sink → dynamic/complex routing |
| A5 Schema handling | ×2 | fixed → quarantine/DLQ required |
| A6 Idempotency | ×3 | append-only → idempotent across stages |
| A7 Cross-stage invariants | ×3 | none → as-of / no-overlap / reconciliation |
| A8 Custom logic (UDF) | ×1 | none → stateful/complex UDF |

`complexity_score` is stored per task in `TASKS.lock.json` and used as a pre-registered
task-level covariate in H4.

## 3. Task inventory, scores, and aggressive elevation plan
Baseline scores (from rubric) and the **aggressive** target bin. Elevations reuse existing
tasks by adding a cross-stage invariant + an idempotency/MERGE requirement (A6/A7 are the
high-weight levers).

| id | base score | base bin | TARGET bin | elevation action (if any) |
|---|---|---|---|---|
| orders_silver_gold | 13 | Low | **Med** | add a small SCD1 product dim joined in gold (+A2,+A3,+A7) |
| p1_medallion | 19 | Med | Med | — |
| p2_cdc | 23 | Med | **High** | add cross-stage reconciliation of current-vs-history (+A7→3) + MERGE (+A6→2) |
| p3_windows | 7 | Low | Low | — |
| p4_fanout | 9 | Low | Low | — |
| p5_mart | 10 | Low | replace | replace with a self-contained mart (no cross-pipeline dep) |
| p6_dedup_watermark | 6 | Low | Low | — |
| p7_late_data | 7 | Low | Low | — |
| p8_currency_normalize | 9 | Low | Med | as-of FX + reconciliation invariant (+A7) |
| p9_enrich_join | 7 | Low | Low | — |
| p10_scd2 | 27 | Med | **High** | add MERGE-idempotent current table + no-overlap+reconciliation (+A6,+A7) |
| p11_schema_evolution | 10 | Low | Med | array/scalar drift + revenue-preservation invariant |
| p12_quarantine_dlq | 12 | Low | Med | nested error-envelope DLQ + no-loss invariant |
| p13_cdc_windowed | 29 | Med | **High** | reconcile windowed totals to silver CDC (+A7→3) + MERGE (+A6→2) → 32 |
| p14_fx_settlement | 20 | Med | **High** | full-outer reconciliation vs external settlements + as-of (+A7,+A3) |
| new_merge_upsert | 17 | Med | Med | — |
| new_stream_stream_join | 12 | Low | Med | event-time bound + as-of invariant |
| new_scd2_as_of_join | 33 | **High** | High | — |
| new_cdc_tombstone | 23 | Med | Med | — |
| new_udf_classifier | 8 | Low | Low | — |
| **HC-1 fx_trade_ledger** | 36 | **High** | High | NEW (see §7) |
| **HC-2 session_funnel** | 31 | **High** | High | NEW (see §7) |

Target distribution (aggressive): **Low ~7, Med ~8, High ~7** (HC-1, HC-2, new_scd2_as_of,
p2_cdc↑, p10_scd2↑, p13_cdc_windowed↑, p14_fx_settlement↑). Final counts confirmed after the
floor-effect pilot; iterate later.

## 4. Per-task realism wrinkles (15 existing)
Apply (keep contracts deterministic, no leak): mixed ISO/epoch-millis timestamps
(orders_silver_gold); null/string amounts (p1); out-of-order CDC (p2); heavier lateness
(p3,p7); more malformed JSON (p4); conflicting-payload duplicates (p6); unknown merchant IDs
→ "Unmapped" surrogate, no revenue drop (p9); daily-changing FX (p8); array-vs-scalar `amount`
drift with revenue preservation (p11); nested error-envelope DLQ preserving original schema
(p12). Full per-task table: see appendix specs A (v1) and B (complexity layer).

## 5. New pattern tasks
- **Idempotent MERGE/upsert** — re-run replays a subset of offsets; `silver`/positions must
  not double-count. Contract: keyed table, exactly 1 row per business key. Defects: D6, D5.
- **Stream-stream temporal join** — FX (or budget) as a *stream*; join the closest rate prior
  to event time. Contract: enriched rows with `*_as_of_ts`. Defects: D7, D8. *(Grading risk:
  control event-time skew in the generators so the "correct" window is unambiguous.)*
- **Point-in-time / as-of SCD2 join** — join fact to the dimension *as it looked at event
  time* (effective_from/to). Defects: D7, D6.
- **CDC tombstones / hard-deletes** — `op='D'`/null payloads must remove rows. Defects: D6.

## 6. UDF task (required)
Email-subject classifier (`urgent`/`spam`/`routing`/`info`) implemented as a UDF.
Silent defects: `null` subject misclassified (e.g. → `spam` instead of `routing`); non-ASCII
subjects misclassified; (optional) non-determinism if the UDF calls `now()`/random.
Contract: `classified_emails(email_id, subject, category)`. Quantifier counts null-subject and
non-ASCII misclassifications against ground truth. UDF must register/run in **both** imperative
(`spark.udf.register`) and SDP/Connect. Optionally add a `pandas_udf` variant. Paradigm note:
UDFs bypass Catalyst — a real silent-correctness footgun worth surfacing per paradigm.

## 7. High-complexity tasks (platform-in-miniature)
**HC-1 Multi-stage FX Trade Ledger (score 36).** bronze `trades` + CDC `fx_rates` → SCD2 rate
dim → as-of-join USD valuation (`gold_trades`) → idempotent MERGE `mart_positions`.
Per-stage contracts: `fx_rates_scd2` (correct is_current/validity, no overlap); `gold_trades`
(non-null `amount_usd` at the rate active at trade time); `mart_positions` (per-currency sum
ties to trades). Cross-stage invariants: gold reconciles to trades×rates; mart reconciles to
gold. Defects: D1,D2,D4,D5,D7.

**HC-2 Streaming E-commerce Session Funnel (score 31).** clickstream + user CDC → SCD1 user
dim → 30-min inactivity sessionization (late-data correct) → funnel rollup + DLQ.
Contracts: `silver_sessions` (correct session assignment incl. late events); `gold_funnel`
(per-user funnel state). Invariants: no event dropped/double-counted; unique-user counts match.
Defects: D1,D2,D6,D8.

Complexity comes from the **number and interdependence of required-correct behaviors**, never
from non-determinism — every stage has a deterministic contract + oracle-checked invariants.

## 8. Data generator enrichment (deterministic seeds preserved)
- `infra/gen_messy_orders.py`: nested JSON in `category`, `line_items` array-of-structs with
  drift, unstructured junk rows (e.g. a `502 Bad Gateway` HTML string in the topic).
- `infra/gen_customers_cdc.py`: `op='D'` tombstones / null payloads; out-of-order sequences.
- `infra/gen_payments.py`: daily-changing FX; more exotic currency codes.

## 9. Quantifier changes (`experiments/defect_battery/quantify.py`, `quantify_ext.py`)
Deterministic truth computation for: D2 over ISO+epoch-millis; D8 over null amounts + nested
arrays; daily-FX `pay_d7`/`pay_d8`; the UDF misclassification counts; and the HC cross-stage
invariants (reconciliation, no-overlap, as-of correctness). Each new/changed defect needs an
offline ground-truth quantifier.

## 10. H4 moderation analysis + Part 1.5
GLMM with interaction: `silent_defect ~ paradigm * complexity_score + (1|task_id)` (binomial);
`compute_to_correct ~ paradigm * complexity_score + (1|task_id)` (Gamma/log). Hypothesis H4
confirmed if the `paradigm:complexity_score` coefficient is significant and in the predicted
direction (SDP advantage increases with complexity). `complexity_score` is a continuous,
pre-registered, a-priori task-level covariate (binning is only for presentation). Paper:
add **Part 1.5 "Does the advantage scale?"** between Part 1 and Part 2.

## 11. Logistics
- Re-freeze `TASKS.lock.json` (bump version, add `complexity_score` + axis breakdown per task,
  refresh `coverage_matrix`), log a DEVIATIONS entry (pre-data corpus revision; legitimate).
- Floor-effect pilot for HC-1/HC-2 (and elevated p13/p14) before finalizing.
- Tests: corpus integrity, new quantifiers, prompt-no-leak guard, `complexity_score` presence,
  HC invariant checks. Full suite must stay green.
- Scope/runtime note: ~22 tasks × 5 arms (incl. A2) × N seeds × 2 substrates — materially
  bigger; pilot-vs-full to be decided at calibration.

Appendices (detailed per-task tables) live in the orchestration record: Spec A (v1 upgrade
table, new-pattern + UDF + generator/quantifier detail) and Spec B (rubric + full scored table
+ HC designs + moderation plan).

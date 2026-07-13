# SP4 capstone: Polly builds a governed medallion fleet for 3 customers (S4.5 demonstrated core)

*Status: build contract for the §4 demonstrated-core capstone. Maps to ONE anchor: S4.5
(ii) heterogeneous orchestration, "the pattern this paper was built with," now shown
data-engineering-native over the live §3 custody chain. This is a DEMONSTRATION (the
mechanism runs), NOT a numbers claim. PAPER.md stays frozen; any S4.5 status note is
Lisa's to accept at a paper gate. SP4.2 cost/catch-rate numbers remain a separate paper
and are never folded into this demonstration's claims.*

## Why this exists (the anchor it satisfies)
S4.5 already claims a demonstrated core: (i) credential custody (SP4.1, proven) and (ii)
heterogeneous orchestration. Today (ii)'s only concrete instance is meta ("this paper was
built by the orchestration it describes"). This capstone makes (ii) concrete and native to
the paper's own domain: Omnigent's Polly takes a large, realistic data-engineering brief,
decomposes it into a fleet of sub-agent tasks, and builds end-to-end medallion pipelines
for three different customers over the live §3 per-tenant isolation, composing all four §4
axes at once. It does not add a claim; it supplies evidence for one that already stands.

## What Polly is handed (the large task)
One brief: "Here is raw operational data for three customers with different data needs.
Build each an end-to-end medallion pipeline (bronze -> silver -> gold) in OSS SDP, correct
under messy input, honoring each customer's data-governance policy." Polly (the packaged
`omnigent polly` orchestrator, driven headless in a reproducible, logged wrapper) breaks
this into per-customer bronze/silver/gold subtasks and fans out cross-vendor sub-agents.

## The three customers (reusing §1 substrates for comparability)
| # | Customer | Substrate (§1) | Medallion need | Contextual policy (governance) |
|---|---|---|---|---|
| 1 | Northwind Retail | orders | bronze raw -> silver one-row-per-order (dedup, corrupt quarantined not dropped) -> gold daily revenue (UTC-day, currency-normalized) | **standard**: corrupt rows quarantined, never silently dropped |
| 2 | Helios SaaS | customers_cdc | bronze raw CDC -> silver current-state (out-of-order safe, dedup) -> gold SCD2 history (non-overlapping periods, reconciles) | **PII-restricted**: the region attribute is PII; gold must not expose it raw (mask/tokenize) |
| 3 | Meridian Payments | payments | bronze raw multi-currency -> silver FX-normalized (rate on event-date, unknown currency quarantined) -> gold USD daily settlement | **financial-grade**: value conservation, no silent row drop, audit trail |

Each customer is a **tenant** on the live §3 chain: tenant_a = Customer 1, tenant_b =
Customer 2, tenant_c = Customer 3 (NEW, provisioned for this capstone). Each medallion runs
over that tenant's own Spark Connect server, credential-free via the custodian.

## How the four §4 axes compose (all at once, over §3)
- **S4.1 cost / routing.** Polly routes each subtask to a model matched to difficulty:
  bronze (mechanical) -> cheap local (`ollama/qwen3-coder:30b`, free); silver (moderate) ->
  mid cloud (`openrouter/deepseek/deepseek-chat`); gold (correctness-critical) -> frontier
  (`anthropic/claude-opus-4-8`). Routing decisions + real usage tokens are logged (pilot
  cost only; the powered claim is SP4.2, separate).
- **S4.2 quality / cross-vendor review.** Each authored dataset gets a different-vendor
  reviewer (e.g. `openrouter/openai/gpt-5` reviewing Claude-authored gold); findings logged.
- **S4.3 governance / custody.** A single custodian process holds every per-tenant
  credential and exposes sub-agents only `submit(customer, code) -> pass|fail`. On each
  submission it mints a fresh 300s per-tenant token, runs the SDP pipeline over that
  tenant's Connect server (§3 chain: gateway -> tenant Connect -> OpenFGA authz ->
  prefix-scoped vend -> own executors), and returns only pass/fail. Sub-agents hold zero
  credentials. This is SP4.1's proven interface, now used by a real medallion build.
- **Contextual policies.** The custodian enforces each customer's policy at submit time:
  Customer 2's gold rejected if it exposes raw `region` (PII); Customer 3's rejected if it
  drops rows or breaks value conservation. Policy is per-customer context, not global.
- **S4.4 knowledge / shared skill.** Every sub-agent is injected the same governed
  `pyspark-sdp` primer/skill (load-bearing per §1: without it, agents hallucinate DLT).

## Definition of done (binary acceptance for the demonstrated core)
1. Polly emits an explicit per-customer subtask decomposition (the DAG), not a monolith. [captured]
2. Subtasks are routed across >= 2 distinct vendors (heterogeneous routing shown, logged). [captured]
3. Every pipeline submission goes through the custodian; sub-agents hold 0 credentials
   (verified: no tenant token in any sub-agent context/log). [captured]
4. Each customer's pipeline runs over its OWN tenant Connect server; a deliberate
   cross-tenant submission is DENIED (PERMISSION_DENIED / NotAuthorized), so §3 isolation
   holds under fleet custody. [captured]
5. A contextual policy is enforced for >= 1 customer (a violating pipeline is rejected,
   a compliant one accepted). [captured]
6. Cross-vendor review runs on >= 1 dataset and its findings are recorded. [captured]
7. Architecture diagram of the whole capstone produced (house style, 0 em-dashes). [captured]

## Explicitly NOT claimed here (anti-drift)
- No powered cost-per-correct or catch-rate number (that is SP4.2, a separate paper).
- No edit to any PAPER.md claim. Evidence lands in `paper/notes/` + a diagram; the S4.5
  status note is Lisa's to accept at a paper-revision gate.
- "Correct medallion output" is graded by the same blind-grader proxy as the SP4.2 pilot,
  not §1's full Spark oracle suite; labeled as such.

## Evidence artifacts
- `paper/notes/proof_2026-07-12_sp4_capstone.log` (decomposition DAG, routing, custody,
  policy enforcement, per-tenant runs, cross-tenant deny, review findings, repair loop).
- `paper/diagrams/section4_capstone_fleet.svg` (the architecture, rendered + verified).
- Provisioning + wrapper scripts kept in the session scratchpad and cited.

## Results (2026-07-12, Stage 1 deterministic wrapper)
All seven acceptance criteria captured over the live Section 3 chain (3 tenants), every model call
routed through Omnigent:
1. Decomposition: Polly emitted the 3-customer bronze/silver/gold DAG. MET.
2. Routing (S4.1): 3 distinct vendors by complexity (qwen-local / opus / deepseek); routed cost $0.35. MET.
3. Custody (S4.3): sub-agents got only submit() -> pass/fail; the custodian held the PSK + every tenant
   token; agents held 0 credentials. MET.
4. Isolation (Section 3): each medallion ran over its OWN tenant; all 3 cross-tenant probes DENIED
   (NoSuchNamespace). MET.
5. Contextual policy: standard / pii_restricted / financial_grade all enforced; the financial_grade
   policy REJECTED the first payments pipeline (silent row drop) before repair fixed it. MET.
6. Cross-vendor review (S4.2): gpt-5 flagged real silent defects in all three drafts. MET.
7. Diagram produced and verified. MET.
Dev-loop (Section 2 at fleet scale): all three converged in 2 attempts (routed author -> opus repair
-> PASS). Materialized: retail bronze/quarantine/silver/gold; saas bronze/silver/gold (region
tokenized); payments bronze x2 / quarantine / silver / gold (value conserved 6 == 5 + 1).

Status note for Lisa (paper gate): S4.5's demonstrated core (ii) heterogeneous orchestration now has a
concrete, data-engineering-native instance over the live platform. This is status-supporting evidence,
not a claim change; PAPER.md is unchanged. Stage 2 (native-Omnigent, autonomous) is the next SP.

# Section 4 (Omnigent) build plan: WRAPPER then AUTONOMOUS

*Work contract for §4, in the BUILD_PROGRAM discipline (every task maps to an SP; every SP maps to a
paper anchor; the paper stays frozen; SP4.2 numbers stay a separate paper). §4 was executed
opportunistically so far; this doc gives it a real plan. Two stages, in order: a deterministic
**wrapper** (reproducible, the demonstrated-core artifact), then **autonomous** Polly (the frontier,
where most of the Omnigent wiring lives).*

## Where §4 stands (anchor map + evidence)
| Anchor | Claim | Status |
|---|---|---|
| S4.1 cost / heterogeneous routing | match model to task difficulty | mechanism shown (routed qwen/deepseek/opus, real per-vendor cost); **numbers = SP4.2 separate paper** |
| S4.2 quality / cross-vendor review | different-vendor reviewer catches correlated blind spots | mechanism shown (gpt-5 flagged real defects in qwen/opus/deepseek output); numbers = SP4.2 |
| S4.3 governance / credential custody | custodian holds creds, agent credential-free | **DEMONSTRATED** (SP4.1) + now composed in the capstone |
| S4.4 knowledge / shared skill | one governed pyspark-sdp skill fleet-wide | mechanism (injected primer); load-bearing per §1 |
| S4.5 demonstrated core (ii) heterogeneous orchestration | the pattern, data-engineering-native | **Stage 1 wrapper (this plan) supplies the concrete instance** |
| S4.5 frontier | scale-out, autonomous fleet, live-job rotation | **Stage 2 autonomous (this plan)** |

## Two-stage arc (why this order)
- **Stage 1, WRAPPER.** Polly does the *decomposition*; a bounded, logged wrapper deterministically
  drives the cross-vendor fleet + custodian + policy + review + repair loop over the live §3 chain.
  Reproducible and citable. It de-risks the whole composition with full control. **This is the S4.5
  demonstrated-core artifact.** (Substantially built; finishing now.)
- **Stage 2, AUTONOMOUS.** Hand the brief to real `omnigent polly` and let *it* decompose AND
  dispatch sub-agents AND call the custodian tool AND iterate, unattended. The wrapper's deterministic
  orchestration is replaced by Polly's own agentic orchestration. **This is the frontier and where the
  Omnigent wiring is heaviest.** It reuses every proven Stage 1 piece (custodian, policy engine, the
  3-tenant isolation substrate, the routing + review + repair patterns).

---

## STAGE 1 - WRAPPER  (SP4.3; anchor S4.5 demonstrated core)
Deterministic wrapper; Polly decomposes, the wrapper executes. Reuses the live §3 custody chain.

| Phase | Deliverable | Acceptance (binary) | State |
|---|---|---|---|
| 1.1 3rd customer | tenant_c provisioned (warehouse + grant + Connect server) | catalog deny c<->a/b (404); tenant_c full runtime path (Connect->authz->vend->own exec) | DONE |
| 1.2 custodian + executor + policy | `custodian_capstone.py` (submit interface, SDP executor, per-customer contextual policy) | a medallion materializes over a live tenant; policy enforced; agent gets only pass/fail | DONE |
| 1.3 Polly decomposition | structured per-customer subtask DAG from headless `omnigent polly` | Polly emits the DAG; wrapper consumes it | DONE |
| 1.4 cross-vendor fleet | routed authoring (S4.1) + cross-vendor review (S4.2) via omnigent.llms | >=2 vendors used; review flags real defects | DONE |
| 1.5 dev-loop at fleet scale | author -> custodian feedback -> repair (escalate) -> converge | each medallion reaches pass under isolation + policy, or is honestly reported unmet | DONE: 3/3 converged in 2 attempts each (routed author -> opus repair -> PASS); all policies held; all 3 cross-tenant probes denied; routed cost $0.35 |
| 1.6 evidence + diagram | proof log + `section4_capstone_fleet.svg` + `SP4_capstone_design.md` | 0 em-dashes; account/secrets redacted; diagram rendered + read | NEXT |
| 1.7 write-up | S4.5 demonstrated-core note (evidence, not a claim rewrite) | Lisa accepts the status note at a paper gate | NEXT |

Definition of done for Stage 1: the seven S4.5 acceptance criteria in `SP4_capstone_design.md` all
captured, with the repair loop showing convergence, over the live chain.

---

## STAGE 2 - AUTONOMOUS  (SP4.4; anchor S4.5 frontier / S4.6)
Real Polly owns decomposition AND dispatch AND the loop. Native in Omnigent, no bridges.

### Confirmed native primitives (verified 2026-07-12, no hacky workarounds needed)
- **Custodian = a native MCP server.** Omnigent has first-class MCP: an agent declares
  `tools/mcp/<name>.yaml` with `transport: stdio`, `command`, `args`, `env` (`spec.MCPServerConfig`,
  validated by `spec/validator.py`). The `mcp` SDK is installed in system python3 alongside pyspark
  4.1, so the custodian runs as its OWN process (holds PSK + tenant tokens, exposes
  submit_pipeline/seed_raw/probe_isolation) and Omnigent connects over stdio. Custody boundary is a
  real process boundary; agents call the tool and never see a credential.
- **Cross-vendor fleet = Polly's native roster.** Polly delegates to three sub-agents: `claude_code`
  (Anthropic), `codex` (OpenAI), `pi` (the only worker that runs ANY gateway model, i.e.
  OpenRouter/local). So S4.1 heterogeneous routing is a property of Polly's own dispatch.
- **Capstone agent = a config.yaml** (own dir) with the brief + governance rules + the custodian MCP
  in `mcp_servers` + the pyspark-sdp skill (S4.4). Launch with `omnigent run <capstone-agent>`.
- **Demo connectivity caveat (not a hack in the orchestration):** the custodian process reaches the
  in-cluster tenant Connect servers via port-forward; production would co-locate the custodian in the
  cluster. This is plumbing, orthogonal to the native tool/agent wiring.


### The wiring (the hard part), as discrete phases
| Phase | Deliverable | Why it is needed | Risk |
|---|---|---|---|
| 2.1 custodian as an Omnigent tool | MCP (stdio) server wrapping submit/seed/probe; runs OUT-OF-PROCESS from agents | so Polly's sub-agents call `submit(customer, code)->pass/fail` while the PSK/tokens stay in the custodian (S4.3 boundary preserved autonomously) | **DONE (2026-07-12): `custodian_mcp.py` FastMCP stdio server, 3 tools, self-bootstraps PSK + port-forwards; validated end-to-end (submit materialized the retail medallion over the live tenant, pass=True)** |
| 2.2 Polly agent definition | a capstone agent dir/YAML: brief + governance rules + coding toolset + the custodian MCP + pyspark-sdp skill | Polly needs the task, the tools, and the shared skill (S4.4) in its own config | NEXT |
| 2.3 cross-vendor sub-agent routing | dispatch to different harnesses by role/complexity (claude_code / codex / pi-gateway) | make S4.1 routing a property of Polly's dispatch, not the wrapper | **roster CONFIRMED (2026-07-12): claude_code + codex + pi all available; pi reaches openrouter + local ollama** |
| 2.4 autonomous loop | let Polly run author->submit->repair itself off the custodian tool's pass/fail | close the §2 loop inside Polly (it already frames work as implement sub-agents + review + PR) | high: unbounded runs; needs iteration/budget guardrails |
| 2.5 custody boundary audit | verify sub-agents never hold PSK/token (only the tool result) | the keystone claim at autonomous scale | med |
| 2.6 reproducible capture | extract Polly's trace (decompose, dispatch, tool calls, submits, repairs) from omnigent chat.db / logs | a citable autonomous-run artifact | low |

### Stage 2 acceptance (frontier demonstrated, not a numbers claim)
Real Polly, given only the brief + the custodian tool, autonomously (a) decomposes, (b) dispatches
cross-vendor sub-agents, (c) builds all three medallions over their own tenants via the custodian,
(d) repairs from custodian feedback, (e) never lets a sub-agent hold a credential, (f) preserves
cross-tenant isolation, with the trace captured. Anything unmet is reported as remaining frontier.

---

## Anti-drift + gates (binding)
- PAPER.md frozen; both stages produce evidence in `paper/notes/` + diagrams; S4.5 status notes are
  Lisa's to accept at a paper gate, never a side-effect edit.
- SP4.2 cost/catch-rate NUMBERS stay a separate paper; the capstone is a mechanism-runs demonstration.
- Live-infra + spend + shared-auth mutations need Lisa's explicit go (as with the tenant_c JWKS change).
- Correctness graded by the blind-grader/custodian proxy, not §1's full oracle suite; labeled as such.

## Open decisions for Lisa
1. Stage 1 close-out scope: stop at "repair loop converges + diagram + write-up," or also add a 2nd
   cross-vendor review pass on the repaired code?
2. Stage 2 sub-agent vendors: which harnesses/models Polly should route to (claude-sdk / codex / pi /
   local), and the cost/iteration budget guardrail.
3. Stage 2 substrate: same live 3-tenant chain, or a scaled variant (more tenants) to also touch the
   scale-out frontier.

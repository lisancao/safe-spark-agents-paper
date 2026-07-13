# sdp-capstone: a governed data-engineering fleet, native in Omnigent

This is the Section 4 (Omnigent) capstone as a reproducible, production-shaped Omnigent agent. It
decomposes a multi-customer medallion brief, fans out a cross-vendor fleet of coding sub-agents to
author OSS Spark Declarative Pipelines, and submits each pipeline through a governed **custodian**
that holds every per-tenant credential and runs the work over that customer's own Section 3 tenant.
It composes Section 4's four axes over the Section 3 platform, with no backdoors: every governance
concern is a native Omnigent primitive.

## What is native (no wrappers, no backdoors)
- **Custody** is a native MCP server (`tools/mcp/custodian.yaml` -> `custodian/custodian_mcp.py`,
  `transport: stdio`). Omnigent spawns it in its own process; the Spark Connect PSK and every
  per-tenant catalog token live only inside it, so a sub-agent that calls `submit_pipeline` never
  sees a credential. Custody is a real process boundary.
- **Cost control** is a native policy: `omnigent.policies.builtins.cost.cost_budget` with a hard
  `max_cost_usd` ceiling over the whole spawn tree (`guardrails.policies` in `config.yaml`). A
  supervised run adds `ask_thresholds_usd` for soft approval checkpoints.
- **Cross-vendor routing** (cost + quality) is the orchestrator's own sub-agent roster:
  `claude_code` (Anthropic), `codex` (OpenAI), `pi` (any OpenRouter or local gateway model). The
  orchestrator routes authoring by difficulty and has a different vendor review.
- **Contextual data policy** (per customer: quarantine, PII masking, value conservation) is enforced
  by the custodian at submit time, over the materialized data, on the customer's own tenant.
- **Shared knowledge** is one governed skill, `skills/pyspark-sdp/SKILL.md`, injected fleet-wide.
- **User management / isolation** is Section 3: each customer is a tenant with its own OIDC identity,
  per-principal catalog authorization (Lakekeeper + OpenFGA), token-injected Connect server, and
  prefix-scoped storage. The Omnigent server itself runs the native `accounts` auth provider.

## Layout
```
config.yaml                     the agent: brief, roster, native cost + governance policies
tools/mcp/custodian.yaml        the custodian wired as a native stdio MCP server
custodian/custodian_mcp.py      the MCP server (FastMCP): seed_raw_data, submit_pipeline, probe_isolation
custodian/custodian_capstone.py the SDP executor + contextual-policy engine (data plane)
custodian/seed_raw.py           per-customer messy raw-data seeder
skills/pyspark-sdp/SKILL.md     the shared, governed SDP authoring skill
agents/{claude_code,codex,pi}/  the cross-vendor sub-agent definitions (vendored from omnigent examples)
```

## Reproduce
Prerequisites: Omnigent installed and set up (`omnigent setup`); the Section 3 EKS platform up with
per-tenant Spark Connect servers (`spark-connect-tenant-a/-b/-c`), `lakekeeper-authz` + `openfga` +
the OIDC IdP (see `deploy/eks/lakekeeper/SETUP.md`); `kubectl` + `aws` on PATH with a profile that can
reach the cluster; a python with both `mcp` and `pyspark` for the custodian process.

1. Point `tools/mcp/custodian.yaml` `args` at your checkout path and set its `env.AWS_PROFILE` +
   a kubeconfig for the cluster. The custodian self-bootstraps connectivity (resolves the Connect PSK
   from the cluster, opens per-tenant port-forwards). In production the custodian is co-located in the
   cluster and the port-forward step drops out; the tool contract is unchanged.
2. Launch the agent:
   ```
   omnigent run deploy/omnigent/sdp-capstone -p "Build the three customer medallions per your brief, then write RESULT.md."
   ```
   The orchestrator seeds each customer, routes authoring cross-vendor, reviews, submits through the
   custodian, repairs on failure until each medallion passes under its contextual policy, probes
   cross-tenant isolation, and writes `RESULT.md`.

## What it proves (a demonstration, not a numbers claim)
The mechanism runs: Polly decomposes; a cross-vendor fleet builds three customers' medallions over
three isolated live tenants; the custodian holds every credential; contextual data policy is enforced
(and correctly rejects a non-conserving draft); the dev loop repairs to convergence; cross-tenant
reads are denied. The quantitative fleet study (cost-per-correct, cross-vendor catch-rate) is a
separate, pre-registered experiment (paper Section 4 S4.7); no number here feeds the paper's claims.
Evidence for the deterministic run: `../../../paper/notes/proof_2026-07-12_sp4_capstone.log`.

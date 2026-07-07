# Unity Catalog (OSS) — fleet-scoped governance for the shared Connect server

This is the **catalog** half of Option A. Identity is established and verified at the Connect layer
(mTLS proxy + `PrincipalPinningInterceptor`); Unity Catalog OSS then provides **audit** and
**fleet-level grants** on top of that verified identity. Be honest about the ceiling: UC **OSS**
does **not** enforce per-user grants from a Spark Connect session, and has **no** row-level security
or column masking.

## Wiring

Append `spark-defaults.uc.conf` to the **Connect server's** `spark-defaults.conf` (server-side).
Substitute:

| Placeholder | Value |
|---|---|
| `__UC_URI__` | Unity Catalog OSS REST endpoint, e.g. `http://uc-host:8080` |
| `__UC_FLEET_TOKEN__` | the **single** service-principal token (fleet scope) |
| `__CONNECT_PSK__` | the Connect pre-shared bearer (same value Envoy injects) |

> This snippet documents the server-side lines only. The actual edit lands in
> `deploy/connect-server/` (out of this task's scope) — see the root `README.md`
> "Required integration".

## Version pin (security)

- Use **Unity Catalog OSS >= 0.5.0**.
- **CVE-2026-27478** affects UC OSS **< 0.4.1** — never deploy a version below the pin.

## What you get vs. what you don't

**Enforced by UC OSS (fleet scope):**
- A governed catalog (`unity`) shared by the fleet under one service principal.
- **Audit**: catalog operations are recorded centrally.
- **Fleet grants**: the service principal is granted READ on permitted sources and
  CREATE/MODIFY only where policy allows; production schemas have no fleet grant.

**NOT enforced by UC OSS — do not claim it:**
- **Per-user UC grants.** All fleet traffic hits the catalog as the one service principal, so UC
  cannot distinguish `alice` from `agent_b` for grant decisions. Per-principal isolation is achieved
  by the **schema convention** below + the verified identity from the Connect layer, **not** by UC
  grants.
- **Row-level security / column masking.** Not available in UC OSS.

## Per-agent schema convention

Each principal writes only to its own schema, named `sandbox_<principal>`:

| Principal | Writable schema |
|---|---|
| `alice` | `unity.sandbox_alice` |
| `agent_a` | `unity.sandbox_agent_a` |
| `agent_b` | `unity.sandbox_agent_b` |

Because `user_id` is pinned to the verified principal (it cannot be spoofed), a tool/policy that
maps `user_id -> sandbox_<user_id>` has a **trustworthy** key to enforce on. This is **convention
enforced above the catalog** (e.g. in the agent harness / SDP `database:` overlay), not a UC OSS
grant. Provisioning each `sandbox_<principal>` schema is an operational step.

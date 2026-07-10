# Unity Catalog (OSS): fleet-scoped governance for the shared Connect server

> **Scope correction (2026-07-10).** The "ceiling" described below is a property of *this fleet-scoped
> binding* (one shared UC service-principal token), **not** of UC OSS itself. UC OSS **0.5.0 does**
> enforce per-principal grants and authz-gated, prefix-downscoped, external-id-pinned credential vending
> when each tenant presents its **own non-owner token** against an authz-enabled server using external
> locations, verified in v0.5.0 source and reproduced live (`paper/notes/proof_2026-07-10_uc_vending.log`,
> 6/6). See the co-equal-binding evaluation in §3.3. What is genuinely UC-limited: per-principal authz is
> not expressible on UC's Iceberg-REST path (metastore-`OWNER`, all-or-nothing), authorization is off by
> default, and OSS has no row-level security / column masking.

This is the **catalog** half of Option A. Identity is established and verified at the Connect layer
(mTLS proxy + `PrincipalPinningInterceptor`); Unity Catalog OSS then provides **audit** and
**fleet-level grants** on top of that verified identity. The ceiling of *this binding*: because all
fleet traffic reaches the catalog as **one** shared service principal, UC cannot distinguish tenants
for grant decisions here, so per-principal isolation in this setup is by the schema convention below
plus the Connect-layer verified identity, not by UC grants (the per-tenant-token path that lifts this
is in §3.3). Row-level security and column masking are absent in UC OSS regardless.

## Wiring

Append `spark-defaults.uc.conf` to the **Connect server's** `spark-defaults.conf` (server-side).
Substitute:

| Placeholder | Value |
|---|---|
| `__UC_URI__` | Unity Catalog OSS REST endpoint, e.g. `http://uc-host:8080` |
| `__UC_FLEET_TOKEN__` | the **single** service-principal token (fleet scope) |
| `__CONNECT_PSK__` | the Connect pre-shared bearer (same value Envoy injects) |

> This snippet documents the server-side lines only. The actual edit lands in
> `deploy/connect-server/` (out of this task's scope), see the root `README.md`
> "Required integration".

## Version pin (security)

- Use **Unity Catalog OSS >= 0.5.0**.
- **CVE-2026-27478** affects UC OSS **< 0.4.1**, never deploy a version below the pin.

## What you get vs. what you don't

**Enforced by UC OSS (fleet scope):**
- A governed catalog (`unity`) shared by the fleet under one service principal.
- **Audit**: catalog operations are recorded centrally.
- **Fleet grants**: the service principal is granted READ on permitted sources and
  CREATE/MODIFY only where policy allows; production schemas have no fleet grant.

**NOT enforced in THIS fleet-scoped binding (UC OSS itself can; see the scope correction at top):**
- **Per-user UC grants.** In this binding all fleet traffic hits the catalog as the one service
  principal, so UC cannot distinguish `alice` from `agent_b` for grant decisions here. Per-principal
  isolation in this setup is achieved by the **schema convention** below + the verified identity from
  the Connect layer, not by UC grants. (Per-tenant tokens lift this, proven in §3.3.)
- **Row-level security / column masking.** Genuinely not available in UC OSS (any binding).

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

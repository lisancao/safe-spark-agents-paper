# Option A — verified per-principal identity + TLS for Spark Connect 4.1

This directory is the **auth/identity layer** for the durable, shared Spark Connect 4.1 server. It
gives every human and agent a **cryptographically verified, non-spoofable identity** and **TLS**,
without forking Spark.

## Why this exists (ground truth)

OSS Spark Connect 4.1 has **no native TLS** and **no per-user authentication**:

- The only native auth is a **single shared pre-shared bearer token**
  (`spark.connect.authenticate.token`) — same secret for everyone.
- The request `user_id` (`sc://...;user_id=alice`) is **client-asserted and spoofable** — a client
  can claim to be anyone.
- The only server-side extension points are
  **`spark.connect.grpc.interceptor.classes`** (comma-separated `io.grpc.ServerInterceptor` classes,
  each requiring a **zero-arg constructor**) and a **fronting proxy**.

**Decision = Option A:** keep one shared server; establish and **verify identity at the Connect
layer** with an mTLS front proxy; pin the spoofable `user_id` to the verified identity with a custom
interceptor; isolate agents by **schema convention**; govern with **Unity Catalog OSS at fleet
(service-principal) scope**.

> Honest scope: per-**USER** UC grant enforcement is **not achievable in OSS** and is **not**
> claimed here. UC OSS gives audit + fleet grants; isolation between principals is convention on top
> of a *trustworthy* (pinned) `user_id`, plus the catalog's fleet-level grants. See `uc/README.md`.

## Architecture (in words)

```
   client (alice / agent_a)                         ON THE CONNECT INSTANCE
   ├─ client cert: CN=alice, SAN URI=spiffe://safe-spark-agents/alice
   └─ sc://connect.internal:15009/;user_id=alice;use_ssl=true
            │
            │  (1) [B1 network layer] Client VPN mTLS — SEPARATE CA, network admission
            │      NLB must TCP PASSTHROUGH :15009 (no TLS termination at the NLB) ──────┐
            ▼                                                                            │
   ┌─────────────────────────── Envoy auth proxy  (:15009) ────────────────────────────┘
   │  (2) [Connect layer] TERMINATES client TLS; require_client_certificate: true
   │      validates the client cert against the Connect-layer CA (certs/)
   │  (3) Lua: strip any client-supplied x-connect-principal, then SET it from the verified
   │      cert (SAN URI last segment, CN fallback)  ->  x-connect-principal: alice
   │  (4) inject authorization: Bearer <PSK>   (the server's shared token; clients never hold it)
   └──────────────────────────────── gRPC/HTTP2 over loopback ─────────────────────────┐
                                                                                        ▼
   ┌──────────────────── Spark Connect server (127.0.0.1:15002) ───────────────────────┐
   │  spark.connect.authenticate.token = <PSK>          (accepts the proxy's bearer)    │
   │  spark.connect.grpc.interceptor.classes = PrincipalPinningInterceptor              │
   │    (5) reject if x-connect-principal absent  -> request bypassed Envoy             │
   │    (6) reject if request user_id != x-connect-principal  -> user_id spoofing       │
   │  => the session's user_id is PINNED to the verified principal                      │
   └──────────────────────────────── Unity Catalog OSS (fleet token) ──────────────────┘
        reads/writes as ONE service principal; audit + fleet grants; sandbox_<principal> by convention
```

Only loopback reaches `:15002`; all external traffic must enter through Envoy on `:15009`. A direct
dial to `:15002` carries no `x-connect-principal` and is rejected by the interceptor (step 5).

## Two layers of mTLS — defence in depth (do not conflate)

| | **B1 — Client VPN mTLS** | **This (B3) — Connect-layer mTLS** |
|---|---|---|
| Layer | Network admission (who can reach the subnet) | Application identity (who the Spark principal *is*) |
| Terminates at | AWS Client VPN | Envoy `:15009` |
| CA | B1's VPN CA | **Connect-layer CA** (`certs/`) — a **separate** root |
| Proves | device/user may enter the network | the verified Spark `user_id` |

They are **independent trust roots**. Do **not** reuse one CA for both. A compromised/misissued VPN
cert still cannot speak to Spark without a valid Connect-layer client cert, and vice-versa.

## What's ENFORCED vs CONVENTION (threat model)

**ENFORCED (cryptographic / code):**
- **TLS in transit** to the Connect endpoint (Envoy terminates; native Spark Connect has none).
- **Verified identity** — principal derives from a CA-signed client cert; `require_client_certificate: true`.
- **No spoofable `user_id` (fail-closed)** — `PrincipalPinningInterceptor` requires every pinned RPC
  to assert a **non-blank** `user_id` that **equals** the verified principal. Absent/blank `user_id`,
  null `UserContext`, an unextractable request shape, or a request that sends no identity-bearing
  message are all **rejected, never forwarded** — there is no path to Spark that skips the pin.
  (Standard gRPC health/reflection RPCs are an explicit, identity-neutral allowlist; they still
  require the verified header.)
- **No non-Envoy bypass** — an RPC without the trusted `x-connect-principal` header (i.e. a direct
  hit on `:15002`) is rejected. The header is non-overridable: Envoy strips any client copy and
  re-sets it from the cert.
- **Constrained cert acceptance** — Envoy accepts only client certs whose URI SAN is in the
  `spiffe://safe-spark-agents/` trust domain (`match_typed_subject_alt_names`), so a cert misissued
  under the same CA without that SAN is refused at the TLS layer.
- **Shared PSK never leaves the instance** — Envoy injects the bearer; clients never hold it.

**CONVENTION (operational, above the catalog):**
- **Per-agent schema isolation** — `sandbox_<principal>` is the only writable namespace; enforced by
  the agent harness / SDP overlay keyed on the now-trustworthy `user_id`, not by UC OSS grants.
- **Fleet-scoped UC** — one service-principal token; UC OSS gives audit + fleet grants but **not**
  per-user grants and **not** RLS/masking.

**Residual risks (named honestly):**
- A holder of a valid client **key** is that principal until the cert expires — protect keys; keep
  `CERT_DAYS` short; revocation is by re-issuing the CA/short TTLs (no CRL/OCSP wired here).
- The Lua filter prefers the URI SAN as the authoritative principal and keeps a CN fallback; with
  `match_typed_subject_alt_names` enforced every accepted cert carries the URI SAN, so the CN
  fallback is a defensive no-op (it cannot be reached by a cert lacking the trust-domain SAN).
- UC OSS cannot tell fleet members apart for grant decisions; cross-schema reads are bounded by the
  schema convention + fleet grants, not by per-user UC enforcement.
- The PSK is a single shared secret between Envoy and the server on loopback; rotate it with the
  EnvironmentFile.

## Layout

```
deploy/auth/
├── README.md                 # this file
├── envoy/envoy.yaml          # mTLS termination + principal injection + PSK + gRPC forward
├── interceptor/              # Maven project: PrincipalPinningInterceptor (+ unit tests)
├── certs/                    # Connect-layer CA + per-principal client/server cert tooling
├── systemd/                  # durable Envoy: unit, config renderer, env, container option
└── uc/                       # Unity Catalog OSS fleet wiring + schema convention
```

## Principal onboarding — issue a cert

```bash
cd deploy/auth/certs
./make-ca.sh                        # once: create the Connect-layer CA
./issue-server-cert.sh              # once: the Envoy server cert (SERVER_DNS in vars.sh)
./issue-client-cert.sh agent_c      # per principal: a client bundle under out/clients/agent_c/
# or issue everything in vars.sh PRINCIPALS at once:
./issue-all.sh
```

Each bundle (`out/clients/<principal>/`) contains `client.crt`, `client.key`, `connect-ca.crt`, and
`CONNECT.md` with the ready-to-use connection string. **The principal id = CN = SAN URI last segment
= the required Spark `user_id`.** A client MUST connect with `user_id=<principal>` or every RPC is
rejected by the interceptor.

Defaults issue the **3 human users** (`alice`, `bob`, `carol`) and the **agent sandboxes**
(`agent_a`, `agent_b`); override with `PRINCIPALS="..."` (see `certs/vars.sh`).

## Wire the interceptor (server-side)

1. Build the jar:
   ```bash
   cd deploy/auth/interceptor
   mvn -q -DskipTests package      # -> target/connect-auth-interceptor-0.1.0.jar
   ```
2. Drop the jar on the **Spark Connect server** classpath (e.g. `$SPARK_HOME/jars/`, or add it to
   the server launch `--jars`/classpath).
3. Set in the **server's** `spark-defaults.conf`:
   ```
   spark.connect.grpc.interceptor.classes com.safesparkagents.connect.auth.PrincipalPinningInterceptor
   spark.connect.authenticate.token       <PSK>
   ```
   Spark constructs the interceptor via its zero-arg constructor.

> These lines belong in `deploy/connect-server/` (out of this task's scope). They are documented
> here, not edited there — see "Required integration".

## Install Envoy (durable)

`func-e` or the official static Envoy build provides `/usr/local/bin/envoy`. Then follow the header
of `systemd/envoy.service` (copy `envoy/envoy.yaml` to `/etc/envoy/envoy.yaml.tmpl`, the renderer to
`/usr/local/bin/`, the certs to `/etc/envoy/certs/`, set `/etc/envoy/auth-proxy.env`, enable the
unit). A container alternative is in `systemd/docker-compose.yml`. The committed config keeps the
PSK as the `__CONNECT_PSK__` placeholder; `systemd/render-config.sh` injects it at start from the
EnvironmentFile so the secret is never committed.

## Required integration (must be reconciled by a human at merge)

This task's scope is **`deploy/auth/` only**. Three changes land in components owned by other tasks
and are flagged here instead of edited:

1. **B1 NLB must run in TCP PASSTHROUGH to the auth-proxy port.** For the client certificate to reach
   Envoy, the NLB in `deploy/aws/` must **not** terminate TLS — it must be a **TCP** listener that
   passes through to Envoy `:15009`. Set `enable_auth_proxy=true` and make the NLB listener **TCP,
   not TLS**. If the NLB terminates TLS, mTLS breaks and every request fails the interceptor's
   bypass check.
2. **Connect server `spark-defaults.conf`** (`deploy/connect-server/`) must add the two lines under
   "Wire the interceptor" (`spark.connect.grpc.interceptor.classes` + `spark.connect.authenticate.token`)
   and ship the jar on the classpath. The full UC fleet snippet is in `uc/spark-defaults.uc.conf`.
3. **Spark Connect server must bind loopback only** (`spark.connect.grpc.binding.host=127.0.0.1`,
   port `15002`) so the only path in is through Envoy.

## Gates (run in this build)

| Gate | Command | Result |
|---|---|---|
| Interceptor compiles | `mvn -q -DskipTests package` | **PASS** — `target/connect-auth-interceptor-0.1.0.jar` |
| Interceptor tests | `mvn -q test` | **PASS** — 12/12, fail-closed (missing-header reject, mismatch reject, match pass, absent-user_id reject, blank-user_id reject, null-UserContext reject, unextractable-shape reject, zero-message half-close reject, blank-header reject, identity-neutral allowlist bypass + exclusion, extractUserId) |
| Envoy config valid | `envoy --mode validate -c envoy/envoy.yaml` | **PASS** — validated via `envoyproxy/envoy:v1.31-latest` (`configuration OK`); host has no `envoy` binary so the Docker image was used, with the `__CONNECT_PSK__` placeholder rendered and the cert paths pointed at a generated test CA |
| Cert scripts lint | `shellcheck certs/*.sh systemd/*.sh` | **PASS** — clean; scripts were also run end-to-end (CA + server cert + 5 client bundles, chains verified with `openssl verify`) |

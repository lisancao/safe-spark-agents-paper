# spark-omnigent container (Phase 2)

A thin-client agent sandbox. Engine-free: it runs the 1.8 MB `pyspark-client` and talks to a
remote, per-agent-authenticated Spark Connect server. Full design in `../../OMNIGENT_SANDBOX.md`.

This README documents the **Option A** wiring: the sandbox reaches the remote Connect server over
**Envoy mTLS with identity pinning**, with per-agent schema isolation.

## Connection model (Option A, end to end)

```
  spark-omnigent SANDBOX (one network namespace, two containers)
  ┌───────────────────────────────┐        ┌──────────────────────────────────────┐
  │ sandbox (thin client)         │        │ egress (Envoy sidecar)                │
  │   pyspark-client, NO JVM      │ h2c    │   holds client cert/key (mounted RO)  │
  │   user_id = <principal>  ─────┼───────►│   127.0.0.1:15002 ──► mTLS, ALPN h2   │
  │   sc://127.0.0.1:15002        │ plain  │   presents URI-SAN client cert        │
  │   (no cert here)              │ loopbk │   verifies server against CA          │
  └───────────────────────────────┘        └───────────────┬──────────────────────┘
                                                            │ mTLS (TLS 1.2+)
                                                            ▼
                                       remote NLB ──► on-box Envoy  (deploy/auth, PR #3)
                                         - verifies client cert
                                         - principal := URI SAN spiffe://safe-spark-agents/<p>
                                         - sets trusted x-connect-principal, injects PSK
                                         - forwards gRPC ▼
                                       Spark Connect server  (deploy/connect-server)
                                         - interceptor REJECTS unless request user_id == principal
                                         ▼
                                       GOVERNED UNITY CATALOG
                                         reads: grants + masking + RLS
                                         writes: convention -> sandbox_<principal>.* only
```

Three layers keep a sandbox pinned to its own principal. **The authoritative guarantee is
server-side (layer 3): the remote interceptor rejects any `user_id` that isn't the cert-verified
principal.** Layers 1–2 are defaults/hardening that make the honest path trivial and a stolen cert
useless — they are *not* a sandbox-side guarantee against a malicious agent (see the caveat).

1. **One identity var (default, not a guarantee).** `AGENT_PRINCIPAL` is the single source.
   `entrypoint.sh` derives the gRPC `user_id`, the `AGENT_SCHEMA` (`sandbox_<principal>`), and
   `SPARK_REMOTE` from it, and aborts if a supplied `AGENT_SCHEMA` != `sandbox_<principal>`. This
   makes the *default* `user_id` correct, but it is **not** a hard wall: code running inside the
   sandbox is in-process and can build its own `SparkSession` with `sc://…;user_id=<anything>`.
   That is exactly why the wall has to live on the server.
2. **Cert ↔ principal pinning (sidecar).** The sidecar refuses to start unless the mounted client
   cert's URI SAN is exactly `spiffe://safe-spark-agents/<principal>` (`openssl` check), so a
   mismatched/wrong cert can't even open the tunnel.
3. **Server-side enforcement (authoritative).** The remote on-box Envoy re-derives the principal
   from the verified cert SAN and the Spark interceptor (deploy/auth, PR #3) **rejects any request
   whose `user_id` != that principal**. So even a malicious in-process agent that forges a different
   `user_id` is refused at the server. This is the guarantee; the negative smoke test below proves
   it is actually in force.

## Feasibility decision: sidecar, not in-client mTLS (with evidence)

**Question.** Can `pyspark-client` (Spark Connect Python client, 4.1.x) present a **client
certificate** for mTLS via the `sc://` string or the builder?

**Finding — the `sc://` string CANNOT.** In `pyspark/sql/connect/client/core.py`:
- the connection-string param allowlist is only `use_ssl, token, user_id, user_agent, session_id`
  (`ChannelBuilder.PARAM_*`, core.py:138–142); there is no client-cert / key param;
- the SSL branch builds creds with **`grpc.ssl_channel_credentials()` and no arguments**
  (core.py:466) — server-auth TLS only. `token` is added as a *call* credential, never a client
  cert.

So mTLS client-cert auth is **not expressible** in `SPARK_REMOTE`/`sc://`.

**A custom builder technically could — and is rejected anyway.** `grpc.ssl_channel_credentials`
*does* accept `(root_certificates, private_key, certificate_chain)`, and `SparkSession.builder`
exposes `.channelBuilder(...)` (session.py:169) to plug a custom `ChannelBuilder` subclass that
overrides `toChannel()`. So in-client mTLS is *possible*. We deliberately do **not** use it:

- **Threat model / key isolation.** The agent is the untrusted party. A custom builder loads the
  private key into the very process the AI agent controls — it could exfiltrate the cert (which is
  longer-lived than the PSK/token). The sidecar holds the key in a **separate container the agent
  cannot read** (the key is mounted only into `egress`).
- **Contract.** This repo's "change one URL" model is built on `sc://` strings. The sidecar keeps
  pyspark 100% stock (`sc://127.0.0.1:15002`, plaintext loopback); a custom builder forks that.
- **Brittleness.** `.channelBuilder()` requires subclassing `DefaultChannelBuilder` and reaching
  into private internals (`_host`, `endpoint`, `_secure_channel`) — fragile across versions. The
  sidecar decouples the mTLS mechanism from the client library entirely.
- **Symmetry.** The server side already runs Envoy (deploy/auth). An Envoy egress sidecar mirrors
  it and negotiates correct HTTP/2 / ALPN `h2` for gRPC.

**→ Decision: a local Envoy mTLS egress sidecar.** The thin client speaks plaintext h2c to
`127.0.0.1:15002`; the sidecar presents the per-principal client cert and originates mTLS to the
remote.

> Evidence is reproducible: `pip download pyspark-client` and read `core.py` `DefaultChannelBuilder.toChannel`.

## What is baked vs injected vs linked
- **Baked (sandbox image):** `pyspark-client` + Connect deps, the catalog config template, the
  entrypoint, and (in a real build) the Omnigent harness. No JVM, no secrets, no cert.
- **Baked (sidecar image):** Envoy + `openssl` + the egress config template + its entrypoint.
- **Linked at run time:** the SDP skill pack (see `skills/README.md`).
- **Injected at run time:** `AGENT_PRINCIPAL` (the identity), `GIT_REPO`, `GIT_TOKEN`; and into the
  sidecar `REMOTE_HOST`/`REMOTE_PORT` + the mounted cert material. `SPARK_REMOTE`, `AGENT_SCHEMA`,
  and `user_id` are **derived**, never injected.

## Provisioning a sandbox identity (operator)

```bash
# 1. Issue the per-principal client cert (URI SAN pinned). In production Omnigent's identity issuer
#    vends this short-lived; the script lives in the auth PR:
deploy/auth/certs/issue-client-cert.sh agent_42        # -> agent_42.crt, agent_42.key (SAN-pinned)

# 2. Place cert material where the sidecar mounts it (see certs/README.md). NOT committed.
cp agent_42.crt agent_42.key deploy/spark-omnigent/certs/
cp ca.crt                    deploy/spark-omnigent/certs/    # CA that signed the REMOTE server

# 3. Launch the sandbox + sidecar together (one identity, two containers):
cd deploy/spark-omnigent
AGENT_PRINCIPAL=agent_42 \
REMOTE_HOST=connect.example.internal REMOTE_PORT=443 \
GIT_REPO=lisancao/safe-spark-agents GIT_TOKEN=$GH_TOKEN \
docker compose up --build
```

`AGENT_PRINCIPAL` feeds **both** services from a single value, so the cert SAN (verified in the
sidecar) and the `user_id` (set in the sandbox) are guaranteed equal. The agent then connects to
`sc://127.0.0.1:15002` and runs against its own `sandbox_agent_42` schema.

Dev (single sandbox container, link the live WIP skill checkout) still works as before — but
without a sidecar there is no mTLS egress, so it only reaches a **plaintext/local** Connect server:
```bash
docker run --rm -it \
  -e AGENT_PRINCIPAL=agent_42 -e SIDECAR_GRPC_ADDR=host.docker.internal:15002 \
  -e GIT_REPO=lisancao/safe-spark-agents -e GIT_TOKEN=$GH_TOKEN \
  -v ~/repos/pyspark-sdp/.claude/skills:/opt/spark-omnigent/skills:ro \
  spark-omnigent:dev
```

## Per-agent schema isolation (honest)

Schema isolation here is **convention + interceptor-pinned identity**, not per-user Unity Catalog
grants:
- **Convention:** every agent's only writable namespace is `sandbox_<principal>`, derived from the
  identity and written into the SDP specs' `database:` via the catalog config template.
- **Enforced by:** the server interceptor pins `user_id == principal` (so an agent cannot *claim* to
  be another principal), and UC governs reads (grants/masking/RLS on the authenticated principal).
- **NOT claimed:** there are **no per-user UC grants** carving `sandbox_<principal>` write-ACLs in
  this layer. The UC principal the Connect server uses is **fleet-scoped**. A determined agent that
  bypassed the SDP convention and issued raw SQL could in principle target another `sandbox_*`
  schema, because UC isn't enforcing per-principal write boundaries yet. Closing that gap (mapping
  each principal to a UC identity with `CREATE`/write only on its own schema) is server-side work
  tracked for `deploy/connect-server` + UC, not solvable in the sandbox.

## Smoke test (post-deploy)

`connect/sandbox_smoke.py` runs **inside** the sandbox once a server exists. No server is deployed
yet, so locally we only `py_compile` it; the real run is the post-deploy step.

```bash
# positive: connect as our own principal, spark.range(5), touch sandbox_<principal>
python connect/sandbox_smoke.py --mode positive

# negative: assert a user_id that != our cert principal; the server interceptor MUST reject.
python connect/sandbox_smoke.py --mode negative; echo "exit=$?"
```

The negative check is strict about what counts as proof, so it can't false-pass on a server that is
simply down. It connects with `user_id=<principal>_evil` while the sidecar still presents the
genuine `<principal>` cert, and classifies the outcome by the **gRPC status code** (never by message
text):

| Exit | Meaning | When |
|---|---|---|
| `0` PASS | a genuine pinning rejection was **observed** | server returned gRPC `UNAUTHENTICATED` or `PERMISSION_DENIED` |
| `1` FAIL | the mismatched request was **accepted** | no error — pinning is broken |
| `2` INCONCLUSIVE | proves nothing about pinning | any transport/TLS/DNS/connection/other error (server down, sidecar not up, `UNAVAILABLE`, no gRPC status) |

So **exit 0 means, specifically, that the server rejected a mismatched `user_id` with an auth
status** — not merely that "some error happened". A down/unreachable server is exit 2, not a pass.
(You can prove the sidecar half locally and independently: mounting a cert whose SAN ≠
`AGENT_PRINCIPAL` makes the sidecar refuse to start — see "Validation" below.)

## What must be true on the server side (dependencies, not edited here)

This PR wires the **client/egress** half. It assumes the auth + server PRs provide:
- **deploy/auth (PR #3):** an on-box Envoy that (a) requires a client cert with URI SAN
  `spiffe://safe-spark-agents/<principal>`, (b) strips any client-supplied `x-connect-principal` and
  sets it from the verified cert, (c) injects the server PSK, (d) forwards gRPC; **and** a Spark
  interceptor that rejects unless `request.user_context.user_id == verified principal`. The
  cert/CA come from `deploy/auth/certs/issue-client-cert.sh`.
- **deploy/connect-server:** the multi-tenant Spark Connect server behind that Envoy, reachable at
  `REMOTE_HOST:REMOTE_PORT` (via the NLB), session-isolated per agent.
- **CA:** `certs/ca.crt` must be the CA that signed the **remote server** cert (for the sidecar to
  verify the server). For defence in depth, also pin the server SAN by setting `REMOTE_SAN` (and
  optionally `REMOTE_SAN_TYPE`, default `DNS`) on the `egress` service once the server cert SAN is
  known — the sidecar entrypoint then appends a `match_typed_subject_alt_names` matcher so the CA
  alone is not sufficient. Left unset, the server is verified by CA only.

## Validation (what was checked here; no real server contacted)

| Gate | Result |
|---|---|
| `python -m py_compile connect/sandbox_smoke.py` | OK |
| `shellcheck` entrypoint.sh + sidecar/entrypoint.sh | OK |
| `envoy --mode validate` (rendered config, via envoyproxy/envoy:v1.31) | `configuration OK` (with and without `REMOTE_SAN`) |
| `docker build` sandbox image | OK |
| `docker build` sidecar image | OK |
| sidecar SAN gate, **matching** principal | verifies SAN, renders, validates, Envoy listens |
| sidecar SAN gate, **mismatched** principal | `FATAL: cert SAN mismatch` → exit 1 |
| entrypoint guards (missing/!regex principal, schema mismatch) | each fails fast, exit 1 |
| derived `user_id` (parsed by pyspark in-image) | `user_id=agent_42`, `secure/use_ssl=False` (plaintext loopback) |
| Envoy admin on UNIX socket (HARDENING 1) | `/ready` → `LIVE` over the socket; TCP `127.0.0.1:9901` **connection refused** |
| negative-smoke classification (all branches, in-image) | `UNAUTHENTICATED`/`PERMISSION_DENIED`→exit 0; `UNAVAILABLE`/`UNKNOWN`/non-gRPC→exit 2; accepted→exit 1 |

The runtime end-to-end smoke (`sandbox_smoke.py` positive+negative against the live server) is the
**post-deploy** step — it needs the server stood up.

## Files

| Path | What |
|---|---|
| `Dockerfile` | thin-client (pyspark-client, no JVM) image |
| `entrypoint.sh` | derives identity (user_id/schema/SPARK_REMOTE) from `AGENT_PRINCIPAL`, fails fast |
| `conf/spark-defaults.template.conf` | catalog config template (rendered at start) |
| `sidecar/Dockerfile` | Envoy + openssl egress image |
| `sidecar/envoy.template.yaml` | egress mTLS config (loopback h2c → mTLS upstream) |
| `sidecar/entrypoint.sh` | cert-SAN ↔ principal check, render, `envoy --mode validate`, exec |
| `docker-compose.yml` | wires sandbox + sidecar in one netns; cert mounted **only** into sidecar |
| `certs/` | per-agent cert material drop (gitignored; see `certs/README.md`) |

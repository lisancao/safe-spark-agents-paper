# Per-agent cert material (NOT committed)

This directory holds the client cert/key the egress sidecar presents for mTLS, plus the CA that
verifies the remote server. **Nothing here is committed** (see `.gitignore`): certs are issued at
provision time, short-lived, and per principal.

Expected files (for `AGENT_PRINCIPAL=agent_42`):

| File | What | Source |
|---|---|---|
| `agent_42.crt` | client cert, `URI SAN = spiffe://safe-spark-agents/agent_42` | `deploy/auth/certs/issue-client-cert.sh agent_42` (PR #3) |
| `agent_42.key` | client private key (paired with the cert) | issued alongside the cert |
| `ca.crt` | CA that signed the REMOTE server cert (to verify the server) | `deploy/auth` / `deploy/connect-server` |

The mounted key must be **readable by the sidecar's `envoy` user (uid 101)** — Envoy reports a
mounted-but-unreadable key as `Failed to load incomplete private key`. Issue/copy it world-readable
or chown to uid 101 (it is short-lived and lives only in the sidecar container).

The sidecar refuses to start unless `agent_42.crt`'s SAN is exactly
`spiffe://safe-spark-agents/agent_42` — so the cert cannot belong to a different principal than the
one the sandbox authenticates as.

Provisioning (operator):

```bash
# 1. Issue the per-principal cert (in the deploy/auth tree; DO NOT do this by hand in prod —
#    Omnigent's identity issuer vends these short-lived, see OMNIGENT_SANDBOX.md "One identity").
deploy/auth/certs/issue-client-cert.sh agent_42        # -> agent_42.crt, agent_42.key (SAN-pinned)

# 2. Drop them here (or bind-mount from the issuer's output), plus the server CA:
cp /path/to/agent_42.crt /path/to/agent_42.key deploy/spark-omnigent/certs/
cp /path/to/ca.crt        deploy/spark-omnigent/certs/
```

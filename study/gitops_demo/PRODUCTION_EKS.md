# Production target: EKS Spark Connect (documented, NOT enabled)

This document describes how the local GitOps slice would point at a **production**
Spark Connect endpoint on EKS. It is **documentation only** — nothing here is wired
into the workflows in this PR. The shipped workflows
(`.github/workflows/gitops-sdp-*.yml`) target a **runner-local** Spark Connect server
(`local_spark_connect.sh`) so the demo is self-contained and safe.

Enabling the production path is a separate, deliberate change owned by a human, gated
on a real cluster and credentials.

## What changes vs. the local slice

| Aspect | Local slice (shipped) | Production EKS (documented) |
|--------|------------------------|------------------------------|
| Spark Connect endpoint | `sc://localhost:15055` (runner-local) | `sc://<connect-svc>.<ns>.svc.cluster.local:15002` (or an ingress/NLB) |
| `storage:` in `spark-pipeline.yml` | `file:///tmp/safe-spark-agents-gitops/<slug>/storage` | `s3a://<bucket>/gitops_demo/<slug>/storage` |
| Cloud credentials | none | GitHub OIDC → AWS IAM role (no long-lived keys) |
| Who holds the session | CI/controller on the runner | controller identity only (IRSA on EKS) |
| Human approval | PR review + merge | PR review + merge **+** an Environment protection gate (optional) |

## 1. GitHub OIDC → AWS role (no static keys)

The reconcile workflow assumes an AWS role via GitHub's OIDC provider — no AWS access
keys stored as secrets. Sketch:

```yaml
permissions:
  id-token: write          # required for OIDC
  contents: read

steps:
  - uses: aws-actions/configure-aws-credentials@v4
    with:
      role-to-assume: arn:aws:iam::<acct>:role/gitops-spark-reconciler
      aws-region: <region>
```

The IAM role's trust policy restricts `token.actions.githubusercontent.com` to this
repo and (recommended) to the `main` branch / a specific Environment, so only the
post-merge reconcile job can assume it.

## 2. Network path to the EKS Spark Connect endpoint

The Connect server runs in the cluster (e.g. a `spark-connect` Deployment + Service).
Two options to reach it from a GitHub-hosted runner:

- **Private (preferred):** a self-hosted runner inside the VPC (or VPC-peered),
  reaching `sc://spark-connect.<ns>.svc.cluster.local:15002` directly. No public
  exposure of the Connect port.
- **Brokered:** an internal NLB / API gateway with mTLS in front of the Connect
  service, reachable only from the runner's security group.

The Connect port is **never** exposed publicly. The reconcile job sets:

```bash
export SPARK_REMOTE="sc://<connect-endpoint>:15002"
```

and runs the same `reconcile.py` / `cli.py run --spec` as the local slice — only the
endpoint and storage scheme differ.

## 3. Production storage

Specs render with `storage: s3a://<bucket>/gitops_demo/<slug>/storage` instead of the
local `file://` root. The S3 bucket is written by the **executors via IRSA** (the same
mechanism the study's live backend uses — see `harness/backends/live.py`), so no AWS
credentials live in the spec or the pipeline code. `sdp_artifact.render_spec(...,
storage_root="s3a://<bucket>/gitops_demo")` already supports this.

## 4. Identity asymmetry (the whole point, in production terms)

- **Controller identity** (the reconcile job / the EKS Connect service account) has
  the IAM role and the network path: it can open a session and run `spark-pipelines
  run`. This is the only identity with `SPARK_REMOTE`.
- **Agent identity** has neither. The agent runs `agent_pr_author.py`, which refuses
  to start if `SPARK_REMOTE` is set and shells out only to `git`/`gh`. It cannot
  assume the reconciler role and has no route to the Connect endpoint.

This is the production realization of the safety-boundary table in
[`README.md`](README.md): the session lives with the reconciler, never with the author.

## 5. Optional human-approval Environment gate

For production reconcile, protect the job with a GitHub **Environment** (e.g.
`production`) that requires a human reviewer:

```yaml
jobs:
  reconcile:
    environment: production     # requires approval before the job runs
    runs-on: [self-hosted, vpc]
```

This adds a second human checkpoint *after* merge and *before* any cluster-side
materialization — defense in depth on top of PR review.

---

**Status:** documented, intentionally not enabled. The shipped slice runs entirely
against a runner-local Spark Connect server.

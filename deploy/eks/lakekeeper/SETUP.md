# Section 3 reference architecture: end-to-end setup

This is the single entry point for standing up the **five-layer per-tenant isolation stack** from §3 on a
live EKS cluster, and reproducing the proof for each layer. It sequences four sub-deployments; each has its
own detailed README, this file is the map and the order.

The five layers (outside-in), and where each is built:

| # | Layer | Built by | Proof artifact |
|---|-------|----------|----------------|
| 1 | mTLS ingress routing (by client-cert principal) | `authz/` (gateway) | `paper/notes/proof_2026-07-10_ingress_routing.log` |
| 2 | Token custody (per-tenant Connect servers) | `authz/` (multi-server) | `paper/notes/proof_2026-07-10_multiserver.log` |
| 3 | Execution isolation (disjoint executor pods) | `authz/` (multi-server) | `paper/notes/proof_2026-07-10_multiserver.log` |
| 4 | Per-principal catalog authorization | `authz/` (Lakekeeper + OpenFGA) | `paper/notes/proof_2026-07-10_perprincipal_authz.log` |
| 5 | Storage scoping (prefix-scoped vended creds) | `spike/` (Lakekeeper vend + IRSA) | `paper/notes/cloudtrail_vend_evidence.md`, `proof_2026-07-10_delta_and_frontier.log` |

## 0. Prerequisites (once)
- An EKS cluster with the substrate applied: `../terraform` (private API + bastion, S3 state, OIDC).
  See `../terraform/README.md`.
- IRSA configured: the vending roles + the `spark` and `lakekeeper` ServiceAccount role bindings
  (`../terraform/lakekeeper-vending.tf`, `../terraform/irsa.tf`).
- The warehouse configs (`spike/eks/warehouse-tenant_*.aws.json`) carry `REPLACE-external-id`; set it to the
  **same value as `TF_VAR lakekeeper_external_id`** (the vending role's `sts:ExternalId` trust condition) or
  `AssumeRole` fails and no credential is vended.
- The `spark-connect` image in ECR (Spark 4.1.2 + Iceberg 1.11.0). Build and push it with
  `deploy/eks/images/spark-connect/build.sh --push` (see `deploy/eks/RUNBOOK.md` §3 "Deployment order";
  `build.sh` reads `INTERCEPTOR_JAR`/`IMAGE_TAG`, and the registry is part of `IMAGE_TAG`), then `export CONNECT_IMAGE=<ecr>/…`.
- A Connect-layer CA + certs, issued with `../../auth/certs/issue-*.sh`:
  a gateway server cert and one client cert per tenant with URI-SAN `spiffe://safe-spark-agents/<tenant>`.
- `kubectl` context on the cluster (via the bastion SSM tunnel), and a Python venv with
  `pyspark[connect]==4.1.2`, `boto3`, `pyjwt`, `cryptography`.

> **Placeholders are not shell-expanded.** The base Connect overlay (`../connect/overlays/example`) and the
> warehouse JSONs carry `REPLACE-*` / `${…}` tokens; `kubectl apply -k` does **not** expand them. Copy each
> overlay to your own and substitute the real values (from `terraform output`: bucket, account, image, role
> ARNs, external-id) before applying.

## 1. Storage-scoping layer (layer 5): `spike/`
Stands up Lakekeeper (allow-all) + Postgres + the two tenant warehouses on real S3 with prefix-scoped
STS vending, then proves cross-tenant `AccessDenied` and the CloudTrail vend-not-IRSA discriminator.
```bash
kubectl apply -k spike/eks           # Lakekeeper + Postgres + Spark Connect + warehouses (substitute REPLACE-* first)
```
The isolation proof (`run_spike.py`) is run from a **client** with a tenant client cert plus a port-forward to
the Connect gRPC (see `PLATFORM_LAB_NOTEBOOK.md` for the exact harness); the shipped `spike/eks/spike-test-job.yaml`
still needs a mounted client cert + `use_ssl` on the mTLS remote, so it is not a single `kubectl apply`. The proof
yields 13/13 (cross-tenant `AccessDenied` both directions, executor-pod off-driver), plus the CloudTrail
vend-not-IRSA discriminator and the ablation.
Provides for later stages: `lakekeeper-postgres`, the `lakekeeper` vending ServiceAccount, and the two
warehouse storage profiles (`spike/eks/warehouse-tenant_*.aws.json`).

## 2. Catalog-authorization layer (layer 4): `authz/`
Adds authentication (OIDC) + authorization (OpenFGA) on a **fresh, isolated** Lakekeeper instance
(do not flip the allow-all one in place). Proves a tenant-A identity is denied at the catalog for tenant-B.
Follow `authz/README.md` (mock IdP + OpenFGA + fresh-DB migrate + serve + bootstrap + grants + `authz_proof.sh`).

## 3. Token-custody + execution-isolation layers (2 + 3): `authz/` multi-server
One Spark Connect server per tenant, each injecting only its tenant's catalog token server-side.
Proves a session on tenant-A's server cannot reach tenant-B, and the two tenants run on disjoint executor pods.
See `authz/README.md` "Multi-server Connect" (`gen_connect_server.py` + `test` harness).

## 4. Ingress-routing layer (layer 1): `authz/` gateway
A gateway Envoy that terminates client mTLS and routes by the certificate's principal to the matching
tenant server. Proves a tenant-A cert reaches only tenant-A's server; un-granted principal → 403; no cert → refused.
See `authz/README.md` "Per-principal ingress routing" (`gateway-envoy.yaml` + `20-connect-gateway.yaml`).

## Reproduce all five proofs
Each stage's harness writes a log under `paper/notes/proof_2026-07-10_*.log` (+ `cloudtrail_vend_evidence.md`).
Together they are the per-layer evidence cited in §3.3. A single request traversing all five links (a Spark job
over client-cert mTLS through the gateway to authz-catalog-vended, prefix-scoped storage on the tenant's own
executors) is the remaining composition step, not yet captured.

## Secrets (never committed)
JWTs, the RS256 private key, `LAKEKEEPER__PG_ENCRYPTION_KEY`, the warehouse `external-id`, client-cert keys,
and the Connect PSK all live outside git (session scratchpad / TF vars / k8s Secrets). Every committed manifest
uses placeholders (`REPLACE-*`, `<ECR>`, `<ACCT>`).

## Teardown
Delete the namespaced workloads and, if desired, the CloudTrail trail `ssa-isolation-audit` + its bucket;
`terraform destroy` the substrate to stop the meter. The two catalogs (allow-all `spike/` and authz `authz/`)
are independent and can be torn down separately.

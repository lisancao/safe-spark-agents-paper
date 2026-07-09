# FINDINGS — does the Lakekeeper-vended credential reach the Spark executor and enforce per-tenant S3 isolation?

De-risk spike for **SP3.4** (Appendix S3-A P5). The whole frontier isolation proof rests on ONE assumption:

> A governed catalog's per-tenant, **prefix-scoped VENDED credential** actually reaches the Spark
> **executor** (which does the S3 FileIO) through Spark Connect and is **what touches storage** —
> not an ambient/static credential (locally: none; on EKS: the executor pod's full-bucket IRSA role).

## VERDICT

| Tier | Result |
|---|---|
| **Local shakeout, generic Spark image (RAN, CI path)** | **YES — 14/14.** Vended cred reaches a separate executor JVM via `S3FileIO`; cross-tenant S3 = AccessDenied. |
| **Local shakeout, REAL project image (RAN)** | **YES — 14/14** with `ssa-spark/spark-connect:4.1.2-iceberg1.11.0` as driver+executor. De-risks the actual shipped artifact, not a stock Spark. |
| **EKS load-bearing proof (manifests delivered, `kustomize build` = 19 objects, UNRUN — no cluster)** | Retires the risk local cannot: an executor **pod** falling back to its full-bucket **IRSA** role. Run on the cluster; steps + discriminators below. |

The local tiers de-risk the config (the point of a W0 spike — don't spend on EKS to find the REST/vending wiring is wrong). The EKS run converts "the vend works when it's the only cred" into "the vend is what's used **even when a broad ambient cred is present**."

## Pinned versions (the working BOM)
- Spark `apache/spark:4.1.2-scala2.13-java17-python3-ubuntu` (Hadoop 3.4.2). No Spark 4.3 exists; 4.1.2 is the anchor. The 4.1.2 image already bundles `spark-connect_2.13-4.1.2.jar` + standalone scripts.
- Iceberg `iceberg-spark-runtime-4.1_2.13:1.11.0` + `iceberg-aws-bundle:1.11.0` (local) / `software.amazon.awssdk:bundle:2.29.52` (the real EKS image — verified sufficient for S3FileIO vending).
- Catalog **Lakekeeper `quay.io/lakekeeper/catalog:v0.13.1`** (latest, 2026-06-30) + Postgres 17.
- Object store (local) MinIO `RELEASE.2025-04-08T15-41-24Z` / mc `RELEASE.2025-04-08T15-39-49Z`. Client `pyspark[connect]==4.1.2` + boto3.

## The exact working config (this is what made it work)
Per-tenant REST catalog on the driver (`config/spark-defaults.conf`; on EKS `eks/patches/rest-catalog.yaml`):
```
spark.sql.catalog.lk_a                                     org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lk_a.type                                rest
spark.sql.catalog.lk_a.uri                                 http://lakekeeper:8181/catalog
spark.sql.catalog.lk_a.warehouse                           tenant_a
spark.sql.catalog.lk_a.header.X-Iceberg-Access-Delegation  vended-credentials   # <-- turns on vending
spark.sql.catalog.lk_a.io-impl                             org.apache.iceberg.aws.s3.S3FileIO   # <-- carries the
                                                                                #     vended cred to executors
```
Load-bearing by ABSENCE: no `spark.sql.catalog.*.s3.access-key-id/.secret-access-key`, no `spark.hadoop.fs.s3a.access.key`, no `AWS_*` env. The endpoint AND the credential both arrive from the vend (in `loadTable`, serialized into tasks). So the vended cred is the *sole* path to storage locally.

Lakekeeper warehouse (MinIO, `config/warehouse-tenant_a.json`): `flavor: s3-compat`, `sts-enabled: true`, `key-prefix: tenant_a`, and a **whole-bucket** `storage-credential` user that Lakekeeper downscopes per warehouse. (MinIO root cannot AssumeRole, so a dedicated broad user is the base identity — the fleet-role analog.) Lakekeeper runs with auth disabled / authz allow-all here (deliberate; see out-of-scope).

## What the local smoke proved (RAN — `out/results.json`, all 14 PASS)
| Check | Establishes | Observed |
|---|---|---|
| `A.write` | multi-partition Iceberg write+read as A over Spark Connect | 8 partitions, `row_count=4000` |
| `A.executor` | a **separate executor** ran the fanned-out write | `executor host=172.22.0.5 totalTasks=12` (worker=172.22.0.5, driver=172.22.0.6) |
| `A.vend` | catalog vends a **temporary session-token** cred (not a static key) | `access-key-id=GHXTTO… session-token=yes` |
| `A.own` | that vended cred is live for A's own data | `head_object` on A's metadata.json succeeds |
| **`A.cross_read`** | same vended cred **DENIED on B's prefix at S3** | `GetObject tenant_b/…` → **AccessDenied** |
| **`A.cross_write`** | …and cannot write B's prefix | `PutObject tenant_b/…` → **AccessDenied** |
| `ablation.broad_ambient` | the vend is **load-bearing** | broad whole-bucket cred **succeeds** on both prefixes (had the executor used a broad ambient cred, cross-tenant would go through) |
| B.* | symmetric B→A | all PASS |

Corroboration: MinIO request trace shows the **8 Iceberg data-file PUTs** under `tenant_a/…/data/*.parquet` issued from client `172.22.0.5` = the executor container. Structural check (`out/structural.txt`): no static S3 key directive, no `AWS_*` env on executor or driver. So a successful executor write can *only* have used the vended credential.

**Mechanism:** Iceberg's `SparkCatalog` loads the table on the driver; the REST `loadTable` (with `X-Iceberg-Access-Delegation: vended-credentials`) returns temp `s3.access-key-id/secret/session-token` + endpoint; those land in the table's `S3FileIO` properties, which Iceberg **serializes into every scan/write task**. The executor rebuilds `S3FileIO` with a static-credentials client and never consults the ambient AWS chain. That serialization is the propagation path the assumption hinges on — and it holds on 4.1.2 / Iceberg 1.11.0, on both the generic and the real project image.

## What the EKS run MUST confirm (the part local can't)
On EKS the executor **pod** carries a full-bucket IRSA role, so cross-tenant AccessDenied can only happen if the executor uses the vended (scoped) cred and **not** IRSA. The manifests wire exactly that: keep the broad `…-irsa-spark` role (the ambient to beat); add a separate `…-lakekeeper-vending` role that Lakekeeper assumes and **downscopes per warehouse** via STS session policy (`eks/terraform/lakekeeper-vending.tf`); warehouses use `credential-type: aws-system-identity` + `sts-role-arn` (`eks/warehouse-tenant_*.aws.json`); run the same `run_spike.py` via `eks/spike-test-job.yaml`.

Two discriminators make it a proof (not a false GREEN):
- **(c) CloudTrail — the direct "vend, not IRSA" check.** The tenant_a **data-file `PutObject`** must show `userIdentity` = a session of `…-lakekeeper-vending`, **not** `…-irsa-spark`. That is the executor telling you which credential it used.
- **(d) Ablation (mirrors R7).** Remove the `…header.X-Iceberg-Access-Delegation` line, re-apply, re-run: with vending off, `S3FileIO` falls back to the executor's IRSA (full bucket) → **cross-tenant SUCCEEDS** → isolation breaks. The delta is the entire frontier result.

Executor-side FileIO is forced by design: `run_spike.py` does a `repartition(8)` shuffle write, so the write stage fans out to executor tasks (locally: 8 data-file PUTs from the executor container; on EKS with dyn-alloc up to 10, across executor pods). Asserted via the driver UI executors API (`totalTasks ≥ partitions`) — a driver-only path cannot produce that.

Stated up front to be falsifiable: with this config, A's executor is denied B (AccessDenied attributable to the downscoped vending session), and the ablation flips it to allowed. If instead tenant_a writes show up under `…-irsa-spark`, the vended cred is NOT what touches storage on EKS and the assumption is **false on 4.1.2** — a build gap to retire before any isolation number is claimed.

## Deliberately out of scope (and why it's fine)
- **Catalog RBAC / per-principal grants (OpenFGA + IdP).** This spike isolates the *storage* layer (Appendix S3-A's "money shot: a storage-layer denial, not an app-level check"). Catalog authz is layer 1, well-understood, orthogonal, and added on EKS (`LAKEKEEPER__OPENID_PROVIDER_URI` + OpenFGA backend + grant A on warehouse_a only). Allow-all here does not weaken the storage result — the STS-downscoped credential enforces it, and authz does not touch it.
- **Envoy mTLS / principal-pinning ingress.** Reused unchanged on EKS; irrelevant to which credential the executor uses.

## Risks / notes for the EKS run
- MinIO root can't AssumeRole → local uses a dedicated broad user; AWS analog is `aws-system-identity` + the vending role (wired).
- Vended creds are short-lived (`max_session_duration 3600`). For very long jobs, Iceberg `remote-signing` delegation is the alternative (prefix-scoped, no creds on the executor).
- Lakekeeper's Spark integration is less documented than Polaris's (why this spike mattered more). The working REST config was verified against Lakekeeper's own docs (`/engines`, `/storage`) and confirmed by the runs here; no Lakekeeper/Spark workaround was needed on v0.13.1 for this path.

## How to reproduce
- Local (no AWS): `cd deploy/eks/lakekeeper/spike && ./run.sh` (exit 0 iff all checks pass; artifacts in `out/`). Real artifact: `./run.sh --real-image`. Tear down: `./run.sh --down`.
- CI: `.github/workflows/lakekeeper-spike.yml` — no AWS, no secrets.
- EKS: `terraform apply` (now includes `eks/terraform/lakekeeper-vending.tf`) → fill `REPLACE-*` in `eks/lakekeeper.yaml` + `eks/warehouse-*.aws.json` → `kubectl -n spark create configmap spike-runner-code --from-file=run_spike.py=../run_spike.py` → `kustomize build deploy/eks/lakekeeper/spike/eks | kubectl apply -f -` → inspect the test Job + CloudTrail + run the (d) ablation.

## Sources
- Lakekeeper Spark/engine config: https://docs.lakekeeper.io/docs/latest/engines/
- Lakekeeper S3/MinIO storage + STS vending: https://docs.lakekeeper.io/docs/latest/storage/
- Iceberg REST credential vending (`X-Iceberg-Access-Delegation`): https://iceberg.apache.org/rest-catalog-spec/
- Lakekeeper release v0.13.1: https://github.com/lakekeeper/lakekeeper/releases/latest

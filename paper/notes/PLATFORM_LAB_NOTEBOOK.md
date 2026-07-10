# Platform Build — Lab Notebook (§3 / Sections 3–4 substrate)

Running forensic record of building the governed Connect-on-Kubernetes + Lakekeeper platform
that the §3/§4 frontier result runs on. Companion to the §1 `experiments/safe_agent_study/LAB_NOTEBOOK.md`.
**Purpose:** reproducibility + the honest build narrative for the paper's supplemental. **Discipline:**
PAPER.md claim-flips happen only at a paper-revision gate with Lisa (see the SSOT `BUILD_PROGRAM.md`);
this notebook is the live documentation that feeds that gate. Every AWS account id / ARN is redacted here.

Build sequence (SSOT `SECTION3_platform_build_checklist.md`): **W0 spike → W1 OIDC → W2 substrate → W2.5 proof → W3+**.

---

## 2026-07-09 — W0: Lakekeeper vended-credential de-risk (GREEN, config level)
**Question (the load-bearing assumption of SP3.4):** does a catalog's per-tenant, prefix-scoped *vended*
credential actually reach the Spark **executor** through Spark Connect and become the sole path to storage —
not the executor pod's ambient IRSA role?
**Result:** GREEN, 14/14, twice — on a generic Spark 4.1.2 image AND the real project image
(`ssa-spark/spark-connect:4.1.2-iceberg1.11.0`). Evidence: an 8-partition Iceberg write produced 8 data-file
PUTs from a *separate* executor container; the catalog vended a temp session-token credential; that cred was
`AccessDenied` on the other tenant's prefix; a broad-ambient ablation showed the vend is load-bearing; no static
S3 key / `AWS_*` env on the Spark path. Mechanism: Iceberg's `SparkCatalog` puts the vended cred (from
`loadTable` with `X-Iceberg-Access-Delegation: vended-credentials` + `io-impl=S3FileIO`) into the table's
FileIO properties, which serialize into every task; the executor rebuilds `S3FileIO` with those static creds and
never consults the ambient chain. Full detail: `deploy/eks/lakekeeper/spike/FINDINGS.md`.
**Scope honesty:** this de-risks the *config*. The definitive "executor **pod** uses the vend, not IRSA" proof
is EKS-only (W2.5), because the failure mode (pod falling back to a full-bucket IRSA role) only exists on k8s.

## 2026-07-09 — W1: GitHub→AWS OIDC bootstrap + gated terraform-apply (DONE)
`deploy/eks/bootstrap-oidc/` applied once by hand (admin creds): GitHub OIDC provider, a read-only *plan* role,
a write *apply* role scoped to the `eks-apply` GitHub Environment (required-reviewer gate = the AWS-mutation
boundary), and the encrypted S3 + DynamoDB remote-state backend. 5 repo Variables set; `eks-apply` environment
created with Lisa as required reviewer. **No long-lived AWS keys anywhere.** Snag: the DynamoDB lock table
pre-existed → adopted with `ignore_changes=all` (never modify a shared table).

## 2026-07-09 — W2: substrate (SP3.1) — the June-collision saga (DONE, hard-won)
First CI apply (gated, approved) assumed the OIDC role and built ~90 resources, then **collided** with a set of
"already exists" resources. **Root cause (mis-diagnosed at first):** a **live-but-forgotten June cluster**
(`ssa-spark-eks`, created 2026-06-23, ACTIVE, never torn down) plus its data plane — a **live RDS billing since
June 24**, a 69 MB warehouse bucket, and cluster-scoped IAM/KMS/OIDC — all sharing globally-unique names with
the new stack. My applies had built a **parallel duplicate** (a second VPC + 86 resources) next to the live June
deployment.
**Resolution (Lisa: full clean slate):** captured a manifest of the June warehouse (8,207 objects) for the
record, then tore down BOTH deployments — `terraform destroy` of the duplicate, and manual teardown of June's
cluster + node groups + RDS + bucket (versioned; purged 10,769 versions) + IAM + OIDC + KMS + the whole
orphan VPC (NAT/subnets/IGW/SGs). Then a **fresh apply → one clean cluster: `ssa-spark-eks` k8s 1.31, 118
resources**, S3-backed state, 2 node groups, RDS metastore, `irsa_spark` (fleet-wide, the SP3.4 lockdown target)
+ `irsa_hms`, S3 warehouse, OIDC.
**Never-run-manifest snags fixed along the way:** RDS deletion-protection blocked destroy (disable first);
a Secrets Manager secret stuck in the deletion recovery window (restore + force-purge); EKS auto-recreates its
own CloudWatch log group (import, don't fight).
**LESSON (recorded in SSOT):** before any apply, `aws eks describe-cluster` to check for a pre-existing
deployment. A stale "L0 ran 2026-06-24" note in the paper hid the fact the cluster was *still live*.

## 2026-07-09 — W2.5: EKS-native isolation proof (IN PROGRESS)
**Access design:** cluster API is **private-only** (good posture). Chose the bastion path over opening the API:
`create_bastion=true` → an SSM-managed bastion (no inbound/SSH) → `session-manager-plugin` port-forward →
kubectl reaches the private API. Cluster stays private; admin flows through the governed jump host.
**Deployed so far:** the two vending IAM roles (`ssa-spark-lakekeeper-catalog` IRSA can *only* assume
`ssa-spark-lakekeeper-vending`, which downscopes per-warehouse). **Lakekeeper + Postgres RUNNING** in ns `spark`.
**Snag (never-run on EKS):** the Lakekeeper pod's `runAsNonRoot: true` clashed with the root Lakekeeper image
(`Init:CreateContainerConfigError`) → fixed by pinning `runAsUser: 1000` (static Rust binary, runs fine).
**Remaining:** deploy the Connect-on-k8s + Envoy mTLS overlay (needs generated CA/certs/PSK + envsubst on
`${ECR_REGISTRY}/${AWS_ACCOUNT_ID}/${WAREHOUSE_BUCKET}/${RDS_ENDPOINT}`) → provision the 2 tenant warehouses →
run the isolation test → capture the **CloudTrail "vend-not-IRSA"** discriminator + the **R7 ablation**
(vending off → cross-tenant succeeds). Design + acceptance: `SECTION3_isolation_experiment.md`.

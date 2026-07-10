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
**Connect + Envoy mTLS deployed + RUNNING (2/2).** Steps that worked: issued CA + server cert (SANs =
`spark-connect-mtls[.spark.svc...]`, matching the in-cluster service the test dials) + per-tenant client certs
via `deploy/auth/certs/issue-all.sh`; created the two out-of-band secrets `spark-connect-psk` +
`spark-connect-envoy-certs`; rendered the overlay with `kubectl kustomize | envsubst` (scoped to the 4 vars) and
applied. **Snags fixed:** a third out-of-band secret `spark-iceberg-jdbc` (keys `ICEBERG_JDBC_USER/PASSWORD`,
the RDS metastore creds) is required and not in kustomize — created it from the terraform-managed Secrets Manager
entry `ssa-spark/metastore/connection` (via `--from-file`, password never on argv). After that + a rollout
restart the pod came up 2/2. **Net: the governed platform (P1 mTLS ingress + P2 Connect-on-k8s + P5 Lakekeeper)
is LIVE on EKS.**
**REST-catalog patch applied** (JSON6902 appends `lk_a`/`lk_b` vended-credential catalogs to the connect-server
args; container 0 confirmed = connect server; rolled out clean).

### Findings from warehouse provisioning (paper-worthy; Lakekeeper storage model)
Running the provision job surfaced two real Lakekeeper behaviors (its Spark/AWS integration is under-documented):
1. **Warehouse-management writes use the pod's base identity, not the vended role.** With
   `credential-type: aws-system-identity`, Lakekeeper's warehouse-creation validation `PutObject` ran as the
   catalog pod's IRSA identity (`ssa-spark-lakekeeper-catalog`), which by design had *no* direct S3 → denied.
   **Fix:** grant the trusted catalog role its own warehouse S3 (`catalog-manage-s3` policy in
   `lakekeeper-vending.tf`). This does **not** weaken the isolation result — tenant isolation is enforced at the
   AGENT **executor** via vended, prefix-scoped creds, not at Lakekeeper's identity.
2. **Assume-role vending requires an `external-id`.** After (1), Lakekeeper returns
   `An 'external-id' is required when using 'assume-role-arn'`. Adding `external-id` to the warehouse
   `storage-profile` did NOT satisfy it → the correct field placement (likely the `storage-credential` block,
   and/or the vending role's trust policy must add a matching `sts:ExternalId` condition) needs the Lakekeeper
   v0.13.1 storage-credential schema. **This is the current blocking point for provisioning.**

### RESOLVED: the external-id fix (source-confirmed) + tenants provisioned
Root cause (confirmed against Lakekeeper `crates/lakekeeper/src/service/storage/s3.rs`): `external-id` belongs
**inside the `storage-credential` object**, not `storage-profile` (where it was silently ignored). The working
warehouse config: `storage-credential: {type:s3, credential-type:aws-system-identity, external-id:<secret>}` +
`storage-profile: {..., flavor:aws, assume-role-arn:<vending-role>, sts-enabled:true}`; and the vending role's
trust policy needs a matching `sts:ExternalId` StringEquals condition (Principal = the catalog pod's IRSA role;
`sts:TagSession` NOT needed — Lakekeeper downscopes via an inline session policy on the key-prefix). Applied
(external-id kept as a TF-var secret, never committed) → **both tenant_a + tenant_b warehouses provisioned,
HTTP 201, status active.**

### UC-OSS verdict (answered Lisa's "would UC work better?")
No — swapping would be a *downgrade + replatform*, not an escape: UC-OSS avoids external-id only because it uses
static long-lived AWS keys (no IRSA, no confused-deputy protection — losing the keyless posture that IS our
security story); **Spark Connect is undocumented/unsupported in UC-OSS**; per-tenant prefix isolation needs a
data-model remap; days-to-weeks of rework. Lakekeeper stays load-bearing; UC-OSS stays the non-load-bearing
second binding. (Full analysis + sources in the session research.)

### Final remaining step: run the isolation test (one wiring detail)
The platform + catalog are fully up and provisioned. Last piece: connect the test client to Connect. Note:
**pyspark's standard Spark-Connect client can't present an mTLS *client* cert** via the connection string, so the
test either (a) uses the platform's client-cert-presenting pattern (`connect/client.py`) to go through Envoy, or
(b) bypasses Envoy for the *storage* proof via `kubectl port-forward` to the loopback `15002` (the mTLS ingress
is a separately-demonstrated P1 piece; the load-bearing storage-isolation result doesn't require it). Then
run_spike.py runs the R1–R3 + R7 ablation; capture CloudTrail (vend-not-IRSA).

### RESULT: isolation proof RAN on live EKS — core claim demonstrated (2026-07-09)
Ran `run_spike.py` (TEST-only) against the live Connect+Lakekeeper stack. **Core isolation PROVEN, both
directions:** a vended tenant_a credential is `AccessDenied` on tenant_b's S3 prefix (read AND write), and
symmetrically tenant_b→tenant_a — while each tenant reads/writes its OWN data and gets a temporary session-token
cred. That is the frontier claim: untrusted per-tenant agents provably confined at the storage layer, on a real
cluster, through Spark Connect + Lakekeeper vended credentials.

**Auth chain validated en route** (the interceptor made the client satisfy all of): `x-connect-principal`
present + PSK bearer + `user_id == principal`. For the TEST harness we bypass the Envoy mTLS door via a
`kubectl port-forward` to the loopback gRPC and supply those three via the connection string — the mTLS ingress
is a separately-demonstrated P1 piece; the storage-isolation result does not depend on it.

**Two remaining refinements (harness artifacts, NOT isolation failures):**
1. **`*.executor`**: the small write ran on the *driver* (Spark local execution) rather than a separate executor
   *pod*, so run_spike's "a distinct executor ran it" check fails. The driver also carries the full-bucket IRSA
   role and used the scoped vend anyway, so "a full-IRSA process uses the vend not its role" IS shown — just not
   yet on a distinct executor pod. Fix: force executor-only execution (disable dynamic-alloc + pin
   `spark.executor.instances`, or a larger shuffle) and re-check.
2. **`ablation`**: the broad `ssa-deploy` cred returned **`NoSuchKey`** (not `AccessDenied`) on `tenant_b/probe`
   — which CONFIRMS the mechanism (broad cred is *not* scoped out; the vended cred IS). The check wanted a
   successful GET on a probe key that was never seeded. Fix: seed `tenant_b/probe` first (or treat
   not-AccessDenied as pass).

**Harness note:** long-lived `kubectl port-forward` over the SSM tunnel drops on the longer (executor) run — the
robust path for the airtight rerun is an in-cluster test job (socat/ghostunnel sidecar → Envoy), not a
port-forward. All infra + provisioning committed; the venv, certs, deploy work-dir, external-id + PSK live in the
session scratchpad (secrets, never committed).

### State at pause (2026-07-09)
**Platform is LIVE on EKS:** Lakekeeper+Postgres, Spark Connect + Envoy mTLS (2/2), vending roles, `lk_a`/`lk_b`
catalogs. **Blocked:** warehouse provisioning, on the Lakekeeper `external-id` schema (finding #2). All infra +
the SSOT are committed; the bastion SSM tunnel + the filled deploy work-dir + the issued certs live in the
session scratchpad (secrets, never committed).
**REMAINING recipe (resumable):** (a) fix the `external-id` placement per Lakekeeper's storage docs (+ add the
`sts:ExternalId` trust condition on `lakekeeper-vending` for correctness) → provision the 2 tenants; (b) run the
spike-test Job (mount a tenant client cert; `SPARK_REMOTE=sc://spark-connect-mtls:15009`) → (c) capture the
**CloudTrail "vend-not-IRSA"** discriminator + the **R7 ablation** (drop the `X-Iceberg-Access-Delegation` conf →
cross-tenant succeeds). Design + acceptance: `SECTION3_isolation_experiment.md`.

### RESOLVED — airtight 13/13 (2026-07-09, later same day)
Both harness gaps from the "State at pause" run were **harness artifacts, not isolation failures**, and are now
closed. Full log: `paper/notes/proof_2026-07-09_airtight_13of13.log`.

- **`*.executor` (was FAIL → PASS).** Root cause was *detection*, not execution: the Connect server runs
  `dynamicAllocation` ON, so executors recycle after an idle timeout, and run_spike queried the *active*
  `/executors` view (empty by scrape time) against a possibly-stale `apps[0]`. Fix (committed in
  `run_spike.py::executor_ran_tasks`): query **`/allexecutors`** (retains removed executors + their `totalTasks`
  history) across **all** app attempts. (First cut summed the *cumulative* `totalTasks` and reported "3 pods,
  38/50 tasks"; the adversarial audit below caught that as a shared-app-lifetime artifact. Corrected to a
  per-write **delta**, see the 2026-07-10 note.) The write runs on a **separate executor pod, not the driver**.
- **`ablation.broad_ambient` (was FAIL → PASS).** The prior `NoSuchKey` was just an unseeded probe key. Seeded
  `tenant_a/probe` + `tenant_b/probe`; the broad `ssa-deploy` (whole-bucket) credential now **succeeds** on
  *both* tenants' prefixes → confirms the base identity CAN cross tenants, so the cross-tenant `AccessDenied` on
  the vended path is caused by the **downscoping vend**, not the base policy. The vend is load-bearing.

**Net result (the frontier claim, stated precisely).** Two evidence channels, kept distinct: (1) the Spark UI
shows each tenant's write, an 8-partition shuffle, running on a **separate executor pod** (12-task per-write
delta), not the driver; (2) CloudTrail
shows all cluster-side warehouse FileIO went through the **tenant-scoped vend** (fleet IRSA role: 0 warehouse
data calls). The **cross-tenant deny** (`AccessDenied` A→B and B→A, read and write) is observed by **replaying
that same vended credential** against the other prefix, not by a pod being refused in-cluster; the ablation shows
a full-bucket credential would have crossed freely, so the deny is the downscoping vend. What this demonstrates
is **per-tenant DATA isolation / credential scoping at storage**, NOT principal-gated confinement: the harness
runs one session pinned to `tenant_a` and reuses it for both tenants, so that session freely obtains tenant_b's
vended credential (the catalog vends by warehouse address, not by pinned principal). **Still frontier:**
per-principal authorization (the principal→tenant binding = §4 custody), and per-tenant *execution* isolation
(the same shared executor pods served both tenants sequentially; dedicated per-tenant pools/servers need multi-server Connect
orchestration) and multi-tenant scale (P6). Harness: `scratchpad/run_proof2.sh` (Connect gRPC + Spark-UI 4040
port-forwards, `SPARK_DRIVER_UI=localhost:4041`); secrets stay in scratchpad, never committed.

### CloudTrail "vend-not-IRSA" discriminator (2026-07-10)
Independent confirmation from AWS's own audit log that the isolation is enforced by the **vended** credential,
not the executor's ambient fleet role. Full writeup: `paper/notes/cloudtrail_vend_evidence.md`.
- **STS (Event history, always-on):** the vending role `ssa-spark-lakekeeper-vending` is assumed ONLY by the
  catalog IRSA identity, always with the **external-id** (confused-deputy), and the tenant vends carry a
  **session policy** (the downscoping).
- **S3 (data-event trail `ssa-isolation-audit`, re-ran the 13/13 proof under it):** all 41 warehouse object
  events were made under the **vended session**; cross-tenant `GetObject`+`PutObject` = `AccessDenied` both
  directions; own-tenant Get/Put/Head/List = OK; and the fleet role `ssa-spark-irsa-spark` made **0** warehouse
  data calls. Trail + delivery bucket kept up (cost is not a constraint).

### Adversarial audit + hardening (2026-07-10)
Ran a 32-agent adversarial audit (6 skeptic dimensions, each finding independently verified) against the
isolation claim + its paper wording. Verdict: **keep the DEMONSTRATED tier; fix 4 real overclaims in the framing
verbs** (all in the same few sentences). Applied, and where cheap, *closed* rather than reworded:
1. **(major) credential-scoping, not agent-confinement.** The harness runs one session pinned to `tenant_a` and
   reuses it for both tenants, so that session freely obtains `tenant_b`'s vended cred. What is demonstrated is
   per-tenant DATA isolation / credential scoping at storage, not principal-gated confinement; the principal→
   tenant binding is §4 custody. Split this everywhere in §3. **Closed** with an explicit `principal_gate_probe`:
   the `tenant_a`-pinned session was handed `tenant_b`'s vended cred and read its table, so the frontier is now
   empirically located, not just asserted.
2. **(minor) denial locus.** Every cross-tenant `AccessDenied` is a boto3 replay of the vended cred, not an
   executor pod refused in-cluster. Reworded.
3. **(minor) two channels, not one run.** Spark UI (pod fan-out) and CloudTrail (vend-not-IRSA) are different
   channels/runs; state them separately.
4. **(minor) per-write vs cumulative task count.** `totalTasks` was a shared-app-lifetime counter; the "3 pods,
   38/50" figure double-counted earlier tenants/probes. **Closed** with a per-write **delta** in
   `executor_ran_tasks` (snapshot before/after the write): the honest number is **1 executor pod, 12 tasks per
   write**. Paper corrected accordingly. Log: `paper/notes/proof_2026-07-10_delta_and_frontier.log`.

### Per-principal catalog authorization CLOSED (2026-07-10) -- the audit's strongest finding
The major audit finding (the catalog scopes a *credential* but does not gate *which principal* may request
*which* tenant) is now closed at the catalog. Built a fresh, isolated authz stack in the `spark` namespace
alongside the allow-all one (do NOT flip authz in place: allow-all warehouses have no OpenFGA ownership tuples):
- **mock IdP** (`idp-authz`, nginx) serving a static OIDC discovery doc + JWKS; hand-minted RS256 JWTs for
  `admin`/`tenant_a`/`tenant_b` (sub -> Lakekeeper principal `oidc~<sub>`). No Keycloak/dex needed: Spark/curl
  send a static bearer, so the IdP only needs to publish discovery + JWKS for signature verification.
- **OpenFGA** (`openfga`, v1.14, memory store) as the authorization backend.
- **Lakekeeper v0.13.1** fresh instance (`lakekeeper-authz`, own DB `catalog_authz`, SA `lakekeeper` for the
  vending IRSA): `migrate` auto-installs the OpenFGA store + model **v4.7**; serve with
  `AUTHZ_BACKEND=openfga` + `OPENID_PROVIDER_URI/AUDIENCE/SUBJECT_CLAIM=sub`. Boot confirmed OIDC discovery +
  OpenFGA authorizer active.
- Bootstrap (admin JWT, `is-operator`) -> provision `oidc~tenant_a/b` -> create both warehouses (reused the real
  S3 storage-profile + vending role + external-id) -> **grant each tenant `select/describe/modify/create` on
  ONLY its own warehouse** via `POST /management/v1/permissions/warehouse/{wid}/assignments`.

**Result (`proof_2026-07-10_perprincipal_authz.log`):** a `tenant_a` JWT is **denied at the catalog** for
`tenant_b` across warehouse resolution (`config`), namespace list/create, and the credential-vending path
(**404**, existence hidden for a zero-relation principal), while `admin` and `tenant_b` get **200** and no-token
is **401**; symmetric for `tenant_b`. The deny is proven to be *authorization* (not nonexistence) by toggling:
granting `tenant_a` a `describe` relation on `tenant_b` flips **404 -> 200**, revoking flips back. So per-principal
authorization AT THE CATALOG is demonstrated. **Still frontier:** binding the per-tenant identity token to each
Spark Connect session (so an agent can't present another tenant's token) = §4 custody; per-tenant execution
isolation. Manifests + runbook: `deploy/eks/lakekeeper/authz/`; secrets (JWTs, keys, external-id) in scratchpad,
never committed.

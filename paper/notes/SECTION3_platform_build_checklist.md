# Section 3 — Open Governed Platform: Build Checklist
**Purpose:** turn Appendix S3-A (the reference architecture) into a build plan — **what we salvage, what we build, in what order.** Derived from the reverse-engineering map (PAPER.md Appendix S3-A §R). Layers P0–P6; cites are file:line on `origin/dev`.

**Reading:** P0→P4 is mostly **salvage + wiring** (the pieces exist, they need connecting + proving on EKS). **P5/P6 is genuine new construction** (a governed catalog + multi-server Connect). Build bottom-up; don't claim a layer while a lower one is unproven.

## Ownership key
HUMAN (Lisa: gates/creds/spend) · INFRA OPS (terraform/kubectl/ECR — cluster creds) · CODE (delegate → coding sub-agent PR + cross-review) · RUN (execute on EKS) · NEW BUILD (does not exist yet).

---

## Phase 0 — Decision gate  [HUMAN]
- [x] **Scope picked (2026-07-09, Lisa): BOTH in sequence, one runway** — single-tenant P0–P4 as de-risking, then multi-tenant frontier P5–P6. Headline = SP3.4 isolation proof.
- [x] **AWS spend APPROVED (2026-07-09, Lisa): "don't worry about cost, you have my blessing."** Sequencing to not waste it: de-risk the load-bearing Lakekeeper-vending→executor assumption LOCALLY + SP2.2 first, then `terraform apply` (SP3.1).
- [x] **Governed-catalog LOCKED (2026-07-09, Lisa): Lakekeeper primary + Unity Catalog OSS second binding** (Polaris rejected on operational weight; Lakekeeper = single Rust binary, vendor-neutral, per-tenant grants + vending. UC-OSS non-load-bearing → the "catalog-agnostic" claim + an honest in-paper UC evaluation). Design: `SECTION3_isolation_experiment.md`.

## Build harness — CI / GitHub Actions  (2026-07-09, Lisa's choice)
The platform is built THROUGH CI: dogfoods the paper's GitOps thesis, reproducible, every run a citable artifact. Repo `sdp-paper-local` (public) is safe because AWS steps use **GitHub OIDC** (role trust scoped to this repo+branch, no long-lived keys) behind **GitHub Environments with required approval** (Lisa approves each AWS-mutating apply); fork PRs get no secrets/OIDC. Sequence:
- **W0 (no AWS) — DONE 2026-07-09, GREEN.** Config shakeout ran **14/14 on the real project image** (`ssa-spark/spark-connect:4.1.2-iceberg1.11.0`): the vended cred reaches a *separate executor*, cross-tenant S3 = `AccessDenied`, and a broad-ambient ablation shows the vend is load-bearing. Working BOM pinned (Spark 4.1.2 / Iceberg 1.11.0 / **Lakekeeper v0.13.1**); the `X-Iceberg-Access-Delegation: vended-credentials` + `S3FileIO` config is the propagation path. Verdict + exact config: `deploy/eks/lakekeeper/spike/FINDINGS.md`. Config is de-risked; the pod-vs-IRSA proof is W2.5.
- **W1 (bootstrap) — DONE 2026-07-09 (applied via `ssa-deploy`).** `deploy/eks/bootstrap-oidc/` applied: GitHub→AWS OIDC provider + read-only *plan* role + write *apply* role (scoped to the `eks-apply` environment) + encrypted/versioned S3 state bucket `ssa-tfstate-lisancao-use1` live; the pre-existing `ssa-tf-locks` DynamoDB table was adopted (`ignore_changes = all`, never modified). The 5 repo Variables are set and the **`eks-apply` GitHub Environment exists with 1 required reviewer (Lisa)**. Gated apply workflow: `.github/workflows/eks-terraform-apply.yml`. No long-lived AWS keys anywhere. **W2 is now one gated dispatch.**
- **W2 (SP3.1) — DONE 2026-07-09.** Cluster `ssa-spark-eks` (k8s 1.31) live: 118 resources, 2 node groups (system + executor), RDS metastore, IRSA roles (**`irsa_spark`** in ns `spark` = the fleet-wide role to lock down for SP3.4; `irsa_hms` in ns `hive-metastore`), S3 warehouse, cluster OIDC. Sensitive outputs (ARNs, endpoints) live in terraform state (S3), NOT committed. **Recovery lesson:** the first CI apply collided with a LIVE June cluster (`ssa-spark-eks`, built 2026-06-23, never torn down) + its data plane (live RDS billing since June, 69MB warehouse). Resolved by full clean-slate teardown of BOTH the June deployment and the accidental duplicate, then a fresh apply. **Before any apply, `aws eks describe-cluster` to check for a pre-existing deployment.**
- **W2.5 (EKS-native de-risk — the REAL proof):** on the cluster, Connect-on-k8s (driver + separate executor pods) + Lakekeeper + 2 tenants; prove the vended cred reaches executors and cross-tenant is `AccessDenied`. Load-bearing; MUST be on EKS (local can't test IRSA-vs-vended on real pods). Two discriminators (per FINDINGS): **(c)** CloudTrail shows the tenant data-file `PutObject` under the *vending* STS session, NOT the `irsa-spark` role; **(d)** the vending-off ablation flips cross-tenant to SUCCEEDS. Manifests delivered + kustomize-validated (19 objects) under `spike/eks/`. Also demonstrates §2's prototype-local → submit-to-external-cluster loop. Proceed to W3+ only if it passes.
- **W3+:** deploy Connect+Envoy → native mTLS (SP2.2) → elastic scaling (SP3.2) → EKS-wired GitOps (SP3.3) → frontier (Lakekeeper + IRSA lockdown + per-tenant servers + R1–R7).

**W2.5 progress (2026-07-09):** private admin access via **bastion** (`create_bastion=true`) + SSM port-forward (cluster API stays private, no public exposure). Vending IAM roles applied (`ssa-spark-lakekeeper-catalog` IRSA + `ssa-spark-lakekeeper-vending`). **Lakekeeper + Postgres RUNNING in ns `spark`** (fixed a never-run-on-EKS bug: pod `runAsNonRoot: true` needed an explicit `runAsUser: 1000` for the root Lakekeeper image). Filled deploy manifests live in a scratchpad work dir (account-specific values NOT committed; repo keeps the `REPLACE-*` templates). **REMAINING for the proof:** deploy the Connect-on-k8s overlay (needs generated mTLS certs + PSK + envsubst on `${ECR_REGISTRY}/${AWS_ACCOUNT_ID}/${WAREHOUSE_BUCKET}/${RDS_ENDPOINT}`) → provision the 2 tenant warehouses → run the isolation test → the CloudTrail "vend-not-IRSA" check + the R7 ablation.

**PAPER GATE (deferred, Lisa 2026-07-09):** the substrate status-flip (P0 / L0–L1: "not-applied / design-only" → **DEMONSTRATED**, terraform-applied + CI-gated) is DEFERRED and bundled with the **W2.5 isolation result** — ONE paper-revision-gate update with Lisa, touching PAPER.md once. Until then PAPER.md stays read-only, and everything above P0 (P2–P6, the isolation proof) stays FRONTIER. The W0 spike is a config de-risk, NOT the proof.

## Phase 1 — Substrate  (P0)  [INFRA OPS] — SALVAGE: terraform stack, manifests, HMS, image all built
- [ ] `terraform apply` → EKS + IRSA + S3 + RDS/HMS; capture `terraform output`. (Closes "NOT APPLIED" `[terraform/README.md:121-130]`.)
- [ ] Build + push the Spark Connect image (Spark 4.1.2 / Iceberg 1.11.0 / interceptor) to ECR `[images/spark-connect/build.sh]`. *Caveat: use the env-var form, not the stale `--registry` flag.*
- [ ] `kustomize build deploy/eks/connect/overlays/example | kubectl apply -f -`; capture `kubectl -n spark get pods,svc` `[RUNBOOK.md:111-116]`. *Caveat: secret names = `spark-connect-psk` / `spark-connect-envoy-certs` (manifest names, not RUNBOOK's).*
- [ ] Record the mTLS NLB endpoint `[connect/base/service-mtls.yaml:23-42]`.

## Phase 2 — Governed ingress  (P1)  [CODE + RUN] — SALVAGE: Envoy mTLS, principal interceptor
- [ ] Establish **native client mTLS** (client cert + Connect CA) from the SDP/Connect client — **no socat tunnel** (prior runs cheated). Prove principal pinning end-to-end: a mismatched `user_id` is rejected `[envoy.yaml:86-158; PrincipalPinningInterceptor.java:90-145]`.

## Phase 3 — Elastic execution  (P2)  [RUN] — SALVAGE: dyn-alloc, executor pod template, node group
- [ ] Prove a **0→N→0 executor scale cycle** under load (dyn-alloc enabled, never reproduced) `[connect/base/deployment.yaml:171-191]`. Capture executor-pod counts + Spark stages.
- [ ] [NEW BUILD] Add a node autoscaler (Karpenter or Cluster-Autoscaler) — none committed; node-level scaling is design-only `[terraform/README.md:192-198]`.

## Phase 4 — GitOps boundary on EKS  (P3/P4)  [CODE] — SALVAGE: agent_pr_author, dry-run+reconcile workflows, tests
- [ ] [CODE] Repoint the dry-run + reconcile workflows at the **EKS Connect endpoint** (currently runner-local) `[.github/workflows/gitops-sdp-*.yml]`.
- [ ] Capture a **real agent-opened PR** running the gate end-to-end (mechanism exists, no captured artifact).
- [ ] Enable production reconcile on merge (today doc-only `[gitops_demo/PRODUCTION_EKS.md:1-10]`).
- [ ] [NEW BUILD, future / not-a-focus] Add **data-quality / expectation tests** to CI — the net for §1's silent residue (gate is structural-only today).

## Phase 5 — Tenant isolation  (P5, FRONTIER)  [NEW BUILD] — SALVAGE: Envoy mTLS + principal interceptor, backends/live.py, blind oracle
**Isolation is enforced on FOUR independent planes; the full proof design is in `SECTION3_isolation_experiment.md`.**
- [ ] [DE-RISK SPIKE — do FIRST] Prove the per-tenant **Lakekeeper**-vended credential reaches the **executor pods** (not their IRSA role) through Spark Connect on the 4.1.2 image, and B's prefix returns `AccessDenied`. Verify Lakekeeper's Spark-Connect vending path specifically (less documented than Polaris, so it matters more) + pin Spark/Iceberg/Lakekeeper versions. Load-bearing assumption of the whole proof.
- [ ] [NEW BUILD · plane 1 identity · MOST load-bearing] Strip the fleet-wide IRSA role → per-tenant ServiceAccount + IRSA role scoped to that tenant's S3 prefix. Vending only isolates if the vended token is the SOLE path to data.
- [ ] [NEW BUILD · plane 2 data/authz] Governed catalog for per-principal grants + prefix-scoped credential vending. **Primary: Lakekeeper** (single Rust binary; per-tenant grants via OpenFGA + credential vending); **second binding: Unity Catalog OSS** (non-load-bearing → the "catalog-agnostic" claim). Replaces HMS+Iceberg-JDBC `[deploy/auth/README.md]`.
- [ ] [NEW BUILD · plane 3 compute] Per-namespace ResourceQuota + dedicated node pool (taints/affinity) + node selectors.
- [ ] [NEW BUILD · plane 4 network] Default-deny NetworkPolicy between tenant namespaces.
- [ ] [EXPERIMENT] Run the two-agent isolation proof (positive control + R1–R7 red-team incl. the R7 ablation) per `SECTION3_isolation_experiment.md`. Acceptance: mechanism ON = 0/N crossings, OFF = m/N, each denial a citable artifact.

## Phase 6 — Multi-tenant scale  (P6, FRONTIER)  [NEW BUILD]
- [ ] [NEW BUILD] Per-tenant Connect servers behind ONE governed ingress that routes by client-cert SAN (extends P1; pattern: Kimahriman spark-connect-proxy). A single shared server is structurally NOT a tenant boundary (session isolation ≠ security isolation — state as a finding).
- [ ] [NEW BUILD] Tie per-tenant executor pools to node autoscaling (Karpenter / Cluster-Autoscaler).

---

## What this lets us claim, by phase
- **P0–P2 done** → "demonstrated single-tenant governed, elastically-scaled Connect-on-k8s."
- **P3–P4 done on EKS** → "demonstrated GitOps integration boundary against a production substrate."
- **P5–P6** → the multi-tenant platform — explicitly NEW BUILD; until then §3 claims the single-tenant boundary + names multi-tenancy as the frontier (per Appendix S3-A).

## Salvage summary (what already exists, by the reverse-eng map)
Strong salvage: GitOps loop code + workflows + tests; Envoy mTLS + interceptor; Connect-on-k8s manifests + dyn-alloc + pod template + node group; the container image; terraform/HMS/RDS/S3 IaC. **Genuine gaps to build:** native client mTLS, proven elastic scaling, autoscaler, EKS-wired GitOps, governed catalog (per-tenant authz), multi-server Connect.

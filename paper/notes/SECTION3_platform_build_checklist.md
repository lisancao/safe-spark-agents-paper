# Section 3 — Open Governed Platform: Build Checklist
**Purpose:** turn Appendix S3-A (the reference architecture) into a build plan — **what we salvage, what we build, in what order.** Derived from the reverse-engineering map (PAPER.md Appendix S3-A §R). Layers P0–P6; cites are file:line on `origin/dev`.

**Reading:** P0→P4 is mostly **salvage + wiring** (the pieces exist, they need connecting + proving on EKS). **P5/P6 is genuine new construction** (a governed catalog + multi-server Connect). Build bottom-up; don't claim a layer while a lower one is unproven.

## Ownership key
HUMAN (Lisa: gates/creds/spend) · INFRA OPS (terraform/kubectl/ECR — cluster creds) · CODE (delegate → coding sub-agent PR + cross-review) · RUN (execute on EKS) · NEW BUILD (does not exist yet).

---

## Phase 0 — Decision gate  [HUMAN]
- [x] **Scope picked (2026-07-09, Lisa): BOTH in sequence, one runway** — single-tenant P0–P4 as de-risking, then multi-tenant frontier P5–P6. Headline = SP3.4 isolation proof.
- [ ] Approve AWS spend (terraform apply = first irreversible cost; EKS + RDS + S3 + node groups). *Path committed; confirm the actual `terraform apply` trigger at SP3.1 execution.*
- [x] **Governed-catalog LOCKED (2026-07-09, Lisa): Lakekeeper primary + Unity Catalog OSS second binding** (Polaris rejected on operational weight; Lakekeeper = single Rust binary, vendor-neutral, per-tenant grants + vending. UC-OSS non-load-bearing → the "catalog-agnostic" claim + an honest in-paper UC evaluation). Design: `SECTION3_isolation_experiment.md`.

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

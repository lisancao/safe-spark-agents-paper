# Section 2 — Standing Up the Control-Boundary Architecture (agent → SDP → remote Connect)
**North star (keep top of mind):** the **control boundary** — agent *proposes inert desired state*; a
*governed system, on a different host, validates and executes*; the agent never holds a live session,
Connect credentials, or touches data. The dev loop (propose → dry-run gate → reconcile/execute) *is* that
boundary. Declarative makes it expressible; **Connect is the enforcement mechanism, not the motivation.**

**The test this checklist must pass:** the boundary is *demonstrated* only when **(author host) ≠ (execution
substrate)** AND **the agent never holds the Connect creds.** Anything less is a co-located simulation.

## Verified current state (two read-only lenses + lab-notebook/git history, origin/dev @422c571)
- **The remote EKS Connect substrate WAS exercised (2026-06-23/24) — NOT "localhost only."** Recorded
  endpoint `sc://localhost:15008` was a **local mTLS tunnel** (`socat mTLS -> kubectl pf -> svc/spark-connect-mtls`)
  to the **remote EKS Spark Connect service** (`6ff8139`; `.worktrees/BENCH2/.../study.config.live.json:2,13`).
- **Across-host execution partially DEMONSTRATED.** On the GO calibration: driver+executors ran in k8s pods,
  **Arm A materialized silver/gold/quarantine tables on the cluster**, S3/IRSA staging worked, and an in-cluster
  compute probe ran (`spark.range(80M).sum()` -> stages [60,62]) `[DEVIATIONS.md:184-227, 345-368]`.
- **But NO Arm B SDP pipeline completed/graded green remotely.** Remote Arm B rows are all
  `max_iterations / reached_correct=false`, failing on INTEGRATION errors (PIPELINE_SPEC_FILE_NOT_FOUND,
  RUN_EMPTY_PIPELINE, PATH_NOT_FOUND, PARSE_EMPTY_STATEMENT) `[.worktrees/h2-review/.../calib_1782318596.jsonl:81-82]`.
- **The localhost switch was DELIBERATE, not a failure** — `8f15bfd` (2026-06-24 10:53 PDT) added `--backend local`
  to remove the imperative-can't-run-on-remote-Connect confound from Part 1's paradigm comparison; the remote
  `live` path was left intact `[DEVIATIONS.md:498-522]`.
- **Submission mechanism = remote-capable in principle** (PySpark ships plans not files over Connect gRPC)
  `[cli.py:221-263; spark_connect_graph_element_registry.py:51-136]`.
- **EKS substrate = built + previously applied/deployed; not currently torn-down state-tracked in origin/dev.**
  Terraform README says "NOT APPLIED" but the notebook shows a live `ssa-spark-eks` cluster existed 2026-06-24.
- **Imperative-on-Connect incompatibility = operationally CORROBORATED** (it's why Part 1 moved local,
  `DEVIATIONS.md:516-522`) but never captured as a clean pass/fail probe artifact.
- **All remote evidence lives in UNTRACKED worktrees** (`.worktrees/BENCH2`, `.worktrees/h2-review`) — ephemeral,
  not committed to origin/dev. Must be preserved.

> Honest one-liner for the draft: *"The control-boundary architecture was stood up and exercised across hosts on
> a real EKS Connect cluster (Arm A materialized tables; compute measured in-cluster), but no Arm B SDP pipeline
> has yet COMPLETED and graded green remotely — the remaining failures are integration bugs, not architectural.
> Part 1 deliberately reverted to a local substrate to isolate the paradigm comparison."*


---

# PART A — Make the control boundary REAL (the load-bearing architecture work)
*This is the section's actual contribution. EKS (Part B) is just the substrate it submits to.*

- [ ] **A1 — [confirmed IMPLEMENTED] client-side plan submission.** Agent files → local driver imports →
      plans over Connect gRPC; server needs no files. *No build needed — but state it explicitly in §6 as the
      mechanism that makes a remote boundary possible.* `[cli.py:221-263]`
- [ ] **A2 — [EXERCISED] Input data staging to remote-visible storage.** `live.stage_input` ships input NDJSON
      to S3 (`live.py:630-662`) and was proven staging rows over Connect + writing S3 via executors during the
      EKS calibration `[DEVIATIONS.md:205-227]`. Re-confirm on a fresh cluster; treat as working, not unbuilt.
- [ ] **A3 — [PARTIAL] Real remote catalog.** Image configures Iceberg JDBC/HMS (`spark-defaults.template.conf:33-40`);
      Arm A `saveAsTable` to the S3-backed cluster catalog was "proven materializing in calibration"
      `[DEVIATIONS.md:223-227]`. Confirm SDP storage path resolves the same remote catalog (some SDP failures were
      PATH_NOT_FOUND / spec-storage related — likely an SDP-side catalog/storage wiring bug to fix).
- [ ] **A4 — [PARTIAL — reached via tunnel, native client unproven] mTLS/PSK auth.** mTLS to the Envoy svc was
      reached, but terminated by a **local `socat` tunnel**, not native PySpark-client TLS
      (`study.config.live.json:2`). Open question stays: can `SparkSession.builder.remote("sc://...:15009;use_ssl=true")`
      present cert + `Bearer PSK` + `x-connect-principal` (`envoy.yaml:71-158`) NATIVELY, no tunnel? Testable against
      a local Envoy before any EKS spend.
- [ ] **A5 — [PARTIALLY DEMONSTRATED — still the north-star item] Execution off the agent host.** The EKS run
      already put the **Spark driver + executors in k8s pods** and materialized Arm A tables there
      `[DEVIATIONS.md:191-194]` — execution genuinely ran off the agent host. What's NOT yet clean: the SDP CLI
      *client/reconciler* still runs as a subprocess on the harness host holding the creds (`live.py:675-689`). For
      the boundary to be fully real, run that reconciler in a **governed controller the agent does not control**.
      *(This is what separates "demonstrated control boundary" from "ran SDP on a remote server.")*

# PART B — The substrate Part A submits to (EKS infra = plumbing, demoted)
- [ ] **B0 — [HUMAN GATE: Lisa] approve AWS spend** (terraform apply is the first irreversible cost) + pick demo
      sweep size. (Consistent with §1's spend gate.)
- [ ] **B1 — [INFRA OPS] apply Terraform** → EKS + IRSA + S3 + RDS/HMS; capture `terraform output`. Closes
      `terraform/README.md:3-5` "NOT APPLIED".
- [ ] **B2 — [INFRA OPS] build/push the Spark Connect image** (Spark 4.1.2 / Iceberg 1.11.0) to ECR.
- [ ] **B3 — [INFRA OPS] apply k8s manifests** (`kustomize build .../overlays/example | kubectl apply`),
      `kubectl -n spark get pods,svc` — **capture output**; this is the missing endpoint evidence.
- [ ] **B4 — [INFRA OPS] record the mTLS NLB endpoint** (Envoy LB :15009, `service-mtls.yaml:15-42`).

# PART C — The demonstration run + the probe
- [ ] **C1 — [CODE/delegate] point `live` config at EKS:** `spark_remote` = NLB endpoint, `spark_rest_url` set,
      `warehouse_uri` = S3, catalog = remote (currently localhost/null/file://, `study.config.json:16-17`).
- [ ] **C2 — [CODE/delegate] stamp the Connect endpoint into ResultRow provenance** so a row PROVES it ran on
      EKS (today rows carry git_sha/spark_version but not the remote endpoint).
- [ ] **C3 — [THE narrow remaining gap] Get an Arm B SDP pipeline to COMPLETE + grade green on the `live`/EKS
      backend** end-to-end (propose → gate → execute-on-EKS → blind-grade). Remote Arm B previously only failed on
      INTEGRATION bugs (PIPELINE_SPEC_FILE_NOT_FOUND / RUN_EMPTY_PIPELINE / PATH_NOT_FOUND) — debug those, don't
      re-architect. **This is the missing run-and-evidenced artifact + the north-star demonstration.**
- [ ] **C4 — Confirm a correct Arm B SDP pipeline COMPLETES on EKS** (not just connects). No current artifact
      shows this.
- [ ] **C5 — [knockout probe — partly corroborated] imperative patterns** (`sparkContext`/`_jvm`/RDD/static-config)
      **against the EKS Connect endpoint** → record pass/fail + error class → compatibility matrix. Note: imperative
      already proved un-runnable on remote Connect operationally (it's why Part 1 moved local, `DEVIATIONS.md:516-522`)
      — this probe just CAPTURES that as a clean artifact. Flips §8 argued→executed; feeds §3 + §1 H3.
- [ ] **C6 — [CODE/delegate] update README** (drop "sweep NOT run", `README.md:11-12`) + commit EKS run
      artifacts to origin/dev (today untracked worktree files only).

---

- [ ] **C7 — [preserve evidence] Recover/commit the prior remote run artifacts** from `.worktrees/BENCH2` and
      `.worktrees/h2-review` (env.json + jsonl + study.config.live.json) into a tracked location before they're lost
      — they're the only record that the remote path ran at all.

## What converts in the draft, by part
- **Part A done** → §6/§7 stop borrowing §1's *local* data; the section gains its OWN remote control-boundary
  evidence, and the "demonstration not proposal" header becomes true for the architecture.
- **A4/A5 specifically** are the difference between "we ran SDP on a remote server" and "we **demonstrated the
  control boundary**" — keep these as the headline, not the EKS deploy.
- **Part B** is necessary plumbing; it is NOT the contribution. Don't let the draft frame EKS deployment as the
  achievement.
- **C5 done** → §8 Connect-incompatibility flips argued→executed.

## Sequencing reality
A1/A2 are done; A3 is config; **A4 and A5 are the genuine unknowns** and should be de-risked FIRST (A4 is even
testable against a local Envoy before any EKS spend). B (infra) and C (the run) only matter once A4/A5 hold —
otherwise we'd deploy a cluster we can't actually submit to under the boundary.

# H3-on-EKS integration log — issues, root causes, fixes

**Purpose:** running record of every blocker hit wiring the H3 (data-processing compute) A-vs-B
sweep onto the real EKS Spark-Connect cluster, so (a) it's reproducible and (b) genuine
**Spark-4.1 / Connect / SDP framework** issues can be handed to the Spark engineers, separated from
our own harness/config fixes.

**Environment:** pyspark **4.1.0.dev4**; EKS Spark Connect (`${EKS_CLUSTER}`, driver pod
`spark-connect-*`, executors as pods); mTLS via socat `localhost:15008` → NLB `:15009` (principal
`alice`); Spark-UI REST `localhost:18080`; S3 warehouse `s3a://${WAREHOUSE_BUCKET}/warehouse`.
Instrument: worktree branch `h3-per-attempt-compute` (frozen `ca48c8c` + per-attempt compute + L3 S3 staging).

## Classification key
- **[OURS-config]** — our run configuration; fix on our side.
- **[OURS-harness]** — our harness code; fix on our side.
- **[FW-Connect]** — a Spark **Connect** framework behavior/limitation (⇒ engineers).
- **[FW-SDP]** — a Spark **Declarative Pipelines** framework behavior/limitation (⇒ engineers).

## Issue table

| # | Symptom (error) | Root cause | Class | Fix | Status |
|---|---|---|---|---|---|
| 1 | Runner would hit the stale local docker Connect | `study.config.json` `spark_remote = sc://localhost:15002` (local docker), not EKS | OURS-config | `study.config.eks.json` → `sc://localhost:15008/;user_id=alice` | ✅ fixed |
| 2 | `UNAUTHENTICATED: user_id 'lnc' ≠ verified principal 'alice'` | Connect `PrincipalPinningInterceptor` pins `user_id` to the mTLS client-cert SAN (deployment security, by design) | OURS-config | `;user_id=alice` in the remote URI | ✅ fixed (expected behavior) |
| 3 | **`PATH_NOT_FOUND (SQLSTATE 42K03)`** on both arms (agent's pipeline reads a path the remote executors can't see) | `warehouse_uri` unset in config → `ConnectExecutor.staging_base` empty → `stage_input()` no-ops back to the **local** `file:///…` path (guard at `live.py:667`) | OURS-config | set `warehouse_uri = s3a://${WAREHOUSE_BUCKET}/warehouse` (derives the per-cell `_ssa_staging/…` base) | ✅ fixed — verifying |
| 4 | Arm A (imperative): **`CANNOT_CONFIGURE_SPARK_CONNECT_MASTER`**, **`JVM_ATTRIBUTE_NOT_SUPPORTED`** (3/12 iters; the dominant error was actually #3 `PATH_NOT_FOUND`, 8/12) | Imperative that reaches for `.master()`/`sparkContext`/`_jvm`/RDD can't run through Connect (§8) — BUT DataFrame-API imperative runs fine over Connect | **FW-Connect** (residual, narrow) | **✅ resolved in practice**: once wall #3 (data path) was fixed, the imperative agent wrote clean Connect-compatible DataFrame code and **completed on EKS in 1 iteration** (`exec_s=2.445`, `cpu_s=1.54`). No spark-submit path needed. §8 residual risk only if an agent's imperative reaches for `sparkContext`/`_jvm`/RDD | ✅ resolved (DataFrame-API imperative); §8 residual noted |
| 5 | Arm B (SDP): **`SESSION_MUTATION_IN_DECLARATIVE_PIPELINE.SET_RUNTIME_CONF`** | The declarative pipeline **rejects runtime session-config mutation** (the agent tried to set a session conf). This is the same immutable-config property that blocks `session.timeZone` — the root cause of the D7 timezone defect (Section 1 §4.1.2) | **FW-SDP** | agent must use a column-level idiom (no session conf). **The framework gap: SDP offers no declarative, symmetric way to pin `session.timeZone`** | ⏳ noted for upstream |
| 6 | Arm B grading: **`TABLE_OR_VIEW_NOT_FOUND: 'gold_daily'`** (SDP materialized it fine — the table + storage exist) | The EKS session's **default catalog is `iceberg`** (`spark.catalog.currentCatalog()`), but SDP writes to `spark_catalog.default` (its spec); grading's `read_table("gold_daily")` was **unqualified** → resolved in `iceberg` → not found. Qualified `spark_catalog.default.gold_daily` reads fine | OURS-harness | `ConnectExecutor.read_table` now qualifies unqualified names with the SDP spec's `catalog.database` (`live.py:641`) | ✅ fixed — verifying |

## Framework issues to raise with the Spark engineers (upstream-worthy)

**A. [FW-SDP] No declarative way to pin session config (esp. `spark.sql.session.timeZone`) in a
declarative pipeline.** `spark.conf.set(...)` inside a `@dp.materialized_view`/pipeline function is
rejected (`SESSION_MUTATION_IN_DECLARATIVE_PIPELINE.SET_RUNTIME_CONF` / the D5 `CANNOT_MODIFY_CONFIG`
class). Imperative code sets `session.timeZone=UTC` in one line and gets correct UTC day-bucketing;
SDP has **no equivalent** — no `spark-pipeline.yml` `configuration:` block or per-view arg that the
engine applies before evaluation. Consequence measured in Section 1: SDP agents ship timezone/day-bucket
(D7) correctness defects that imperative never does, purely because they lack this lever. **Ask:** a
symmetric, declarative session-config surface for SDP.

**B. [FW-Connect] No clean local→executor file-staging primitive for a Connect client.** Remote k8s
executors can't see the client's filesystem, so `spark.read.text(file:/local/…)` is `PATH_NOT_FOUND`.
`SparkSession.copyFromLocalToFs` only writes to the **driver** pod's default FS (here `file:`), so a
scheme'd `s3a://` dest is rejected (`NO_SCHEMA_AND_DRIVER_DEFAULT_SCHEME`) and a scheme-less dest lands
on driver-local disk executors can't read. Our workaround: ship rows over Connect via
`createDataFrame` → `df.write.text(s3://…)` (executors write S3 via IRSA). Works, but **there is no
first-class way to stage a local dataset to executor-readable storage from a Connect client.**

**C. [FW-Connect] Imperative PySpark is unsupported on Connect (context for uniform-substrate compute
studies).** `.master()`, `sparkContext`, `_jvm`, RDDs all fail (`CANNOT_CONFIGURE_SPARK_CONNECT_MASTER`,
`JVM_ATTRIBUTE_NOT_SUPPORTED`). Documented/expected, but it means a fair A(imperative)-vs-B(SDP) compute
comparison on one cluster needs imperative to run via cluster spark-submit alongside Connect — noting
the exact error classes for anyone attempting the same.

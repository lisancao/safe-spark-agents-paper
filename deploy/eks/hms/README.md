# Hive Metastore on EKS — durable, Postgres-backed, S3-aware

The **catalog for the reference architecture.** A real Hive Metastore (HMS) — not the throwaway
`apache/hive` on embedded Derby — backed by **Postgres (RDS)** for durability and wired for
**S3 (s3a)** so it can manage `s3://`/`s3a://` warehouse locations. Runs on **EKS** with
**IRSA** (no static AWS keys). Spark Connect points `spark.sql.catalogImplementation=hive` at it
and uses Iceberg (Hive-type catalog) on top.

> We standardized on **HMS** for this reference architecture (not Unity Catalog).

---

## What's here

```
deploy/eks/hms/
├── image/
│   ├── Dockerfile              # apache/hive:4.0.1 + Postgres JDBC, S3A promoted onto classpath
│   └── build-and-push.sh       # build + (optional) ECR push
├── base/                       # kustomize base (raw, environment-agnostic YAML)
│   ├── serviceaccount.yaml     # IRSA-annotated SA
│   ├── configmap-env.yaml      # non-secret tunables (warehouse URI, region)
│   ├── configmap-hive-site.yaml# hive-site.xml TEMPLATE + render script
│   ├── secret.example.yaml     # shape of the DB Secret (NOT applied; create the real one out-of-band)
│   ├── schema-init-job.yaml    # idempotent `schematool -initSchema -dbType postgres`
│   ├── deployment.yaml         # HMS Deployment (2 replicas, probes, initContainer)
│   ├── service.yaml            # ClusterIP `hive-metastore:9083` + headless
│   ├── pdb.yaml                # PodDisruptionBudget
│   └── kustomization.yaml
└── overlays/example/           # per-environment values (namespace, image, IRSA ARN, bucket)
    ├── namespace.yaml
    └── kustomization.yaml
```

**Why kustomize (not Helm):** HMS is a single, small component with a handful of resources. The
only thing that varies per environment is a few literals (image ref, IRSA ARN, warehouse bucket,
region, namespace). Kustomize keeps the base as plain, directly-validatable YAML and isolates
those literals in one overlay — no templating language, no `helm` binary required to read or
review the manifests. If you later fold HMS into an umbrella Helm release, the base YAML drops in
cleanly as chart templates.

---

## Dependency on `eks-cluster-iac`

This directory contains **no cluster infrastructure** — it consumes outputs from the separate
`eks-cluster-iac` task. Provide these (every `REPLACE-*` placeholder maps to one):

| `eks-cluster-iac` output | Used in | Purpose |
|--------------------------|---------|---------|
| `hms_irsa_role_arn`      | `serviceaccount.yaml` annotation / overlay patch | IAM role the HMS pods assume (S3 access) |
| `warehouse_bucket`       | `configmap-env.yaml` → `HMS_WAREHOUSE_DIR` | S3 bucket for the managed warehouse |
| `aws_region`             | `configmap-env.yaml` → `HMS_S3A_REGION` | S3A region |
| `ecr_repo_url`           | overlay `images[].newName` | where the built image lives |
| RDS endpoint + creds     | the `hive-metastore-db` Secret | Postgres connection |

The IAM role (`hms_irsa_role_arn`) **must** be created by `eks-cluster-iac` with:
- a **trust policy** federating this cluster's OIDC provider for
  `system:serviceaccount:<namespace>:hive-metastore`, and
- a **permissions policy** granting `s3:GetObject/PutObject/DeleteObject/ListBucket` (and
  `s3:GetBucketLocation`) on `arn:aws:s3:::<warehouse_bucket>` and `.../*`.

RDS must be a Postgres instance/cluster reachable from the cluster's node/pod subnets (security
group ingress on 5432 from the EKS nodes), with a database (e.g. `metastore`) and a login role.

---

## The image

`image/Dockerfile` layers two things onto `apache/hive:4.0.1` (verified contents of that image):

| Jar | Version | Source | Why |
|-----|---------|--------|-----|
| PostgreSQL JDBC | `42.7.4` | downloaded from Maven Central, **sha256-verified** | stock image only knows Derby |
| `hadoop-aws` | `3.3.6` | **already in the image** (`$HADOOP_HOME/share/hadoop/tools/lib`), symlinked onto `$HIVE_HOME/lib` | S3A filesystem |
| `aws-java-sdk-bundle` | `1.12.367` | **already in the image**, symlinked onto `$HIVE_HOME/lib` | AWS SDK v1 that `hadoop-aws 3.3.6` was built against |

> **Version lock:** `apache/hive:4.0.1` ships **Hadoop 3.3.6**, which pairs with **`hadoop-aws`
> 3.3.6** and the **AWS SDK v1 bundle `1.12.367`**. Do not mix in `hadoop-aws` from a different
> Hadoop line or the SDK v2 bundle — S3A will fail to load. We reuse the jars already in the base
> image (via symlink) precisely so this pairing can never drift. `hadoop-aws` lives under
> `tools/lib`, which the `hive --service metastore` launcher does not scan, so the symlink onto
> `$HIVE_HOME/lib` is what actually makes S3A available.

Build / push:

```bash
cd image
./build-and-push.sh                                  # local build -> hive-metastore-pg:4.0.1
REGISTRY=<acct>.dkr.ecr.<region>.amazonaws.com ./build-and-push.sh --push
```

---

## Configuration: how `hive-site.xml` is produced

The metastore reads its DB connection **only from `hive-site.xml`** — verified against
`apache/hive:4.0.1`, neither JVM `-D` system properties nor Hadoop `${env.*}` substitution feed
the JDO connection to `schematool`. To keep the **password out of git and off the process argv**,
`hive-site.xml` is **rendered at container start** from environment variables:

1. `configmap-hive-site.yaml` holds a `hive-site.xml.tmpl` with `@TOKEN@` placeholders and a
   `render-hive-site.sh`.
2. On start, each container runs `render-hive-site.sh`, which substitutes the tokens with env
   values (**XML-escaping** them, so `& < > " '` in a password are safe), writes the file
   `0600`, and symlinks it into `$HIVE_HOME/conf/hive-site.xml`.
3. DB creds (`HMS_DB_*`) come from the **Secret** (`envFrom`); warehouse + region
   (`HMS_WAREHOUSE_DIR`, `HMS_S3A_REGION`) come from the **`hive-metastore-env` ConfigMap**.

The password therefore exists only in: the Secret, the pod env, and the in-pod `0600` file —
never in a ConfigMap, in git, or on any command line.

**Warehouse scheme (`s3://` vs `s3a://`):** `s3a://` is the `hadoop-aws` filesystem scheme and
is what we default to (`HMS_WAREHOUSE_DIR=s3a://<bucket>/warehouse`). For convenience the bare
`s3://` scheme is also mapped to S3A in `hive-site.xml` (`fs.s3.impl`), so warehouse paths written
either way resolve through the same IRSA-authenticated filesystem.

---

## Secret wiring

`secret.example.yaml` documents the shape; it is **not** applied by kustomize, so a placeholder
credential can never be rendered.

**Production (recommended):** don't hand-create the Secret at all — drive it from **AWS Secrets
Manager** via the **External Secrets Operator** or the **Secrets Store CSI driver** so RDS
credential rotation flows through automatically. The keys (`HMS_DB_URL`, `HMS_DB_USER`,
`HMS_DB_PASSWORD`, `HMS_DB_DRIVER`) map straight onto the `hive-metastore-db` Secret this chart
consumes.

**If you must create it manually**, never pass the password with `--from-literal` — that leaks
the real secret into shell history and the process argv (the very thing this design avoids at
runtime). Read it from a protected file (or stdin) instead:

```bash
# write the password to a 0600 file (or pull it straight from the RDS secret), then:
umask 077
printf '%s' "$RDS_PASSWORD" > /run/hms-db-pass    # or have a tool drop it here; not on argv

kubectl -n data-platform create secret generic hive-metastore-db \
  --from-literal=HMS_DB_URL='jdbc:postgresql://<rds-endpoint>:5432/metastore' \
  --from-literal=HMS_DB_USER='hive' \
  --from-literal=HMS_DB_DRIVER='org.postgresql.Driver' \
  --from-file=HMS_DB_PASSWORD=/run/hms-db-pass
shred -u /run/hms-db-pass 2>/dev/null || rm -f /run/hms-db-pass
```

`--from-file` keeps the password out of argv and shell history (URL/user/driver are not
sensitive). `.gitignore` blocks committing any real `*secret*.yaml`.

---

## IRSA wiring

`serviceaccount.yaml` carries:

```yaml
metadata:
  annotations:
    eks.amazonaws.com/role-arn: <hms_irsa_role_arn>   # from eks-cluster-iac
```

That annotation is the whole binding: the EKS pod-identity webhook injects a projected
web-identity token plus `AWS_ROLE_ARN` / `AWS_WEB_IDENTITY_TOKEN_FILE` into every pod using this
SA. `hive-site.xml` sets
`fs.s3a.aws.credentials.provider=com.amazonaws.auth.WebIdentityTokenCredentialsProvider`, which
exchanges that token for short-lived S3 credentials. **No static keys anywhere.** The IAM role's
trust policy (owned by `eks-cluster-iac`) must federate this cluster's OIDC provider for
`system:serviceaccount:<namespace>:hive-metastore`.

---

## Deploy

```bash
# 0. Build + push the image; point overlays/example image.newName at your ECR repo.
# 1. Create the DB Secret (above) in the target namespace.
# 2. Edit overlays/example/kustomization.yaml: IRSA ARN, warehouse bucket, region, image ref.
# 3. Apply:
kubectl apply -k overlays/example
```

Order of operations on the cluster:
- The **schema-init Job** renders `hive-site.xml` and runs `schematool -initSchema -dbType postgres`
  (idempotent — a `-info` guard makes re-runs a no-op).
- The Deployment's **`wait-for-schema` initContainer** blocks until the schema exists, so serving
  pods never crash-loop against an empty DB.
- The **metastore** containers start `hive --service metastore` on **9083** with
  `IS_RESUME=true` (they never touch the schema; `schema.verification=true` makes them refuse a
  mismatched schema).

---

## Pointing Spark Connect at it

The Spark Connect server (separate task) sets these `spark-defaults`:

```properties
# --- catalog: Hive Metastore ---
spark.sql.catalogImplementation        hive
spark.hadoop.hive.metastore.uris       thrift://hive-metastore.data-platform.svc.cluster.local:9083
# (within the same namespace, thrift://hive-metastore:9083 is enough)

# --- table format: Iceberg, Hive-type catalog backed by this HMS ---
spark.sql.extensions                   org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.iceberg              org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type         hive
spark.sql.catalog.iceberg.uri          thrift://hive-metastore:9083
spark.sql.catalog.iceberg.warehouse    s3://<warehouse_bucket>/warehouse

# --- warehouse + S3A (must match HMS) ---
spark.sql.warehouse.dir                s3a://<warehouse_bucket>/warehouse
spark.hadoop.fs.s3a.aws.credentials.provider  com.amazonaws.auth.WebIdentityTokenCredentialsProvider
spark.hadoop.fs.s3a.endpoint.region    <aws_region>
```

Notes:
- `thrift://hive-metastore:9083` is the in-cluster Service DNS name from `service.yaml`.
- The Spark pods need **their own IRSA role** with S3 access to the same warehouse bucket (the
  metastore stores table *locations*; the Spark executors do the actual S3 I/O). Use the matching
  `hadoop-aws`/SDK pairing on the Spark image as well.
- Iceberg's `SparkCatalog` with `type=hive` uses this HMS as its catalog — table metadata is
  tracked in HMS/Postgres, and Iceberg metadata + data files live under the warehouse prefix.
- HMS itself is **table-format-agnostic** — it just stores databases/tables/locations. Nothing in
  these manifests or the image is Delta- or Iceberg-specific; the format lives entirely in the
  Spark client config above.

---

## HA, probes, disruption

- **Replicas:** HMS is stateless (all state is in Postgres), so `replicas: 2` run safely against
  the same RDS database; scale higher for more throughput. Rolling updates use `maxUnavailable: 0`.
- **Spread:** a `topologySpreadConstraint` spreads replicas across nodes.
- **Probes:** `startup`/`readiness`/`liveness` are **TCP checks on 9083** (the metastore is a raw
  Thrift server with no HTTP health endpoint). The startup probe absorbs cold-JVM + DB-connect time.
- **PDB:** `minAvailable: 1` keeps one metastore reachable during voluntary disruptions (drains,
  upgrades). It only protects availability when `replicas >= 2`.
- **Durability** is RDS's job: use Multi-AZ + automated backups/PITR on the metastore database.

---

## Validation performed (gates)

All run locally; **nothing was deployed to a real cluster.**

| Gate | Tool | Result |
|------|------|--------|
| Manifests render | `kustomize build base` / `overlays/example` | ✅ 8 / 9 objects |
| Schema validation | `kubeconform -strict` | ✅ 9/9 valid, 0 errors |
| YAML lint | `yamllint` | ✅ clean |
| Image builds | `docker build` | ✅ `hive-metastore-pg:4.0.1` |
| Jars on classpath | inspected built image | ✅ `org.postgresql.Driver`, `S3AFileSystem`, `WebIdentityTokenCredentialsProvider` present |
| **Runtime, end-to-end** | `docker` + Postgres 16, using the **manifest-embedded** template/render script | ✅ render → `schematool -initSchema` (83 tables) → idempotent re-run no-op → server boots on 9083 → **real `HiveMetaStoreClient` RPC** create/get/drop database round-trip; verified with a password containing `& < > "` |

A real Kubernetes dry-run (`kubectl apply --dry-run=server`) and `kubeconform` against your
cluster's CRDs are recommended once a cluster context exists, but were intentionally not run here.

---

## Hardening notes / alternatives

- **Secret source:** prefer External Secrets / Secrets Store CSI over a hand-created Secret.
- **Schema upgrades:** the init Job uses a guarded `-initSchema` (no-op once present). A Hive
  version bump is a deliberate, separate action — run `schematool -upgradeSchema` (or switch the
  Job to `-initOrUpgradeSchema`) under review, not silently on every rollout.
- **mTLS / authz:** this exposes plain Thrift in-cluster. Restrict reachability with a
  NetworkPolicy (only Spark pods → 9083) and consider metastore Thrift SASL/Kerberos if you need
  authenticated clients.
- **Postgres TLS:** append `?ssl=true&sslmode=require` to the JDBC URL for RDS in-transit
  encryption.

# Reproduction and Infrastructure Reference (companion to PAPER.md)

> **Purpose.** This is the exhaustive, in-repository master resource for reproducing the
> safe-agent study end to end, and for the full infrastructure specification that the paper's
> Part 3 deliberately summarizes. It is kept *out* of the paper's academic narrative on purpose:
> the paper argues the science and the trust boundary; this document holds the operational detail.
>
> For higher-level orientation see `ARCHITECTURE.md` (architecture overview) and
> `deploy/eks/RUNBOOK.md` (operation). Nothing here changes any experimental result — all study
> outcomes remain `[PENDING clean run — N=TBD]` in the paper until a clean live sweep completes.

## Contents

1. Infrastructure specification (the deployment Part 3 condenses)
2. End-to-end reproduction (the former PAPER §30–40)

---

# 1. Infrastructure specification

The live substrate is a layered, mTLS-fronted Spark Connect cluster on Amazon EKS with an
Iceberg-on-S3 warehouse backed by a Hive Metastore. Part 3 of the paper presents only the trust
boundary; the full provisioning specification is here.

## 1.1 Request path and trust boundary

The end-to-end path: an agent sandbox or operator laptop connects through a local client/sidecar
to an **internal NLB**; the NLB forwards (TCP passthrough, no TLS termination) to an **Envoy
sidecar** that terminates mTLS; Envoy verifies the client certificate against the Connect-layer
CA, derives the principal from the certificate, injects a pre-shared key, and forwards over
loopback to a **Spark Connect server bound to `127.0.0.1:15002`**; the Connect driver schedules
**executor pods**; the catalog is **Hive Metastore** (Thrift on 9083); tables are **Iceberg on
S3**; and **IRSA** grants the Spark driver/executor and Hive Metastore pods warehouse access
without static credentials. There is no path to raw `15002` — it is loopback-bound and no
Kubernetes Service targets it.

## 1.2 Terraform (`deploy/eks/terraform/`)

The Terraform stack provisions the cloud substrate:

- **EKS cluster** (`eks.tf`, via `terraform-aws-modules/eks`): private API endpoint by default
  (public access gated by a CIDR allowlist), `enable_irsa = true` (an OIDC provider backs role
  assumption), and core addons (`coredns`, `kube-proxy`, `vpc-cni`, pod-identity-agent).
- **VPC** (`vpc.tf`): two private and two public subnets across two availability zones, with
  subnets tagged for internal and public load balancers respectively.
- **Two node groups** (`eks.tf`): a **system node group** (label `workload=system`, untainted)
  for system DaemonSets, the Hive Metastore, and the Connect driver; and an **executor node
  group** (label `workload=spark-executor`, taint `spark-role=executor:NO_SCHEDULE`, min size can
  be 0 for scale-to-zero) dedicated to Spark executor pods.
- **IRSA roles** (`irsa.tf`): a Spark role federated to
  `system:serviceaccount:<spark-namespace>:<spark-sa>` and a Hive Metastore role federated to its
  service account, each scoped to S3 warehouse read/write.
- **S3 warehouse** (`s3.tf`): a versioned, AES256-encrypted, public-access-blocked bucket with
  medallion prefixes (bronze/silver/gold).
- **RDS PostgreSQL** (`rds.tf`): the Hive Metastore backing database (private subnets, security
  group admitting 5432 only from the EKS node security group, password generated and stored in
  AWS Secrets Manager and fetched by HMS pods via IRSA).
- **Optional SSM bastion** (`bastion.tf`): an SSM-managed instance (no SSH keys, no inbound rules)
  for private-API operator access.

`outputs.tf` exposes the values the Kubernetes overlays need: cluster name/endpoint/OIDC ARN,
subnet IDs, node security group, the two IRSA role bindings, the warehouse bucket and prefixes,
and the RDS endpoint/secret.

## 1.3 Kubernetes manifests (`deploy/eks/connect/`, `deploy/eks/hms/`)

The Connect manifests assemble the mTLS-fronted server as a single-replica Deployment with two
containers — the Spark Connect driver (ports 7078/7079 for driver↔executor, 15002 loopback for
Connect gRPC) and an Envoy sidecar (the only externally-reachable listener, on 15009) — plus a
service account carrying the Spark IRSA annotation, RBAC permitting the driver to create executor
pods and headless services, an internal NLB `LoadBalancer` service on 15009, a headless service
for driver↔executor traffic, an executor pod template (node selector `workload=spark-executor`,
toleration for the executor taint, the Spark service account), and a PodDisruptionBudget for the
singleton driver. Dynamic allocation is configured (`minExecutors=0`, a configurable ceiling,
shuffle tracking enabled) so executor pods scale with workload. The HMS manifests deploy Hive
4.0.1 (with the PostgreSQL JDBC driver and `hadoop-aws` baked in), a schema-init Job
(`schematool -initSchema -dbType postgres`), a multi-replica metastore Deployment behind a
ClusterIP service on 9083, and IRSA-based S3 access. Spark Connect is wired to the catalog via
`spark.sql.catalog.iceberg` (type `hive`, the HMS Thrift URI, and the `s3a://<bucket>/warehouse`
location) with S3A using the web-identity (IRSA) credential provider.

## 1.4 mTLS and principal pinning (`deploy/auth/`)

Identity is enforced in two independent layers. The certificate tooling in `deploy/auth/certs/`
issues a Connect-layer CA, a server certificate for the Envoy sidecar, and per-principal client
bundles whose SAN is `spiffe://safe-spark-agents/<principal>` and whose CN equals the principal —
the invariant being *principal = CN = SAN last segment = required Spark `user_id`*. The Envoy
configuration (`deploy/auth/envoy/envoy.yaml`) terminates client TLS with
`require_client_certificate: true`, validates against the Connect CA, accepts only SANs in the
`spiffe://safe-spark-agents/*` trust domain, strips any client-supplied `x-connect-principal`
header and re-sets it from the verified certificate, injects the PSK as a bearer token, and
forwards to the loopback Connect server. The `PrincipalPinningInterceptor` (FQCN
`com.safesparkagents.connect.auth.PrincipalPinningInterceptor`, registered via
`spark.connect.grpc.interceptor.classes`) then **fails closed** on every data-plane RPC: it
rejects requests lacking the `x-connect-principal` header (proof the request traversed Envoy),
rejects a blank or absent `user_id`, and rejects any request whose `user_id` does not equal the
verified principal — with a small identity-neutral allowlist for health and reflection RPCs. The
practical guarantee is that a malicious in-process agent forging a different `user_id` is refused
at the server.

The local client/sidecar path is documented in `deploy/spark-omnigent/README.md`: a thin
`pyspark-client` sandbox (no JVM, no secrets, no certificate) speaks plain h2c over loopback to an
Envoy egress sidecar that holds the client certificate (mounted read-only, in a separate
container) and presents it over mTLS to the cluster. The sidecar pattern is deliberate — the
Spark Connect Python `sc://` string cannot express client certificates, and keeping the private
key out of the untrusted agent process is the point; `SPARK_REMOTE`, `AGENT_SCHEMA`, and
`user_id` are *derived* from the injected `AGENT_PRINCIPAL`, never trusted from the agent.

---

# 2. End-to-end reproduction

This is the end-to-end reproduction path across all four parts: deploy the infrastructure
(Part 3), bring up a client path, configure the runner, validate offline, run Part 1 locally, and
run Part 2 against the remote cluster. Commands assume the repository root is the working
directory unless noted.

## 2.1 Checkout

```bash
git clone https://github.com/lisancao/safe-spark-agents.git
cd safe-spark-agents
# The study harness, arms, skills, deploy/, and gitops_demo/ live on the
# integration line (dev/main).
git checkout dev
```

## 2.2 Deploy the EKS infrastructure (Part 3a)

Follow `deploy/eks/RUNBOOK.md` and `deploy/eks/terraform/README.md`.

```bash
cd deploy/eks/terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars for the target AWS profile, region, CIDRs, and operator access.
terraform init \
  -backend-config="bucket=${TFSTATE_BUCKET}" \
  -backend-config="key=eks/terraform.tfstate" \
  -backend-config="dynamodb_table=ssa-tf-locks" \
  -backend-config="region=us-east-1" \
  -backend-config="encrypt=true"
terraform plan -var-file=terraform.tfvars -out tfplan
terraform apply tfplan
aws eks update-kubeconfig --name <cluster-name> --profile ssa-deploy --region us-east-1
cd ../../..
```

Record the Terraform outputs the Kubernetes overlays need: Spark IRSA role ARN, Hive Metastore
IRSA role ARN, warehouse bucket, RDS endpoint/secret, and cluster name.

## 2.3 Build and publish the Spark image

Use the image instructions referenced by `deploy/eks/RUNBOOK.md` and `deploy/eks/images/README.md`.

```bash
cd deploy/eks/images/spark-connect
# Build/push the Spark Connect + Iceberg image as documented here.
# Include the principal-pinning interceptor jar when the image README requires it.
cd ../../../..
```

## 2.4 Deploy Hive Metastore and Spark Connect (Part 3a)

Create out-of-band secrets as documented in `deploy/eks/RUNBOOK.md`, `deploy/eks/hms/README.md`,
and `deploy/eks/connect/README.md`. Do not commit certs, keys, PSKs, kubeconfigs, or tfvars.

```bash
# Hive Metastore
kubectl create namespace hive-metastore || true
# Create the hms-db secret as described in deploy/eks/hms/README.md.
kustomize build deploy/eks/hms/overlays/example | kubectl apply -f -

# Spark Connect (mTLS-fronted)
kubectl create namespace spark || true
# Create spark-connect-psk and the Envoy cert secret as described in deploy/eks/connect/README.md.
cp -r deploy/eks/connect/overlays/example deploy/eks/connect/overlays/prod
# Edit overlays/prod to replace role ARN, bucket, image, account, and region placeholders.
kustomize build deploy/eks/connect/overlays/prod | kubectl apply -f -
kubectl -n spark rollout status deploy/spark-connect
kubectl -n spark get svc spark-connect-mtls -o wide
```

Issue the Connect-layer CA, server certificate, and per-principal client bundles with the tooling
in `deploy/auth/certs/` (`make-ca.sh`, `issue-server-cert.sh`, `issue-client-cert.sh`).

## 2.5 Bring up the client / tunnel path

The runner expects a Spark Connect URL such as `sc://localhost:15002` or a local sidecar/tunnel
endpoint. Raw 15002 is not exposed by the EKS Connect service; use the mTLS sidecar path for
production.

```bash
# Test endpoint via port-forward (non-mTLS, for a private test only):
kubectl -n spark port-forward deploy/spark-connect 15002:15002

# Production mTLS path: configure the local egress sidecar from deploy/spark-omnigent/
# with the client cert, key, CA, and AGENT_PRINCIPAL, then expose a local sc:// endpoint.
```

See `deploy/eks/connect/README.md` and `deploy/spark-omnigent/README.md` for the connection model.

## 2.6 Configure the study runner

Edit `experiments/safe_agent_study/study.config.json` for the live environment (the fields below
are a shape, not results — fill them with actual deployment parameters):

```json
{
  "base_model_id": "claude-opus-4-8",
  "spark_remote": "sc://localhost:15002",
  "spark_rest_url": "<Spark driver REST URL or null>",
  "image_digest": "<deployed image digest>",
  "executor_config": {
    "instances": "<match live executor plan>",
    "cores_per_executor": "<match live executor plan>",
    "memory_gb_per_executor": "<match live executor plan>",
    "price_usd_per_executor_hour": "<declared price input>",
    "provider": "k8s",
    "instance_type": "<node instance type>"
  }
}
```

Set the LLM key:

```bash
export ANTHROPIC_API_KEY=<redacted>
```

## 2.7 Offline validation before any live sweep

```bash
cd experiments/safe_agent_study
python3 tests/test_corpus.py
python3 tests/test_runner_offline.py
python3 tests/test_workspace_contract.py
python3 tests/test_remote_staging.py
python3 tests/test_live_sampling.py
python3 tests/test_oracles.py
python3 tests/test_oracles_ext.py
python3 tests/test_live_path.py
python3 tests/test_local_backend.py
python3 tests/test_skill_loading.py
python3 tests/test_stage_compute.py
python3 tests/test_published_schema.py
```

These validate corpus integrity, identical-except-loop controls, the per-arm workspace contracts,
remote staging mechanics, sampling shape, oracle behavior, the local real-Spark read-back path,
the Part-1 local backend, skill loading, the H2 stage-diff, and published-schema/code parity.
They do not substitute for a live EKS sweep.

## 2.8 Part-1 run (LOCAL substrate)

Part 1's clean cross-paradigm comparison runs each paradigm on its home local engine (no EKS). The
runner brings up a local single-node Connect server for SDP arms and uses classic `local[*]` for
imperative arms; it requires `ANTHROPIC_API_KEY` but no cluster.

```bash
cd experiments/safe_agent_study
python3 harness/runner.py \
  --backend local \
  --config study.config.json \
  --arms-dir arms \
  --tasks TASKS.lock.json \
  --seeds SEEDS.lock.json \
  --out results.part1.jsonl \
  --work-dir .work-part1
```

Note the H2 cross-engine caveat (deviation D-7): Part-1 H1 is the clean cross-paradigm contrast;
compute-to-correct (H2) is reported only *within* a paradigm (e.g. A vs B2), because imperative
runs on classic `local[*]` and SDP on a Connect server and their executor-seconds are not
comparable.

## 2.9 Part-2 run (REMOTE EKS Connect substrate)

The remote sweep uses the live backend, the deployed Connect endpoint, and per-cell S3 staging.
This is where the Spark Connect compatibility of each paradigm is exercised on a real cluster.

```bash
cd experiments/safe_agent_study
python3 harness/runner.py \
  --backend live \
  --config study.config.json \
  --arms-dir arms \
  --tasks TASKS.lock.json \
  --seeds SEEDS.lock.json \
  --out results.part2.jsonl \
  --work-dir .work-part2
```

For a smoke run before the full sweep, restrict tasks/arms/seeds:

```bash
python3 harness/runner.py \
  --backend live \
  --config study.config.json \
  --arms-dir arms \
  --tasks TASKS.lock.json \
  --seeds SEEDS.lock.json \
  --only-tasks orders_silver_gold \
  --only-arms A,B,A2,B1,B2 \
  --max-seeds 1 \
  --out smoke.results.jsonl \
  --work-dir .work-smoke
```

## 2.10 GitOps demonstration (Part 3b)

The GitOps loop is independently runnable against a runner-local Connect server:

```bash
cd experiments/safe_agent_study/gitops_demo
# Agent authors an artifact and opens a PR (no Spark session; git/gh only):
python3 agent_pr_author.py --task tasks/orders_silver_gold.json \
  --pipeline-slug orders-silver-gold --base-branch main
# CI gate / controller (these DO hold a session):
bash local_spark_connect.sh start
SPARK_REMOTE=sc://localhost:15055 python3 ensure_schema.py
python3 changed_pipelines.py --all
SPARK_REMOTE=sc://localhost:15055 python3 reconcile.py --all
bash local_spark_connect.sh stop
```

The two GitHub Actions workflows (`.github/workflows/gitops-sdp-dry-run.yml` for the PR gate and
`gitops-sdp-reconcile-local.yml` for merge reconcile) automate the same steps. See
`gitops_demo/PRODUCTION_EKS.md` for the OIDC-based production wiring.

## 2.11 Outputs and analysis

The runner writes one result row per `(task, arm, seed)` to the chosen `--out` file, an
environment sidecar, and a `.work/` tree with generated datasets, per-cell workspaces, materialized
proposals, transcripts, and profiles (per-cell `transcript.json` under
`.work/<task>__<arm>__seed<seed>/`). Analysis:

```bash
cd experiments/safe_agent_study
python3 -m pip install -r analysis/requirements.txt
python3 analysis/analyze.py results.part2.jsonl \
  --env results.part2.env.json \
  --json-out report.json \
  --md-out HEADLINE.md
```

Only after this analysis is produced from a clean, schema-valid live run should the placeholders
in `PAPER.md` be replaced.

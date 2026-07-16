# Client Stand-Up Runbook: Lakekeeper-Governed Spark-on-EKS + Omnigent

One-person-consultancy runbook for onboarding a client onto the Lakekeeper governed
Spark-on-EKS stack and the Omnigent delivery layer.

Every command below is copied from the repo sources cited on each step. Nothing is
invented. Where a step is proven in a lab but not productionized, or is a mock, or is
not yet packaged, it is labelled inline. Read those labels: honesty about
proven-vs-productionization is the point of this document.

AWS account numbers are redacted to `<ACCT>` throughout.

## Label legend

| Label | Meaning |
|-------|---------|
| **PROVEN** | Ran and passed against the cited proof log. |
| **PROVEN LOCAL ONLY** | Passed on MinIO/local tiers; the EKS/real-S3 tier is delivered as manifests but was never run (no cluster). |
| **ALLOW-ALL / DO NOT SHIP** | Auth-disabled demo instance. Must never be substituted for a client's governed catalog. |
| **MOCK** | Stand-in component (e.g. static OIDC IdP). Replace with the client's real system. |
| **MANUAL** | Hand-run step, no automation/one-shot packaging. |
| **NOT YET PACKAGED** | The mechanism is proven per-layer but not composed into a repeatable install. |
| **PRODUCTIONIZATION TODO** | Known change required before this is client-grade. |

---

## 1. What you are standing up

A single client request travels through five isolation layers over the client's own
live tenant, with Omnigent orchestrating delivery on top.

```
                         Omnigent (Polly orchestrator + custodian MCP)
                                     |  holds PSK + per-tenant catalog tokens
                                     v
 client cert (mTLS) --> [L1 Ingress gateway] --> [L2 per-tenant Connect server]
                          Envoy, principal            token custody +
                          from URI-SAN                 executor isolation
                                                            |
                                                            v
                                              [L3 NetworkPolicy]  (intended)
                                                            |
                                                            v
                                        [L4 Lakekeeper catalog: authz]
                                          OpenFGA + client IdP (OIDC)
                                                            |
                                                            v
                                        [L5 Storage: prefix-scoped vend]
                                          STS session policy per warehouse
                                                            |
                                                            v
                              S3 warehouse (bronze/silver/gold), RDS metastore
                                                            ^
                                                            |
                                     [Substrate: Terraform EKS/VPC/S3/RDS/IRSA]
```

| Layer | What it enforces | Primary source |
|-------|------------------|----------------|
| Substrate | VPC, EKS, S3 warehouse, RDS metastore, IRSA, vending IAM pair | `deploy/eks/terraform/*` |
| L1 Ingress | Authenticated identity (client-cert URI-SAN) selects the upstream server | `authz/gateway-envoy.yaml`, `authz/20-connect-gateway.yaml` |
| L2 Custody + exec | Token is injected server-side (client never holds it); each tenant runs on disjoint executors | `authz/gen_connect_server.py` (script) |
| L3 NetworkPolicy | Direct in-cluster dials bypassing the gateway are dropped | `authz/netpol-tenant-servers.yaml` |
| L4 Authz | Per-principal catalog deny (OpenFGA + OIDC) | `authz/10-lakekeeper-authz.yaml`, `authz/authz_proof.sh` |
| L5 Vending | Vended STS creds are prefix-scoped per tenant even though the executor also carries a full-bucket IRSA role | `terraform/lakekeeper-vending.tf`, `spike/eks/warehouse-*.aws.json` |

**Where Omnigent plugs in:** Omnigent ("Polly") is the delivery layer on top of the
platform. A single Claude-SDK orchestrator decomposes a multi-customer medallion brief,
fans out cross-vendor coding sub-agents, and submits every pipeline through a native
stdio MCP "custodian" that alone holds the Spark Connect PSK and per-tenant catalog
tokens. The sub-agent fleet stays credential-free; every catalog write crosses one
governed process boundary, over each customer's own live tenant.
Source: `deploy/omnigent/sdp-capstone/config.yaml:1-11`, `README.md:1-27`.

**Production shape (read this first):** the governed catalog is exactly **ONE**
Lakekeeper instance with authz enabled (`AUTHZ_BACKEND=openfga`), OIDC pointed at the
client's real IdP, and `ENABLE_AWS_SYSTEM_CREDENTIALS=true` so the same instance also
vends prefix-scoped storage credentials. The separate `spike/` Lakekeeper is
**ALLOW-ALL / DO NOT SHIP**: it exists only to prove the storage-isolation layer and
must never stand in for the client's catalog. See Section 4, step S4 for how vend and
authz are (and are not yet fully) unified on one instance.

---

## 2. Prerequisites (once per engagement)

**Tooling on the operator workstation** (substrate step, `terraform/README.md:86-95`):

- Terraform `>= 1.7`
- AWS CLI v2
- `kubectl` (plus the SSM Session Manager plugin if you use the bastion)
- `python3` with `mcp` and `pyspark` (for the Omnigent custodian process),
  `pyjwt>=2.8` and `cryptography` (for token minting)
- `kustomize`, `docker`

**AWS account and identity:**

- AWS profile `ssa-deploy` for account `<ACCT>` / `us-east-1` (swap per engagement).
- The remote-state backend must **already exist**: S3 bucket `${TFSTATE_BUCKET}` and
  DynamoDB lock table `ssa-tf-locks`. These are provisioned per engagement out of band.
  Source: `terraform/README.md:86-95`.

Verify caller identity before anything else:

```bash
aws sts get-caller-identity --profile ssa-deploy
```

Source: `terraform/README.md:86-95` (`RUNBOOK.md:52-62`).

**Secrets directory (out-of-git custody).** All key material lives outside the repo,
by documented convention (there is no vault/Secrets-Manager integration for these;
custody is a convention, not enforced tooling). Keep a `~/ssa-deploy/` tree at perms
`700` holding: CAs and keys, the PSK, JWTs, the RS256 private key,
`LAKEKEEPER__PG_ENCRYPTION_KEY`, warehouse external-id, client-cert keys, tfvars,
`.ovpn`, and kubeconfig. Source: `SETUP.md:73-76`, `RUNBOOK.md:60-61`.
The RDS master password is generated by Terraform and written **only** to Secrets
Manager plus encrypted state; you read it back yourself (no output emits it).

**Layered dependencies that must exist before higher layers work** (called out so you
do not discover them mid-stand-up):

- The base Connect overlay (`deploy/eks/connect`, **not in the extract sources**)
  supplies `spark-connect-env`, `spark-connect-executor-podtemplate`, the
  `spark-connect-psk` Secret, and the `spark` ServiceAccount that every per-tenant
  Connect server reuses. Source: `authz/README.md:16-18`.
- Per-tenant client certs and the gateway server cert are issued out of band by the
  Connect-layer CA (Section 4, step S2).

---

## 3. Per-client fill-in checklist

Fill every value below before running Section 4. Nothing here contains a secret that
belongs in git. The `tfvars` file is git-ignored; the `warehouse-*.aws.json` files are
committed with placeholder `REPLACE-*` values (only non-secret ARNs/bucket, no secrets),
and you substitute the real values into your working copy. Secrets go to `~/ssa-deploy/`
or `TF_VAR_*`.

**Templatized fill-in:** copy `client.env.example` to `client.env` (git-ignored), fill the values,
then `render.sh` substitutes the `REPLACE-*` tokens into any manifest/JSON on the fly:
`set -a; . ./client.env; set +a; ./render.sh < authz/10-lakekeeper-authz.yaml | kubectl apply -f -`.
Verified by running: it fills all six `REPLACE-*` tokens and leaves `__CONNECT_PSK__` (runtime-injected)
and `__OPENFGA__` (an env-var-name separator) untouched.

| Value | Where it is set | Default / example | Notes | Source |
|-------|-----------------|-------------------|-------|--------|
| `account_id` / `<ACCT>` | `terraform.tfvars` | (12-digit id) | Optional wrong-account guard; also embedded in bucket name + every role ARN. **No TF output emits it**: derive downstream from `aws sts get-caller-identity`. | `terraform.tfvars.example:6`; gaps |
| `region` / `aws_profile` | `terraform.tfvars`, `providers.tf` | `us-east-1` / `ssa-deploy` | Swap per engagement. | `variables.tf:5-15` |
| `name_prefix` | `terraform.tfvars` | `ssa-spark` | Namespaces ALL resource names (cluster `<prefix>-eks`, bucket `<prefix>-warehouse-<ACCT>`, roles `<prefix>-irsa-*`, `<prefix>-lakekeeper-*`). | `variables.tf:23-27` |
| `vpc_cidr` / subnets | `terraform.tfvars` | `10.40.0.0/16` + `/20`s | Change to avoid CIDR collisions with peered nets. | `variables.tf:45-73` |
| `warehouse_bucket_name` | `terraform.tfvars` | empty => `<prefix>-warehouse-<ACCT>` | Override to set explicitly. | `variables.tf:186-190` |
| `cluster_endpoint_public_access(_cidrs)` | `terraform.tfvars` | private (false) | Break-glass only; `validations.tf` blocks empty lists and `0.0.0.0/0`. | `variables.tf:85-95` |
| `cluster_version` | `terraform.tfvars` | `1.31` | K8s control-plane version. | `variables.tf:79-83` |
| RDS sizing/identity | `terraform.tfvars` | `metastore` / `hive` / `16.4` / `db.t3.medium` / Multi-AZ | Master password auto-generated, never set here. | `variables.tf:208-266` |
| `lakekeeper_external_id` | `TF_VAR_lakekeeper_external_id` only | (per-engagement secret) | Sensitive input, never committed; must match `warehouse-*.aws.json` external-id. Pins the production vending role's `sts:ExternalId` trust condition (confused-deputy protection). | `lakekeeper-vending.tf:76-96` |
| `${TFSTATE_BUCKET}` | `-backend-config` at init | (per-engagement) | Remote-state bucket. | `README.md:93,107` |
| `IMAGE_TAG` / registry / ECR | build env | `<registry>/spark-connect-iceberg:4.1.2-iceberg1.11.0` | Full pushable ref incl. registry. | `build.sh:39` |
| `INTERCEPTOR_JAR` | build env | (abs path to PR#3 jar) | Not vendored in-repo; built from `deploy/auth/interceptor`. | `build.sh:10` |
| `CONNECT_IMAGE` | shell env (manual handoff) | `<ECR>/ssa-spark/spark-connect:4.1.2-iceberg1.11.0` | Consumed by `gen_connect_server.py`; the build->deploy link is manual. | `gen_connect_server.py:11` |
| `TRUST_DOMAIN` | `deploy/auth/certs/vars.sh` | `safe-spark-agents` | SPIFFE domain in client-cert URI-SAN. | `vars.sh:13-16` |
| `SERVER_DNS` | `vars.sh` | `connect.internal localhost` | Gateway server cert SANs (first = CN). | `vars.sh:22-24` |
| `PRINCIPALS` / tenant list | `vars.sh` / roster | `alice bob carol agent_a agent_b` | Client's real users/agents; also the tenant set for warehouses/grants. | `vars.sh:36-38` |
| `REPLACE-PG-ENCRYPTION-KEY` | `10-lakekeeper-authz.yaml:17,41` | (fresh 32 bytes) | Must be IDENTICAL in migrate Job + serve Deployment. | `README.md:49` |
| Client IdP: `OPENID_PROVIDER_URI` / `OPENID_AUDIENCE` / `OPENID_SUBJECT_CLAIM` | `10-lakekeeper-authz.yaml:46-48` | mock `idp-authz` / `lakekeeper` / `sub` | **Repoint to the client's real IdP** (issuer/audience/claim). | `10-lakekeeper-authz.yaml:46-48` |
| Warehouse bucket + key-prefix | `warehouse-tenant_*.aws.json:2,11-13` | `REPLACE-warehouse-bucket` | Real S3 bucket; region hardcoded `us-east-1`. | `warehouse-tenant_a.aws.json:11-12` |
| `REPLACE-external-id` | `warehouse-tenant_*.aws.json:7` | (per-engagement) | Gates AssumeRole: must equal `TF_VAR lakekeeper_external_id` (the production vending role's `sts:ExternalId` trust condition) or no credential is vended. | `warehouse-tenant_a.aws.json:7` |
| Vending role ARN (`sts-role-arn` / `assume-role-arn`) | `warehouse-*.aws.json:16` | TF output `lakekeeper_vending_role_arn` | **Field-name inconsistency**: output doc says `sts-role-arn`, JSON uses `assume-role-arn`; confirm the Lakekeeper schema key. | `lakekeeper-vending.tf:136-139` vs `warehouse-tenant_a.aws.json:16` |
| Catalog role ARN (SA annotation) | `lakekeeper.yaml:23` / `10-lakekeeper-authz.yaml` SA | TF output `lakekeeper_catalog_role_arn` | Annotate the `lakekeeper` ServiceAccount. | `lakekeeper-vending.tf:131-134` |
| `AWS_PROFILE` (Omnigent) | `tools/mcp/custodian.yaml:19` | `ssa-deploy` | Profile the custodian uses to reach the cluster. | `custodian.yaml:19` |
| `max_cost_usd` | `config.yaml:91` | `8.0` | Hard session cost ceiling. | `config.yaml:91` |
| Customer set + briefs | `config.yaml:37-43` | retail / saas / payments | The per-engagement workload + data policies. | `config.yaml:37-43` |

---

## 4. Stand-up sequence

Run the layers in order. Each step lists purpose, exact command(s), placeholders
consumed, and source.

### S0. Substrate (Terraform): PROVEN (applied 2026-07-09 via CI, ~118 resources)

**Purpose:** stand up the bottom layer: VPC, EKS, S3 warehouse, RDS metastore, IRSA
roles, and the Lakekeeper vending IAM pair. Its outputs feed every later layer.

Create the tfvars, init the partial S3 backend, plan, then apply. Note that for the
live stack, `apply` runs through the reviewer-gated `eks-terraform-apply.yml` CI
workflow (GitHub OIDC, S3-backed state), **not** ad-hoc from a workstation.

```bash
# tfvars from example (git-ignored; no secrets in it)
cd deploy/eks/terraform && cp terraform.tfvars.example terraform.tfvars
# edit: profile, region, name_prefix, CIDRs, RDS sizing, operator-access options

# init with partial backend (encrypt=true is required: RDS master password lands in state)
terraform init \
  -backend-config="bucket=${TFSTATE_BUCKET}" \
  -backend-config="key=eks/terraform.tfstate" \
  -backend-config="dynamodb_table=ssa-tf-locks" \
  -backend-config="region=us-east-1" \
  -backend-config="encrypt=true"

# plan (account_guard precondition aborts on the wrong account if account_id is set)
terraform plan -var-file=terraform.tfvars -out tfplan

# apply (real billable infra: EKS, NAT, RDS, S3). Live stack goes via CI, not here.
terraform apply tfplan

# point kubectl at the private cluster (needs bastion/VPN reach)
aws eks update-kubeconfig --region us-east-1 --name ssa-spark-eks --profile ssa-deploy
```

**Set `TF_VAR_lakekeeper_external_id` before plan/apply** (sensitive input, never
committed): `export TF_VAR_lakekeeper_external_id=<per-engagement-secret>`.

**Placeholders consumed:** `${TFSTATE_BUCKET}`, `name_prefix`, `region`/`aws_profile`,
`account_id`, `vpc_cidr`, RDS sizing, `lakekeeper_external_id`.

**What it creates** (source: `vpc.tf`, `eks.tf`, `s3.tf`, `rds.tf`, `irsa.tf`,
`lakekeeper-vending.tf`):

- VPC `10.40.0.0/16` (2 private + 2 public subnets, IGW, single NAT).
- EKS `ssa-spark-eks` (K8s 1.31, private API, OIDC/IRSA), node groups `system`
  (untainted) and `executor` (taint `spark-role=executor:NoSchedule`, min0/max10).
- S3 warehouse `<prefix>-warehouse-<ACCT>` (AES256 + versioning + public-access-block),
  `bronze/silver/gold` prefixes as zero-byte placeholders.
- RDS Postgres 16.4 Multi-AZ metastore; master password only in Secrets Manager +
  state; SG allows 5432 only from the EKS node SG.
- IRSA: `<prefix>-irsa-spark` (full warehouse RW), `<prefix>-irsa-hms`.
- Vending pair: `<prefix>-lakekeeper-catalog` (IRSA, can only AssumeRole the vending
  role, plus a 2026-07-09 `catalog_manage_s3` policy granting the catalog pod full
  warehouse S3) and `<prefix>-lakekeeper-vending` (`max_session_duration 3600`, broad
  bucket RW; per-tenant scoping happens at vend time via session policy).

**Outputs to capture for later layers** (`outputs.tf:63-133`; the two Lakekeeper role
ARNs live in `lakekeeper-vending.tf:131-139`, not `outputs.tf`): `irsa_spark`, `irsa_hms`,
`warehouse_bucket`, `warehouse_prefixes`, `metastore_endpoint`, `metastore_secret_arn`,
`lakekeeper_catalog_role_arn`, `lakekeeper_vending_role_arn`, `oidc_provider_arn`,
`update_kubeconfig_command`.

Static gates run on every change:

```bash
terraform fmt -recursive && terraform init -backend=false && terraform validate
```

Source: `terraform/README.md:86-134,209-219`, `RUNBOOK.md:52-85`.

### S1. Interceptor jar (BUILT + 12 tests pass here) + Spark Connect image (build/push registry-gated)

**Purpose:** build the Spark Connect + Iceberg image every per-tenant Connect server
runs, then push it and hand its tag downstream as `CONNECT_IMAGE`.

```bash
cd /home/lnc/sdp-paper-local/deploy/eks/images/spark-connect

# FIRST build the interceptor jar (VERIFIED 2026-07-13: BUILD SUCCESS, Tests run: 12, Failures: 0):
#   (cd /home/lnc/sdp-paper-local/deploy/auth/interceptor && mvn -B clean package)
#   produces target/connect-auth-interceptor-0.1.0.jar, shaded so io.grpc is relocated to
#   org.sparkproject.connect.grpc (REQUIRED: Connect 4.1.x loads gRPC under that shaded package).
# Tier A (released, production): Spark 4.1.2 + Iceberg 1.11.0, with the real interceptor jar
INTERCEPTOR_JAR=/home/lnc/sdp-paper-local/deploy/auth/interceptor/target/connect-auth-interceptor-0.1.0.jar \
IMAGE_TAG=<registry>/spark-connect-iceberg:4.1.2-iceberg1.11.0 \
./build.sh

# push (registry is carried inside IMAGE_TAG; no ECR login step exists in build.sh)
PUSH=1 INTERCEPTOR_JAR=/home/lnc/sdp-paper-local/deploy/auth/interceptor/target/connect-auth-interceptor-0.1.0.jar \
IMAGE_TAG=<registry>/spark-connect-iceberg:4.1.2-iceberg1.11.0 \
./build.sh          # or: ./build.sh --push

# manual downstream handoff (build.sh never sets CONNECT_IMAGE)
export CONNECT_IMAGE=<your-ecr>/ssa-spark/spark-connect:4.1.2-iceberg1.11.0
```

**Placeholders consumed:** `INTERCEPTOR_JAR`, `IMAGE_TAG` (registry/ECR), `CONNECT_IMAGE`.

**Caveats you must not gloss over:**

- **INTERCEPTOR JAR: BUILT + TESTED (2026-07-13).** `deploy/auth/interceptor` is a real Maven
  project: `mvn -B clean package` gives BUILD SUCCESS with `Tests run: 12, Failures: 0` and
  produces the shaded `connect-auth-interceptor-0.1.0.jar`. The source is vendored; the jar is a
  one-command build (not committed). Build the client image WITH this jar; never ship
  `ALLOW_MISSING_INTERCEPTOR=1` (that image has non-functional Connect auth).
- **RESIDUAL (registry-gated, Group B):** only the docker image build + ECR push is unverified
  here (needs a registry + creds); the jar itself is built and tested.
- **MANUAL (registry auth):** there is no `aws ecr get-login-password`, no repo
  creation, no digest pin in `build.sh`. Registry login is assumed done out of band.
  The recorded gate did not push.
- **Tier B (native AUTO CDC):** `./build.sh --tier-b` switches to base
  `lakehouse/spark:5.0.0-snapshot-cdc` (Spark 5.0-SNAPSHOT). That base is your own
  source build (`~/lakehouse-stack`) and is documented+parameterized but **not rebuilt
  or verified** by this repo. Released Spark 4.2 + released Iceberg for it do not exist
  yet.

Source: `images/README.md:121-154`, `spark-connect/build.sh:9-105`.

### S2. Connect-layer CA, certs, and PSK: MANUAL (openssl scripts + kubectl)

**Purpose:** create the Connect-layer mTLS trust chain (separate root from any
VPN/network CA), the gateway server cert, one client cert per tenant carrying
`spiffe://<TRUST_DOMAIN>/<tenant>`, and the shared PSK bearer. Load them as the exact
k8s Secret names the gateway and Connect servers expect.

**Templatized (recommended):** `bootstrap-secrets.sh` (this dir) wraps all of the below with the
correct paths (verified by running) and creates the Secrets with the exact names the manifests expect:

```bash
set -a; . ./client.env; set +a          # from deploy/eks/lakekeeper/ : TENANTS, SERVER_DNS, CONNECT_PSK
./bootstrap-secrets.sh                   # issue CA + server + per-tenant client certs + PSK into ./secrets-out (git-ignored)
./bootstrap-secrets.sh --apply           # ON THE CLUSTER: also create spark-connect-psk, connect-gateway-certs, connect-gateway-config
```

Manual steps it wraps (for reference):

```bash
cd deploy/auth/certs

# 1) Connect-layer root CA (self-signed, 3650d). FORCE=1 to overwrite invalidates all certs.
./make-ca.sh

# 2) Envoy gateway server cert (SANs come only from SERVER_DNS; takes NO argument)
./issue-server-cert.sh

# 3) one client cert per tenant: CN=<principal>, SAN URI spiffe://<TRUST_DOMAIN>/<principal>
./issue-client-cert.sh <principal>          # e.g. ./issue-client-cert.sh alice
# or all at once from the PRINCIPALS roster:
./issue-all.sh                              # or: PRINCIPALS="alice bob carol" ./issue-all.sh

# 4) shared Connect PSK (never committed)
openssl rand -hex 32 > ~/ssa-deploy/psk/connect.psk
```

Load into k8s (namespace `spark`). The PSK Secret name **must** be `spark-connect-psk`
with key `token` (the base Connect overlay and the custodian both read this exact name):

```bash
kubectl -n spark create secret generic spark-connect-psk --from-file=token=psk.token
```

**Placeholders consumed:** `TRUST_DOMAIN`, `SERVER_DNS`, `PRINCIPALS`/`<principal>`,
`ORG`/`CA_CN`, `AUTH_PROXY_PORT`.

**Caveats:**

- **Path drift: RESOLVED by `bootstrap-secrets.sh`** (2026-07-13). The issue scripts write to
  `secrets-out/certs/{server/server.*, clients/<t>/client.*, ca/connect-ca.crt}` and the PSK to
  `secrets-out/connect.psk`; `bootstrap-secrets.sh` uses those exact paths in its
  `kubectl create secret --from-file=` commands (verified by running). Only if you run the raw
  scripts by hand do you need to reconcile paths yourself.
- **Secret-name drift:** RUNBOOK §4 uses `spark-connect-psk` / `spark-connect-envoy-certs`;
  §7 (rotation) refers to `connect-psk` / `connect-mtls`. Use the §4 names. The gateway
  path (S5 below) uses its own Secret `connect-gateway-certs`; do not confuse the two.
- **No revocation:** there is no CRL/OCSP. "Revoke" means expire/remove the client cert;
  hard cutoff = rotate the whole CA (invalidates every issued cert at once).
- The PSK is a single shared bearer for the whole Connect server, not per-principal;
  per-principal identity is carried only by the client-cert SAN.

Source: `deploy/auth/certs/{make-ca,issue-server-cert,issue-client-cert,issue-all}.sh`,
`vars.sh`, `RUNBOOK.md:126-152`.

### S3. Authz support services: mock IdP + OpenFGA - MOCK IdP, in-memory OpenFGA

**Purpose:** bring up the OIDC IdP and OpenFGA that the governed Lakekeeper depends on.
The IdP here is a **MOCK** (nginx serving static discovery + JWKS for hand-minted RS256
JWTs). In production you skip the mint/mock steps and point Lakekeeper at the client's
real IdP (S4 env).

```bash
cd deploy/eks/lakekeeper/authz

# mint the mock OIDC artifacts + one RS256 JWT per subject (admin, tenant_a, tenant_b)
pip install "pyjwt>=2.8" cryptography
python3 mint_tokens.py

# publish the discovery + JWKS to the nginx mock IdP
kubectl -n spark create configmap idp-authz-files \
  --from-file=openid-configuration --from-file=jwks.json

# deploy the mock IdP (first: OIDC discovery is a hard Lakekeeper startup gate) + OpenFGA
kubectl apply -f 00-idp-openfga.yaml
kubectl -n spark rollout status deploy/idp-authz deploy/openfga
```

**Placeholders consumed:** tenant names (the hardcoded `SUBJECTS` tuple), OIDC
`ISSUER`/`AUDIENCE`/`KID`.

**Caveats:**

- **MOCK IdP:** `idp-authz` has no login/token/user flow. A client MUST substitute a
  real IdP and repoint `OPENID_PROVIDER_URI` / `OPENID_AUDIENCE` / `OPENID_SUBJECT_CLAIM`.
- **OpenFGA is demo-grade:** in-memory datastore (tuples do not survive restart),
  `OPENFGA_AUTHN_METHOD=none`, `OPENFGA_HTTP_TLS_ENABLED=false`. Production needs
  Postgres + a migrate Job, authn, and TLS.
- **NOT per-tenant parameterized:** `mint_tokens.py` takes no args; the tenant set is the
  hardcoded tuple `(admin, tenant_a, tenant_b)`. It regenerates the RS256 key on every
  run (invalidating previously minted tokens); token TTL is 10 years (flagged in-file as
  a security smell). Adding one tenant means editing source, not passing an argument.

Source: `authz/README.md:38-45`, `mint_tokens.py`, `00-idp-openfga.yaml`.

### S4. The ONE governed Lakekeeper: authz + prefix-scoped vending - PROVEN (authz deny), vend path PROVEN LOCAL ONLY

**Purpose:** stand up the single production catalog: authz enabled (`AUTHZ_BACKEND=openfga`)
AND credential vending enabled (`ENABLE_AWS_SYSTEM_CREDENTIALS=true`) on its own fresh DB
`catalog_authz`, reusing the vending IRSA ServiceAccount `lakekeeper` from S0.

> **DO NOT deploy the `spike/` Lakekeeper in its place.** The `spike/` instance
> (`eks/lakekeeper.yaml`) runs auth-disabled / authz allow-all: it has no OIDC, no
> OpenFGA, and no per-warehouse grants. It exists only to prove the storage-isolation
> layer. Warehouses created under allow-all have no OpenFGA ownership tuples and become
> ungovernable, so you must never flip `AUTHZ_BACKEND` in place on it. Source:
> `vending` FINDINGS.md:38,67-68; `authz/README.md:8-10`.

```bash
cd deploy/eks/lakekeeper/authz

# authz-OWNED Postgres + IRSA ServiceAccount (05-postgres-and-sa.yaml). REPLACES the old
#   "borrow from the allow-all spike" step: it creates ONLY lakekeeper-postgres + the `lakekeeper`
#   ServiceAccount (no allow-all serve), and POSTGRES_DB is already `catalog_authz`, so NO separate
#   CREATE DATABASE step is needed. Before applying, set the SA annotation to the S0 output
#   lakekeeper_catalog_role_arn (the file ships a REPLACE-* placeholder ARN).
kubectl apply -f 05-postgres-and-sa.yaml
kubectl -n spark rollout status deploy/lakekeeper-postgres

# edit 10-lakekeeper-authz.yaml: set LAKEKEEPER__PG_ENCRYPTION_KEY at BOTH lines 17 and 41
#   to the SAME fresh 32-byte value (REPLACE-PG-ENCRYPTION-KEY)
# For production: also set OPENID_PROVIDER_URI/AUDIENCE/SUBJECT_CLAIM to the client's real IdP.

kubectl apply -f 10-lakekeeper-authz.yaml

kubectl -n spark wait --for=condition=complete job/lakekeeper-authz-migrate --timeout=150s
kubectl -n spark rollout status deploy/lakekeeper-authz
kubectl -n spark logs deploy/lakekeeper-authz | grep -iE 'openid|openfga'
```

The serve Deployment runs `quay.io/lakekeeper/catalog:v0.13.1` with
`AUTHZ_BACKEND=openfga`, `OPENFGA__ENDPOINT=http://openfga:8081`,
`OPENID_PROVIDER_URI=<idp>`, `OPENID_AUDIENCE=lakekeeper`, `OPENID_SUBJECT_CLAIM=sub`,
`ENABLE_AWS_SYSTEM_CREDENTIALS=true`, ServiceAccount `lakekeeper` (reuses the vending
IRSA role). Source: `authz/README.md:48-53`, `10-lakekeeper-authz.yaml:4-58`.

Bootstrap the catalog as admin (then per-tenant provisioning is in Section 5):

```bash
kubectl -n spark port-forward svc/lakekeeper-authz 8182:8181
export LK=http://localhost:8182 ADMIN=$(cat jwt_admin.txt)

curl -sf -X POST $LK/management/v1/bootstrap \
  -H "Authorization: Bearer $ADMIN" -H 'content-type: application/json' \
  -d '{"accept-terms-of-use":true,"is-operator":true}'
```

**How vend + authz unify on this one instance (and where it is not yet packaged):**

- The same instance vends because `ENABLE_AWS_SYSTEM_CREDENTIALS=true` and it runs as the
  IRSA-bound `lakekeeper` SA whose base identity can assume the vending role. At
  `loadTable` time (driver sends `X-Iceberg-Access-Delegation: vended-credentials`) it
  attaches a per-warehouse STS session policy scoped to that tenant's S3 key-prefix, so
  the credential reaching the executor is prefix-scoped even though the executor pod also
  carries a full-bucket IRSA role. Source: `vending` purpose; `authz` gap
  `10-lakekeeper-authz.yaml:33,50`.
- **PARTIALLY UNIFIED (2026-07-13):** the Postgres + IRSA `lakekeeper` ServiceAccount are now
  authz-OWNED (`authz/05-postgres-and-sa.yaml`, faithfully lifted from the spike minus the
  allow-all serve, `POSTGRES_DB=catalog_authz`), so a client install applies `authz/` only and
  never touches the allow-all spike. **Still NOT YET PACKAGED for full production:** (a) the authz
  Postgres is still an `emptyDir` pod (ephemeral: catalog metadata is lost on restart), so repoint
  `LAKEKEEPER__PG_DATABASE_URL_*` at the substrate RDS for a client; (b) `PG_ENCRYPTION_KEY` and the
  Postgres password are still plaintext env, move to k8s Secrets; (c) per-tenant warehouse
  provisioning still uses the spike storage-profile JSONs (Section 5).
- **PROVEN status:** the per-principal catalog **deny** is proven by `authz_proof.sh`
  (Section 7). The credential **vend-deny** is proven only on LOCAL tiers (14/14 on
  MinIO). On the spike instance the EKS/real-S3 vend, the CloudTrail discriminator, and
  the ablation were never run (no cluster). On the authz instance the vend-deny is a
  separate MANUAL curl, not composed into the proof script, and needs a real
  `sales.orders` table first.

### S4b. Base Connect overlay (shared per-tenant resources): PREREQUISITE

**Purpose:** apply the base Connect overlay, which provides the `spark-connect-env` and
`spark-connect-executor-podtemplate` ConfigMaps, the `spark-connect-psk` Secret, and the
`spark` ServiceAccount that every per-tenant Connect server (S5) reuses.

```bash
kustomize build deploy/eks/connect/overlays/example | kubectl apply -f -
```

`deploy/eks/connect` is required before S5: without it, `gen_connect_server.py` output
references resources that do not exist and will not schedule. Source: `RUNBOOK.md:116`,
`authz/README.md:16-18`.

### S5. Per-tenant Connect servers (custody + exec isolation): L2. Script-generated

**Purpose:** one Spark Connect server per tenant, each injecting only that tenant's JWT
server-side (token custody: the client never holds or chooses the token). Each tenant's
compute runs on its own disjoint executor pods.

```bash
export CONNECT_IMAGE=<your-ecr>/ssa-spark/spark-connect:4.1.2-iceberg1.11.0

python3 gen_connect_server.py tenant_a jwt_tenant_a.txt | kubectl apply -f -
python3 gen_connect_server.py tenant_b jwt_tenant_b.txt | kubectl apply -f -
```

Each generated server sets, per tenant:
`spark.sql.catalog.lk.header.Authorization='Bearer '+<that tenant's JWT>`,
`spark.sql.catalog.lk.warehouse=<tenant>`, executor label `tenant=<tenant>`, catalog
`lk` = Iceberg REST at `http://lakekeeper-authz:8181/catalog` with
`X-Iceberg-Access-Delegation=vended-credentials` and `S3FileIO`. The generator also emits a
per-tenant **L3 NetworkPolicy** applied by the same `kubectl apply` (see S5b).
Source: `authz/README.md:112-115`, `gen_connect_server.py:4-11,37-43`.

**Placeholders consumed:** `CONNECT_IMAGE`, `<tenant>`, `jwt_<tenant>.txt`.

**Caveats:**

- **NOT YET PACKAGED (no static manifest):** the per-tenant server exists only as
  `gen_connect_server.py` output; there is no static YAML to review. The generated
  server depends on pre-existing resources it does not create: configmaps
  `spark-connect-env` and `spark-connect-executor-podtemplate`, secret
  `spark-connect-psk` key `token`, ServiceAccount `spark`, and the `lakekeeper-authz`
  service. These come from the base Connect overlay (applied in S4b, above).
- The generator's default `CONNECT_IMAGE` still contains a literal `<ECR>` placeholder;
  it must be overridden or the manifest references a non-resolvable image.

**Proof (relayed):** a session on tenant_a's server writes/reads tenant_a; reaching
tenant_b is refused `NotAuthorized: Missing Authorization Header`; the two tenants'
executor pods are disjoint (distinct IPs, own driver, `tenant=<t>` label).
Source: `authz/README.md:121-126`, proof log
`paper/notes/proof_2026-07-10_multiserver.log` (not in sources).

### S5b. NetworkPolicy (L3): generated per-tenant by `gen_connect_server.py` (applied in S5); CNI-gated

The per-tenant L3 NetworkPolicy is now emitted by `gen_connect_server.py` alongside each tenant's
server + Service, so the `| kubectl apply -f -` in S5 already applies it. For tenant `T` it enforces:

- **Rule 1:** only the `connect-gateway` pod may reach that tenant's Connect gRPC `:15002`.
- **Rule 2:** only **that tenant's own executors** (`spark-role=executor` AND `tenant=T`) may dial the
  driver back on `7078`/`7079`. Scoping rule 2 to `tenant=T` (verified 2026-07-13: the generator emits
  `{spark-role: executor, tenant: <T>}`) is what makes executor->driver isolation per-tenant, fixing
  the old fleet-wide rule.

```bash
# already applied by S5; to (re)apply one tenant's server + netpol:
python3 gen_connect_server.py tenant_a jwt_tenant_a.txt | kubectl apply -f -
```

**RESIDUAL (CNI-gated, Group B):** the policy is INERT unless the cluster CNI enforces NetworkPolicy
(EKS VPC CNI with `ENABLE_NETWORK_POLICY=true`, or Calico/Cilium). Enable it on the cluster, then verify
a cross-tenant in-cluster dial is refused AND same-tenant execution still works before citing L3 as
enforced. The older static `netpol-tenant-servers.yaml` (one fleet-wide policy whose rule 2 admitted
ANY executor) is **superseded** by the generated per-tenant policy and kept only for reference.

### S6. Ingress gateway (L1): Envoy, principal from client-cert URI-SAN

**Purpose:** a single Envoy `connect-gateway` terminates client mTLS on port 15009,
derives the principal from the client cert's URI-SAN (last path segment of
`spiffe://safe-spark-agents/<tenant>`), and routes strictly by that derived principal to
that tenant's own Connect server. The authenticated identity, not the client's choice,
selects the upstream.

```bash
kubectl -n spark create secret generic connect-gateway-certs \
  --from-file=server.crt --from-file=server.key --from-file=connect-ca.crt

kubectl -n spark create configmap connect-gateway-config \
  --from-file=envoy.yaml=gateway-envoy.yaml

kubectl apply -f 20-connect-gateway.yaml
```

The Deployment's init container `render` (busybox) seds `__CONNECT_PSK__` (from Secret
`spark-connect-psk` key `token`) into the Envoy config; the main `envoy`
(`envoyproxy/envoy:v1.31.5`) listens on 15009 and injects `Authorization: Bearer <PSK>`
toward the upstream (so the client never holds the PSK), with
`forward_client_cert_details: SANITIZE`. Source: `authz/README.md:137-141`,
`20-connect-gateway.yaml:9-52`, `gateway-envoy.yaml:9-123`.

**Placeholders consumed:** `server.crt`/`server.key`/`connect-ca.crt`,
`__CONNECT_PSK__`, `spiffe://<TRUST_DOMAIN>/`, tenant route names, namespace `spark`.

**Caveats:**

- **MANUAL / not GitOps-composed:** the Secret and ConfigMap are imperative one-liners;
  only the Deployment + Service are declarative.
- **Hardcoded routes:** `gateway-envoy.yaml` enumerates exactly `tenant_a` and `tenant_b`
  (two route blocks + two clusters). No generator exists. Onboarding a tenant is a manual
  edit-and-reapply of the ConfigMap.
- **Nothing here forces clients through the gateway:** the direct multi-server path
  (`sc://<svc>:15002/;x-connect-principal=...;token=<psk>`) carries a client-chosen
  principal and the raw PSK in the clear and bypasses the gateway. The gateway guarantee
  only holds if the tenant Connect Services reject non-gateway traffic, which depends on
  the (not-yet-applied) NetworkPolicy (S5b). Flag as an unverified composition dependency.
- **No HA:** `replicas: 1`, single point of failure for all tenant ingress. Config/PSK
  rotation requires a pod restart (no hot-reload).
- **Gotcha:** if an old `spark-connect-mtls` port-forward is still bound to 15009,
  forward the gateway on a different local port or probes hit the wrong proxy.

### S7. Point Lakekeeper at the client's real IdP: PRODUCTIONIZATION step

**Purpose:** replace the S3 MOCK IdP with the client's real IdP.

- In `10-lakekeeper-authz.yaml:46-48`, set `OPENID_PROVIDER_URI` to the client's real
  issuer, `OPENID_AUDIENCE` and `OPENID_SUBJECT_CLAIM` to the client's values, then
  re-apply and roll out.
- Stop minting JWTs with `mint_tokens.py`; catalog tokens now come from the client's IdP.
  Per-tenant Connect servers (S5) inject the real IdP-issued token instead of
  `jwt_<tenant>.txt`.
- The mTLS client certs (S2) stay on the Connect-layer CA; the two identity mechanisms
  (client-cert URI-SAN at the gateway, OIDC JWT at the catalog) are independent.

Source: `authz` gaps (mock IdP), `10-lakekeeper-authz.yaml:46-48`, `mint_tokens.py:39`.

---

## 5. Onboard a data domain (repeatable per tenant)

Run once per tenant `<t>`. This is the closest thing to a repeatable per-tenant command,
but note the MANUAL / hand-edit points.

**1) Fill the warehouse storage profile.** Edit
`../spike/eks/warehouse-<t>.aws.json` REPLACE-* values: bucket, key-prefix, external-id,
and the vending role ARN. Use the `aws-system-identity` variant
(`eks/warehouse-*.aws.json`), NOT the local/MinIO `config/warehouse-*.json`.

**2) Register the OIDC user and create the warehouse** (admin token, from S4 port-forward):

```bash
for s in tenant_a tenant_b; do
  curl -sf -X POST $LK/management/v1/user \
    -H "Authorization: Bearer $ADMIN" -H 'content-type: application/json' \
    -d "{\"id\":\"oidc~$s\",\"name\":\"$s\",\"user-type\":\"application\",\"update-if-exists\":true}"
  curl -sf -X POST $LK/management/v1/warehouse \
    -H "Authorization: Bearer $ADMIN" -H 'content-type: application/json' \
    -H 'x-project-id: 00000000-0000-0000-0000-000000000000' \
    --data @../spike/eks/warehouse-$s.aws.json
done
```

**3) Grant OpenFGA permissions on that tenant's own warehouse only:**

```bash
for s in tenant_a tenant_b; do
  WID=$(curl -sf $LK/management/v1/warehouse -H "Authorization: Bearer $ADMIN" \
    -H 'x-project-id: 00000000-0000-0000-0000-000000000000' \
    | python3 -c "import json,sys;print({x['name']:x['id'] for x in json.load(sys.stdin)['warehouses']}['$s'])")
  curl -sf -X POST $LK/management/v1/permissions/warehouse/$WID/assignments \
    -H "Authorization: Bearer $ADMIN" -H 'content-type: application/json' \
    -d "{\"writes\":[{\"type\":\"select\",\"user\":\"oidc~$s\"},{\"type\":\"describe\",\"user\":\"oidc~$s\"},{\"type\":\"modify\",\"user\":\"oidc~$s\"},{\"type\":\"create\",\"user\":\"oidc~$s\"}],\"deletes\":[]}"
done
```

**4) Mint (or obtain from the real IdP) the tenant token**, then **generate the Connect
server** (S5) and **issue the client cert** (S2):

```bash
# dev/mock: python3 mint_tokens.py  (regenerates ALL tokens; see caveat)
python3 gen_connect_server.py <t> jwt_<t>.txt | kubectl apply -f -
./issue-client-cert.sh <t>          # from deploy/auth/certs
```

**5) Add the gateway route** for `<t>`: hand-edit `gateway-envoy.yaml` (add the route
block + cluster), re-create the `connect-gateway-config` ConfigMap, restart the gateway
pod. (No generator; hardcoded per tenant.)

**Placeholders consumed:** `<t>`, warehouse bucket/prefix/external-id, vending role ARN,
`CONNECT_IMAGE`, `TRUST_DOMAIN`.

**Caveats:** grants are coarse and manual (looped curls, warehouse ids resolved by
name); `mint_tokens.py` is not incremental (regenerating keys invalidates prior tokens);
gateway routing is a manual edit. Source: `authz/README.md:59-73`,
`onboarding-scripts` gaps, `ingress` gaps.

---

## 6. Deploy Omnigent (the delivery control plane) on the cluster

**Purpose:** Omnigent is the governed delivery layer: a control-plane server you deploy on the cluster,
with coding agents that build the client's pipelines and a custodian that holds every credential so the
agent fleet stays credential-free. This section deploys the server the official way, points it at the
client's identity, and runs the Section 4 delivery loop on top.

**Architecture:** see `paper/diagrams/section4_contained_omnigent.svg` (this repo): the server, custodian,
and credential-free fleet as pods in the client's EKS, over the §3 platform, one IdP over both.

> **Omnigent manifests are vendored in-repo** at `deploy/kubernetes/` (a pinned snapshot from
> `github.com/omnigent-ai/omnigent@046246fb`, vendored 2026-07-14; see `deploy/kubernetes/VENDORED.md`).
> Use them directly, no second clone needed. Omnigent moves fast (the local `~/omnigent` was ~800 commits
> behind and even lacked `overlays/sandbox-runners`), so re-vendor periodically per VENDORED.md for the
> latest overlays. Full guide: `deploy/kubernetes/README.md`.

### 6.1 Deploy the Omnigent server (Kustomize)

The server is a single-replica Deployment (`ghcr.io/omnigent-ai/omnigent-server`, port 8000) + Service +
PVC + Secret/ConfigMap, in namespace `omnigent`. Back it with the substrate RDS from S0 (create an
`omnigent` database there), or the in-cluster `postgres` overlay for dev. Single replica only (the runner
registry is in-memory).

```bash
# Omnigent manifests are vendored at deploy/kubernetes/ (pinned snapshot; resync per VENDORED.md), run from the repo root.

# Edit deploy/kubernetes/base/secret.yaml:
#   DATABASE_URL: "postgresql+psycopg://<user>:<pass>@<rds-endpoint>:5432/omnigent"
#   OMNIGENT_ACCOUNTS_COOKIE_SECRET: "$(openssl rand -hex 32)"
kubectl kustomize deploy/kubernetes/base/ | kubectl apply -f -
# dev alternative (in-cluster Postgres):
#   kubectl kustomize deploy/kubernetes/overlays/postgres/ | kubectl apply -f -

kubectl -n omnigent rollout status deploy/omnigent
kubectl -n omnigent port-forward svc/omnigent 8000:80   # curl localhost:8000/health -> {"status":"ok"}
```

Prereqs: Kubernetes 1.25+, kubectl with Kustomize, a Postgres (RDS or the overlay).
Source: upstream `deploy/kubernetes/README.md`.

### 6.2 Point Omnigent at the client's IdP (the SAME OIDC as Section 3)

The default `accounts` provider makes the first visitor the admin. For a client, delegate to their real
IdP, the same OIDC issuer you wire into Lakekeeper (S7), so one identity governs both the catalog and the
delivery platform.

```bash
kubectl create secret generic omnigent-oidc -n omnigent \
  --from-literal=OMNIGENT_AUTH_PROVIDER=oidc \
  --from-literal=OMNIGENT_OIDC_ISSUER=<client-idp-issuer> \
  --from-literal=OMNIGENT_OIDC_CLIENT_ID=<client-id> \
  --from-literal=OMNIGENT_OIDC_CLIENT_SECRET=<client-secret> \
  --from-literal=OMNIGENT_OIDC_REDIRECT_URI=https://<omnigent-host>/auth/callback \
  --from-literal=OMNIGENT_OIDC_COOKIE_SECRET=$(openssl rand -hex 32)
# then add  envFrom: [{secretRef: {name: omnigent-oidc}}]  to the server Deployment container.
```

### 6.3 Connect the agent fleet (hosts or in-cluster sandbox runners)

Agents run on hosts that register with the server, or as in-cluster sandbox runners (an overlay). Model
keys ride a projected `omnigent-creds` Secret, never baked into an agent.

```bash
# Option A - register a host (a machine that runs the coding agents):
omnigent login https://<omnigent-host>
omnigent host  --server https://<omnigent-host>

# Option B - in-cluster sandbox runners (CURRENT upstream overlay; NOT in the 803-behind local checkout):
kubectl apply -k deploy/kubernetes/overlays/sandbox-runners
```

### 6.4 Run the governed delivery loop (the Section 4 custodian pattern)

The Section 4 demonstration: the custodian holds the Spark Connect PSK + per-tenant catalog tokens, and
the cross-vendor sub-agent fleet (`claude_code` / `codex` / `pi`) builds each tenant's medallion through
it, credential-free, under a native cost ceiling. Agent + custodian live in
`deploy/omnigent/sdp-capstone/` (this repo).

```bash
# config.yaml: roster claude_code (Anthropic) / codex (OpenAI) / pi (gateway); guardrails max_cost_usd.
# tools/mcp/custodian.yaml (transport stdio): the custodian holds PSK + per-tenant tokens in-process,
#   exposing only seed_raw_data / submit_pipeline / probe_isolation to the fleet.
omnigent run deploy/omnigent/sdp-capstone \
  -p "Build the customer medallions per your brief, then write RESULT.md."
```

**Honest status of 6.4 (NOT YET VERIFIED against a deployed server):**

- **Dev shim:** the custodian's bootstrap opens a `kubectl port-forward` per tenant with a fixed
  `sleep(6)` (brittle, no readiness check). Co-located in-cluster the port-forward drops out; the
  custodian should run inside the deployment, not from a laptop. The reproduced path is not the
  production path.
- **Pattern, not turn-key:** the capstone config is the Section 4 demonstration wiring (three fixed
  customers, headless, `pi` model unpinned, `spawn: true`). For a client it is the pattern to adapt.
- **Integration unverified here:** how this agent runs on a registered host / sandbox runner of a
  DEPLOYED Omnigent server (vs. the local `omnigent run`) is not verified in this environment (needs the
  cluster). Verify end to end on the deployed server before citing it as the production delivery path.

---

## 7. Verify (per-layer proofs, by name)

Run each layer's proof and name its log. Proof logs marked "(relayed)" are referenced
but their raw contents are outside the extract sources.

| Layer | How to verify | Expected | Proof log |
|-------|---------------|----------|-----------|
| L1 Ingress routing | `kubectl -n spark logs deploy/connect-gateway | grep GWLOG` | tenant_a cert -> spark-connect-tenant-a only; tenant_b -> tenant-b only; un-granted principal -> 403; no cert -> TLS handshake refused | `paper/notes/proof_2026-07-10_ingress_routing.log` (relayed) |
| L2 Custody + exec | run a session on each tenant server; inspect executor pods | cross-tenant refused `NotAuthorized: Missing Authorization Header`; executor pods disjoint | `paper/notes/proof_2026-07-10_multiserver.log` (relayed) |
| L4 Authz deny | `./authz_proof.sh` (after `wids.env`) | tenant_a own=200 other=404; admin 200/200; no-token 401; a `describe` grant/revoke toggles the deny (proving 404 is authz, not nonexistence) | inline; `paper/notes/proof_2026-07-10_perprincipal_authz.log` (relayed) |
| L4 Vend-deny (data plane) | MANUAL curl pair with `X-Iceberg-Access-Delegation: vended-credentials` on `loadTable` (needs a real `sales.orders` in tenant_b) | tenant_b owner -> 200 + storage-credentials; tenant_a cross -> 404 no credentials vended | section D of the authz proof log (relayed); **not composed into authz_proof.sh** |
| L5 Storage isolation | run `run_spike.py` (repartition(8) forces executor-side FileIO); CloudTrail + ablation | tenant_a executor DENIED tenant_b (AccessDenied); PutObject userIdentity is the `-lakekeeper-vending` session, not `-irsa-spark`; removing the header makes cross-tenant SUCCEED | LOCAL: `out/results.json` 14/14 PASS; **EKS/real-S3 tier UNRUN** |

```bash
cd deploy/eks/lakekeeper/authz
# capture warehouse ids the proof needs
curl -sf $LK/management/v1/warehouse -H "Authorization: Bearer $ADMIN" \
  -H 'x-project-id: 00000000-0000-0000-0000-000000000000' \
  | python3 -c "import json,sys;w={x['name']:x['id'] for x in json.load(sys.stdin)['warehouses']};print(f\"WID_A={w['tenant_a']}\nWID_B={w['tenant_b']}\")" > wids.env
./authz_proof.sh
```

**Honest status of the proofs:** the per-principal catalog **deny** (L4) is PROVEN by
`authz_proof.sh`. Token custody + executor isolation (L2) and ingress routing (L1) are
relayed from README summaries of proof logs not in the extract sources. The storage
vend-deny (L5) is PROVEN LOCAL ONLY (14/14 on MinIO); the load-bearing EKS/real-S3 tier
(`kustomize build` = 19 objects) was never run, so the CloudTrail discriminator and
ablation are described but not observed. The full five-layer end-to-end composition
(client cert -> gateway -> authz catalog -> prefix-scoped vend on the tenant's executors)
is explicitly "the remaining composition step, not yet captured."
Source: `SETUP.md:67-71`, `vending` FINDINGS.md:9-17,40, `authz` proof_ref.

---

## 8. Operate / rotate / cost / teardown

### Operate

```bash
# generate + load the deterministic messy dataset (bronze)
python -m generator generate --seed 42 --chaos-rate 0.08 --output ~/ssa-deploy/data
python load/load_to_iceberg.py --data-dir ~/ssa-deploy/data --catalog iceberg
python load/stream_to_iceberg.py --chaos-rate 0.08 --seed 42     # Kafka -> Iceberg

# run SDP pipelines by pointing SPARK_REMOTE at the mTLS endpoint
SPARK_REMOTE=<mTLS endpoint>     # exact sc://<nlb-dns>:15009 value is not given in ops scope
```

CDC: hand-rolled MERGE SCD1/SCD2 on the Tier-A (released 4.1) image, or native
`create_auto_cdc_flow(stored_as_scd_type=1)` on the Tier-B (5.0-SNAPSHOT) image.
Scale executors by bumping the executor node-group `max` in `deploy/eks/terraform`
and/or `spark.dynamicAllocation.maxExecutors` in the connect ConfigMap (no
apply/rollout command is given in scope). Source: `RUNBOOK.md:189-203`.

### Monitor

```bash
kubectl -n spark logs deploy/spark-connect -c spark-connect     # driver
kubectl -n spark logs deploy/spark-connect -c envoy             # mTLS sidecar
kubectl -n spark get pods -l spark-role=executor                # live executors
```

### Rotate

```bash
# PSK: update the Secret, then restart. (§7 names it connect-psk; the DEPLOYED name is
#   spark-connect-psk key `token` from §4. Use the §4 name.)
kubectl rollout restart deploy/spark-connect -n spark
```

- Client certs: re-issue per principal (`./issue-client-cert.sh <principal>`, S2).
- CA (hard revocation): issue a new Connect-layer CA, update the cert Secret, restart
  the Connect pod, re-issue ALL client certs (invalidates every old cert at once). Prose
  only in scope; the deployed Secret is `spark-connect-envoy-certs` / `connect-ca.crt`
  (RUNBOOK §7's `connect-mtls` / `ca.crt` names are drifted, do not target them).

### Cost (rough, 24/7)

EKS control plane ~$73/mo + executor/system nodes (scale to zero when idle) + RDS
Postgres ~$30-60/mo + NLB ~$18/mo + S3 (usage-based). Expect a few hundred $/mo at rest;
scale node groups to zero overnight to cut compute. A single-EC2 dev tier is far cheaper.
Source: `RUNBOOK.md:256-262`.

### Teardown

```bash
# app layers first
kubectl delete -k deploy/eks/connect/overlays/example
kubectl delete -k deploy/eks/hms/overlays/example

# then the substrate (billable). Prod-safety defaults BLOCK a clean destroy: first flip
#   rds_deletion_protection=false, rds_skip_final_snapshot=true, force_destroy_warehouse=true
cd deploy/eks/terraform && terraform destroy -var-file=terraform.tfvars
```

Manual cleanup (not automated by destroy): delete leftover S3 warehouse objects and RDS
snapshots; revoke unneeded IAM keys; optionally delete the CloudTrail trail
`ssa-isolation-audit` + its bucket (only mentioned in `SETUP.md`, not RUNBOOK §9). The
allow-all `spike/` catalog and the `authz/` catalog are independent and can be torn down
separately. Note teardown targets the literal `example` overlays, but each engagement
should have copied `example` to its own overlay, so adjust paths to what you applied.
Source: `RUNBOOK.md:246-252`, `SETUP.md:78-81`.

---

## 9. Known gaps + productionization TODO (consolidated)

The honest list. Do not tell a client any of these run today.

**Isolation / trust wiring**

1. **External-id IS enforced (not a gap): an L5 property.** The production vending role
   pins an `sts:ExternalId` trust condition (`var.lakekeeper_external_id`, confused-deputy
   protection), so the warehouse `external-id` must equal `TF_VAR lakekeeper_external_id`
   or `AssumeRole` fails and no credential is vended. Only the allow-all `spike/` role
   lacks it. Source: `terraform/lakekeeper-vending.tf:76,92-93`,
   `SETUP.md:22-24`; spike-only omission at `spike/eks/terraform/lakekeeper-vending.tf:76-85`.
2. **Per-tenant isolation is not enforced by Terraform.** The vending role's own policy is
   broad ALLOW on the whole bucket; downscoping happens only at vend time via a runtime
   session policy. The catalog pod (`catalog_manage_s3`) and the executor SA
   (`irsa-spark`) both hold FULL bucket RW. Isolation is proven only at the executor's
   vended, prefix-scoped STS session.
3. **NetworkPolicy (L3) generated per-tenant (2026-07-13), CNI-gated.** Now emitted per tenant by
   `gen_connect_server.py` with rule 2 scoped to that tenant's executors (`tenant=T`), fixing the old
   fleet-wide scope. Residual: inert unless the cluster CNI enforces NetworkPolicy (enable it), then
   verify a cross-tenant dial is refused AND same-tenant execution still works before citing L3 as enforced.
4. **Nothing forces clients through the gateway.** The plaintext `:15002` path carries a
   client-chosen principal + raw PSK and bypasses the gateway; the gateway guarantee
   depends on the per-tenant NetworkPolicy (now generated per tenant, but still CNI-gated) locking `:15002` to the gateway.
5. **RDS SG egress is allow-all** (`0.0.0.0/0`), as is the optional bastion SG egress
   (inbound is tightly scoped).

**Auth / secrets**

6. **IdP is a MOCK.** Static nginx OIDC discovery + JWKS for hand-minted RS256 JWTs
   (10-year TTL, key regenerated per run). Replace with the client's real IdP (S7).
7. **OpenFGA is demo-grade:** in-memory (no persistence), unauthenticated, no TLS. Needs
   Postgres + migrate Job + authn + TLS.
8. **Plaintext secrets:** `PG_ENCRYPTION_KEY` and Postgres creds are inline env in the
   manifests, not k8s Secrets. No vault/Secrets-Manager integration; out-of-git custody
   is a documented convention, not enforced tooling.
9. **No revocation path:** no CRL/OCSP for client certs; hard cutoff = rotate the whole CA.
10. **Cert-path + placeholder drift: RESOLVED (2026-07-13)** by `render.sh` (fills the six `REPLACE-*`
    tokens) and `bootstrap-secrets.sh` (issues certs + PSK and creates the Secrets with the correct
    `--from-file` paths), both verified by running. Residual doc nit: the older `RUNBOOK.md` §7
    (rotation) still names `connect-psk`/`connect-mtls`; the live manifests and this runbook use the
    correct `spark-connect-psk`/`connect-gateway-certs`.

**Packaging / composition**

11. **Auth interceptor NOT vendored.** The verified Tier-A image was built with
    `ALLOW_MISSING_INTERCEPTOR=1`, so it has no real interceptor and Connect auth is
    non-functional until rebuilt with the jar (built separately from
    `deploy/auth/interceptor`). Registry login/push is manual and was not part of the gate.
12. **Tier B (native AUTO CDC) not rebuilt or verified:** depends on a Spark 5.0-SNAPSHOT
    base you build yourself; released Spark 4.2 + Iceberg for it do not exist yet.
13. **`CONNECT_IMAGE` handoff is manual and undocumented in `build.sh`.**
14. **Per-tenant Connect server has no static manifest** (script-generated only) and
    depends on base-overlay resources it does not create. Gateway routing is hardcoded per
    tenant with no generator.
15. **Single-instance vend+authz PARTIALLY packaged (2026-07-13).** The Postgres + IRSA
    `lakekeeper` ServiceAccount are now authz-owned (`authz/05-postgres-and-sa.yaml`), so a client
    install no longer touches the allow-all spike. Remaining before shipping: repoint
    `LAKEKEEPER__PG_DATABASE_URL_*` at RDS (the authz Postgres is still `emptyDir`/ephemeral), move
    secrets to k8s Secrets, and self-contain per-tenant warehouse provisioning (still uses the spike
    storage-profile JSONs).
16. **`mint_tokens.py` is not per-tenant / not incremental:** hardcoded subject tuple,
    regenerates the signing key each run.
17. **Field-name inconsistency:** TF output docs say `sts-role-arn`; the warehouse JSON
    key is `assume-role-arn`. Confirm the Lakekeeper schema key before provisioning.
18. **Gateway has no HA** (`replicas: 1`), no hot-reload (PSK/config rotation needs a
    restart).

**Proof status**

19. **EKS/real-S3 storage tier UNRUN.** The vend-deny is PROVEN LOCAL ONLY (14/14 on
    MinIO); the load-bearing EKS proof (`kustomize build` = 19 objects), CloudTrail
    discriminator, and ablation were never executed.
20. **Vend-deny not composed into the authz proof script** (separate manual curl, needs a
    real `sales.orders` table first).
21. **Five-layer end-to-end composition is not yet captured** ("the remaining composition
    step"). Each layer is proven in isolation; the single request crossing all five is not.
22. **`account_id` is not a TF output.** Downstream stages must derive it from
    `aws sts get-caller-identity` or off the emitted ARNs.
23. **The authoritative apply path (CI `eks-terraform-apply.yml`) and its OIDC role are
    not in the sources**; the manual `terraform apply` command exists but the README states
    apply is "not run ad-hoc from a workstation."
24. **Operator access to the private API is only partially packaged:** SSM bastion is
    built-in but off by default; the recommended Client VPN path is not provisioned.
25. **Omnigent per-tenant token custody + isolation enforcement live in
    `custodian_capstone.py`** (not in sources), so those claims are only partially
    verifiable; the port-forward bootstrap is a brittle dev shim, and Omnigent here is a
    demonstration, not a metrics claim.

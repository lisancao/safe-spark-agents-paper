# EKS Spark stack (Terraform), `deploy/eks/terraform`

> **APPLIED via reviewer-gated CI (2026-07-09).** This stack was applied through the gated
> `eks-terraform-apply.yml` workflow (GitHub OIDC, S3-backed remote state), standing up the live
> `ssa-spark-eks` cluster (Kubernetes 1.31, ~118 resources) that Section 3's isolation proof runs on;
> provenance in `paper/notes/PLATFORM_LAB_NOTEBOOK.md` (entries W1/W2). Static checks
> (`terraform fmt`, `terraform init -backend=false`, `terraform validate`) still gate every change.
> **Caution:** a fresh `terraform apply` creates real, billable AWS infrastructure (EKS, NAT, RDS,
> S3); `terraform destroy` tears it down to stop the meter.

Kubernetes-native **production** target for the OSS Spark stack, chosen over EMR so the
team owns the Spark version (**4.1 now, 4.2 soon**; the engine version lives in the Spark
Connect / executor container images, not in this infra). This stack stands up:

- an **EKS** cluster (private API by default, OIDC/IRSA enabled),
- a **fresh VPC** (2 private + 2 public subnets, 2 AZs, IGW, single NAT),
- two **managed node groups**, a small on-demand *system* pool and a taint-isolated,
  scalable *executor* pool,
- **IRSA** roles for the Spark driver/executor SA and the Hive Metastore SA,
- a **durable Hive Metastore** backing DB on **RDS PostgreSQL** (private, Multi-AZ),
  with the password in **Secrets Manager** (never in code/committed files),
- an **S3 Delta warehouse** bucket (encrypted, versioned, public access blocked) with
  `bronze/ silver/ gold/` medallion prefixes,
- an optional **SSM bastion** for operator access to the private API.

This is a **separate stack** from `deploy/aws` (B1). It reuses B1's access *philosophy*
(everything private; operators reach in over VPN/SSM) but shares no state.

---

## Architecture

```
                          ┌────────────────────────── VPC 10.40.0.0/16 (2 AZs) ──────────────────────────┐
  operator                │                                                                               │
  (kubectl) ──VPN/SSM──▶  │  public subnets ── IGW / NAT GW (single)                                      │
                          │                                                                               │
                          │  private subnets ┌───────────────── EKS (private API, OIDC) ───────────────┐ │
                          │                  │  system NG (on-demand)   executor NG (taint: spark-role) │ │
                          │                  │   ├ CoreDNS/kube-proxy     └ Spark executor pods         │ │
                          │                  │   ├ Hive Metastore  ◀─IRSA(hms)─┐    (workload=spark-     │ │
                          │                  │   └ Spark Connect + driver       │     executor)         │ │
                          │                  └───────────│──────────────────────│──────────────────────┘ │
                          │                              │ IRSA(spark)           │                         │
                          │            ┌─────────────────▼────────┐   ┌─────────▼──────────┐              │
                          │            │ S3 warehouse (Delta)      │   │ RDS PostgreSQL     │              │
                          │            │ bronze/ silver/ gold/     │   │ (Hive Metastore DB)│              │
                          │            │ SSE + versioning + BPA    │   │ private, Multi-AZ  │              │
                          │            └───────────────────────────┘   └────────────────────┘              │
                          │                       ▲                          ▲ SG: nodes only              │
                          │       Secrets Manager │ (metastore conn bundle)  │                             │
                          └───────────────────────┴──────────────────────────┴─────────────────────────────┘
```

**Access model (IRSA, least privilege):**

| Service account (ns/sa)            | IAM role               | Grants                                                        |
|------------------------------------|------------------------|--------------------------------------------------------------|
| `spark/spark`                      | `ssa-spark-irsa-spark` | S3 read/write on the warehouse bucket (all medallion prefixes) |
| `hive-metastore/hive-metastore`    | `ssa-spark-irsa-hms`   | S3 read/write on the warehouse + `GetSecretValue` on the metastore secret |

HMS reaches Postgres at the network layer (RDS SG allows **only** the EKS node SG); the
DB credentials come from Secrets Manager, so the HMS role needs no `rds:*` IAM.

After apply, the role ARNs and the exact SA annotations are in the `irsa_spark` /
`irsa_hms` outputs. Annotate each Kubernetes SA:

```yaml
# spark SA
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark
  namespace: spark
  annotations:
    eks.amazonaws.com/role-arn: <irsa_spark.role_arn from outputs>
```

**Executor scheduling:** the executor node group is labeled `workload=spark-executor`
and tainted `spark-role=executor:NoSchedule`. Spark executor pod templates must set the
matching `nodeSelector` + `toleration` so executors land on that pool and nothing else
does; driver/HMS/system pods stay on the untainted system pool.

---

## Prerequisites

- Terraform `>= 1.7`, AWS CLI v2, `kubectl`, the **Session Manager plugin** (if using the bastion).
- AWS profile **`ssa-deploy`** configured for account **${AWS_ACCOUNT_ID}** / **us-east-1**:
  ```bash
  aws sts get-caller-identity --profile ssa-deploy
  ```
- The remote-state backend already exists: S3 `${TFSTATE_BUCKET}`, DynamoDB
  `ssa-tf-locks`.

---

## Init (partial S3 backend)

The `backend "s3" {}` block in `versions.tf` is intentionally empty, pass the
environment specifics at init time:

```bash
cd deploy/eks/terraform

terraform init \
  -backend-config="bucket=${TFSTATE_BUCKET}" \
  -backend-config="key=eks/terraform.tfstate" \
  -backend-config="dynamodb_table=ssa-tf-locks" \
  -backend-config="region=us-east-1" \
  -backend-config="encrypt=true"
```

> **State holds a secret.** The RDS master password is generated by Terraform and,
> although it's also written to Secrets Manager, it lands in the Terraform **state**
> too. So `encrypt=true` is passed explicitly above (server-side encryption of the
> state object). The `${TFSTATE_BUCKET}` bucket is already SSE-encrypted, but
> we make it explicit so a misconfigured/replacement backend can't silently store the
> secret in cleartext. For a stronger guarantee, also pass a CMK:
> `-backend-config="kms_key_id=arn:aws:kms:us-east-1:${AWS_ACCOUNT_ID}:key/<id>"`.
>
> Tip: put those lines in a `backend.hcl` (git-ignored) and run
> `terraform init -backend-config=backend.hcl`.

## Plan / apply (against `ssa-deploy`)

```bash
cp terraform.tfvars.example terraform.tfvars   # adjust as needed; no secrets go here
terraform plan  -var-file=terraform.tfvars -out tfplan
terraform apply tfplan
```

The provider reads `profile = "ssa-deploy"` and `region = "us-east-1"` from the vars,
and a precondition aborts the run if the caller is not account `${AWS_ACCOUNT_ID}`.

## Operator access (private API)

The API is **private by default**. Pick one path:

1. **SSM bastion (built in, minimal).** Set `create_bastion = true`. No inbound SG, no
   SSH key. Reach the API by port-forwarding through the bastion:
   ```bash
   aws ssm start-session --target <bastion_instance_id> --profile ssa-deploy
   # then run update-kubeconfig from the bastion, or set up an SSM port-forward to :443.
   ```
2. **AWS Client VPN (recommended at team scale, reuse the B1 pattern).** Associate a
   Client VPN endpoint with the private subnets, push routes to the VPC CIDR, then run
   `update-kubeconfig` from your laptop while connected. Not provisioned here (it needs
   ACM server/client certs); add it as a sibling stack or extend B1's VPN to this VPC.
3. **Restricted public access (break-glass).** Set
   `cluster_endpoint_public_access = true` with a tight
   `cluster_endpoint_public_access_cidrs` allowlist. Never `0.0.0.0/0`.

Configure kubectl (from a host with network reach to the private endpoint):

```bash
aws eks update-kubeconfig --region us-east-1 --name ${EKS_CLUSTER} --profile ssa-deploy
kubectl get nodes
```

## Teardown

```bash
terraform destroy -var-file=terraform.tfvars
```

Prod-safety defaults will block a clean destroy on purpose, flip these first if you
really mean it:

- `rds_deletion_protection = false` and `rds_skip_final_snapshot = true` (or keep the
  final snapshot), to drop the metastore DB.
- `force_destroy_warehouse = true`, to delete a non-empty warehouse bucket.

Also delete the remote state object/lock only if you are decommissioning the stack
entirely.

---

## What's in here

| File                       | Purpose                                                        |
|----------------------------|----------------------------------------------------------------|
| `versions.tf`              | Terraform/provider pins + partial S3 backend.                  |
| `providers.tf`             | AWS provider (profile/region) + default tags.                  |
| `variables.tf`             | All inputs (sane prod defaults).                               |
| `terraform.tfvars.example` | Copy to `terraform.tfvars`; **no secrets**.                    |
| `locals.tf` / `data.tf`    | Derived names, AZs, account guard, reuse-VPC alternative.      |
| `vpc.tf`                   | Fresh VPC (terraform-aws-modules/vpc).                         |
| `eks.tf`                   | EKS cluster + addons + the two node groups (terraform-aws-modules/eks). |
| `irsa.tf`                  | IRSA roles for Spark and HMS service accounts.                 |
| `s3.tf`                    | Warehouse bucket + medallion prefixes.                         |
| `rds.tf`                   | Metastore Postgres + SG + Secrets Manager bundle.             |
| `bastion.tf`               | Optional SSM bastion + cluster-API ingress.                   |
| `outputs.tf`               | Cluster, IRSA bindings, warehouse, metastore secret refs.     |

**Design choices**

- **Managed node groups over Karpenter.** Fully declarative in Terraform with no
  in-cluster controller to bootstrap (no Helm/CRD dependency in the IaC), which is the
  simpler, more reproducible first prod stand-up. Karpenter can be layered on later for
  finer-grained executor bin-packing, the executor pool's taint/label contract stays
  the same. The executor group already scales `min=0..max=10`.
- **Fresh VPC, variabilized.** Default path; a documented `data.tf` alternative reuses
  an existing VPC (e.g. B1's) if the team prefers one network.
- **Secrets never in code/state-committed files.** The RDS password is generated by
  Terraform and written only to Secrets Manager (and the *remote, encrypted* S3 state).
  No password is emitted as an output. `terraform.tfvars` is git-ignored.

## Gates (static checks on every change)

```bash
terraform fmt -recursive
terraform init -backend=false
terraform validate
```

All three pass and gate every change. **Apply** is not run ad-hoc from a workstation: it goes through
the reviewer-gated `eks-terraform-apply.yml` CI workflow (GitHub OIDC, S3-backed remote state), which
applied this stack on 2026-07-09 to produce the live `ssa-spark-eks` cluster (`paper/notes/PLATFORM_LAB_NOTEBOOK.md`, W1/W2).

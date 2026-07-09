# W1 - GitHub -> AWS OIDC bootstrap (one-time, by hand)

This is the **root of trust** for the CI-built platform. It is the only step that
needs a manual AWS touch: CI can't create the GitHub->AWS trust until that trust
exists. After this, every EKS terraform run happens in CI by assuming a role via
OIDC - **no long-lived AWS keys are ever created or stored.**

## What it creates
- A **GitHub OIDC identity provider** in your AWS account.
- A **plan role** (read-only, assumable from any ref) for PR `terraform plan`.
- An **apply role** (write) assumable **only** from the `eks-apply` GitHub
  Environment - so the required-reviewer gate on that environment *is* the
  boundary on who can mutate AWS.
- The **remote-state backend** the EKS stack expects: an encrypted, versioned,
  private S3 bucket + a DynamoDB lock table (`ssa-tf-locks`).

## Run it once (admin creds, local)
```bash
cd deploy/eks/bootstrap-oidc
cp terraform.tfvars.example terraform.tfvars   # set github_org/repo, region, a unique bucket name
terraform init      # local state
terraform apply     # review + approve
terraform output    # copy the values below
```
> If your account already has a GitHub OIDC provider, import it first so this
> doesn't try to create a duplicate:
> `terraform import aws_iam_openid_connect_provider.github <existing-arn>`

## Wire the outputs into the repo (Settings -> Secrets and variables -> Actions -> **Variables**)
Role ARNs contain the account id, so they live as repo **Variables**, never in a
committed workflow file:

| Repo Variable | From output |
|---|---|
| `AWS_TF_PLAN_ROLE_ARN` | `plan_role_arn` |
| `AWS_TF_APPLY_ROLE_ARN` | `apply_role_arn` |
| `TFSTATE_BUCKET` | `tfstate_bucket` |
| `TFLOCK_TABLE` | `tflock_table` |
| `AWS_REGION` | your region (e.g. `us-east-1`) |

## Create the gated environment (Settings -> Environments)
1. New environment named **`eks-apply`**.
2. Add **Required reviewers = you**. This is the approval you click before any
   `terraform apply` runs, and it's what the apply role's trust policy is scoped to.

## Then W2 runs itself
`.github/workflows/eks-terraform-apply.yml` plans on every PR touching
`deploy/eks/terraform/**`, and applies only via **Actions -> EKS Terraform ->
Run workflow -> action: apply**, which pauses for your approval on `eks-apply`.

## Security notes (honest)
- The apply role's policy is **service-scoped** (the services the EKS stack
  manages) but broad within those services, because Terraform creates resources
  whose ARNs aren't known ahead of time. For production, attach a **permissions
  boundary** and tighten per-resource. This role is infra plumbing, separate from
  the paper's tenant-isolation claim.
- State is encrypted (`aws:kms`) because it contains the RDS master password.
- This module uses **local** state (it bootstraps the remote backend); keep its
  `terraform.tfstate` out of git (already covered by the repo `.gitignore` for
  `*.tfstate`; verify before committing).

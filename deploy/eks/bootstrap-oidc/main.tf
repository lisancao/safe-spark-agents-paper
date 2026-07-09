# =============================================================================
# W1 bootstrap: GitHub Actions -> AWS OIDC trust + the remote-state backend the
# EKS stack initializes against. Applied ONCE, by hand, with admin creds (the
# chicken-and-egg root of trust). After this, every EKS terraform run happens in
# CI by ASSUMING the roles below via OIDC -- no long-lived AWS keys ever exist.
# Uses LOCAL state on purpose (it bootstraps the remote backend). See README.md.
# =============================================================================

# ---- GitHub OIDC identity provider -----------------------------------------
data "tls_certificate" "github" {
  url = "https://token.actions.githubusercontent.com/.well-known/openid-configuration"
}

resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.github.certificates[0].sha1_fingerprint]
  # NOTE: if this account already has a GitHub OIDC provider, import it instead
  # of creating a duplicate: terraform import aws_iam_openid_connect_provider.github <arn>
}

locals {
  repo_sub = "repo:${var.github_org}/${var.github_repo}"
}

# ---- Remote-state backend (S3 + DynamoDB lock) ------------------------------
resource "aws_s3_bucket" "tfstate" {
  bucket = var.tfstate_bucket_name
}

resource "aws_s3_bucket_versioning" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tfstate" {
  bucket = aws_s3_bucket.tfstate.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "aws:kms" }
  }
}

resource "aws_s3_bucket_public_access_block" "tfstate" {
  bucket                  = aws_s3_bucket.tfstate.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_dynamodb_table" "tflock" {
  name         = var.lock_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"
  attribute {
    name = "LockID"
    type = "S"
  }
  # This table often pre-exists (shared with the EKS stack's state locking). We
  # adopt it into state so the module is complete + reproducible on a fresh
  # account, but ignore all drift so we never modify a shared table in place.
  lifecycle {
    ignore_changes = all
  }
}

# ---- Plan role: READ-ONLY, assumable from any ref (PR plans) ----------------
# Plan runs with -lock=false (read-only to state), so ReadOnlyAccess suffices.
resource "aws_iam_role" "gha_plan" {
  name = "${var.name_prefix}-gha-tf-plan"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = aws_iam_openid_connect_provider.github.arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = { "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com" }
        StringLike   = { "token.actions.githubusercontent.com:sub" = "${local.repo_sub}:*" }
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "gha_plan_readonly" {
  role       = aws_iam_role.gha_plan.name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}

# ---- Apply role: WRITE, assumable ONLY from the gated environment -----------
# The :environment: condition is the security boundary -- this role can only be
# assumed by a job running in the GitHub Environment "${var.apply_environment}",
# which carries required-reviewer protection (you approve every apply).
resource "aws_iam_role" "gha_apply" {
  name = "${var.name_prefix}-gha-tf-apply"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = aws_iam_openid_connect_provider.github.arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          "token.actions.githubusercontent.com:sub" = "${local.repo_sub}:environment:${var.apply_environment}"
        }
      }
    }]
  })
}

# Scoped to the SERVICES the EKS stack manages. Broad within-service by
# necessity (Terraform creates many resources whose ARNs are not known ahead of
# time). For production: attach a permissions boundary and tighten per-resource.
resource "aws_iam_role_policy" "gha_apply" {
  name = "eks-stack-apply"
  role = aws_iam_role.gha_apply.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "StackServices"
      Effect = "Allow"
      Action = [
        "eks:*", "ec2:*", "elasticloadbalancing:*", "rds:*", "s3:*",
        "iam:*", "kms:*", "logs:*", "autoscaling:*", "application-autoscaling:*",
        "cloudwatch:*", "secretsmanager:*", "dynamodb:*", "sts:AssumeRole"
      ]
      Resource = "*"
    }]
  })
}

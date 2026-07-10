###############################################################################
# lakekeeper-vending.tf  —  IAM for the EKS credential-vending isolation proof.
#
# Drop-in addition to deploy/eks/terraform (it reuses aws_s3_bucket.warehouse,
# module.eks OIDC locals, var.name_prefix, var.spark_namespace and local.tags
# from that stack). `terraform apply` in that directory picks it up.
#
# THE POINT OF THE PROOF (why these roles are shaped this way):
#   * The EXISTING aws_iam_role.spark (irsa.tf) grants the Spark driver+executor SA
#     read/write on the WHOLE warehouse bucket. We KEEP it, unchanged. It is the
#     "ambient" credential the executor pod could silently fall back to. If the
#     executor ever used it, cross-tenant access would SUCCEED and isolation would
#     collapse. The proof is that it does NOT — because the vended, prefix-scoped
#     credential is what S3FileIO uses on the executor.
#   * Lakekeeper mints the vended, downscoped credential by assuming a SEPARATE
#     role (aws_iam_role.lakekeeper_vending) and attaching a per-warehouse session
#     policy scoped to that tenant's key-prefix. Lakekeeper's own pod identity
#     (aws_iam_role.lakekeeper_catalog, via IRSA) is the only principal allowed to
#     assume the vending role.
#
# Net: executor pod HAS a full-bucket role (IRSA) yet is DENIED tenant_b, because
# the data path uses the vending role's tenant_a-scoped STS session. That denial,
# with a full-bucket ambient role present, is the load-bearing result.
###############################################################################

# --- (1) Lakekeeper catalog pod identity (IRSA) --------------------------------
data "aws_iam_policy_document" "lakekeeper_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [local.oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider}:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_provider}:sub"
      # SA `lakekeeper` in the spark namespace (see eks/lakekeeper.yaml).
      values   = ["system:serviceaccount:${var.spark_namespace}:lakekeeper"]
    }
  }
}

resource "aws_iam_role" "lakekeeper_catalog" {
  name               = "${var.name_prefix}-lakekeeper-catalog"
  description        = "IRSA: Lakekeeper catalog pod -> assume the vending role"
  assume_role_policy = data.aws_iam_policy_document.lakekeeper_assume.json
  tags               = local.tags
}

# The catalog pod's ONLY power is to assume the vending role. It has NO direct S3.
data "aws_iam_policy_document" "lakekeeper_can_assume_vending" {
  statement {
    sid       = "AssumeVendingRole"
    effect    = "Allow"
    actions   = ["sts:AssumeRole"]
    resources = [aws_iam_role.lakekeeper_vending.arn]
  }
}

resource "aws_iam_role_policy" "lakekeeper_assume_vending" {
  name   = "assume-vending"
  role   = aws_iam_role.lakekeeper_catalog.id
  policy = data.aws_iam_policy_document.lakekeeper_can_assume_vending.json
}

# --- (2) The vending role Lakekeeper assumes + DOWNSCOPES per warehouse ---------
# Its own policy is broad (whole bucket). Lakekeeper narrows each vend to one
# key-prefix via an AssumeRole session policy — so the ISSUED credential is
# tenant-scoped even though this role is not.
data "aws_iam_policy_document" "vending_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.lakekeeper_catalog.arn]
    }
  }
}

resource "aws_iam_role" "lakekeeper_vending" {
  name               = "${var.name_prefix}-lakekeeper-vending"
  description        = "Assumed by Lakekeeper; downscoped per-warehouse when vending"
  assume_role_policy = data.aws_iam_policy_document.vending_trust.json
  tags               = local.tags
  # Vended credentials are short-lived by construction (that is the point).
  max_session_duration = 3600
}

data "aws_iam_policy_document" "vending_bucket_rw" {
  statement {
    sid       = "ListWarehouseBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.warehouse.arn]
  }
  statement {
    sid    = "RWWarehouseObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
      "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.warehouse.arn}/*"]
  }
}

resource "aws_iam_role_policy" "vending_bucket_rw" {
  name   = "warehouse-rw"
  role   = aws_iam_role.lakekeeper_vending.id
  policy = data.aws_iam_policy_document.vending_bucket_rw.json
}

output "lakekeeper_catalog_role_arn" {
  value       = aws_iam_role.lakekeeper_catalog.arn
  description = "Annotate the `lakekeeper` ServiceAccount with this (eks.amazonaws.com/role-arn)."
}

output "lakekeeper_vending_role_arn" {
  value       = aws_iam_role.lakekeeper_vending.arn
  description = "Put this in each warehouse storage-profile's sts-role-arn (config/warehouse-*.aws.json)."
}

# --- (2026-07-09 fix) Lakekeeper catalog manages warehouse metadata directly ----
# Finding: Lakekeeper's aws-system-identity does warehouse-MANAGEMENT storage writes
# with the pod's base identity (not the vended role). So the trusted catalog needs
# warehouse S3 access. Tenant isolation is still enforced at the AGENT executor via
# vended, prefix-scoped creds (the load-bearing proof), NOT at Lakekeeper's identity.
resource "aws_iam_role_policy" "catalog_manage_s3" {
  name = "catalog-manage-s3"
  role = aws_iam_role.lakekeeper_catalog.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:ListBucket", "s3:GetBucketLocation"], Resource = aws_s3_bucket.warehouse.arn },
      { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"], Resource = "${aws_s3_bucket.warehouse.arn}/*" }
    ]
  })
}

###############################################################################
# IRSA: IAM Roles for Service Accounts. Each role trusts the cluster OIDC provider
# and is scoped to exactly one (namespace, serviceaccount). Annotate the matching
# Kubernetes SA with eks.amazonaws.com/role-arn = <role arn> (see outputs / README).
###############################################################################

locals {
  oidc_provider_arn = module.eks.oidc_provider_arn
  oidc_provider     = module.eks.oidc_provider # issuer host/path, no https://

  warehouse_arn = aws_s3_bucket.warehouse.arn
}

# ---- Shared S3 warehouse access (read/write all medallion prefixes) ----------
data "aws_iam_policy_document" "warehouse_rw" {
  statement {
    sid       = "ListWarehouseBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [local.warehouse_arn]
  }
  statement {
    sid    = "ReadWriteWarehouseObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${local.warehouse_arn}/*"]
  }
}

# =============================================================================
# (a) Spark driver + executor SA  ->  warehouse S3 read/write
# =============================================================================
data "aws_iam_policy_document" "spark_assume" {
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
      values   = ["system:serviceaccount:${var.spark_namespace}:${var.spark_service_account}"]
    }
  }
}

resource "aws_iam_role" "spark" {
  name               = "${var.name_prefix}-irsa-spark"
  description        = "IRSA: Spark driver/executor SA -> warehouse S3 RW"
  assume_role_policy = data.aws_iam_policy_document.spark_assume.json
  tags               = local.tags
}

resource "aws_iam_role_policy" "spark_warehouse" {
  name   = "warehouse-rw"
  role   = aws_iam_role.spark.id
  policy = data.aws_iam_policy_document.warehouse_rw.json
}

# =============================================================================
# (b) Hive Metastore SA  ->  warehouse S3 + read the metastore secret
#     (RDS connectivity is network-level via the SG; the password comes from
#      Secrets Manager, so HMS only needs GetSecretValue here.)
# =============================================================================
data "aws_iam_policy_document" "hms_assume" {
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
      values   = ["system:serviceaccount:${var.hms_namespace}:${var.hms_service_account}"]
    }
  }
}

data "aws_iam_policy_document" "hms_secret" {
  statement {
    sid       = "ReadMetastoreSecret"
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
    resources = [aws_secretsmanager_secret.metastore.arn]
  }
}

resource "aws_iam_role" "hms" {
  name               = "${var.name_prefix}-irsa-hms"
  description        = "IRSA: Hive Metastore SA -> warehouse S3 + metastore secret"
  assume_role_policy = data.aws_iam_policy_document.hms_assume.json
  tags               = local.tags
}

resource "aws_iam_role_policy" "hms_warehouse" {
  name   = "warehouse-rw"
  role   = aws_iam_role.hms.id
  policy = data.aws_iam_policy_document.warehouse_rw.json
}

resource "aws_iam_role_policy" "hms_secret" {
  name   = "metastore-secret-read"
  role   = aws_iam_role.hms.id
  policy = data.aws_iam_policy_document.hms_secret.json
}

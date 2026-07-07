# ---------------------------------------------------------------------------
# Instance profile: SSM Session Manager (admin shell, NO SSH) + the minimal
# EC2 permissions the instance needs to self-attach its dedicated data volume
# after an ASG replacement.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "connect" {
  name               = "${local.name}-instance-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
  tags               = { Name = "${local.name}-instance-role" }
}

# SSM Session Manager for shell access (no public SSH).
resource "aws_iam_role_policy_attachment" "ssm_core" {
  role       = aws_iam_role.connect.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# Self-attach the data volume on boot (scoped to this VPC's volumes by tag at runtime;
# AttachVolume/DescribeVolumes do not support fine-grained resource ARNs uniformly, so
# we scope by the project tag condition where supported and rely on the data volume's
# explicit id passed via user-data).
data "aws_iam_policy_document" "data_volume_attach" {
  statement {
    sid    = "DescribeForAttach"
    effect = "Allow"
    actions = [
      "ec2:DescribeVolumes",
      "ec2:DescribeTags",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "AttachDetachProjectVolumes"
    effect = "Allow"
    actions = [
      "ec2:AttachVolume",
      "ec2:DetachVolume",
    ]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/Project"
      values   = [var.project_name]
    }
  }
}

resource "aws_iam_role_policy" "data_volume_attach" {
  name   = "${local.name}-data-volume-attach"
  role   = aws_iam_role.connect.id
  policy = data.aws_iam_policy_document.data_volume_attach.json
}

resource "aws_iam_instance_profile" "connect" {
  name = "${local.name}-instance-profile"
  role = aws_iam_role.connect.name
}

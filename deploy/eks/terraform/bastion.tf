###############################################################################
# Optional operator access path: an SSM-managed bastion in a private subnet.
# No inbound rules, no SSH key — reach it with `aws ssm start-session` and port
# -forward to the private EKS API. Gated by var.create_bastion (default false).
# For team scale, prefer AWS Client VPN instead (see README, "Operator access").
###############################################################################

resource "aws_iam_role" "bastion" {
  count = var.create_bastion ? 1 : 0
  name  = "${var.name_prefix}-bastion"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "bastion_ssm" {
  count      = var.create_bastion ? 1 : 0
  role       = aws_iam_role.bastion[0].name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "bastion" {
  count = var.create_bastion ? 1 : 0
  name  = "${var.name_prefix}-bastion"
  role  = aws_iam_role.bastion[0].name
  tags  = local.tags
}

resource "aws_security_group" "bastion" {
  count       = var.create_bastion ? 1 : 0
  name        = "${var.name_prefix}-bastion"
  description = "SSM bastion - egress only (SSM is outbound)"
  vpc_id      = module.vpc.vpc_id
  tags        = merge(local.tags, { Name = "${var.name_prefix}-bastion" })
}

resource "aws_vpc_security_group_egress_rule" "bastion_all_out" {
  count             = var.create_bastion ? 1 : 0
  security_group_id = aws_security_group.bastion[0].id
  description       = "Allow all egress (SSM + API)"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

# Let the bastion reach the private Kubernetes API (443) on the cluster SG.
resource "aws_vpc_security_group_ingress_rule" "cluster_api_from_bastion" {
  count                        = var.create_bastion ? 1 : 0
  security_group_id            = module.eks.cluster_security_group_id
  description                  = "HTTPS to private EKS API from SSM bastion"
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
  referenced_security_group_id = aws_security_group.bastion[0].id
}

resource "aws_instance" "bastion" {
  count                  = var.create_bastion ? 1 : 0
  ami                    = data.aws_ssm_parameter.al2023[0].value
  instance_type          = var.bastion_instance_type
  subnet_id              = module.vpc.private_subnets[0]
  iam_instance_profile   = aws_iam_instance_profile.bastion[0].name
  vpc_security_group_ids = [aws_security_group.bastion[0].id]

  metadata_options {
    http_tokens   = "required"
    http_endpoint = "enabled"
  }

  root_block_device {
    encrypted = true
  }

  tags = merge(local.tags, { Name = "${var.name_prefix}-bastion" })
}

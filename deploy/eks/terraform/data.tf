data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

# Guard against applying into the wrong account. Disabled when account_id == "".
resource "null_resource" "account_guard" {
  count = var.account_id == "" ? 0 : 1

  lifecycle {
    precondition {
      condition     = data.aws_caller_identity.current.account_id == var.account_id
      error_message = "Caller account ${data.aws_caller_identity.current.account_id} != expected ${var.account_id}. Check the ${var.aws_profile} profile."
    }
  }
}

# Latest Amazon Linux 2023 AMI for the optional SSM bastion.
data "aws_ssm_parameter" "al2023" {
  count = var.create_bastion ? 1 : 0
  name  = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

# -----------------------------------------------------------------------------
# REUSE-AN-EXISTING-VPC alternative (documented, not active).
#
# The default of this stack is a FRESH VPC (see vpc.tf). If the team would rather
# land this stack inside an existing VPC (e.g. the deploy/aws B1 network), delete
# vpc.tf, uncomment the data sources below, and point the EKS module / RDS subnet
# group at data.aws_subnets.private.ids instead of module.vpc.* :
#
# data "aws_vpc" "existing" {
#   tags = { Name = "ssa-shared" }   # or: id = "vpc-xxxxxxxx"
# }
# data "aws_subnets" "private" {
#   filter { name = "vpc-id" values = [data.aws_vpc.existing.id] }
#   tags = { "kubernetes.io/role/internal-elb" = "1" }
# }
# data "aws_subnets" "public" {
#   filter { name = "vpc-id" values = [data.aws_vpc.existing.id] }
#   tags = { "kubernetes.io/role/elb" = "1" }
# }
# -----------------------------------------------------------------------------

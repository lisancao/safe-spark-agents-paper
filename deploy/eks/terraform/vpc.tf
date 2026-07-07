# Fresh VPC: 2 private + 2 public subnets across 2 AZs, IGW, single NAT (default).
# To reuse an existing VPC instead, see the documented data sources in data.tf.
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.13"

  name = "${var.name_prefix}-vpc"
  cidr = var.vpc_cidr

  azs             = local.azs
  private_subnets = var.private_subnet_cidrs
  public_subnets  = var.public_subnet_cidrs

  enable_nat_gateway     = true
  single_nat_gateway     = var.single_nat_gateway
  one_nat_gateway_per_az = !var.single_nat_gateway

  enable_dns_hostnames = true
  enable_dns_support   = true

  # EKS subnet discovery tags: load balancers land in the right tier.
  public_subnet_tags = {
    "kubernetes.io/role/elb" = "1"
  }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = "1"
    # Cluster-owned discovery tag is added by the EKS module via the cluster name.
  }

  tags = local.tags
}

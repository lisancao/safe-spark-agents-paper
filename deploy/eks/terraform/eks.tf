module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.24"

  cluster_name    = local.cluster_name
  cluster_version = var.cluster_version

  # Private API by default; flip cluster_endpoint_public_access + supply an allowlist
  # of CIDRs to expose it. OIDC/IRSA is enabled so service accounts can assume IAM roles.
  cluster_endpoint_private_access      = true
  cluster_endpoint_public_access       = var.cluster_endpoint_public_access
  cluster_endpoint_public_access_cidrs = var.cluster_endpoint_public_access_cidrs
  enable_irsa                          = true

  vpc_id                   = module.vpc.vpc_id
  subnet_ids               = module.vpc.private_subnets
  control_plane_subnet_ids = module.vpc.private_subnets

  # Grant the identity running terraform cluster-admin via an EKS access entry,
  # so the operator can immediately kubectl after apply.
  enable_cluster_creator_admin_permissions = true
  authentication_mode                      = "API_AND_CONFIG_MAP"

  cluster_addons = {
    coredns                = {}
    kube-proxy             = {}
    vpc-cni                = { before_compute = true }
    eks-pod-identity-agent = {}
  }

  eks_managed_node_groups = {
    # Small on-demand pool for system addons, the Hive Metastore, and the
    # Spark Connect server / driver pods. Untainted so system DaemonSets land here.
    system = {
      ami_type       = "AL2023_x86_64_STANDARD"
      instance_types = var.system_node_instance_types
      capacity_type  = "ON_DEMAND"

      min_size     = var.system_node_min_size
      max_size     = var.system_node_max_size
      desired_size = var.system_node_desired_size

      labels = {
        "workload" = "system"
      }
    }

    # Executor pool. Tainted so ONLY pods that tolerate spark-role=executor land
    # here; scales from min (0 ok) to max. Spark executor pod templates must set the
    # matching nodeSelector (workload=spark-executor) + toleration.
    executor = {
      ami_type       = "AL2023_x86_64_STANDARD"
      instance_types = var.executor_node_instance_types
      capacity_type  = var.executor_capacity_type

      min_size     = var.executor_node_min_size
      max_size     = var.executor_node_max_size
      desired_size = var.executor_node_desired_size

      labels = {
        "workload" = "spark-executor"
      }

      taints = {
        dedicated = {
          key    = "spark-role"
          value  = "executor"
          effect = "NO_SCHEDULE"
        }
      }
    }
  }

  tags = local.tags
}

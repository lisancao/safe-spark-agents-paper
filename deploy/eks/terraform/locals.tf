locals {
  cluster_name = "${var.name_prefix}-eks"

  # Spread across the first az_count AZs in the region.
  azs = slice(data.aws_availability_zones.available.names, 0, var.az_count)

  warehouse_bucket_name = coalesce(
    var.warehouse_bucket_name != "" ? var.warehouse_bucket_name : null,
    "${var.name_prefix}-warehouse-${data.aws_caller_identity.current.account_id}"
  )

  tags = merge(
    {
      Project     = "safe-spark-agents"
      Stack       = "eks-spark"
      Environment = var.environment
      ManagedBy   = "terraform"
      TFRoot      = "deploy/eks/terraform"
    },
    var.tags,
  )
}

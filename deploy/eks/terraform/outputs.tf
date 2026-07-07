###############################################################################
# Cluster
###############################################################################

output "cluster_name" {
  description = "EKS cluster name."
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "EKS API server endpoint (private unless public access enabled)."
  value       = module.eks.cluster_endpoint
}

output "cluster_version" {
  description = "Kubernetes control-plane version."
  value       = module.eks.cluster_version
}

output "cluster_certificate_authority_data" {
  description = "Base64 cluster CA (for kubeconfig)."
  value       = module.eks.cluster_certificate_authority_data
}

output "oidc_provider_arn" {
  description = "IAM OIDC provider ARN backing IRSA."
  value       = module.eks.oidc_provider_arn
}

output "node_security_group_id" {
  description = "Security group attached to managed node groups."
  value       = module.eks.node_security_group_id
}

output "update_kubeconfig_command" {
  description = "Run this to configure kubectl against the cluster."
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name} --profile ${var.aws_profile}"
}

###############################################################################
# Networking
###############################################################################

output "vpc_id" {
  value       = module.vpc.vpc_id
  description = "VPC ID."
}

output "private_subnet_ids" {
  value       = module.vpc.private_subnets
  description = "Private subnet IDs (nodes, RDS, API ENIs)."
}

output "public_subnet_ids" {
  value       = module.vpc.public_subnets
  description = "Public subnet IDs (NAT, public LBs)."
}

###############################################################################
# IRSA role <-> service account bindings  (annotate each SA with its role ARN)
###############################################################################

output "irsa_spark" {
  description = "Spark driver/executor IRSA role and the SA it binds to."
  value = {
    role_arn        = aws_iam_role.spark.arn
    namespace       = var.spark_namespace
    service_account = var.spark_service_account
    annotation      = "eks.amazonaws.com/role-arn=${aws_iam_role.spark.arn}"
  }
}

output "irsa_hms" {
  description = "Hive Metastore IRSA role and the SA it binds to."
  value = {
    role_arn        = aws_iam_role.hms.arn
    namespace       = var.hms_namespace
    service_account = var.hms_service_account
    annotation      = "eks.amazonaws.com/role-arn=${aws_iam_role.hms.arn}"
  }
}

###############################################################################
# S3 warehouse
###############################################################################

output "warehouse_bucket" {
  description = "Delta warehouse bucket name."
  value       = aws_s3_bucket.warehouse.id
}

output "warehouse_bucket_arn" {
  value       = aws_s3_bucket.warehouse.arn
  description = "Warehouse bucket ARN."
}

output "warehouse_prefixes" {
  description = "Medallion prefixes (s3:// URIs)."
  value       = [for p in var.warehouse_prefixes : "s3://${aws_s3_bucket.warehouse.id}/${p}/"]
}

###############################################################################
# RDS metastore  (NO password is output — read it from Secrets Manager)
###############################################################################

output "metastore_endpoint" {
  description = "RDS endpoint host:port for the Hive Metastore DB."
  value       = aws_db_instance.metastore.endpoint
}

output "metastore_db_name" {
  value       = aws_db_instance.metastore.db_name
  description = "Metastore database name."
}

output "metastore_secret_arn" {
  description = "Secrets Manager ARN holding the metastore connection bundle (HMS reads this via IRSA)."
  value       = aws_secretsmanager_secret.metastore.arn
}

output "metastore_secret_name" {
  value       = aws_secretsmanager_secret.metastore.name
  description = "Secrets Manager secret name for the metastore connection."
}

###############################################################################
# Optional bastion
###############################################################################

output "bastion_instance_id" {
  description = "SSM bastion instance ID (null when create_bastion=false). Connect: aws ssm start-session --target <id>."
  value       = var.create_bastion ? aws_instance.bastion[0].id : null
}

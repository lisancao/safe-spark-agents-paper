###############################################################################
# Provider / identity
###############################################################################

variable "aws_profile" {
  description = "Named AWS CLI/SDK profile used for plan/apply (shared-config credentials)."
  type        = string
  default     = "ssa-deploy"
}

variable "region" {
  description = "AWS region for the whole stack."
  type        = string
  default     = "us-east-1"
}

variable "account_id" {
  description = "Expected AWS account ID. Asserted against the caller identity to prevent a wrong-account apply. Empty string disables the check."
  type        = string
  default     = ""
}

variable "name_prefix" {
  description = "Prefix for all resource names and the EKS cluster name."
  type        = string
  default     = "ssa-spark"
}

variable "environment" {
  description = "Environment tag (prod target for the k8s-native Spark stack)."
  type        = string
  default     = "prod"
}

variable "tags" {
  description = "Extra tags merged onto every resource."
  type        = map(string)
  default     = {}
}

###############################################################################
# VPC
###############################################################################

variable "vpc_cidr" {
  description = "CIDR block for the fresh VPC."
  type        = string
  default     = "10.40.0.0/16"
}

variable "private_subnet_cidrs" {
  description = "Two private subnet CIDRs (one per AZ). Hosts EKS nodes, RDS, and the API ENIs."
  type        = list(string)
  default     = ["10.40.0.0/20", "10.40.16.0/20"]
}

variable "public_subnet_cidrs" {
  description = "Two public subnet CIDRs (one per AZ). Hosts the NAT gateway and (optionally) public LBs."
  type        = list(string)
  default     = ["10.40.128.0/20", "10.40.144.0/20"]
}

variable "az_count" {
  description = "Number of AZs to spread across. Must match the subnet list lengths (2)."
  type        = number
  default     = 2
}

variable "single_nat_gateway" {
  description = "Use a single NAT gateway (cheaper) vs one-per-AZ (more available)."
  type        = bool
  default     = true
}

###############################################################################
# EKS
###############################################################################

variable "cluster_version" {
  description = "EKS Kubernetes control-plane version."
  type        = string
  default     = "1.31"
}

variable "cluster_endpoint_public_access" {
  description = "Expose the Kubernetes API publicly. Default false (private API; reach it via VPN/SSM bastion — see README)."
  type        = bool
  default     = false
}

variable "cluster_endpoint_public_access_cidrs" {
  description = "When public access is enabled, the allowlisted source CIDRs. Enforced at plan time (see validations.tf): if public access is on this must be non-empty and must not contain 0.0.0.0/0 or ::/0."
  type        = list(string)
  default     = []
}

###############################################################################
# Node groups
###############################################################################

variable "system_node_instance_types" {
  description = "Instance types for the small on-demand system pool (HMS, Connect driver, system addons)."
  type        = list(string)
  default     = ["m6i.large"]
}

variable "system_node_min_size" {
  type    = number
  default = 2
}

variable "system_node_max_size" {
  type    = number
  default = 4
}

variable "system_node_desired_size" {
  type    = number
  default = 2
}

variable "executor_node_instance_types" {
  description = "Instance types for the Spark executor pool."
  type        = list(string)
  default     = ["m6i.2xlarge"]
}

variable "executor_capacity_type" {
  description = "ON_DEMAND or SPOT for the executor pool."
  type        = string
  default     = "ON_DEMAND"

  validation {
    condition     = contains(["ON_DEMAND", "SPOT"], var.executor_capacity_type)
    error_message = "executor_capacity_type must be ON_DEMAND or SPOT."
  }
}

variable "executor_node_min_size" {
  type    = number
  default = 0
}

variable "executor_node_max_size" {
  type    = number
  default = 10
}

variable "executor_node_desired_size" {
  type    = number
  default = 2
}

###############################################################################
# IRSA service-account bindings
###############################################################################

variable "spark_namespace" {
  description = "Kubernetes namespace for the Spark Connect server, driver, and executor pods."
  type        = string
  default     = "spark"
}

variable "spark_service_account" {
  description = "Service account used by Spark driver + executor pods (bound to the warehouse-RW IRSA role)."
  type        = string
  default     = "spark"
}

variable "hms_namespace" {
  description = "Kubernetes namespace for the Hive Metastore."
  type        = string
  default     = "hive-metastore"
}

variable "hms_service_account" {
  description = "Service account used by the Hive Metastore (bound to warehouse-S3 + metastore-secret IRSA role)."
  type        = string
  default     = "hive-metastore"
}

###############################################################################
# S3 warehouse
###############################################################################

variable "warehouse_bucket_name" {
  description = "Delta warehouse bucket name. Empty => derived as <name_prefix>-warehouse-<account_id>."
  type        = string
  default     = ""
}

variable "warehouse_prefixes" {
  description = "Medallion prefixes created as keys in the warehouse bucket."
  type        = list(string)
  default     = ["bronze", "silver", "gold"]
}

variable "force_destroy_warehouse" {
  description = "Allow terraform destroy to delete a non-empty warehouse bucket. Keep false in prod."
  type        = bool
  default     = false
}

###############################################################################
# RDS PostgreSQL (Hive Metastore backing DB)
###############################################################################

variable "metastore_db_name" {
  description = "Initial database created for the Hive Metastore schema."
  type        = string
  default     = "metastore"
}

variable "metastore_db_username" {
  description = "Master username for the metastore RDS instance."
  type        = string
  default     = "hive"
}

variable "rds_engine_version" {
  description = "PostgreSQL engine version."
  type        = string
  default     = "16.4"
}

variable "rds_instance_class" {
  description = "RDS instance class for the metastore DB."
  type        = string
  default     = "db.t3.medium"
}

variable "rds_allocated_storage" {
  description = "Initial storage (GiB)."
  type        = number
  default     = 50
}

variable "rds_max_allocated_storage" {
  description = "Storage autoscaling ceiling (GiB)."
  type        = number
  default     = 200
}

variable "rds_multi_az" {
  description = "Run the metastore DB Multi-AZ (recommended for prod durability)."
  type        = bool
  default     = true
}

variable "rds_backup_retention_days" {
  description = "Automated backup retention in days."
  type        = number
  default     = 7
}

variable "rds_deletion_protection" {
  description = "Block accidental RDS deletion. Keep true in prod."
  type        = bool
  default     = true
}

variable "rds_skip_final_snapshot" {
  description = "Skip the final snapshot on destroy. Keep false in prod."
  type        = bool
  default     = false
}

###############################################################################
# Operator access (optional SSM bastion)
###############################################################################

variable "create_bastion" {
  description = "Create a small SSM-managed bastion (no inbound, no SSH key) in a private subnet to reach the private API. See README for the Client-VPN alternative."
  type        = bool
  default     = false
}

variable "bastion_instance_type" {
  description = "Instance type for the optional SSM bastion."
  type        = string
  default     = "t3.small"
}

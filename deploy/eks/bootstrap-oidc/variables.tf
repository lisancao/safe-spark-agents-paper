variable "region" {
  description = "AWS region (match the EKS stack; the existing backend example uses us-east-1)."
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "Admin profile for the ONE-TIME local apply. Empty string uses the default credential chain."
  type        = string
  default     = ""
}

variable "github_org" {
  description = "GitHub org/owner of the repo CI runs in."
  type        = string
}

variable "github_repo" {
  description = "GitHub repo name (without the org)."
  type        = string
}

variable "apply_environment" {
  description = "GitHub Environment name that gates terraform apply (must have required reviewers)."
  type        = string
  default     = "eks-apply"
}

variable "name_prefix" {
  description = "Prefix for the created IAM role names."
  type        = string
  default     = "ssa"
}

variable "tfstate_bucket_name" {
  description = "Globally-unique S3 bucket name for remote Terraform state (you choose it; no account id needed)."
  type        = string
}

variable "lock_table_name" {
  description = "DynamoDB table for state locking (the EKS backend example expects ssa-tf-locks)."
  type        = string
  default     = "ssa-tf-locks"
}

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }

  # -------------------------------------------------------------------------
  # REMOTE STATE — TODO before any team/shared use.
  # This config defaults to LOCAL state (terraform.tfstate on disk) so the
  # very first `init`/`plan`/`apply` works with zero prerequisites. State for
  # a durable, multi-user server SHOULD live in S3 with DynamoDB locking.
  # To switch: create the bucket+table once, then uncomment and `terraform init
  # -migrate-state`.
  #
  # backend "s3" {
  #   bucket         = "CHANGE-ME-spark-connect-tfstate"
  #   key            = "spark-connect/aws/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "CHANGE-ME-spark-connect-tflock"
  #   encrypt        = true
  #   profile        = "personal"
  # }
}

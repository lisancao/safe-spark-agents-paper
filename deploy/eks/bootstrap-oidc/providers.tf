provider "aws" {
  region  = var.region
  profile = var.aws_profile # your admin profile for the one-time local apply; "" to use the default chain

  default_tags {
    tags = {
      Project   = "safe-spark-agents"
      Component = "bootstrap-oidc"
      ManagedBy = "terraform"
    }
  }
}

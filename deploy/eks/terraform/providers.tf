provider "aws" {
  region  = var.region
  profile = var.aws_profile

  default_tags {
    tags = local.tags
  }
}

# random is used for the RDS master password (written only to Secrets Manager +
# remote state, never to a committed file). No provider config required.

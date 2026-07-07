provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile

  default_tags {
    tags = merge(
      {
        Project   = var.project_name
        ManagedBy = "terraform"
        Component = "spark-connect-server"
      },
      var.tags,
    )
  }
}

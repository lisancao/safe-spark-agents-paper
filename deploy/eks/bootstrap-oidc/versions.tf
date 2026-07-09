terraform {
  required_version = ">= 1.7, < 2.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.61"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }

  # LOCAL state on purpose: this module CREATES the remote backend the EKS stack
  # uses, so it cannot itself live in that backend. Applied once, by hand.
}

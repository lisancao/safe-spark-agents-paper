# Plan-time guardrails. These fail `terraform plan` (before any apply) on misconfig.
# Implemented as preconditions so they can reference multiple variables on Terraform
# >= 1.7 without requiring cross-variable `validation` (1.9+).
resource "null_resource" "validations" {
  lifecycle {
    # BLOCKING: never let the EKS API be opened to the world. If public access is
    # enabled the allowlist must be non-empty and must not contain a /0.
    precondition {
      condition = !var.cluster_endpoint_public_access || (
        length(var.cluster_endpoint_public_access_cidrs) > 0 &&
        !contains(var.cluster_endpoint_public_access_cidrs, "0.0.0.0/0") &&
        !contains(var.cluster_endpoint_public_access_cidrs, "::/0")
      )
      error_message = "When cluster_endpoint_public_access = true, cluster_endpoint_public_access_cidrs must be non-empty and must NOT contain 0.0.0.0/0 or ::/0. Keep the API private, or supply a tight operator allowlist."
    }

    # Subnet CIDR lists must match the AZ count, for both tiers.
    precondition {
      condition     = length(var.private_subnet_cidrs) == var.az_count
      error_message = "private_subnet_cidrs must have exactly az_count (${var.az_count}) entries; got ${length(var.private_subnet_cidrs)}."
    }
    precondition {
      condition     = length(var.public_subnet_cidrs) == var.az_count
      error_message = "public_subnet_cidrs must have exactly az_count (${var.az_count}) entries; got ${length(var.public_subnet_cidrs)}."
    }

    # Node group sizing must be coherent: min <= desired <= max.
    precondition {
      condition = (
        var.system_node_min_size <= var.system_node_desired_size &&
        var.system_node_desired_size <= var.system_node_max_size
      )
      error_message = "system node sizing must satisfy min <= desired <= max (min=${var.system_node_min_size}, desired=${var.system_node_desired_size}, max=${var.system_node_max_size})."
    }
    precondition {
      condition = (
        var.executor_node_min_size <= var.executor_node_desired_size &&
        var.executor_node_desired_size <= var.executor_node_max_size
      )
      error_message = "executor node sizing must satisfy min <= desired <= max (min=${var.executor_node_min_size}, desired=${var.executor_node_desired_size}, max=${var.executor_node_max_size})."
    }
  }
}

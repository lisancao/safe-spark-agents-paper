# Feed these into the repo's GitHub Actions Variables (Settings -> Secrets and
# variables -> Actions -> Variables). ARNs contain the account id, so they are
# repo variables, NOT committed into any workflow file.

output "plan_role_arn" {
  description = "Set as repo variable AWS_TF_PLAN_ROLE_ARN."
  value       = aws_iam_role.gha_plan.arn
}

output "apply_role_arn" {
  description = "Set as repo variable AWS_TF_APPLY_ROLE_ARN."
  value       = aws_iam_role.gha_apply.arn
}

output "oidc_provider_arn" {
  description = "The GitHub OIDC provider ARN (for reference)."
  value       = aws_iam_openid_connect_provider.github.arn
}

output "tfstate_bucket" {
  description = "Set as repo variable TFSTATE_BUCKET."
  value       = aws_s3_bucket.tfstate.id
}

output "tflock_table" {
  description = "Set as repo variable TFLOCK_TABLE."
  value       = aws_dynamodb_table.tflock.name
}

output "vpc_id" {
  description = "ID of the fresh VPC."
  value       = aws_vpc.main.id
}

output "public_subnet_ids" {
  description = "Public subnet IDs (one per AZ)."
  value       = [for s in aws_subnet.public : s.id]
}

output "private_subnet_ids" {
  description = "Private subnet IDs (one per AZ); the server runs in private[data_volume_az_index]."
  value       = [for s in aws_subnet.private : s.id]
}

output "connect_security_group_id" {
  description = "Security group of the Connect server (15002 from VPN CIDR only)."
  value       = aws_security_group.connect.id
}

output "data_volume_id" {
  description = "ID of the durable EBS data volume mounted at /srv/spark."
  value       = aws_ebs_volume.data.id
}

output "autoscaling_group_name" {
  description = "Name of the single-instance ASG that keeps the server alive."
  value       = aws_autoscaling_group.connect.name
}

# --- NLB ---------------------------------------------------------------------
output "nlb_dns_name" {
  description = "Internal NLB DNS name. Point clients (over VPN) at sc://<this>:<listener_port>."
  value       = aws_lb.connect.dns_name
}

output "nlb_listener_port" {
  description = "TLS listener port on the NLB."
  value       = var.nlb_listener_port
}

output "acm_cert_arn" {
  description = "ACM cert ARN used by the NLB TLS listener (created or existing)."
  value       = local.acm_cert_arn
}

output "acm_dns_validation_records" {
  description = <<-EOT
    DNS validation records to create MANUALLY when create_acm_cert = true and no
    route53_zone_id was supplied. Empty otherwise.
  EOT
  value = var.create_acm_cert ? [
    for dvo in aws_acm_certificate.nlb[0].domain_validation_options : {
      name  = dvo.resource_record_name
      type  = dvo.resource_record_type
      value = dvo.resource_record_value
    }
  ] : []
}

# --- Client VPN --------------------------------------------------------------
output "client_vpn_endpoint_id" {
  description = "Client VPN endpoint ID."
  value       = aws_ec2_client_vpn_endpoint.main.id
}

output "client_vpn_network_association_id" {
  description = "The client-config (subnet) association binding the VPN endpoint to the server's private subnet."
  value       = aws_ec2_client_vpn_network_association.main.id
}

output "client_vpn_config_download_command" {
  description = "Run this to export the base .ovpn client config (then append client cert+key — see README)."
  value       = "aws ec2 export-client-vpn-client-configuration --profile ${var.aws_profile} --region ${var.aws_region} --client-vpn-endpoint-id ${aws_ec2_client_vpn_endpoint.main.id} --output text > ${local.name}-client.ovpn"
}

# --- Admin shell -------------------------------------------------------------
output "ssm_start_session_hint" {
  description = "How to open an admin shell via SSM Session Manager (no SSH). Resolve the instance id from the ASG first."
  value       = "aws ssm start-session --profile ${var.aws_profile} --region ${var.aws_region} --target <instance-id-from-ASG ${aws_autoscaling_group.connect.name}>"
}

# ---------------------------------------------------------------------------
# AWS Client VPN (mutual / certificate authentication) — the access path for
# the 3 named users + admin. Server + client-root certs are imported to ACM
# out-of-band (easy-rsa); see README "Onboard a Client VPN user".
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "vpn" {
  name              = "/aws/clientvpn/${local.name}"
  retention_in_days = 30
  tags              = { Name = "${local.name}-vpn-logs" }
}

resource "aws_cloudwatch_log_stream" "vpn" {
  name           = "connection-log"
  log_group_name = aws_cloudwatch_log_group.vpn.name
}

resource "aws_ec2_client_vpn_endpoint" "main" {
  description            = "${local.name} mutual-auth client VPN"
  server_certificate_arn = var.vpn_server_cert_arn
  client_cidr_block      = var.vpn_client_cidr
  split_tunnel           = var.vpn_split_tunnel
  vpc_id                 = aws_vpc.main.id
  security_group_ids     = [aws_security_group.connect.id]

  authentication_options {
    type                       = "certificate-authentication"
    root_certificate_chain_arn = var.vpn_client_root_cert_arn
  }

  connection_log_options {
    enabled               = true
    cloudwatch_log_group  = aws_cloudwatch_log_group.vpn.name
    cloudwatch_log_stream = aws_cloudwatch_log_stream.vpn.name
  }

  # Resolve names inside the VPC (e.g. the NLB DNS) over the tunnel.
  dns_servers = [cidrhost(var.vpc_cidr, 2)]

  tags = { Name = "${local.name}-clientvpn" }
}

# Associate the endpoint with the private subnet the server is pinned to.
# (One association is enough for a single-AZ server; add the other subnet for HA.)
resource "aws_ec2_client_vpn_network_association" "main" {
  client_vpn_endpoint_id = aws_ec2_client_vpn_endpoint.main.id
  subnet_id              = aws_subnet.private[var.data_volume_az_index].id
}

# Authorize connected clients to reach the whole VPC CIDR.
resource "aws_ec2_client_vpn_authorization_rule" "vpc" {
  client_vpn_endpoint_id = aws_ec2_client_vpn_endpoint.main.id
  target_network_cidr    = var.vpc_cidr
  authorize_all_groups   = true
  description            = "Allow VPN clients to reach the VPC"
}

# ---------------------------------------------------------------------------
# Security groups.
#   - Connect server: TCP 15002 ONLY from the Client VPN client CIDR (no public).
#     The NLB health check + data path is allowed from the NLB SECURITY GROUP
#     (not the whole VPC CIDR), so the target port is reachable only from the NLB
#     and from VPN clients — nothing else in the VPC. Still no public 15002.
#   - NLB: internal-only; accepts the TLS listener port from the VPN client CIDR.
# ---------------------------------------------------------------------------

resource "aws_security_group" "connect" {
  name        = "${local.name}-connect-sg"
  description = "Spark Connect server: gRPC 15002 from Client VPN only; no public ingress."
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${local.name}-connect-sg" }
}

# Direct gRPC from VPN clients — ONLY in non-proxy mode (enable_auth_proxy = false).
# In Option A (enable_auth_proxy = true), on-box Envoy is the sole path to Spark Connect:
# a direct client->15002 hit would bypass Envoy's mTLS identity check and could spoof
# user_id, so this rule is absent and the only ingress is from the NLB SG (Envoy port).
resource "aws_security_group_rule" "connect_grpc_from_vpn" {
  count             = var.enable_auth_proxy ? 0 : 1
  type              = "ingress"
  description       = "gRPC from Client VPN clients (non-proxy mode only)"
  security_group_id = aws_security_group.connect.id
  protocol          = "tcp"
  from_port         = var.connect_grpc_port
  to_port           = var.connect_grpc_port
  cidr_blocks       = [var.vpn_client_cidr]
}

# NLB health checks + NLB->target data path, scoped to the NLB SG only (not VPC-wide).
resource "aws_security_group_rule" "connect_target_from_nlb" {
  type                     = "ingress"
  description              = "NLB health check + data path to target port from the NLB SG only"
  security_group_id        = aws_security_group.connect.id
  protocol                 = "tcp"
  from_port                = local.target_port
  to_port                  = local.target_port
  source_security_group_id = aws_security_group.nlb.id
}

resource "aws_security_group_rule" "connect_egress_all" {
  type              = "egress"
  description       = "All egress (NAT for package/Spark/JDK downloads, SSM, etc.)"
  security_group_id = aws_security_group.connect.id
  protocol          = "-1"
  from_port         = 0
  to_port           = 0
  cidr_blocks       = ["0.0.0.0/0"]
}

# NLB security group (NLBs support SGs as of 2023).
resource "aws_security_group" "nlb" {
  name        = "${local.name}-nlb-sg"
  description = "Internal NLB: TLS listener from Client VPN clients only."
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${local.name}-nlb-sg" }
}

resource "aws_security_group_rule" "nlb_listener_from_vpn" {
  type              = "ingress"
  description       = "TLS listener from Client VPN clients"
  security_group_id = aws_security_group.nlb.id
  protocol          = "tcp"
  from_port         = var.nlb_listener_port
  to_port           = var.nlb_listener_port
  cidr_blocks       = [var.vpn_client_cidr]
}

resource "aws_security_group_rule" "nlb_egress_to_targets" {
  type                     = "egress"
  description              = "NLB to Connect targets"
  security_group_id        = aws_security_group.nlb.id
  protocol                 = "tcp"
  from_port                = local.target_port
  to_port                  = local.target_port
  source_security_group_id = aws_security_group.connect.id
}

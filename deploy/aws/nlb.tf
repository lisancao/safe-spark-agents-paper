# ---------------------------------------------------------------------------
# Internal Network Load Balancer fronting the Connect server. Two listener modes,
# selected by var.enable_auth_proxy:
#
#   enable_auth_proxy = false (default — no on-box proxy yet):
#     TLS listener TERMINATES TLS at the NLB with the ACM cert and forwards
#     plaintext TCP to the raw gRPC port. Spark Connect with use_ssl=true is
#     gRPC/HTTP2, so the listener advertises ALPN "HTTP2Preferred".
#
#   enable_auth_proxy = true (OPTION A — on-box Envoy auth proxy, separate PR):
#     PLAIN TCP PASSTHROUGH to auth_proxy_port. The NLB does NOT terminate TLS;
#     it hands the raw TLS bytes (incl. the client cert) to Envoy on the box, which
#     terminates TLS + client mTLS and speaks h2 to the Connect server. No ACM here.
#
#   - Internal scheme: reachable only from inside the VPC / over Client VPN.
# ---------------------------------------------------------------------------

resource "aws_lb" "connect" {
  name_prefix        = "spkc-"
  internal           = true
  load_balancer_type = "network"
  security_groups    = [aws_security_group.nlb.id]

  # Place the NLB in the private subnets (internal).
  subnets = [for s in aws_subnet.private : s.id]

  enable_cross_zone_load_balancing = true

  tags = { Name = "${local.name}-nlb" }
}

resource "aws_lb_target_group" "connect" {
  name_prefix = "spkc-"
  port        = local.target_port
  protocol    = "TCP"
  vpc_id      = aws_vpc.main.id
  target_type = "instance"

  # gRPC over HTTP/2 is opaque to a TCP health check; a TCP connect check is the
  # right liveness signal for a Connect server behind a TCP/TLS NLB.
  health_check {
    protocol            = "TCP"
    port                = "traffic-port"
    healthy_threshold   = 2
    unhealthy_threshold = 2
    interval            = 30
  }

  # Connection-oriented gRPC streams benefit from deregistration drain.
  deregistration_delay = 60

  tags = { Name = "${local.name}-tg" }

  lifecycle {
    create_before_destroy = true
  }
}

# Mode: TLS termination at the NLB (enable_auth_proxy = false).
resource "aws_lb_listener" "tls" {
  count             = var.enable_auth_proxy ? 0 : 1
  load_balancer_arn = aws_lb.connect.arn
  port              = var.nlb_listener_port
  protocol          = "TLS"
  certificate_arn   = local.acm_cert_arn
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"

  # Spark Connect (use_ssl=true) is gRPC over HTTP/2 — negotiate h2 via ALPN so the
  # TLS-terminating NLB forwards an HTTP/2 stream rather than defaulting to HTTP/1.1.
  alpn_policy = "HTTP2Preferred"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.connect.arn
  }

  # A TLS-terminating listener needs a VALIDATED cert. We only auto-validate when a
  # route53_zone_id is supplied; otherwise require bring-your-own (existing) cert so
  # apply never hangs on an un-validated, freshly-created cert.
  lifecycle {
    precondition {
      condition     = !(var.create_acm_cert && var.route53_zone_id == "")
      error_message = "TLS mode (enable_auth_proxy=false) with create_acm_cert=true requires route53_zone_id for DNS auto-validation. Otherwise set create_acm_cert=false and pass a pre-validated existing_acm_cert_arn."
    }
  }
}

# Mode: plain TCP passthrough to the on-box Envoy auth proxy (enable_auth_proxy = true).
# No ACM/TLS at the NLB — the client cert must reach Envoy for mTLS termination.
resource "aws_lb_listener" "passthrough" {
  count             = var.enable_auth_proxy ? 1 : 0
  load_balancer_arn = aws_lb.connect.arn
  port              = var.nlb_listener_port
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.connect.arn
  }
}

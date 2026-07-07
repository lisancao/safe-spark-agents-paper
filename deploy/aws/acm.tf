# ---------------------------------------------------------------------------
# ACM cert for the NLB TLS listener. Two modes (var.create_acm_cert):
#   false -> use var.existing_acm_cert_arn (bring your own)
#   true  -> create a cert for var.domain_name, validated via DNS.
#            If var.route53_zone_id is set, the validation records (and a DNS
#            alias to the NLB) are created automatically; otherwise the required
#            validation records are emitted as outputs for manual creation.
# ---------------------------------------------------------------------------

resource "aws_acm_certificate" "nlb" {
  count             = var.create_acm_cert ? 1 : 0
  domain_name       = var.domain_name
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }

  tags = { Name = "${local.name}-nlb-cert" }
}

# Auto-create DNS validation records when a hosted zone is provided.
resource "aws_route53_record" "acm_validation" {
  for_each = (
    var.create_acm_cert && var.route53_zone_id != ""
    ? {
      for dvo in aws_acm_certificate.nlb[0].domain_validation_options :
      dvo.domain_name => {
        name   = dvo.resource_record_name
        type   = dvo.resource_record_type
        record = dvo.resource_record_value
      }
    }
    : {}
  )

  zone_id = var.route53_zone_id
  name    = each.value.name
  type    = each.value.type
  records = [each.value.record]
  ttl     = 60
}

resource "aws_acm_certificate_validation" "nlb" {
  count                   = var.create_acm_cert && var.route53_zone_id != "" ? 1 : 0
  certificate_arn         = aws_acm_certificate.nlb[0].arn
  validation_record_fqdns = [for r in aws_route53_record.acm_validation : r.fqdn]
}

# Friendly DNS alias domain_name -> internal NLB (only when a zone is supplied).
resource "aws_route53_record" "nlb_alias" {
  count   = var.create_acm_cert && var.route53_zone_id != "" ? 1 : 0
  zone_id = var.route53_zone_id
  name    = var.domain_name
  type    = "A"

  alias {
    name                   = aws_lb.connect.dns_name
    zone_id                = aws_lb.connect.zone_id
    evaluate_target_health = true
  }
}

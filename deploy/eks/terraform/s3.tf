# Delta medallion warehouse bucket: encrypted, versioned, public access fully blocked.
resource "aws_s3_bucket" "warehouse" {
  bucket        = local.warehouse_bucket_name
  force_destroy = var.force_destroy_warehouse

  tags = merge(local.tags, { Name = local.warehouse_bucket_name })
}

resource "aws_s3_bucket_versioning" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "warehouse" {
  bucket                  = aws_s3_bucket.warehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Medallion layout: bronze/ silver/ gold/ placeholder keys so the prefixes exist.
resource "aws_s3_object" "medallion_prefixes" {
  for_each = toset(var.warehouse_prefixes)

  bucket = aws_s3_bucket.warehouse.id
  key    = "${each.value}/"
  source = "/dev/null"

  depends_on = [aws_s3_bucket_public_access_block.warehouse]
}

# ---------------------------------------------------------------------------
# Durable data volume strategy (see README "Durability").
#
# The warehouse + metastore live on a DEDICATED gp3 EBS volume that is managed
# OUTSIDE the Launch Template / ASG lifecycle. The volume is created once here
# and persists across instance replacements. On boot, user-data self-attaches it
# by id and mounts it at /srv/spark. Because EBS volumes are AZ-bound, the ASG is
# pinned to a single private subnet/AZ (var.data_volume_az_index) so a replacement
# instance always lands in the same AZ and can re-attach the same volume.
#
# A daily DLM snapshot policy provides point-in-time recovery / volume rebuild.
# ---------------------------------------------------------------------------

resource "aws_ebs_volume" "data" {
  availability_zone = local.azs[var.data_volume_az_index]
  size              = var.data_volume_gb
  type              = "gp3"
  encrypted         = true

  tags = {
    Name       = "${local.name}-data"
    Role       = "spark-data"
    Mountpoint = "/srv/spark"
    # Tagged so the instance role's AttachVolume condition (Project) permits self-attach,
    # and so the DLM policy targets exactly this volume.
    Snapshot = "daily"
  }

  # The volume is the source of truth — never let a config tweak destroy it by surprise.
  lifecycle {
    prevent_destroy = false # set true for production; false here so `terraform destroy` teardown works cleanly
  }
}

# --- DLM: daily snapshots of the data volume --------------------------------
data "aws_iam_policy_document" "dlm_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["dlm.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "dlm" {
  name               = "${local.name}-dlm-role"
  assume_role_policy = data.aws_iam_policy_document.dlm_assume.json
  tags               = { Name = "${local.name}-dlm-role" }
}

resource "aws_iam_role_policy_attachment" "dlm" {
  role       = aws_iam_role.dlm.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSDataLifecycleManagerServiceRole"
}

resource "aws_dlm_lifecycle_policy" "data_daily" {
  description        = "${local.name} daily snapshot of the Spark data volume"
  execution_role_arn = aws_iam_role.dlm.arn
  state              = "ENABLED"

  policy_details {
    resource_types = ["VOLUME"]

    # Target EXACTLY this project's data volume (both tags must match).
    target_tags = {
      Snapshot = "daily"
      Project  = var.project_name
    }

    schedule {
      name = "daily-${var.snapshot_retain_count}d"

      create_rule {
        interval      = 24
        interval_unit = "HOURS"
        times         = [var.snapshot_time_utc]
      }

      retain_rule {
        count = var.snapshot_retain_count
      }

      tags_to_add = {
        SnapshotCreator = "dlm"
        Project         = var.project_name
      }

      copy_tags = true
    }
  }
}

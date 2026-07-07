# ---------------------------------------------------------------------------
# Compute: Launch Template + single-instance ASG (min=max=desired=1) so a
# failed/rebooted instance is automatically replaced. Amazon Linux 2023 in a
# private subnet; no public IP; reachable only via Client VPN + SSM.
# ---------------------------------------------------------------------------

data "aws_ssm_parameter" "al2023_ami" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

locals {
  user_data = templatefile("${path.module}/templates/user-data.sh.tftpl", {
    region                  = var.aws_region
    data_volume_id          = aws_ebs_volume.data.id
    spark_version           = var.spark_version
    spark_download_base_url = var.spark_download_base_url
    app_repo_url            = var.app_repo_url
    app_repo_ref            = var.app_repo_ref
    connect_grpc_port       = var.connect_grpc_port
  })
}

resource "aws_launch_template" "connect" {
  name_prefix   = "${local.name}-lt-"
  image_id      = data.aws_ssm_parameter.al2023_ami.value
  instance_type = var.instance_type

  iam_instance_profile {
    arn = aws_iam_instance_profile.connect.arn
  }

  vpc_security_group_ids = [aws_security_group.connect.id]

  # gp3 root volume.
  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size           = var.root_volume_gb
      volume_type           = "gp3"
      encrypted             = true
      delete_on_termination = true
    }
  }

  metadata_options {
    http_tokens                 = "required" # IMDSv2 only
    http_endpoint               = "enabled"
    http_put_response_hop_limit = 2
  }

  monitoring {
    enabled = true
  }

  user_data = base64encode(local.user_data)

  tag_specifications {
    resource_type = "instance"
    tags = {
      Name          = "${local.name}-server"
      AllowedUsers  = join(",", var.allowed_users)
      AuthProxyHook = tostring(var.enable_auth_proxy)
    }
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_autoscaling_group" "connect" {
  name_prefix = "${local.name}-asg-"

  min_size         = 1
  max_size         = 1
  desired_capacity = 1

  # Pinned to ONE private subnet/AZ so the AZ-bound data volume can always re-attach.
  vpc_zone_identifier = [aws_subnet.private[var.data_volume_az_index].id]

  # Front the server through the NLB target group.
  target_group_arns = [aws_lb_target_group.connect.arn]

  health_check_type         = "ELB"
  health_check_grace_period = 600 # generous: JDK+Spark download on first boot

  launch_template {
    id      = aws_launch_template.connect.id
    version = "$Latest"
  }

  # Replace the instance on launch-template changes.
  instance_refresh {
    strategy = "Rolling"
    preferences {
      min_healthy_percentage = 0 # single instance: tear down then bring up
    }
  }

  tag {
    key                 = "Name"
    value               = "${local.name}-server"
    propagate_at_launch = true
  }

  tag {
    key                 = "Project"
    value               = var.project_name
    propagate_at_launch = true
  }

  lifecycle {
    create_before_destroy = true
  }
}

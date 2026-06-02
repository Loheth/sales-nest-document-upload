# ---------------------------------------------------------------------------
# ECS Cluster Module
#
# Provisions:
#   - ECS cluster
#   - Launch templates for GPU (AL2_x86_64_GPU) and CPU (AL2_x86_64) instances
#   - Auto Scaling Groups for GPU and CPU instances
#   - Capacity providers linking ASGs to ECS cluster
#   - IAM roles for ECS tasks and container instances
#   - Security groups for ECS tasks
# ---------------------------------------------------------------------------

data "aws_partition" "current" {}
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Get latest AL2 GPU AMI
data "aws_ami" "gpu" {
  count = var.gpu_capacity_provider_enabled ? 1 : 0

  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["amzn2-ami-ecs-gpu-hvm-*-x86_64-ebs"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# Get latest AL2 standard AMI
data "aws_ami" "cpu" {
  count = var.cpu_capacity_provider_enabled ? 1 : 0

  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["amzn2-ami-ecs-hvm-*-x86_64-ebs"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# ---------------------------------------------------------------------------
# ECS Cluster
# ---------------------------------------------------------------------------

resource "aws_ecs_cluster" "main" {
  name = var.cluster_name

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = merge(var.tags, {
    Name = var.cluster_name
  })
}

resource "aws_cloudwatch_log_group" "ecs_containerinsights_performance" {
  name              = "/aws/ecs/containerinsights/${var.cluster_name}/performance"
  retention_in_days = var.containerinsights_log_retention_days

  tags = merge(var.tags, {
    Name = "${var.cluster_name}-containerinsights-performance"
  })
}

# ---------------------------------------------------------------------------
# IAM Role for ECS Container Instances
# ---------------------------------------------------------------------------

resource "aws_iam_role" "container_instance" {
  name = "${var.cluster_name}-container-instance-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = merge(var.tags, {
    Name = "${var.cluster_name}-container-instance-role"
  })
}

resource "aws_iam_role_policy_attachment" "container_instance_ecs" {
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
  role       = aws_iam_role.container_instance.name
}

resource "aws_iam_instance_profile" "container_instance" {
  name = "${var.cluster_name}-container-instance-profile"
  role = aws_iam_role.container_instance.name

  tags = merge(var.tags, {
    Name = "${var.cluster_name}-container-instance-profile"
  })
}

# ---------------------------------------------------------------------------
# Security Group for ECS Tasks
# ---------------------------------------------------------------------------

resource "aws_security_group" "ecs_tasks" {
  name_prefix = "${var.cluster_name}-ecs-tasks-"
  description = "Security group for ECS tasks in cluster ${var.cluster_name}"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound traffic"
  }

  tags = merge(var.tags, {
    Name = "${var.cluster_name}-ecs-tasks-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# ---------------------------------------------------------------------------
# GPU Launch Template and Auto Scaling Group
# ---------------------------------------------------------------------------

resource "aws_launch_template" "gpu" {
  count = var.gpu_capacity_provider_enabled ? 1 : 0

  name_prefix   = "${var.cluster_name}-gpu-"
  image_id      = data.aws_ami.gpu[0].id
  instance_type = var.gpu_instance_type
  ebs_optimized = true

  vpc_security_group_ids = [aws_security_group.ecs_tasks.id]

  iam_instance_profile {
    name = aws_iam_instance_profile.container_instance.name
  }

  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size           = var.gpu_disk_size
      volume_type           = "gp3"
      delete_on_termination = true
      encrypted             = var.kms_key_arn != null
      kms_key_id            = var.kms_key_arn
    }
  }

  user_data = base64encode(<<-EOF
    #!/bin/bash
    echo ECS_CLUSTER=${var.cluster_name} >> /etc/ecs/ecs.config
    echo ECS_ENABLE_GPU_SUPPORT=true >> /etc/ecs/ecs.config
  EOF
  )

  tag_specifications {
    resource_type = "instance"
    tags = merge(var.tags, {
      Name       = "${var.cluster_name}-gpu-instance"
      CostCenter = "ml-inference"
    })
  }

  tags = merge(var.tags, {
    Name = "${var.cluster_name}-gpu-launch-template"
  })
}

resource "aws_autoscaling_group" "gpu" {
  count = var.gpu_capacity_provider_enabled ? 1 : 0

  name                      = "${var.cluster_name}-gpu-asg"
  vpc_zone_identifier       = var.subnet_ids
  min_size                  = var.gpu_min_size
  max_size                  = var.gpu_max_size
  desired_capacity          = var.gpu_desired_size
  health_check_type         = "EC2"
  health_check_grace_period = 300

  launch_template {
    id      = aws_launch_template.gpu[0].id
    version = "$Latest"
  }

  tag {
    key                 = "Name"
    value               = "${var.cluster_name}-gpu-instance"
    propagate_at_launch = true
  }

  dynamic "tag" {
    for_each = var.tags
    content {
      key                 = tag.key
      value               = tag.value
      propagate_at_launch = true
    }
  }

  lifecycle {
    ignore_changes = [desired_capacity]
  }
}

resource "aws_ecs_capacity_provider" "gpu" {
  count = var.gpu_capacity_provider_enabled ? 1 : 0

  name = "${var.cluster_name}-gpu"

  auto_scaling_group_provider {
    auto_scaling_group_arn         = aws_autoscaling_group.gpu[0].arn
    managed_termination_protection = "DISABLED"

    managed_scaling {
      maximum_scaling_step_size = 10
      minimum_scaling_step_size = 1
      status                    = "ENABLED"
      target_capacity           = 100
    }
  }

  tags = merge(var.tags, {
    Name = "${var.cluster_name}-gpu-capacity-provider"
  })
}

# ---------------------------------------------------------------------------
# CPU Launch Template and Auto Scaling Group
# ---------------------------------------------------------------------------

resource "aws_launch_template" "cpu" {
  count = var.cpu_capacity_provider_enabled ? 1 : 0

  name_prefix   = "${var.cluster_name}-cpu-"
  image_id      = data.aws_ami.cpu[0].id
  instance_type = var.cpu_instance_type
  ebs_optimized = true

  vpc_security_group_ids = [aws_security_group.ecs_tasks.id]

  iam_instance_profile {
    name = aws_iam_instance_profile.container_instance.name
  }

  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size           = var.cpu_disk_size
      volume_type           = "gp3"
      delete_on_termination = true
      encrypted             = var.kms_key_arn != null
      kms_key_id            = var.kms_key_arn
    }
  }

  user_data = base64encode(<<-EOF
    #!/bin/bash
    echo ECS_CLUSTER=${var.cluster_name} >> /etc/ecs/ecs.config
  EOF
  )

  tag_specifications {
    resource_type = "instance"
    tags = merge(var.tags, {
      Name       = "${var.cluster_name}-cpu-instance"
      CostCenter = "processing"
    })
  }

  tags = merge(var.tags, {
    Name = "${var.cluster_name}-cpu-launch-template"
  })
}

resource "aws_autoscaling_group" "cpu" {
  count = var.cpu_capacity_provider_enabled ? 1 : 0

  name                      = "${var.cluster_name}-cpu-asg"
  vpc_zone_identifier       = var.subnet_ids
  min_size                  = var.cpu_min_size
  max_size                  = var.cpu_max_size
  desired_capacity          = var.cpu_desired_size
  health_check_type         = "EC2"
  health_check_grace_period = 300

  launch_template {
    id      = aws_launch_template.cpu[0].id
    version = "$Latest"
  }

  tag {
    key                 = "Name"
    value               = "${var.cluster_name}-cpu-instance"
    propagate_at_launch = true
  }

  dynamic "tag" {
    for_each = var.tags
    content {
      key                 = tag.key
      value               = tag.value
      propagate_at_launch = true
    }
  }

  lifecycle {
    ignore_changes = [desired_capacity]
  }
}

resource "aws_ecs_capacity_provider" "cpu" {
  count = var.cpu_capacity_provider_enabled ? 1 : 0

  name = "${var.cluster_name}-cpu"

  auto_scaling_group_provider {
    auto_scaling_group_arn         = aws_autoscaling_group.cpu[0].arn
    managed_termination_protection = "DISABLED"

    managed_scaling {
      maximum_scaling_step_size = 10
      minimum_scaling_step_size = 1
      status                    = "ENABLED"
      target_capacity           = 100
    }
  }

  tags = merge(var.tags, {
    Name = "${var.cluster_name}-cpu-capacity-provider"
  })
}

# ---------------------------------------------------------------------------
# Register Capacity Providers with Cluster
# ---------------------------------------------------------------------------

locals {
  registered_capacity_providers = concat(
    var.gpu_capacity_provider_enabled ? [aws_ecs_capacity_provider.gpu[0].name] : [],
    var.cpu_capacity_provider_enabled ? [aws_ecs_capacity_provider.cpu[0].name] : [],
    var.fargate_capacity_providers_enabled ? ["FARGATE", "FARGATE_SPOT"] : []
  )
}

resource "aws_ecs_cluster_capacity_providers" "main" {
  count = length(local.registered_capacity_providers) > 0 ? 1 : 0

  cluster_name       = aws_ecs_cluster.main.name
  capacity_providers = local.registered_capacity_providers

  dynamic "default_capacity_provider_strategy" {
    for_each = var.cpu_capacity_provider_enabled ? [1] : (var.gpu_capacity_provider_enabled ? [1] : [])
    content {
      capacity_provider = var.cpu_capacity_provider_enabled ? aws_ecs_capacity_provider.cpu[0].name : aws_ecs_capacity_provider.gpu[0].name
      weight            = 1
      base              = 1
    }
  }
}

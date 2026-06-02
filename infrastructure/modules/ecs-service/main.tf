# ---------------------------------------------------------------------------
# ECS Service Module
#
# Creates ECS task definition and service. Supports CPU-only (require_gpu = false)
# or GPU (require_gpu = true) with placement constraints.
# Application Auto Scaling based on SQS backlog per task (optional).
# ---------------------------------------------------------------------------

data "aws_partition" "current" {}

locals {
  is_fargate = var.launch_type == "FARGATE"

  # Metric math IDs are m0, m1, … (MSK does not expose lag without Topic dimension in GovCloud).
  kafka_lag_sum_expr = length(var.kafka_lag_topics) == 1 ? "m0" : join("+", [for i in range(length(var.kafka_lag_topics)) : "m${i}"])

  container_definition = {
    name      = var.container_name
    image     = var.container_image
    essential = true

    portMappings = [
      {
        containerPort = var.container_port
        protocol      = "tcp"
      }
    ]

    environment = [
      for key, value in var.environment_variables : {
        name  = key
        value = value
      }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = var.cloudwatch_log_group_name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "ecs"
      }
    }

    healthCheck = {
      command     = ["CMD-SHELL", var.health_check_command]
      interval    = 30
      timeout     = 5
      retries     = 3
      startPeriod = var.health_check_grace_period
    }

    resourceRequirements = var.require_gpu ? [{ type = "GPU", value = "1" }] : []

    mountPoints = var.efs_file_system_id != "" && var.efs_access_point_id != "" ? [
      {
        sourceVolume  = "models"
        containerPath = "/mnt/models"
        readOnly      = false
      }
    ] : []
    volumesFrom = []
  }
}

# ---------------------------------------------------------------------------
# ECS Task Definition
# ---------------------------------------------------------------------------

resource "aws_ecs_task_definition" "main" {
  family                   = var.service_name
  network_mode             = "awsvpc"
  requires_compatibilities = local.is_fargate ? ["FARGATE"] : ["EC2"]
  cpu                      = tostring(var.cpu)
  memory                   = tostring(var.memory)
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = var.task_role_arn

  dynamic "ephemeral_storage" {
    for_each = local.is_fargate && var.ephemeral_storage_gib > 0 ? [1] : []
    content {
      size_in_gib = var.ephemeral_storage_gib
    }
  }

  dynamic "volume" {
    for_each = var.efs_file_system_id != "" && var.efs_access_point_id != "" ? [1] : []
    content {
      name = "models"
      efs_volume_configuration {
        file_system_id     = var.efs_file_system_id
        root_directory     = "/"
        transit_encryption = "ENABLED"
        authorization_config {
          access_point_id = var.efs_access_point_id
          iam             = "ENABLED"
        }
      }
    }
  }

  container_definitions = jsonencode([local.container_definition])

  dynamic "placement_constraints" {
    for_each = !local.is_fargate && var.require_gpu ? [1] : []
    content {
      type       = "memberOf"
      expression = "attribute:ecs.instance-type =~ g4dn.*"
    }
  }

  tags = merge(var.tags, {
    Name = "${var.service_name}-task"
  })
}

# ---------------------------------------------------------------------------
# ECS Service
# ---------------------------------------------------------------------------

resource "aws_ecs_service" "main" {
  name            = var.service_name
  cluster         = var.cluster_id
  task_definition = aws_ecs_task_definition.main.arn
  desired_count   = var.desired_count

  launch_type      = local.is_fargate ? "FARGATE" : null
  platform_version = local.is_fargate ? var.fargate_platform_version : null

  dynamic "capacity_provider_strategy" {
    for_each = local.is_fargate ? [] : [1]
    content {
      capacity_provider = var.capacity_provider_name
      weight            = 1
      base              = 0
    }
  }

  network_configuration {
    subnets          = var.subnet_ids
    security_groups  = var.security_group_ids
    assign_public_ip = var.assign_public_ip
  }

  dynamic "placement_constraints" {
    for_each = !local.is_fargate && var.require_gpu ? [1] : []
    content {
      type       = "memberOf"
      expression = "attribute:ecs.instance-type =~ g4dn.*"
    }
  }

  health_check_grace_period_seconds = var.health_check_grace_period

  tags = merge(var.tags, {
    Name = "${var.service_name}-service"
  })
}

# ---------------------------------------------------------------------------
# Application Auto Scaling Target
# ---------------------------------------------------------------------------

resource "aws_appautoscaling_target" "ecs_target" {
  count              = var.enable_autoscaling ? 1 : 0
  max_capacity       = var.max_capacity
  min_capacity       = var.min_capacity
  resource_id        = "service/${var.cluster_name}/${aws_ecs_service.main.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

# ---------------------------------------------------------------------------
# Application Auto Scaling Policy (SQS backlog-per-task)
# ---------------------------------------------------------------------------
# Metric: (ApproximateNumberOfMessagesVisible + ApproximateNumberOfMessagesNotVisible)
# / max(RunningTaskCount from Container Insights, treat 0 as full backlog).
# Uses ECS/ContainerInsights RunningTaskCount — cluster must have Container Insights enabled.

resource "aws_appautoscaling_policy" "sqs_scaling" {
  count              = var.enable_autoscaling && var.sqs_queue_name != "" && var.kafka_consumer_group == "" ? 1 : 0
  name               = "${var.service_name}-sqs-scaling"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.ecs_target[0].resource_id
  scalable_dimension = aws_appautoscaling_target.ecs_target[0].scalable_dimension
  service_namespace  = aws_appautoscaling_target.ecs_target[0].service_namespace

  target_tracking_scaling_policy_configuration {
    target_value = var.target_value

    customized_metric_specification {
      metrics {
        id    = "m_visible"
        label = "Visible messages"

        metric_stat {
          metric {
            namespace   = "AWS/SQS"
            metric_name = "ApproximateNumberOfMessagesVisible"
            dimensions {
              name  = "QueueName"
              value = var.sqs_queue_name
            }
          }
          stat = "Sum"
        }

        return_data = false
      }

      metrics {
        id    = "m_inflight"
        label = "In-flight messages"

        metric_stat {
          metric {
            namespace   = "AWS/SQS"
            metric_name = "ApproximateNumberOfMessagesNotVisible"
            dimensions {
              name  = "QueueName"
              value = var.sqs_queue_name
            }
          }
          stat = "Sum"
        }

        return_data = false
      }

      metrics {
        id    = "m_tasks"
        label = "Running tasks"

        metric_stat {
          metric {
            namespace   = "ECS/ContainerInsights"
            metric_name = "RunningTaskCount"
            dimensions {
              name  = "ClusterName"
              value = var.cluster_name
            }
            dimensions {
              name  = "ServiceName"
              value = aws_ecs_service.main.name
            }
          }
          stat = "Average"
        }

        return_data = false
      }

      metrics {
        id          = "backlog_per_task"
        label       = "Backlog per task"
        expression  = "IF(FILL(m_tasks, 0) > 0, (m_visible + m_inflight) / FILL(m_tasks, 0), m_visible + m_inflight)"
        return_data = true
      }
    }

    scale_in_cooldown  = var.scale_in_cooldown
    scale_out_cooldown = var.scale_out_cooldown
  }
}

resource "aws_appautoscaling_policy" "kafka_scaling" {
  count              = var.enable_autoscaling && var.kafka_consumer_group != "" ? 1 : 0
  name               = "${var.service_name}-kafka-lag-scaling"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.ecs_target[0].resource_id
  scalable_dimension = aws_appautoscaling_target.ecs_target[0].scalable_dimension
  service_namespace  = aws_appautoscaling_target.ecs_target[0].service_namespace

  lifecycle {
    precondition {
      condition     = length(var.kafka_lag_topics) > 0
      error_message = "When Kafka autoscaling is enabled (kafka_consumer_group set), kafka_lag_topics must list every consumed topic for MSK Topic-scoped SumOffsetLag."
    }
  }

  target_tracking_scaling_policy_configuration {
    target_value = var.kafka_target_lag

    customized_metric_specification {
      dynamic "metrics" {
        for_each = { for idx, topic in var.kafka_lag_topics : idx => topic }
        content {
          id    = "m${metrics.key}"
          label = "SumOffsetLag ${metrics.value}"

          metric_stat {
            metric {
              namespace   = "AWS/Kafka"
              metric_name = "SumOffsetLag"
              dimensions {
                name  = "Cluster Name"
                value = var.msk_cluster_name
              }
              dimensions {
                name  = "Consumer Group"
                value = var.kafka_consumer_group
              }
              dimensions {
                name  = "Topic"
                value = metrics.value
              }
            }
            stat = "Maximum"
          }
          return_data = false
        }
      }

      metrics {
        label       = "Total SumOffsetLag"
        id          = "total_offset_lag"
        expression  = local.kafka_lag_sum_expr
        return_data = true
      }
    }

    scale_in_cooldown  = var.scale_in_cooldown
    scale_out_cooldown = var.scale_out_cooldown
  }
}

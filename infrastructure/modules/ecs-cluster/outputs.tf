# ---------------------------------------------------------------------------
# ECS Cluster Module Outputs
# ---------------------------------------------------------------------------

output "cluster_name" {
  description = "Name of the ECS cluster"
  value       = aws_ecs_cluster.main.name
}

output "cluster_arn" {
  description = "ARN of the ECS cluster"
  value       = aws_ecs_cluster.main.arn
}

output "cluster_id" {
  description = "ID of the ECS cluster"
  value       = aws_ecs_cluster.main.id
}

output "gpu_capacity_provider_name" {
  description = "Name of the GPU capacity provider (null if disabled)"
  value       = var.gpu_capacity_provider_enabled ? aws_ecs_capacity_provider.gpu[0].name : null
}

output "cpu_capacity_provider_name" {
  description = "Name of the CPU capacity provider (null if disabled)"
  value       = var.cpu_capacity_provider_enabled ? aws_ecs_capacity_provider.cpu[0].name : null
}

output "gpu_autoscaling_group_arn" {
  description = "ARN of the GPU Auto Scaling Group (null if disabled)"
  value       = var.gpu_capacity_provider_enabled ? aws_autoscaling_group.gpu[0].arn : null
}

output "cpu_autoscaling_group_arn" {
  description = "ARN of the CPU Auto Scaling Group (null if disabled)"
  value       = var.cpu_capacity_provider_enabled ? aws_autoscaling_group.cpu[0].arn : null
}

output "ecs_tasks_security_group_id" {
  description = "Security group ID for ECS tasks"
  value       = aws_security_group.ecs_tasks.id
}

output "container_instance_role_arn" {
  description = "ARN of the IAM role for ECS container instances"
  value       = aws_iam_role.container_instance.arn
}

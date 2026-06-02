# ---------------------------------------------------------------------------
# Document Analysis Microservice - Prod Outputs
# ---------------------------------------------------------------------------

output "ecr_repository_url" {
  description = "URL of the ECR repository"
  value       = module.ecr_repository.repository_url
}

output "ecr_repository_arn" {
  description = "ARN of the ECR repository"
  value       = module.ecr_repository.repository_arn
}

output "ecs_cluster_name" {
  description = "Name of the ECS cluster"
  value       = module.ecs_cluster.cluster_name
}

output "ecs_service_worker_name" {
  description = "Name of the Docling worker ECS service"
  value       = module.ecs_service_worker.service_name
}

output "ecs_service_bridge_name" {
  description = "Name of the Kafka→SQS bridge ECS service"
  value       = module.ecs_service_bridge.service_name
}

output "ecs_task_definition_arn" {
  description = "ARN of the worker ECS task definition (same image as bridge)"
  value       = module.ecs_service_worker.task_definition_arn
}

output "sqs_jobs_queue_url" {
  description = "URL of the SQS job queue between bridge and worker"
  value       = module.sqs_queue.queue_url
}

output "ecs_task_role_arn" {
  description = "ARN of the ECS task role"
  value       = module.ecs_task_role.role_arn
}

output "ecs_execution_role_arn" {
  description = "ARN of the ECS execution role"
  value       = module.ecs_execution_role.role_arn
}

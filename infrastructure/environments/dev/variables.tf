# ---------------------------------------------------------------------------
# Document Analysis Microservice - Dev Variables
# ---------------------------------------------------------------------------

variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-gov-west-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "document-analysis-microservice"
}

variable "ecr_repository_name" {
  description = "Name of the ECR repository"
  type        = string
  default     = "document-analysis-microservice"
}

variable "ecs_cluster_name" {
  description = "Name of the ECS cluster (owned by this repo)"
  type        = string
}

variable "vpc_endpoint_security_group_id" {
  description = "Security group ID used by the shared VPC interface endpoints"
  type        = string
  default     = "sg-058478f97b4a0d2d4"
}

variable "container_image_tag" {
  description = "Docker image tag to deploy (e.g. dev-abc1234). Passed by CI to create a new task definition revision on every push."
  type        = string
  default     = "dev-latest"
}

variable "aurora_secret_name" {
  description = "Secrets Manager secret name for Aurora credentials (partitioned pipeline)"
  type        = string
  default     = ""
}

variable "doc_pipeline_desired_count" {
  description = "Desired ECS task count for each new partitioned doc service (dev)"
  type        = number
  default     = 1
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention in days"
  type        = number
  default     = 30
}

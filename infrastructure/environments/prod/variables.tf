# ---------------------------------------------------------------------------
# Document Analysis Microservice - Prod Variables
# ---------------------------------------------------------------------------

variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-gov-west-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "prod"
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
  description = "Docker image tag to deploy (e.g. prod-abc1234). Passed by CI to create a new task definition revision on every push."
  type        = string
  default     = "prod-latest"
}

variable "msk_cluster_name" {
  description = "MSK cluster name in AWS (flash-infrastructure module uses flash-kafka-<env>)."
  type        = string
  default     = "flash-kafka-prod"
}

variable "kafka_clients_security_group_name" {
  description = "Name of the shared kafka-clients security group in the VPC (flash-infrastructure prod MSK stack)."
  type        = string
  default     = "kafka-clients-prod"
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention in days"
  type        = number
  default     = 365
}

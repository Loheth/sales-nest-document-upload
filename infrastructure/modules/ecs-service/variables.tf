# ---------------------------------------------------------------------------
# ECS Service Module Variables
# ---------------------------------------------------------------------------

variable "service_name" {
  description = "Name of the ECS service"
  type        = string
}

variable "cluster_name" {
  description = "Name of the ECS cluster"
  type        = string
}

variable "cluster_id" {
  description = "ID of the ECS cluster"
  type        = string
}

variable "capacity_provider_name" {
  description = "Name of the EC2 capacity provider (GPU or CPU) to use for this service. Ignored when launch_type = FARGATE."
  type        = string
  default     = ""
}

variable "launch_type" {
  description = "ECS launch type. Either EC2 or FARGATE. When FARGATE, capacity_provider_name is ignored and Fargate-specific options apply."
  type        = string
  default     = "EC2"

  validation {
    condition     = contains(["EC2", "FARGATE"], var.launch_type)
    error_message = "launch_type must be either EC2 or FARGATE."
  }
}

variable "fargate_platform_version" {
  description = "Fargate platform version. Only used when launch_type = FARGATE."
  type        = string
  default     = "LATEST"
}

variable "assign_public_ip" {
  description = "Whether to assign a public IP to ENIs. Required true for Fargate tasks in public subnets without a NAT/VPC endpoint path to the internet."
  type        = bool
  default     = false
}

variable "ephemeral_storage_gib" {
  description = "Override task ephemeral storage in GiB (Fargate only, 21-200). 0 means use the default 20 GiB."
  type        = number
  default     = 0
}

variable "task_role_arn" {
  description = "ARN of the IAM role for ECS tasks"
  type        = string
}

variable "execution_role_arn" {
  description = "ARN of the IAM role for ECS task execution (for pulling images, CloudWatch logs)"
  type        = string
}

variable "container_image" {
  description = "Container image URI (from ECR)"
  type        = string
}

variable "container_name" {
  description = "Name of the container"
  type        = string
  default     = "docling"
}

variable "container_port" {
  description = "Port the container listens on"
  type        = number
  default     = 8000
}

variable "cpu" {
  description = "CPU units for the task (1024 = 1 vCPU)"
  type        = number
  default     = 2048
}

variable "memory" {
  description = "Memory for the task in MB"
  type        = number
  default     = 4096
}

variable "environment_variables" {
  description = "Environment variables for the container"
  type        = map(string)
  default     = {}
}

variable "subnet_ids" {
  description = "List of subnet IDs for the ECS service"
  type        = list(string)
}

variable "security_group_ids" {
  description = "List of security group IDs for the ECS service"
  type        = list(string)
}

variable "cloudwatch_log_group_name" {
  description = "Name of the CloudWatch log group"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "desired_count" {
  description = "Desired number of tasks"
  type        = number
  default     = 1
}

variable "require_gpu" {
  description = "If true, task definition requires GPU and uses GPU placement constraints"
  type        = bool
  default     = false
}

variable "health_check_command" {
  description = "Shell command for container health check (e.g. pgrep -f 'python.*document_analysis' || exit 1)"
  type        = string
  default     = "pgrep -f 'python.*document_analysis' || exit 1"
}

# Autoscaling
variable "enable_autoscaling" {
  description = "Whether to enable autoscaling"
  type        = bool
  default     = true
}

variable "min_capacity" {
  description = "Minimum number of tasks"
  type        = number
  default     = 0
}

variable "max_capacity" {
  description = "Maximum number of tasks"
  type        = number
  default     = 10
}

variable "sqs_queue_name" {
  description = "Name of the SQS queue for autoscaling (optional)"
  type        = string
  default     = ""
}

variable "target_value" {
  description = "Target backlog per task for SQS autoscaling: (visible + in-flight messages) / running task count (Container Insights); scale toward this value"
  type        = number
  default     = 3
}

variable "scale_in_cooldown" {
  description = "Cooldown period in seconds before scaling in"
  type        = number
  default     = 120
}

variable "scale_out_cooldown" {
  description = "Cooldown period in seconds before scaling out"
  type        = number
  default     = 0
}

# Kafka consumer-lag scaling (replaces SQS for Kafka-based services)
variable "msk_cluster_name" {
  description = "MSK cluster name for CloudWatch SumOffsetLag metric"
  type        = string
  default     = ""
}

variable "kafka_consumer_group" {
  description = "Kafka consumer group name for lag-based scaling"
  type        = string
  default     = ""
}

variable "kafka_target_lag" {
  description = "Target consumer lag per task that triggers scaling"
  type        = number
  default     = 2
}

# MSK publishes SumOffsetLag per Topic; include every subscribed topic so the scaling metric matches CloudWatch series.
variable "kafka_lag_topics" {
  description = "Kafka topic names (Topic dimension) to sum into total SumOffsetLag for target tracking — required when kafka_consumer_group is set"
  type        = list(string)
  default     = []
}

variable "health_check_grace_period" {
  description = "Grace period in seconds for health checks"
  type        = number
  default     = 180
}

variable "efs_file_system_id" {
  description = "EFS filesystem ID (optional, empty = no EFS mount)"
  type        = string
  default     = ""
}

variable "efs_access_point_id" {
  description = "EFS access point ID (optional, empty = no EFS mount)"
  type        = string
  default     = ""
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}

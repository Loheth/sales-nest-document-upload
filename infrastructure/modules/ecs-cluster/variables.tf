# ---------------------------------------------------------------------------
# ECS Cluster Module Variables
# ---------------------------------------------------------------------------

variable "cluster_name" {
  description = "Name of the ECS cluster"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID where the ECS cluster will be deployed"
  type        = string
}

variable "subnet_ids" {
  description = "List of subnet IDs for ECS tasks and instances"
  type        = list(string)
}

variable "kms_key_arn" {
  description = "KMS key ARN for encryption (optional)"
  type        = string
  default     = null
}

# -- GPU Capacity Provider -------------------------------------------------

variable "gpu_capacity_provider_enabled" {
  description = "Whether to create the GPU capacity provider"
  type        = bool
  default     = true
}

variable "gpu_instance_type" {
  description = "EC2 instance type for GPU instances"
  type        = string
  default     = "g4dn.xlarge"
}

variable "gpu_min_size" {
  description = "Minimum number of GPU instances"
  type        = number
  default     = 0
}

variable "gpu_max_size" {
  description = "Maximum number of GPU instances"
  type        = number
  default     = 10
}

variable "gpu_desired_size" {
  description = "Desired number of GPU instances at cluster creation"
  type        = number
  default     = 1
}

variable "gpu_disk_size" {
  description = "EBS volume size in GB for GPU instances (model weights + temp data)"
  type        = number
  default     = 100
}

# -- CPU Capacity Provider -------------------------------------------------

variable "cpu_capacity_provider_enabled" {
  description = "Whether to create the CPU capacity provider"
  type        = bool
  default     = true
}

variable "cpu_instance_type" {
  description = "EC2 instance type for CPU instances"
  type        = string
  default     = "m5.large"
}

variable "cpu_min_size" {
  description = "Minimum number of CPU instances"
  type        = number
  default     = 2
}

variable "cpu_max_size" {
  description = "Maximum number of CPU instances"
  type        = number
  default     = 20
}

variable "cpu_desired_size" {
  description = "Desired number of CPU instances at cluster creation"
  type        = number
  default     = 2
}

variable "cpu_disk_size" {
  description = "EBS volume size in GB for CPU instances"
  type        = number
  default     = 50
}

# -- Fargate Capacity Providers --------------------------------------------

variable "fargate_capacity_providers_enabled" {
  description = "Whether to register the FARGATE and FARGATE_SPOT capacity providers with the cluster. Required if any service in this cluster uses Fargate via capacity_provider_strategy. Not required when services use launch_type = FARGATE directly."
  type        = bool
  default     = false
}

variable "containerinsights_log_retention_days" {
  description = "Retention for ECS Container Insights performance log group (/aws/ecs/containerinsights/...)"
  type        = number
  default     = 365
}

# -- Tags -------------------------------------------------------------------

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}

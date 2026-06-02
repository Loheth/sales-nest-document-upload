variable "role_name" {
  description = "Name of the IAM role"
  type        = string
}

variable "ecr_repository_arn" {
  description = "ARN of the ECR repository (optional, defaults to all repositories)"
  type        = string
  default     = null
}

variable "kms_key_arn" {
  description = "KMS key ARN for CloudWatch logs encryption (optional)"
  type        = string
  default     = null
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}

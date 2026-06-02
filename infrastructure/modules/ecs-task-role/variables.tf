# ---------------------------------------------------------------------------
# ECS Task Role Module Variables
# ---------------------------------------------------------------------------

variable "role_name" {
  description = "Name of the IAM role"
  type        = string
}

variable "s3_allow_all_buckets" {
  description = "If true, allow GetObject, PutObject, ListBucket on all S3 buckets in the account (partition-aware). If false, use s3_bucket_arns."
  type        = bool
  default     = false
}

variable "s3_bucket_arns" {
  description = "List of S3 bucket ARNs the role can read/write (used when s3_allow_all_buckets is false)"
  type        = list(string)
  default     = []
}

variable "cloudwatch_log_group_arns" {
  description = "List of CloudWatch log group ARNs the role can write to (optional, empty = no CW permissions)"
  type        = list(string)
  default     = []
}

variable "sqs_access_enabled" {
  description = "Whether to attach SQS receive/delete policy (use for plan-time count; set true when SQS queue is used)"
  type        = bool
  default     = false
}

variable "sqs_queue_arns" {
  description = "List of SQS queue ARNs the role can interact with (optional)"
  type        = list(string)
  default     = []
}

variable "efs_file_system_arn" {
  description = "ARN of the EFS filesystem (optional, empty = no EFS permissions)"
  type        = string
  default     = ""
}

variable "efs_access_point_arn" {
  description = "ARN of the EFS access point (optional, empty = no EFS permissions)"
  type        = string
  default     = ""
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}

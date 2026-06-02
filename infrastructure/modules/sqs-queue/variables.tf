variable "queue_name" {
  description = "Name of the SQS queue"
  type        = string
}

variable "visibility_timeout_seconds" {
  description = "Visibility timeout in seconds (should exceed expected processing time)"
  type        = number
  default     = 600
}

variable "message_retention_seconds" {
  description = "Message retention period in seconds"
  type        = number
  default     = 345600 # 4 days
}

variable "receive_wait_time_seconds" {
  description = "Receive wait time in seconds (long polling)"
  type        = number
  default     = 20
}

variable "dead_letter_queue" {
  description = "Dead letter queue configuration. Set to null to disable DLQ."
  type = object({
    name                      = string
    message_retention_seconds = number
    max_receive_count         = number
  })
  default = null
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}

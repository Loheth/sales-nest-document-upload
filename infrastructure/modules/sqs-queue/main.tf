# SQS Queue Module
# Creates an SQS queue with optional dead letter queue

# Dead Letter Queue (if configured)
resource "aws_sqs_queue" "dlq" {
  #checkov:skip=CKV_AWS_27:SQS encryption not required for internal microservice job queues
  count = var.dead_letter_queue != null ? 1 : 0

  name                      = var.dead_letter_queue.name
  message_retention_seconds = var.dead_letter_queue.message_retention_seconds

  tags = merge(var.tags, {
    Name = var.dead_letter_queue.name
  })
}

# Main Queue
resource "aws_sqs_queue" "main" {
  #checkov:skip=CKV_AWS_27:SQS encryption not required for internal microservice job queues
  name                       = var.queue_name
  visibility_timeout_seconds = var.visibility_timeout_seconds
  message_retention_seconds  = var.message_retention_seconds
  receive_wait_time_seconds  = var.receive_wait_time_seconds

  redrive_policy = var.dead_letter_queue != null ? jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq[0].arn
    maxReceiveCount     = var.dead_letter_queue.max_receive_count
  }) : null

  tags = merge(var.tags, {
    Name = var.queue_name
  })
}

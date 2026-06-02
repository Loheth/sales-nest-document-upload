output "queue_id" {
  description = "ID of the main queue"
  value       = aws_sqs_queue.main.id
}

output "queue_arn" {
  description = "ARN of the main queue"
  value       = aws_sqs_queue.main.arn
}

output "queue_url" {
  description = "URL of the main queue"
  value       = aws_sqs_queue.main.url
}

output "queue_name" {
  description = "Name of the main queue"
  value       = aws_sqs_queue.main.name
}

output "dlq_id" {
  description = "ID of the dead letter queue (null if not configured)"
  value       = var.dead_letter_queue != null ? aws_sqs_queue.dlq[0].id : null
}

output "dlq_arn" {
  description = "ARN of the dead letter queue (null if not configured)"
  value       = var.dead_letter_queue != null ? aws_sqs_queue.dlq[0].arn : null
}

output "dlq_url" {
  description = "URL of the dead letter queue (null if not configured)"
  value       = var.dead_letter_queue != null ? aws_sqs_queue.dlq[0].url : null
}

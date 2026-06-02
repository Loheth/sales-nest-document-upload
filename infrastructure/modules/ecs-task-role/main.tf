# ---------------------------------------------------------------------------
# ECS Task Role Module
#
# Creates an IAM role for ECS tasks with:
#   - Standard EC2 trust policy (ECS tasks use this)
#   - s3:GetObject / s3:ListBucket on specified buckets
#   - logs:CreateLogStream / logs:PutLogEvents (optional)
# ---------------------------------------------------------------------------

data "aws_partition" "current" {}
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ---------------------------------------------------------------------------
# IAM Role with EC2 trust (for ECS tasks)
# ---------------------------------------------------------------------------

resource "aws_iam_role" "this" {
  name = var.role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = merge(var.tags, {
    Name = var.role_name
  })
}

# ---------------------------------------------------------------------------
# S3 access (all buckets or specific list)
# ---------------------------------------------------------------------------

resource "aws_iam_role_policy" "s3_all_buckets" {
  count = var.s3_allow_all_buckets ? 1 : 0

  name = "${var.role_name}-s3-all-buckets"
  role = aws_iam_role.this.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = "arn:${data.aws_partition.current.partition}:s3:::*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "arn:${data.aws_partition.current.partition}:s3:::*/*"
      }
    ]
  })
}

# KMS for SSE-KMS S3 objects: Decrypt/GetObject; GenerateDataKey for PutObject when bucket default encryption uses KMS
resource "aws_iam_role_policy" "kms_decrypt_s3" {
  count = var.s3_allow_all_buckets || length(var.s3_bucket_arns) > 0 ? 1 : 0

  name = "${var.role_name}-kms-s3-sse"
  role = aws_iam_role.this.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey",
          "kms:GenerateDataKey"
        ]
        Resource = "arn:${data.aws_partition.current.partition}:kms:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:key/*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "s3_read" {
  count = !var.s3_allow_all_buckets && length(var.s3_bucket_arns) > 0 ? 1 : 0

  name = "${var.role_name}-s3-read"
  role = aws_iam_role.this.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject"
        ]
        Resource = flatten([
          var.s3_bucket_arns,
          [for arn in var.s3_bucket_arns : "${arn}/*"]
        ])
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# CloudWatch Logs (optional)
# ---------------------------------------------------------------------------

resource "aws_iam_role_policy" "cloudwatch_logs" {
  count = length(var.cloudwatch_log_group_arns) > 0 ? 1 : 0

  name = "${var.role_name}-cloudwatch-logs"
  role = aws_iam_role.this.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = [for arn in var.cloudwatch_log_group_arns : "${arn}:*"]
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# SQS Access (optional)
# ---------------------------------------------------------------------------

resource "aws_iam_role_policy" "sqs_access" {
  count = var.sqs_access_enabled ? 1 : 0

  name = "${var.role_name}-sqs-access"
  role = aws_iam_role.this.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sqs:SendMessage",
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl",
          "sqs:ChangeMessageVisibility"
        ]
        Resource = var.sqs_queue_arns
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# EFS Access (optional)
# ---------------------------------------------------------------------------

resource "aws_iam_role_policy" "efs_access" {
  count = var.efs_file_system_arn != "" && var.efs_access_point_arn != "" ? 1 : 0

  name = "${var.role_name}-efs-access"
  role = aws_iam_role.this.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "elasticfilesystem:ClientMount",
          "elasticfilesystem:ClientWrite",
          "elasticfilesystem:ClientRootAccess"
        ]
        Resource = var.efs_file_system_arn
        Condition = {
          StringEquals = {
            "elasticfilesystem:AccessPointArn" = var.efs_access_point_arn
          }
        }
      }
    ]
  })
}

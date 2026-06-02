# ---------------------------------------------------------------------------
# Document Analysis Microservice - Prod Environment
#
# Provisions:
#   - ECR repository for the container image
#   - SQS queue (+ DLQ) between Kafka bridge and Docling workers
#   - ECS task role for S3 + SQS access
#   - ECS execution role for image pulling and CloudWatch logs
#   - Two ECS services: Kafka→SQS bridge (small) and SQS worker (Docling)
#   - Application Auto Scaling for workers on SQS backlog per task
#
# The ECS cluster is provisioned in this repo (modules/ecs-cluster).
# ---------------------------------------------------------------------------

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    # Backend configuration is provided via -backend-config=backend.hcl
  }
}

provider "aws" {
  region = var.aws_region
}

# ---------------------------------------------------------------------------
# Data Sources
# ---------------------------------------------------------------------------

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
data "aws_partition" "current" {}

# ---------------------------------------------------------------------------
# Remote State: Shared (VPC, subnets)
# ---------------------------------------------------------------------------

data "terraform_remote_state" "shared" {
  backend = "s3"
  config = {
    bucket = "flash-infrastructure"
    key    = "flash-infrastructure/shared/terraform.tfstate"
    region = "us-gov-west-1"
  }
}

# MSK is defined in flash-infrastructure (environments/prod/msk.tf), but the
# prod root state file may not yet expose MSK outputs (e.g. only Aurora/PyPI).
# Read brokers and scaling metadata from AWS instead of remote state.
data "aws_msk_cluster" "kafka" {
  cluster_name = var.msk_cluster_name
}

data "aws_security_group" "kafka_clients" {
  vpc_id = data.terraform_remote_state.shared.outputs.vpc_id
  filter {
    name   = "group-name"
    values = [var.kafka_clients_security_group_name]
  }
}

# ---------------------------------------------------------------------------
# ECS Cluster (owned by this repo, Fargate-only)
#
# Docling is CPU-only and stateless, so we run it on Fargate. The shared
# module still supports EC2 GPU/CPU capacity providers for sibling services;
# we just leave them disabled here.
# ---------------------------------------------------------------------------

module "ecs_cluster" {
  source = "../../modules/ecs-cluster"

  cluster_name = var.ecs_cluster_name
  vpc_id       = data.terraform_remote_state.shared.outputs.vpc_id
  subnet_ids   = data.terraform_remote_state.shared.outputs.public_subnet_ids
  kms_key_arn  = null

  gpu_capacity_provider_enabled      = false
  cpu_capacity_provider_enabled      = false
  fargate_capacity_providers_enabled = false

  containerinsights_log_retention_days = var.log_retention_days

  tags = local.tags
}

# ---------------------------------------------------------------------------
# Locals
# ---------------------------------------------------------------------------

locals {
  name_prefix = "${var.project_name}-${var.environment}"

  picture_description_env = {
    PICTURE_DESCRIPTION_BACKEND = "local"
  }

  tags = {
    Project     = "Flash"
    Environment = var.environment
    ManagedBy   = "Terraform"
    Component   = "document-analysis"
  }
}

resource "aws_cloudwatch_log_group" "bridge" {
  name              = "/ecs/${local.name_prefix}-bridge"
  retention_in_days = var.log_retention_days

  tags = local.tags
}

# ---------------------------------------------------------------------------
# SQS — job buffer between Kafka bridge and Docling workers
# ---------------------------------------------------------------------------

module "sqs_queue" {
  source = "../../modules/sqs-queue"

  queue_name = "${local.name_prefix}-jobs"

  visibility_timeout_seconds = 7200
  message_retention_seconds  = 345600
  receive_wait_time_seconds  = 20

  dead_letter_queue = {
    name                      = "${local.name_prefix}-jobs-dlq"
    message_retention_seconds = 1209600
    max_receive_count         = 3
  }

  tags = local.tags
}

# ---------------------------------------------------------------------------
# ECR Repository
# ---------------------------------------------------------------------------

module "ecr_repository" {
  source = "../../modules/ecr-repository"

  repository_name      = var.ecr_repository_name
  image_tag_mutability = "MUTABLE"
  scan_on_push         = true
  force_delete         = false

  tags = local.tags
}

# ---------------------------------------------------------------------------
# CloudWatch Log Group
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "main" {
  name              = "/ecs/${local.name_prefix}-docling"
  retention_in_days = var.log_retention_days

  tags = local.tags
}

# ---------------------------------------------------------------------------
# ECS Execution Role (for pulling images and writing logs)
# ---------------------------------------------------------------------------

module "ecs_execution_role" {
  source = "../../modules/ecs-execution-role"

  role_name          = "${local.name_prefix}-execution-role"
  ecr_repository_arn = module.ecr_repository.repository_arn

  tags = local.tags
}

# ---------------------------------------------------------------------------
# ECS Task Role (for S3 access)
# ---------------------------------------------------------------------------

module "ecs_task_role" {
  source = "../../modules/ecs-task-role"

  role_name            = "${local.name_prefix}-docling-role"
  s3_allow_all_buckets = true
  s3_bucket_arns       = []

  sqs_access_enabled = true
  sqs_queue_arns     = [module.sqs_queue.queue_arn, module.sqs_queue.dlq_arn]

  cloudwatch_log_group_arns = [
    aws_cloudwatch_log_group.main.arn,
    aws_cloudwatch_log_group.bridge.arn,
  ]

  tags = local.tags
}

resource "aws_iam_role_policy" "doc_task_bedrock_invoke" {
  name = "${local.name_prefix}-bedrock-invoke"
  role = module.ecs_task_role.role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "BedrockFoundationModels"
        Effect = "Allow"
        Action = [
          "bedrock:InvokeModel",
          "bedrock:InvokeModelWithResponseStream",
        ]
        Resource = "arn:${data.aws_partition.current.partition}:bedrock:${var.aws_region}::foundation-model/*"
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# VPC Endpoint Access
# ---------------------------------------------------------------------------

resource "aws_security_group_rule" "ecs_to_vpc_endpoints" {
  type                     = "ingress"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
  security_group_id        = var.vpc_endpoint_security_group_id
  source_security_group_id = module.ecs_cluster.ecs_tasks_security_group_id
  description              = "Allow HTTPS from document-analysis ECS tasks (prod)"
}

# ---------------------------------------------------------------------------
# ECS Services: SQS worker (Docling) + Kafka→SQS bridge
# ---------------------------------------------------------------------------

module "ecs_service_worker" {
  source = "../../modules/ecs-service"

  service_name = "${local.name_prefix}-worker"

  cluster_name = module.ecs_cluster.cluster_name
  cluster_id   = module.ecs_cluster.cluster_id

  launch_type              = "FARGATE"
  fargate_platform_version = "LATEST"

  task_role_arn      = module.ecs_task_role.role_arn
  execution_role_arn = module.ecs_execution_role.role_arn

  container_image = "${module.ecr_repository.repository_url}:${var.container_image_tag}"
  container_name  = "docling"
  container_port  = 8000

  cpu    = 2048
  memory = 16384

  require_gpu          = false
  health_check_command = "pgrep -f 'python.*document_analysis' || exit 1"

  environment_variables = merge(
    {
      ENTRYPOINT_MODE             = "worker"
      SQS_QUEUE_URL               = module.sqs_queue.queue_url
      AWS_DEFAULT_REGION          = "us-gov-west-1"
      TEMP_DIR                    = "/tmp/document-analysis"
      MODEL_CACHE_DIR             = "/app/models"
      S3_MODEL_BUCKET             = "flash-container-artifacts"
      S3_MODEL_PREFIX             = "document-analysis-models"
      KAFKA_BOOTSTRAP_SERVERS     = data.aws_msk_cluster.kafka.bootstrap_brokers
      OTEL_EXPORTER_OTLP_ENDPOINT = "http://otel-collector.flash-observability-prod.local:4318"
      OTEL_SERVICE_NAME           = "document-analysis"
      OTEL_SERVICE_VERSION        = "1.0.0"
      ENV                         = "prod"
    },
    local.picture_description_env,
  )

  efs_file_system_id  = ""
  efs_access_point_id = ""

  ephemeral_storage_gib = 30

  subnet_ids = data.terraform_remote_state.shared.outputs.public_subnet_ids
  security_group_ids = [
    module.ecs_cluster.ecs_tasks_security_group_id,
    data.aws_security_group.kafka_clients.id
  ]
  assign_public_ip = true

  cloudwatch_log_group_name = aws_cloudwatch_log_group.main.name
  aws_region                = var.aws_region

  desired_count = 1

  enable_autoscaling = true
  min_capacity       = 1
  max_capacity       = 32
  sqs_queue_name     = module.sqs_queue.queue_name
  target_value       = 1
  scale_in_cooldown  = 180
  scale_out_cooldown = 60

  health_check_grace_period = 180

  tags = local.tags
}

module "ecs_service_bridge" {
  source = "../../modules/ecs-service"

  service_name = "${local.name_prefix}-bridge"

  cluster_name = module.ecs_cluster.cluster_name
  cluster_id   = module.ecs_cluster.cluster_id

  launch_type              = "FARGATE"
  fargate_platform_version = "LATEST"

  task_role_arn      = module.ecs_task_role.role_arn
  execution_role_arn = module.ecs_execution_role.role_arn

  container_image = "${module.ecr_repository.repository_url}:${var.container_image_tag}"
  container_name  = "bridge"
  container_port  = 8000

  cpu    = 256
  memory = 1024

  require_gpu          = false
  health_check_command = "pgrep -f 'python.*document_analysis' || exit 1"

  environment_variables = {
    ENTRYPOINT_MODE             = "bridge"
    SQS_QUEUE_URL               = module.sqs_queue.queue_url
    AWS_DEFAULT_REGION          = "us-gov-west-1"
    KAFKA_BOOTSTRAP_SERVERS     = data.aws_msk_cluster.kafka.bootstrap_brokers
    OTEL_EXPORTER_OTLP_ENDPOINT = "http://otel-collector.flash-observability-prod.local:4318"
    OTEL_SERVICE_NAME           = "document-analysis-bridge"
    OTEL_SERVICE_VERSION        = "1.0.0"
  }

  efs_file_system_id  = ""
  efs_access_point_id = ""

  subnet_ids = data.terraform_remote_state.shared.outputs.public_subnet_ids
  security_group_ids = [
    module.ecs_cluster.ecs_tasks_security_group_id,
    data.aws_security_group.kafka_clients.id
  ]
  assign_public_ip = true

  cloudwatch_log_group_name = aws_cloudwatch_log_group.bridge.name
  aws_region                = var.aws_region

  desired_count      = 1
  enable_autoscaling = false

  health_check_grace_period = 120

  tags = local.tags
}

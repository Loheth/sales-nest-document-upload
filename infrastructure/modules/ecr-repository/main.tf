# ECR Repository Module
# Creates an ECR repository for container images

resource "aws_ecr_repository" "main" {
  name                 = var.repository_name
  image_tag_mutability = var.image_tag_mutability
  force_delete         = var.force_delete

  image_scanning_configuration {
    scan_on_push = var.scan_on_push
  }

  encryption_configuration {
    encryption_type = var.kms_key_id != null ? "KMS" : "AES256"
    kms_key         = var.kms_key_id
  }

  tags = merge(var.tags, {
    Name = var.repository_name
  })
}

resource "aws_ecr_lifecycle_policy" "main" {
  repository = aws_ecr_repository.main.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire untagged after 1 day"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 1
        }
        action = {
          type = "expire"
        }
      },
      {
        rulePriority = 2
        description  = "Keep last 10 dev-* images"
        selection = {
          tagStatus      = "tagged"
          tagPatternList = ["dev-*"]
          countType      = "imageCountMoreThan"
          countNumber    = 10
        }
        action = {
          type = "expire"
        }
      },
      {
        rulePriority = 3
        description  = "Keep last 30 prod-* images"
        selection = {
          tagStatus      = "tagged"
          tagPatternList = ["prod-*"]
          countType      = "imageCountMoreThan"
          countNumber    = 30
        }
        action = {
          type = "expire"
        }
      },
      {
        rulePriority = 4
        description  = "Expire other tagged images after 90 days"
        selection = {
          tagStatus      = "tagged"
          tagPatternList = ["*"]
          countType      = "sinceImagePushed"
          countUnit      = "days"
          countNumber    = 90
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

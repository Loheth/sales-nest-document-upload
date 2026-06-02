aws_region          = "us-gov-west-1"
environment         = "dev"
project_name        = "document-analysis-microservice"
ecr_repository_name = "document-analysis-microservice"
ecs_cluster_name    = "document-analysis-dev"

# Aurora Postgres secret backing the documents_partitioned schema (manifests,
# units, leases, kafka_outbox). Same shared cluster used by evidence-processor /
# audio-analysis-microservice in dev; doc pipeline uses its own schema so the
# kafka_outbox table doesn't collide with video's partitioned_processing.kafka_outbox.
aurora_secret_name = "flash-cluster-dev-migration/credentials"

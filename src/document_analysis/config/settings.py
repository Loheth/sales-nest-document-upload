"""Environment-driven configuration for the document analysis microservice."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings

EntrypointMode = Literal["bridge", "worker", "partitioner", "unit_worker", "aggregator"]
PictureDescriptionBackend = Literal["bedrock", "local"]


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # -- AWS / S3 --------------------------------------------------------
    aws_default_region: str = "us-gov-west-1"
    s3_bucket: str = ""

    # -- Paths -----------------------------------------------------------
    temp_dir: str = "/tmp/document-analysis"
    model_cache_dir: str = "/app/models"

    # -- S3 model cache (optional) ---------------------------------------
    s3_model_bucket: str = ""
    s3_model_prefix: str = "document-analysis-models"

    # -- Local file mode (no Kafka/S3) ------------------------------------
    local_input_file: str = ""
    local_output_dir: str = ""

    # -- Process mode (ECS) ----------------------------------------------
    # bridge: Kafka → SQS. worker: legacy SQS → Docling.
    # partitioner / unit_worker / aggregator: partitioned PDF pipeline (Kafka).
    entrypoint_mode: EntrypointMode = "worker"

    # -- Aurora (partitioned pipeline) ------------------------------------
    aurora_secret_name: str = ""

    # -- Partitioned document pipeline -----------------------------------
    doc_partition_threshold_pages: int = 50
    doc_pages_per_unit: int = 25
    doc_max_units: int = 2000
    lease_ttl_seconds: float = 30.0
    lease_refresh_interval_seconds: float = 10.0

    kafka_topic_partition_requested: str = "document.partition.requested"
    kafka_topic_unit_requested: str = "document.unit.requested"
    kafka_topic_unit_completed: str = "document.unit.completed"
    kafka_consumer_group_partitioner: str = "document-partitioner"
    kafka_consumer_group_unit_worker: str = "document-unit-worker"
    kafka_consumer_group_aggregator: str = "document-aggregator"
    #: Comma-separated topic list for multi-topic consumers (overrides single topic).
    kafka_inbound_topics: str = ""

    # -- SQS (worker receives jobs; bridge sends) ------------------------
    sqs_queue_url: str = ""
    sqs_visibility_timeout_seconds: int = 7200  # 2 hours
    sqs_wait_time_seconds: int = 20
    sqs_max_messages: int = 1
    sqs_visibility_extension_interval_sec: int = 600

    # -- Kafka -----------------------------------------------------------
    kafka_bootstrap_servers: str = ""
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_consumer_group: str = "document-analysis"
    kafka_inbound_topic: str = "document.processing.requested"
    kafka_max_poll_records: int = 5
    # Bridge polls frequently; worker uses SQS only. Kept for the bridge consumer loop.
    kafka_max_poll_interval_ms: int = 3_600_000  # 1 hour

    # -- Docling / picture-description memory tuning ---------------------
    #
    # Docling rasterizes figures for SmolVLM; defaults (2x page scale x large
    # enrichment batches x no cap) exhausted 8 GiB on image-heavy PDFs in ECS.

    #: Max width/height for picture-description crops fed to SmolVLM (pixels).
    picture_crop_max_long_edge_px: int = Field(default=1152, ge=32, le=8192)

    #: Page raster multiplier used by preprocessing (below 1.0 cuts layout/OCR raster cost).
    picture_page_raster_scale: float = Field(default=0.96, ge=0.4, le=2.5)

    #: Docling enrichment multiplies cropped figure resolution by this before the VLM
    # (Docling upstream default PictureDescriptionBaseModel.images_scale is 2.0).
    picture_enrichment_crop_scale: float = Field(default=1.0, ge=0.5, le=4.0)

    #: Passed to PictureDescriptionVlmEngineOptions.batch_size for forward compatibility.
    picture_description_vlm_batch_size: int = Field(default=2, ge=1, le=32)

    #: Optional scale override on PictureDescriptionVlmEngineOptions (Docling semantics).
    picture_description_preset_scale: float = Field(default=1.0, ge=0.25, le=4.0)

    #: Figures smaller than this fraction of page area skip description entirely.
    # Forensics / photo-heavy PDFs often use small inset images on large pages (~1% area or less).
    picture_description_area_fraction_min: float = Field(default=0.01, ge=0.0, le=1.0)

    #: ``bedrock``: Docling ``PictureDescriptionApiOptions`` → Bedrock OpenAI Chat Completions.
    #: ``local``: bundled SmolVLM via ``PictureDescriptionVlmEngineOptions`` (requires model cache).
    picture_description_backend: PictureDescriptionBackend = "local"

    #: Bedrock model identifier for Chat Completions (multimodal).
    picture_description_bedrock_model_id: str = "nvidia.nemotron-nano-12b-v2"

    #: Region for Bedrock Runtime hostname and SigV4 signing; empty uses ``aws_default_region``.
    picture_description_bedrock_region: str = ""

    #: Full Chat Completions URL; empty uses ``bedrock-runtime.{region}.amazonaws.com/v1/chat/completions``.
    picture_description_bedrock_chat_completions_url: str = ""

    #: When set, Docling uses plain Bearer auth (no SigV4 monkey-patch). Optional for local/dev.
    picture_description_bedrock_bearer_token: SecretStr | None = None

    #: HTTP timeout per figure request (Bedrock API path).
    picture_description_api_timeout: float = Field(default=60.0, ge=5.0, le=600.0)

    #: Concurrent Chat Completions calls in Docling's API picture-description model.
    picture_description_api_concurrency: int = Field(default=4, ge=1, le=32)

    picture_description_bedrock_max_tokens: int = Field(default=256, ge=16, le=4096)
    picture_description_bedrock_temperature: float = Field(default=0.2, ge=0.0, le=2.0)

    #: When True, run a pikepdf pass to describe embedded XObject images on pages Docling skipped.
    pdf_image_fallback_enabled: bool = True

    #: Skip embedded images whose long edge is below this (pixels).
    pdf_image_fallback_min_long_edge_px: int = Field(default=256, ge=0, le=8192)

    #: Per page, only the largest N embedded images are sent (after sorting by area).
    pdf_image_fallback_max_images_per_page: int = Field(default=1, ge=1, le=20)

    #: Hard cap on Bedrock calls per document in the fallback path.
    pdf_image_fallback_max_total_images: int = Field(default=200, ge=1, le=2000)

    # -- Logging ---------------------------------------------------------
    log_level: str = "info"

    @property
    def kafka_inbound_topics_list(self) -> list[str]:
        """Topics for consumers that support comma-separated ``kafka_inbound_topics``."""
        raw = (self.kafka_inbound_topics or "").strip()
        if raw:
            return [t.strip() for t in raw.split(",") if t.strip()]
        return [self.kafka_inbound_topic]

    @property
    def picture_description_bedrock_region_resolved(self) -> str:
        r = (self.picture_description_bedrock_region or "").strip()
        return r if r else self.aws_default_region

    @property
    def picture_description_bedrock_chat_url_resolved(self) -> str:
        u = (self.picture_description_bedrock_chat_completions_url or "").strip()
        if u:
            return u
        reg = self.picture_description_bedrock_region_resolved
        return f"https://bedrock-runtime.{reg}.amazonaws.com/v1/chat/completions"

    @property
    def picture_description_bedrock_use_bearer_auth(self) -> bool:
        tok = self.picture_description_bedrock_bearer_token
        if tok is None:
            return False
        return bool(tok.get_secret_value().strip())

    model_config = {
        "env_prefix": "",
        "case_sensitive": False,
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }


def get_settings() -> Settings:
    """Construct and return a cached settings instance."""
    return Settings()

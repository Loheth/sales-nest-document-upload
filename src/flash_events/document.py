"""Document pipeline Kafka events."""

from __future__ import annotations

from flash_events.base import KafkaEvent


class DocumentProcessingRequestedEvent(KafkaEvent):
    TOPIC = "document.processing.requested"

    evidence_id: str
    case_id: str
    user_id: str
    s3_bucket: str
    s3_key: str
    output_key_prefix: str
    facility_id: str = ""


class DocumentProcessingCompletedEvent(KafkaEvent):
    TOPIC = "document.processing.completed"

    evidence_id: str
    case_id: str
    user_id: str
    s3_bucket: str
    document_s3_key: str
    page_count: int = 0
    document_type: str = ""


class DocumentPartitionRequestedEvent(KafkaEvent):
    TOPIC = "document.partition.requested"

    evidence_id: str
    case_id: str
    user_id: str
    s3_bucket: str
    s3_key: str
    output_key_prefix: str
    facility_id: str = ""


class DocumentUnitRequestedEvent(KafkaEvent):
    TOPIC = "document.unit.requested"

    manifest_id: str
    evidence_id: str
    case_id: str
    user_id: str
    unit_index: int
    total_units: int
    s3_bucket: str
    segment_s3_key: str
    manifest_s3_key: str
    output_key_prefix: str
    page_start: int = 0
    page_end: int = 0
    document_type: str = ""
    facility_id: str = ""


class DocumentUnitCompletedEvent(KafkaEvent):
    TOPIC = "document.unit.completed"

    manifest_id: str
    evidence_id: str
    case_id: str
    user_id: str
    unit_index: int
    total_units: int
    s3_bucket: str
    result_s3_key: str = ""
    page_start: int = 0
    page_end: int = 0
    page_count: int = 0
    status: str = "completed"
    processing_time_s: float = 0.0
    error: str = ""

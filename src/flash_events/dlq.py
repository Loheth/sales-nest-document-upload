"""Dead-letter / failure events."""

from __future__ import annotations

from flash_events.base import KafkaEvent


class ProcessingFailedEvent(KafkaEvent):
    TOPIC = "document.processing.failed"

    evidence_id: str
    case_id: str
    user_id: str
    original_topic: str
    original_event: str = ""
    error: str = ""
    retry_count: int = 0
    service: str = "document-analysis"

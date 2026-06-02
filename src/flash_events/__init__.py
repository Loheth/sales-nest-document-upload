"""SalesNest Kafka event models (vendored for local/CI builds)."""

from flash_events.dlq import ProcessingFailedEvent
from flash_events.document import (
    DocumentPartitionRequestedEvent,
    DocumentProcessingCompletedEvent,
    DocumentProcessingRequestedEvent,
    DocumentUnitCompletedEvent,
    DocumentUnitRequestedEvent,
)

__all__ = [
    "DocumentPartitionRequestedEvent",
    "DocumentProcessingCompletedEvent",
    "DocumentProcessingRequestedEvent",
    "DocumentUnitCompletedEvent",
    "DocumentUnitRequestedEvent",
    "ProcessingFailedEvent",
]

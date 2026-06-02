"""Job model for document processing."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DocumentJob:
    """A single document conversion job."""

    bucket: str
    key: str
    output_bucket: str | None = None
    output_key_prefix: str | None = None
    facility_id: str = ""

    @classmethod
    def from_kafka_event(cls, event) -> DocumentJob:
        """Construct a DocumentJob from a DocumentProcessingRequestedEvent."""
        return cls(
            bucket=event.s3_bucket,
            key=event.s3_key,
            output_bucket=event.s3_bucket,
            output_key_prefix=event.output_key_prefix,
            facility_id=getattr(event, "facility_id", "") or "",
        )
